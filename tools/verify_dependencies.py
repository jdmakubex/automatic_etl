#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de verificación de dependencias y secuencialidad de procesos ETL.
Asegura que procesos dependientes no inicien antes de tiempo.
"""
import os
import sys
import json
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional

# Configurar logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


class DependencyError(Exception):
    """Error en verificación de dependencias"""
    pass


def check_service_health(service: str, endpoint: str, timeout: int = 10) -> bool:
    """
    Verifica salud de un servicio HTTP.
    
    Args:
        service: Nombre del servicio
        endpoint: URL del endpoint de healthcheck
        timeout: Timeout en segundos
    
    Returns:
        True si el servicio está saludable
    """
    try:
        import requests
        response = requests.get(endpoint, timeout=timeout)
        if response.status_code == 200:
            log.info(f"✓ {service} está saludable")
            return True
        else:
            log.warning(f"⚠ {service} respondió con código {response.status_code}")
            return False
    except Exception as e:
        log.warning(f"⚠ {service} no disponible: {e}")
        return False


def check_clickhouse_ready() -> bool:
    """Verifica que ClickHouse esté listo"""
    host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    port = os.getenv("CLICKHOUSE_PORT", "8123")
    return check_service_health("ClickHouse", f"http://{host}:{port}/ping")


def check_kafka_ready() -> bool:
    """Verifica que Kafka Connect esté listo"""
    connect_url = os.getenv("CONNECT_URL", "http://connect:8083")
    return check_service_health("Kafka Connect", f"{connect_url}/")


def check_superset_ready() -> bool:
    """Verifica que Superset esté listo"""
    superset_url = os.getenv("SUPERSET_URL", "http://superset:8088")
    return check_service_health("Superset", f"{superset_url}/health")


def check_database_exists(database: str) -> bool:
    """
    Verifica que una base de datos exista en ClickHouse.
    
    Args:
        database: Nombre de la base de datos
    
    Returns:
        True si la base de datos existe
    """
    try:
        import clickhouse_connect
        
        host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
        port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
        user = os.getenv("CLICKHOUSE_USER", "default")
        password = os.getenv("CLICKHOUSE_PASSWORD", "")
        
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password
        )
        
        result = client.query(f"EXISTS DATABASE {database}").result_rows
        exists = result[0][0] == 1 if result else False
        
        if exists:
            log.info(f"✓ Base de datos '{database}' existe")
        else:
            log.warning(f"⚠ Base de datos '{database}' no existe")
        
        return exists
        
    except Exception as e:
        log.error(f"✗ Error verificando base de datos: {e}")
        return False


def check_tables_exist(database: str, min_tables: int = 1) -> bool:
    """
    Verifica que existan tablas en una base de datos.
    
    Args:
        database: Nombre de la base de datos
        min_tables: Número mínimo de tablas esperadas
    
    Returns:
        True si hay suficientes tablas
    """
    try:
        import clickhouse_connect
        
        host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
        port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
        user = os.getenv("CLICKHOUSE_USER", "default")
        password = os.getenv("CLICKHOUSE_PASSWORD", "")
        
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password
        )
        
        result = client.query(f"SHOW TABLES FROM {database}").result_rows
        table_count = len(result)
        
        if table_count >= min_tables:
            log.info(f"✓ Base de datos '{database}' tiene {table_count} tablas (mínimo: {min_tables})")
            return True
        else:
            log.warning(f"⚠ Base de datos '{database}' tiene {table_count} tablas (mínimo requerido: {min_tables})")
            return False
        
    except Exception as e:
        log.error(f"✗ Error verificando tablas: {e}")
        return False


def check_kafka_topics_exist(expected_topics: Optional[List[str]] = None) -> bool:
    """
    Verifica que existan topics en Kafka (opcional: lista específica).
    
    Args:
        expected_topics: Lista opcional de topics esperados
    
    Returns:
        True si los topics existen
    """
    try:
        import requests
        
        connect_url = os.getenv("CONNECT_URL", "http://connect:8083")
        
        # Verificar conectores activos (indicador de que hay topics)
        response = requests.get(f"{connect_url}/connectors", timeout=10)
        if response.status_code != 200:
            log.warning(f"⚠ No se pudo verificar conectores")
            return False
        
        connectors = response.json()
        
        if len(connectors) > 0:
            log.info(f"✓ Kafka tiene {len(connectors)} conectores activos")
            return True
        else:
            log.warning(f"⚠ No hay conectores activos en Kafka")
            return False
        
    except Exception as e:
        log.error(f"✗ Error verificando Kafka topics: {e}")
        return False


def wait_for_service(
    check_func: callable,
    service_name: str,
    max_retries: int = 30,
    retry_interval: int = 2
) -> bool:
    """
    Espera a que un servicio esté listo con reintentos.
    
    Args:
        check_func: Función de verificación
        service_name: Nombre del servicio para logs
        max_retries: Número máximo de reintentos
        retry_interval: Segundos entre reintentos
    
    Returns:
        True si el servicio está listo
    """
    log.info(f"Esperando a que {service_name} esté listo...")
    
    for attempt in range(1, max_retries + 1):
        if check_func():
            log.info(f"✓ {service_name} listo después de {attempt} intentos")
            return True
        
        if attempt < max_retries:
            log.info(f"Intento {attempt}/{max_retries} - Esperando {retry_interval}s...")
            time.sleep(retry_interval)
    
    log.error(f"✗ {service_name} no está listo después de {max_retries} intentos")
    return False


def verify_etl_pipeline_sequence() -> Dict:
    """
    Verifica la secuencia completa del pipeline ETL.
    Asegura que cada componente esté listo antes de pasar al siguiente.
    
    Returns:
        Diccionario con resultados de verificación
    """
    log.info("=== Verificando secuencia del pipeline ETL ===")
    start_time = datetime.utcnow()
    
    results = {
        "timestamp": start_time.isoformat() + "Z",
        "checks": {},
        "sequence_valid": True
    }
    
    # 1. Verificar ClickHouse (servicio base)
    log.info("\n[Paso 1/5] Verificando ClickHouse...")
    ch_ready = wait_for_service(check_clickhouse_ready, "ClickHouse", max_retries=12, retry_interval=5)
    results["checks"]["clickhouse_ready"] = ch_ready
    
    if not ch_ready:
        results["sequence_valid"] = False
        log.error("✗ ClickHouse no está disponible. No se puede continuar.")
        return results
    
    # 2. Verificar base de datos
    log.info("\n[Paso 2/5] Verificando base de datos...")
    database = os.getenv("CLICKHOUSE_DATABASE", "fgeo_analytics")
    db_exists = check_database_exists(database)
    results["checks"]["database_exists"] = db_exists
    
    if not db_exists:
        log.warning("⚠ Base de datos no existe, puede ser creada por el pipeline de ingesta")
    
    # 3. Verificar Kafka Connect (opcional para CDC)
    log.info("\n[Paso 3/5] Verificando Kafka Connect...")
    enable_cdc = os.getenv("ENABLE_CDC", "false").lower() == "true"
    
    if enable_cdc:
        kafka_ready = wait_for_service(check_kafka_ready, "Kafka Connect", max_retries=10, retry_interval=3)
        results["checks"]["kafka_ready"] = kafka_ready
        
        if not kafka_ready:
            log.warning("⚠ Kafka Connect no disponible, CDC no funcionará")
            results["sequence_valid"] = False
    else:
        log.info("⚠ CDC deshabilitado (ENABLE_CDC=false), omitiendo verificación de Kafka")
        results["checks"]["kafka_ready"] = None
    
    # 4. Verificar Superset (opcional)
    log.info("\n[Paso 4/5] Verificando Superset...")
    enable_superset = os.getenv("ENABLE_SUPERSET", "true").lower() == "true"
    
    if enable_superset:
        superset_ready = wait_for_service(check_superset_ready, "Superset", max_retries=20, retry_interval=3)
        results["checks"]["superset_ready"] = superset_ready
        
        if not superset_ready:
            log.warning("⚠ Superset no disponible")
    else:
        log.info("⚠ Superset deshabilitado (ENABLE_SUPERSET=false), omitiendo verificación")
        results["checks"]["superset_ready"] = None
    
    # 5. Verificar dependencias Python
    log.info("\n[Paso 5/5] Verificando dependencias Python...")
    required_packages = ["clickhouse_connect", "requests", "pymysql"]
    
    try:
        for package in required_packages:
            __import__(package)
        log.info(f"✓ Todas las dependencias Python están instaladas")
        results["checks"]["python_dependencies"] = True
    except ImportError as e:
        log.error(f"✗ Falta dependencia Python: {e}")
        results["checks"]["python_dependencies"] = False
        results["sequence_valid"] = False
    
    end_time = datetime.utcnow()
    results["duration_seconds"] = (end_time - start_time).total_seconds()
    
    return results


def save_verification_results(results: Dict, output_file: str = "logs/dependency_verification.json"):
    """Guarda resultados de verificación en archivo JSON"""
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    log.info(f"Resultados guardados en {output_file}")


def main():
    """Punto de entrada principal"""
    # Habilitar/deshabilitar verificaciones por variable de entorno
    enable_verification = os.getenv("ENABLE_DEPENDENCY_VERIFICATION", "true").lower() == "true"
    
    if not enable_verification:
        log.info("Verificación de dependencias deshabilitada (ENABLE_DEPENDENCY_VERIFICATION=false)")
        return 0
    
    try:
        log.info("Iniciando verificación de dependencias y secuencialidad")
        
        results = verify_etl_pipeline_sequence()
        
        # Guardar resultados
        output_file = os.getenv("DEPENDENCY_VERIFICATION_OUTPUT", "logs/dependency_verification.json")
        save_verification_results(results, output_file)
        
        # Imprimir resumen
        print("\n=== Resumen de Verificación de Dependencias ===")
        print(f"Duración: {results['duration_seconds']:.2f}s")
        print(f"Secuencia válida: {'✓ Sí' if results['sequence_valid'] else '✗ No'}")
        
        print("\nResultados de verificaciones:")
        for check_name, check_result in results['checks'].items():
            if check_result is None:
                status = "⊝ Omitido"
            elif check_result:
                status = "✓ OK"
            else:
                status = "✗ Fallido"
            print(f"  {check_name}: {status}")
        
        if not results['sequence_valid']:
            print("\n⚠ Algunas dependencias no están listas. Consulta los logs para detalles.")
            return 1
        
        print("\n✓ Todas las dependencias verificadas correctamente")
        return 0
        
    except Exception as e:
        log.error(f"Error ejecutando verificación: {e}")
        import traceback
        traceback.print_exc()
        return 2


if __name__ == "__main__":
    sys.exit(main())
