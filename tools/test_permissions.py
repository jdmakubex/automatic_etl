#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de pruebas de usuarios y permisos para tecnologías ETL.
Valida permisos en Debezium, Kafka y ClickHouse.
"""
import os
import sys
import json
import logging
from datetime import datetime
from typing import Dict, List, Tuple

# Configurar logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


def save_test_results(results: Dict, output_file: str = "logs/permission_tests.json"):
    """Guarda resultados de pruebas en archivo JSON"""
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    log.info(f"Resultados guardados en {output_file}")


def test_clickhouse_permissions() -> Tuple[bool, str]:
    """
    Prueba permisos de usuario en ClickHouse.
    Valida que el usuario puede crear bases de datos, tablas y realizar operaciones básicas.
    """
    log.info("=== Probando permisos en ClickHouse ===")
    start_time = datetime.utcnow()
    
    try:
        import clickhouse_connect
    except ImportError:
        return False, "clickhouse_connect no instalado. Instalar con: pip install clickhouse-connect"
    
    host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    user = os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "")
    
    try:
        # Conectar
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password
        )
        log.info(f"✓ Conectado a ClickHouse en {host}:{port}")
        
        # Probar permisos de lectura (SHOW DATABASES)
        try:
            databases = client.query("SHOW DATABASES").result_rows
            log.info(f"✓ Permiso de lectura (SHOW DATABASES): {len(databases)} bases encontradas")
        except Exception as e:
            return False, f"No tiene permiso para listar bases de datos: {e}"
        
        # Probar creación de base de datos de prueba
        test_db = "test_permissions_db"
        try:
            client.command(f"CREATE DATABASE IF NOT EXISTS {test_db}")
            log.info(f"✓ Permiso de creación de base de datos: {test_db}")
        except Exception as e:
            return False, f"No tiene permiso para crear bases de datos: {e}"
        
        # Probar creación de tabla
        try:
            client.command(f"""
                CREATE TABLE IF NOT EXISTS {test_db}.test_table (
                    id UInt32,
                    name String,
                    created DateTime DEFAULT now()
                ) ENGINE = MergeTree()
                ORDER BY id
            """)
            log.info(f"✓ Permiso de creación de tabla: {test_db}.test_table")
        except Exception as e:
            client.command(f"DROP DATABASE IF EXISTS {test_db}")
            return False, f"No tiene permiso para crear tablas: {e}"
        
        # Probar inserción de datos
        try:
            client.command(f"INSERT INTO {test_db}.test_table (id, name) VALUES (1, 'test')")
            log.info(f"✓ Permiso de inserción de datos")
        except Exception as e:
            client.command(f"DROP DATABASE IF EXISTS {test_db}")
            return False, f"No tiene permiso para insertar datos: {e}"
        
        # Probar lectura de datos
        try:
            result = client.query(f"SELECT * FROM {test_db}.test_table").result_rows
            log.info(f"✓ Permiso de lectura de datos: {len(result)} filas")
        except Exception as e:
            client.command(f"DROP DATABASE IF EXISTS {test_db}")
            return False, f"No tiene permiso para leer datos: {e}"
        
        # Limpiar base de datos de prueba
        try:
            client.command(f"DROP DATABASE IF EXISTS {test_db}")
            log.info(f"✓ Permiso de eliminación de base de datos")
        except Exception as e:
            log.warning(f"⚠ No se pudo limpiar base de datos de prueba: {e}")
        
        duration = (datetime.utcnow() - start_time).total_seconds()
        log.info(f"✓ Todas las pruebas de ClickHouse pasaron (duración: {duration:.2f}s)")
        return True, f"Usuario {user} tiene todos los permisos necesarios"
        
    except Exception as e:
        duration = (datetime.utcnow() - start_time).total_seconds()
        log.error(f"✗ Error probando ClickHouse (duración: {duration:.2f}s): {e}")
        return False, str(e)


def test_kafka_permissions() -> Tuple[bool, str]:
    """
    Prueba permisos de usuario en Kafka Connect.
    Valida que se puede acceder a la API REST y listar conectores.
    """
    log.info("=== Probando permisos en Kafka Connect ===")
    start_time = datetime.utcnow()
    
    try:
        import requests
    except ImportError:
        return False, "requests no instalado. Instalar con: pip install requests"
    
    connect_url = os.getenv("CONNECT_URL", "http://connect:8083")
    
    try:
        # Probar listado de conectores
        response = requests.get(f"{connect_url}/connectors", timeout=10)
        if response.status_code == 200:
            connectors = response.json()
            log.info(f"✓ Permiso de lectura: {len(connectors)} conectores listados")
        else:
            return False, f"Error listando conectores: HTTP {response.status_code}"
        
        # Probar listado de plugins
        response = requests.get(f"{connect_url}/connector-plugins", timeout=10)
        if response.status_code == 200:
            plugins = response.json()
            log.info(f"✓ Permiso de lectura de plugins: {len(plugins)} plugins disponibles")
        else:
            return False, f"Error listando plugins: HTTP {response.status_code}"
        
        duration = (datetime.utcnow() - start_time).total_seconds()
        log.info(f"✓ Todas las pruebas de Kafka Connect pasaron (duración: {duration:.2f}s)")
        return True, "Permisos de Kafka Connect verificados"
        
    except Exception as e:
        duration = (datetime.utcnow() - start_time).total_seconds()
        log.error(f"✗ Error probando Kafka Connect (duración: {duration:.2f}s): {e}")
        return False, str(e)


def test_debezium_permissions() -> Tuple[bool, str]:
    """
    Prueba que Debezium puede conectarse a las bases de datos configuradas.
    Valida permisos de replicación en MySQL.
    """
    log.info("=== Probando permisos de Debezium (MySQL CDC) ===")
    start_time = datetime.utcnow()
    
    db_connections_json = os.getenv("DB_CONNECTIONS", "[]")
    
    try:
        connections = json.loads(db_connections_json)
    except json.JSONDecodeError as e:
        return False, f"DB_CONNECTIONS inválido: {e}"
    
    mysql_conns = [c for c in connections if c.get("type") == "mysql"]
    
    if not mysql_conns:
        log.info("⚠ No hay conexiones MySQL configuradas, omitiendo pruebas de Debezium")
        return True, "No hay conexiones MySQL para probar"
    
    try:
        import pymysql
    except ImportError:
        return False, "pymysql no instalado. Instalar con: pip install pymysql"
    
    results = []
    
    for conn in mysql_conns:
        name = conn.get("name", "unknown")
        host = conn.get("host")
        port = int(conn.get("port", 3306))
        user = conn.get("user")
        password = conn.get("pass")
        database = conn.get("db")
        
        log.info(f"Probando conexión: {name} ({host}:{port}/{database})")
        
        try:
            # Conectar a MySQL
            connection = pymysql.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                connect_timeout=5
            )
            
            with connection.cursor() as cursor:
                # Verificar permisos de replicación
                cursor.execute("SHOW GRANTS FOR CURRENT_USER()")
                grants = cursor.fetchall()
                grants_str = " ".join([str(g) for g in grants])
                
                has_replication_slave = "REPLICATION SLAVE" in grants_str
                has_replication_client = "REPLICATION CLIENT" in grants_str
                has_select = "SELECT" in grants_str or "ALL PRIVILEGES" in grants_str
                
                if has_replication_slave and has_replication_client and has_select:
                    log.info(f"✓ {name}: Usuario tiene permisos necesarios para CDC")
                    results.append((name, True, "Permisos OK"))
                else:
                    missing = []
                    if not has_replication_slave:
                        missing.append("REPLICATION SLAVE")
                    if not has_replication_client:
                        missing.append("REPLICATION CLIENT")
                    if not has_select:
                        missing.append("SELECT")
                    
                    log.warning(f"⚠ {name}: Permisos faltantes: {', '.join(missing)}")
                    results.append((name, False, f"Faltan permisos: {', '.join(missing)}"))
                
                # Verificar binlog
                cursor.execute("SHOW VARIABLES LIKE 'log_bin'")
                log_bin = cursor.fetchone()
                if log_bin and log_bin[1] == 'ON':
                    log.info(f"✓ {name}: Binlog habilitado")
                else:
                    log.warning(f"⚠ {name}: Binlog no habilitado (requerido para CDC)")
                    results.append((name, False, "Binlog no habilitado"))
            
            connection.close()
            
        except Exception as e:
            log.error(f"✗ {name}: Error conectando: {e}")
            results.append((name, False, str(e)))
    
    duration = (datetime.utcnow() - start_time).total_seconds()
    
    # Verificar si todas pasaron
    all_passed = all(r[1] for r in results)
    
    if all_passed:
        log.info(f"✓ Todas las pruebas de Debezium pasaron (duración: {duration:.2f}s)")
        return True, "Todos los permisos de MySQL verificados"
    else:
        failed = [r[0] for r in results if not r[1]]
        log.warning(f"⚠ Algunas conexiones fallaron: {', '.join(failed)} (duración: {duration:.2f}s)")
        return False, f"Conexiones con problemas: {', '.join(failed)}"


def run_all_tests() -> Dict:
    """
    Ejecuta todas las pruebas de permisos y retorna resultados.
    """
    log.info("=== Iniciando pruebas de permisos ETL ===")
    test_start = datetime.utcnow()
    
    results = {
        "timestamp": test_start.isoformat() + "Z",
        "tests": {},
        "summary": {
            "total": 0,
            "passed": 0,
            "failed": 0
        }
    }
    
    # Prueba ClickHouse
    ch_passed, ch_msg = test_clickhouse_permissions()
    results["tests"]["clickhouse"] = {
        "passed": ch_passed,
        "message": ch_msg
    }
    
    # Prueba Kafka
    kafka_passed, kafka_msg = test_kafka_permissions()
    results["tests"]["kafka"] = {
        "passed": kafka_passed,
        "message": kafka_msg
    }
    
    # Prueba Debezium/MySQL
    debezium_passed, debezium_msg = test_debezium_permissions()
    results["tests"]["debezium"] = {
        "passed": debezium_passed,
        "message": debezium_msg
    }
    
    # Calcular resumen
    results["summary"]["total"] = 3
    results["summary"]["passed"] = sum([ch_passed, kafka_passed, debezium_passed])
    results["summary"]["failed"] = 3 - results["summary"]["passed"]
    
    test_end = datetime.utcnow()
    results["duration_seconds"] = (test_end - test_start).total_seconds()
    
    return results


def main():
    """Punto de entrada principal"""
    # Habilitar/deshabilitar pruebas por variable de entorno
    enable_tests = os.getenv("ENABLE_PERMISSION_TESTS", "true").lower() == "true"
    
    if not enable_tests:
        log.info("Pruebas de permisos deshabilitadas (ENABLE_PERMISSION_TESTS=false)")
        return 0
    
    try:
        results = run_all_tests()
        
        # Guardar resultados
        output_file = os.getenv("PERMISSION_TEST_OUTPUT", "logs/permission_tests.json")
        save_test_results(results, output_file)
        
        # Imprimir resumen
        print("\n=== Resumen de Pruebas de Permisos ===")
        print(f"Total: {results['summary']['total']}")
        print(f"✓ Pasadas: {results['summary']['passed']}")
        print(f"✗ Fallidas: {results['summary']['failed']}")
        print(f"Duración: {results['duration_seconds']:.2f}s")
        
        if results['summary']['failed'] > 0:
            print("\nDetalles de fallos:")
            for test_name, test_result in results['tests'].items():
                if not test_result['passed']:
                    print(f"  - {test_name}: {test_result['message']}")
            return 1
        
        print("\n✓ Todas las pruebas de permisos pasaron")
        return 0
        
    except Exception as e:
        log.error(f"Error ejecutando pruebas: {e}")
        import traceback
        traceback.print_exc()
        return 2


if __name__ == "__main__":
    sys.exit(main())
