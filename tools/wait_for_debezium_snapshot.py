#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Espera a que los conectores Debezium completen el snapshot inicial.
Monitorea el estado de los conectores y verifica que las tablas raw tengan datos.
"""
import sys
import time
import logging
import requests
from typing import Dict, List, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)


def get_connectors_status(connect_url: str) -> Dict[str, dict]:
    """Obtiene el estado de todos los conectores."""
    try:
        r = requests.get(f"{connect_url}/connectors", timeout=10)
        r.raise_for_status()
        connectors = r.json()
        
        status = {}
        for conn in connectors:
            r = requests.get(f"{connect_url}/connectors/{conn}/status", timeout=10)
            r.raise_for_status()
            status[conn] = r.json()
        
        return status
    except Exception as e:
        log.error(f"Error obteniendo estado de conectores: {e}")
        return {}


def check_snapshot_complete(status: Dict[str, dict]) -> Tuple[int, int, List[str]]:
    """
    Verifica si los snapshots están completos.
    Retorna: (completados, total, conectores_en_progreso)
    """
    completed = 0
    in_progress = []
    
    for conn_name, conn_status in status.items():
        connector_state = conn_status.get("connector", {}).get("state", "UNKNOWN")
        
        # Verificar tasks
        tasks = conn_status.get("tasks", [])
        if not tasks:
            in_progress.append(f"{conn_name} (sin tasks)")
            continue
            
        task_state = tasks[0].get("state", "UNKNOWN")
        
        # RUNNING es el estado final después del snapshot
        if connector_state == "RUNNING" and task_state == "RUNNING":
            completed += 1
        else:
            in_progress.append(f"{conn_name} ({connector_state}/{task_state})")
    
    return completed, len(status), in_progress


def check_raw_tables_data(ch_host: str, ch_port: str, ch_user: str, ch_pass: str) -> int:
    """Cuenta cuántas tablas _raw tienen datos."""
    try:
        query = """
        SELECT count() 
        FROM system.tables 
        WHERE database = 'fgeo_analytics' 
        AND name LIKE '%_raw' 
        AND total_rows > 0
        """
        r = requests.get(
            f"http://{ch_host}:{ch_port}/",
            params={"query": query},
            auth=(ch_user, ch_pass),
            timeout=10
        )
        r.raise_for_status()
        return int(r.text.strip())
    except Exception as e:
        log.warning(f"Error verificando tablas raw: {e}")
        return 0


def wait_for_snapshot(
    connect_url: str = "http://connect:8083",
    ch_host: str = "clickhouse",
    ch_port: str = "8123",
    ch_user: str = "etl",
    ch_pass: str = "Et1Ingest!",
    max_wait_minutes: int = 15,
    check_interval: int = 10
):
    """
    Espera a que los conectores Debezium completen el snapshot inicial.
    
    Args:
        connect_url: URL del Kafka Connect REST API
        ch_host: Host de ClickHouse
        ch_port: Puerto HTTP de ClickHouse
        ch_user: Usuario de ClickHouse
        ch_pass: Password de ClickHouse
        max_wait_minutes: Tiempo máximo de espera en minutos
        check_interval: Intervalo entre chequeos en segundos
    """
    log.info("=" * 60)
    log.info("🔄 ESPERANDO SNAPSHOT INICIAL DE DEBEZIUM")
    log.info("=" * 60)
    
    start_time = time.time()
    max_wait_seconds = max_wait_minutes * 60
    attempt = 0
    
    # Primera verificación: esperar a que haya conectores
    log.info("⏳ Esperando a que se registren los conectores...")
    while time.time() - start_time < max_wait_seconds:
        status = get_connectors_status(connect_url)
        if status:
            log.info(f"✅ Encontrados {len(status)} conectores registrados")
            break
        time.sleep(5)
    else:
        log.warning("⚠️  Timeout esperando conectores. Continuando...")
        return False
    
    log.info(f"⏱️  Tiempo máximo de espera: {max_wait_minutes} minutos")
    log.info(f"🔍 Intervalo de verificación: {check_interval} segundos")
    log.info("")
    
    last_completed = 0
    last_raw_tables = 0
    
    while time.time() - start_time < max_wait_seconds:
        attempt += 1
        elapsed = int(time.time() - start_time)
        
        # Verificar estado de conectores
        status = get_connectors_status(connect_url)
        if not status:
            log.warning(f"[{elapsed}s] ⚠️  No se pudo obtener estado de conectores")
            time.sleep(check_interval)
            continue
        
        completed, total, in_progress = check_snapshot_complete(status)
        
        # Verificar tablas raw con datos
        raw_tables_with_data = check_raw_tables_data(ch_host, ch_port, ch_user, ch_pass)
        
        # Mostrar progreso si hubo cambios
        if completed != last_completed or raw_tables_with_data != last_raw_tables:
            log.info(f"[{elapsed}s] 📊 Conectores: {completed}/{total} RUNNING | Tablas raw con datos: {raw_tables_with_data}")
            
            if in_progress and completed < total:
                for conn in in_progress[:3]:  # Mostrar solo los primeros 3
                    log.info(f"         ⏳ {conn}")
                if len(in_progress) > 3:
                    log.info(f"         ... y {len(in_progress) - 3} más")
            
            last_completed = completed
            last_raw_tables = raw_tables_with_data
        
        # Criterios de éxito:
        # - Todos los conectores RUNNING
        # - Al menos algunas tablas raw tienen datos (permitir que no todas tengan datos inicialmente)
        if completed == total and raw_tables_with_data > 0:
            log.info("")
            log.info("=" * 60)
            log.info("✅ SNAPSHOT INICIAL COMPLETADO")
            log.info("=" * 60)
            log.info(f"✅ {completed}/{total} conectores en estado RUNNING")
            log.info(f"✅ {raw_tables_with_data} tablas raw recibiendo datos")
            log.info(f"⏱️  Tiempo transcurrido: {elapsed} segundos")
            return True
        
        time.sleep(check_interval)
    
    # Timeout alcanzado
    elapsed = int(time.time() - start_time)
    log.warning("")
    log.warning("=" * 60)
    log.warning(f"⏰ TIMEOUT después de {elapsed} segundos")
    log.warning("=" * 60)
    log.warning(f"⚠️  Conectores RUNNING: {completed}/{total}")
    log.warning(f"⚠️  Tablas raw con datos: {raw_tables_with_data}")
    log.warning("")
    log.warning("ℹ️  El snapshot puede continuar en background.")
    log.warning("ℹ️  Verifica los conectores con: docker compose logs connect")
    
    # No es un error fatal - el snapshot puede continuar
    return True


def main():
    import os
    
    connect_url = os.getenv("KAFKA_CONNECT_URL", "http://connect:8083")
    ch_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    ch_port = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
    ch_user = os.getenv("CH_USER", "etl")
    ch_pass = os.getenv("CH_PASSWORD", "Et1Ingest!")
    max_wait = int(os.getenv("DEBEZIUM_SNAPSHOT_WAIT_MINUTES", "10"))
    
    try:
        success = wait_for_snapshot(
            connect_url=connect_url,
            ch_host=ch_host,
            ch_port=ch_port,
            ch_user=ch_user,
            ch_pass=ch_pass,
            max_wait_minutes=max_wait
        )
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        log.warning("\n⚠️  Proceso interrumpido por el usuario")
        sys.exit(130)
    except Exception as e:
        log.error(f"❌ Error inesperado: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
