#!/usr/bin/env python3
"""
⚡ VALIDADOR SIMPLE DEL PIPELINE ETL
Verifica el flujo básico de datos y la operatividad del sistema
"""

import subprocess
import sys
import logging

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger(__name__)

def main():
    logger = setup_logging()
    logger.info("⚡ === VALIDACIÓN RÁPIDA DEL PIPELINE ETL ===")
    
    success_count = 0
    total_tests = 4
    
    # Test 1: ClickHouse datos
    try:
        result = subprocess.run([
            "docker", "exec", "clickhouse", "clickhouse-client",
            "--query", "SELECT COUNT(*) FROM fgeo_analytics.archivos"
        ], capture_output=True, text=True, check=True, timeout=10)
        
        count = int(result.stdout.strip())
        logger.info(f"✅ ClickHouse: {count} registros en fgeo_analytics.archivos")
        success_count += 1
    except Exception as e:
        logger.error(f"❌ ClickHouse: {e}")
    
    # Test 2: Kafka topics
    try:
        result = subprocess.run([
            "docker", "exec", "kafka", "kafka-topics",
            "--bootstrap-server", "localhost:19092", "--list"
        ], capture_output=True, text=True, check=True, timeout=10)
        
        mysql_topics = [t for t in result.stdout.split('\n') if 'dbserver_default.archivos' in t]
        logger.info(f"✅ Kafka: {len(mysql_topics)} topics MySQL detectados")
        success_count += 1
    except Exception as e:
        logger.error(f"❌ Kafka: {e}")
    
    # Test 3: Superset health
    try:
        result = subprocess.run([
            "curl", "-s", "--max-time", "5", "http://localhost:8088/health"
        ], capture_output=True, text=True, check=True)
        
        if "OK" in result.stdout:
            logger.info("✅ Superset: Respondiendo correctamente")
            success_count += 1
        else:
            logger.warning("⚠️ Superset: Respuesta inválida")
    except Exception as e:
        logger.error(f"❌ Superset: {e}")
    
    # Test 4: Connect
    try:
        result = subprocess.run([
            "curl", "-s", "--max-time", "5", "http://localhost:8083/"
        ], capture_output=True, text=True, check=True)
        
        if "kafka_cluster_id" in result.stdout:
            logger.info("✅ Connect: Servicio disponible")
            success_count += 1
        else:
            logger.warning("⚠️ Connect: Respuesta inválida")
    except Exception as e:
        logger.error(f"❌ Connect: {e}")
    
    # Resultado final
    logger.info(f"📊 RESULTADO: {success_count}/{total_tests} componentes verificados")
    
    if success_count >= 3:
        logger.info("🎉 PIPELINE ETL OPERATIVO")
        sys.exit(0)
    else:
        logger.warning("⚠️ Pipeline con problemas")
        sys.exit(1)

if __name__ == "__main__":
    main()