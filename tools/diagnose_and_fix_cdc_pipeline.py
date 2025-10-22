#!/usr/bin/env python3
"""
Script de diagnóstico y reparación del pipeline CDC ClickHouse-Kafka-Debezium
"""

import subprocess
import json
import logging
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def run_clickhouse_query(query: str) -> str:
    """Ejecuta una consulta en ClickHouse"""
    try:
        cmd = f'docker compose exec -T clickhouse clickhouse-client -q "{query}"'
        result = subprocess.check_output(cmd, shell=True, text=True, stderr=subprocess.STDOUT)
        return result.strip()
    except subprocess.CalledProcessError as e:
        logger.error(f"Error ejecutando query: {e.output}")
        return ""

def check_kafka_topics():
    """Verifica los tópicos de Kafka"""
    logger.info("\n📋 Verificando tópicos de Kafka...")
    try:
        cmd = "docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep dbserver | wc -l"
        result = subprocess.check_output(cmd, shell=True, text=True)
        count = int(result.strip())
        logger.info(f"✅ Encontrados {count} tópicos de Debezium")
        return count > 0
    except Exception as e:
        logger.error(f"❌ Error verificando tópicos: {e}")
        return False

def check_kafka_messages():
    """Verifica si hay mensajes en un tópico de ejemplo (opcional).
    Algunos contenedores no incluyen GetOffsetShell; en tal caso no falla el diagnóstico.
    """
    logger.info("\n📬 Verificando mensajes en Kafka (opcional)...")
    try:
        cmd = "docker compose exec -T kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic dbserver_archivos.archivos.archivos 2>/dev/null"
        result = subprocess.check_output(cmd, shell=True, text=True)
        total = 0
        for line in result.strip().split('\n'):
            if ':' in line:
                parts = line.split(':')
                if len(parts) >= 3:
                    try:
                        total += int(parts[2])
                    except Exception:
                        pass
        logger.info(f"✅ Total de mensajes en dbserver_archivos.archivos.archivos: {total}")
        return total > 0
    except Exception as e:
        logger.warning(f"ℹ️  No se pudo verificar mensajes con GetOffsetShell: {e}")
        logger.warning("ℹ️  Continuando sin este check (no bloqueante)")
        return True

def check_clickhouse_kafka_engines():
    """Verifica las tablas Kafka en ClickHouse"""
    logger.info("\n🔧 Verificando Kafka engines en ClickHouse...")
    query = "SELECT count() FROM system.tables WHERE database='ext' AND engine LIKE '%Kafka%'"
    result = run_clickhouse_query(query)
    if result:
        count = int(result)
        logger.info(f"✅ Encontradas {count} tablas Kafka")
        return count > 0
    return False

def check_materialized_views():
    """Verifica las Materialized Views"""
    logger.info("\n👁️  Verificando Materialized Views...")
    # En ClickHouse, las Materialized Views tienen engine = 'MaterializedView'
    query = "SELECT count() FROM system.tables WHERE database='ext' AND engine='MaterializedView' AND name LIKE 'mv_%'"
    result = run_clickhouse_query(query)
    if result:
        count = int(result)
        logger.info(f"✅ Encontradas {count} Materialized Views")
        return count > 0
    return False

def check_raw_tables():
    """Verifica las tablas raw"""
    logger.info("\n📊 Verificando tablas raw...")
    query = "SELECT count() FROM system.tables WHERE database='fgeo_analytics' AND name LIKE '%_raw'"
    result = run_clickhouse_query(query)
    if result:
        count = int(result)
        logger.info(f"✅ Encontradas {count} tablas raw")
        
        # Verificar si tienen datos
        query_data = "SELECT name, total_rows FROM system.tables WHERE database='fgeo_analytics' AND name LIKE '%_raw' AND total_rows > 0 LIMIT 5"
        result_data = run_clickhouse_query(query_data)
        if result_data:
            logger.info(f"✅ Tablas con datos:\n{result_data}")
            return True
        else:
            logger.warning("⚠️ Las tablas raw existen pero están vacías")
            return False
    return False

def detach_and_reattach_mvs():
    """Desconecta y reconecta las MVs para forzar consumo de Kafka"""
    logger.info("\n🔄 Reiniciando Materialized Views...")
    
    # Obtener lista de MVs
    query = "SELECT name FROM system.tables WHERE database='ext' AND engine='MaterializedView' AND name LIKE 'mv_%'"
    mvs_result = run_clickhouse_query(query)
    
    if not mvs_result:
        logger.warning("No se encontraron MVs para reiniciar")
        return False
    
    mvs = mvs_result.split('\n')
    logger.info(f"Reiniciando {len(mvs)} Materialized Views...")
    
    for mv in mvs:
        if not mv:
            continue
        # Detach
        run_clickhouse_query(f"DETACH TABLE ext.{mv}")
        # Reattach
        run_clickhouse_query(f"ATTACH TABLE ext.{mv}")
    
    logger.info(f"✅ Reiniciadas {len(mvs)} Materialized Views")
    return True

def restart_kafka_engines():
    """Reinicia los Kafka engines para reconectar a Kafka"""
    logger.info("\n🔄 Reiniciando Kafka engines...")
    
    # Obtener lista de tablas Kafka
    query = "SELECT name FROM system.tables WHERE database='ext' AND engine LIKE '%Kafka%'"
    kafka_tables_result = run_clickhouse_query(query)
    
    if not kafka_tables_result:
        logger.warning("No se encontraron tablas Kafka para reiniciar")
        return False
    
    kafka_tables = kafka_tables_result.split('\n')
    logger.info(f"Reiniciando {len(kafka_tables)} Kafka engines...")
    
    for table in kafka_tables:
        if not table:
            continue
        # Detach
        run_clickhouse_query(f"DETACH TABLE ext.{table}")
        # Reattach
        run_clickhouse_query(f"ATTACH TABLE ext.{table}")
    
    logger.info(f"✅ Reiniciadas {len(kafka_tables)} tablas Kafka")
    return True

def main():
    logger.info("=" * 60)
    logger.info("🔍 DIAGNÓSTICO COMPLETO DEL PIPELINE CDC")
    logger.info("=" * 60)
    
    # 1. Verificar tópicos Kafka
    kafka_topics_ok = check_kafka_topics()
    
    # 2. Verificar mensajes en Kafka
    kafka_messages_ok = check_kafka_messages()
    
    # 3. Verificar Kafka engines en ClickHouse
    kafka_engines_ok = check_clickhouse_kafka_engines()
    
    # 4. Verificar Materialized Views
    mvs_ok = check_materialized_views()
    
    # 5. Verificar tablas raw
    raw_tables_ok = check_raw_tables()
    
    # Resumen
    logger.info("\n" + "=" * 60)
    logger.info("📊 RESUMEN DEL DIAGNÓSTICO")
    logger.info("=" * 60)
    logger.info(f"Tópicos Kafka: {'✅' if kafka_topics_ok else '❌'}")
    logger.info(f"Mensajes en Kafka: {'✅' if kafka_messages_ok else '❌'}")
    logger.info(f"Kafka Engines ClickHouse: {'✅' if kafka_engines_ok else '❌'}")
    logger.info(f"Materialized Views: {'✅' if mvs_ok else '❌'}")
    logger.info(f"Tablas Raw con datos: {'✅' if raw_tables_ok else '❌'}")
    
    # Decisión
    # Si no hay datos en raw pero existen engines Kafka y MVs, intentamos reiniciar componentes CDC
    if not raw_tables_ok and kafka_engines_ok and mvs_ok:
        logger.info("\n" + "=" * 60)
        logger.info("💡 SOLUCIÓN: Reiniciar componentes CDC")
        logger.info("=" * 60)
        logger.warning("\n⚠️  Las MVs y Kafka engines existen, pero no hay datos en raw tables.")
        logger.info("ℹ️  Esto puede ocurrir si las MVs no se iniciaron correctamente.")
        logger.info("\n🔧 Aplicando solución: Reiniciar Kafka engines y MVs...")
        
        # Reiniciar
        restart_kafka_engines()
        detach_and_reattach_mvs()
        
        logger.info("\n✅ Componentes reiniciados.")
        logger.info("ℹ️  Espera 30 segundos y verifica de nuevo:")
        logger.info("   docker compose exec -T clickhouse clickhouse-client -q \"SELECT name, total_rows FROM system.tables WHERE database='fgeo_analytics' AND name LIKE '%_raw' AND total_rows > 0 LIMIT 10\"")
    elif not kafka_messages_ok:
        logger.warning("\nℹ️  No se pudo confirmar mensajes en Kafka (check opcional).")
        logger.info("   Si sospechas de Debezium, ejecuta: python3 tools/diagnose_and_fix_debezium.py")
    elif raw_tables_ok:
        logger.info("\n✅ ¡TODO ESTÁ FUNCIONANDO CORRECTAMENTE!")
    else:
        logger.error("\n❌ Hay problemas en la configuración básica del pipeline")
        logger.info("   Verifica que el pipeline CDC se haya ejecutado correctamente")
    
    logger.info("\n" + "=" * 60)
    logger.info("🏁 DIAGNÓSTICO COMPLETADO")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
