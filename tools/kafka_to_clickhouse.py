#!/usr/bin/env python3
"""
Script para cargar datos desde Kafka hacia ClickHouse manualmente.
Este script complementa el pipeline ETL cuando el JdbcSinkConnector no funciona con ClickHouse.
"""

import json
import sys
import time
from kafka import KafkaConsumer
import clickhouse_connect
from typing import Dict, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaToClickHouseLoader:
    def __init__(self):
        # Configuración de Kafka
        self.kafka_config = {
            'bootstrap_servers': ['kafka:9092'],
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': True,
            'group_id': 'clickhouse_loader',
            'value_deserializer': lambda m: json.loads(m.decode('utf-8'))
        }
        
        # Configuración de ClickHouse  
        self.ch_client = None
        
    def wait_for_clickhouse(self, max_wait=120, interval=5):
        """Espera adaptativa hasta que ClickHouse esté listo"""
        logger.info("⏳ Esperando que ClickHouse esté disponible...")
        start = time.time()
        while time.time() - start < max_wait:
            try:
                self.ch_client = clickhouse_connect.get_client(
                    host='clickhouse',
                    port=8123,
                    database='fgeo_analytics'
                )
                # Prueba simple de conexión
                self.ch_client.command('SELECT 1')
                logger.info("✅ ClickHouse está listo")
                return True
            except Exception as e:
                logger.info(f"⏳ ClickHouse no disponible aún: {e}")
                time.sleep(interval)
        logger.error("❌ ClickHouse no se pudo conectar tras espera adaptativa")
        return False
    
    def create_table_if_not_exists(self, table_name: str, sample_data: Dict[str, Any]):
        """Crear tabla en ClickHouse basada en los datos de ejemplo. Si no hay datos válidos, crear tabla dummy."""
        try:
            # Extraer el payload 'after' que contiene los datos reales
            if 'after' in sample_data and sample_data['after']:
                data = sample_data['after']
            else:
                logger.warning(f"No hay datos 'after' en el mensaje para {table_name}, omitiendo creación de tabla.")
                return False

            columns = []
            if data and isinstance(data, dict) and len(data) > 0:
                for field, value in data.items():
                    if value is None:
                        col_type = "Nullable(String)"
                    elif isinstance(value, int):
                        if value > 2147483647:
                            col_type = "Int64"
                        else:
                            col_type = "Int32"
                    elif isinstance(value, float):
                        col_type = "Float64"
                    elif isinstance(value, str):
                        col_type = "String"
                    else:
                        col_type = "String"
                    columns.append(f"{field} {col_type}")
            else:
                logger.warning(f"No hay columnas válidas en el mensaje para {table_name}, omitiendo creación de tabla.")
                return False

            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(columns)}
            ) ENGINE = MergeTree()
            ORDER BY tuple()
            """
            self.ch_client.command(create_sql)
            logger.info(f"✅ Tabla {table_name} creada/verificada")
            return True
        except Exception as e:
            logger.error(f"❌ Error creando tabla {table_name}: {e}")
            return False
    
    def insert_data(self, table_name: str, data: Dict[str, Any]):
        """Insertar datos en ClickHouse"""
        try:
            if 'after' in data and data['after']:
                payload = data['after']
                
                # Convertir None a valores apropiados
                for key, value in payload.items():
                    if value is None:
                        payload[key] = ''
                
                # Insertar usando clickhouse_connect
                self.ch_client.insert(table_name, [payload])
                return True
            else:
                logger.debug("Mensaje sin datos 'after', saltando...")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error insertando en {table_name}: {e}")
            return False
    
    def wait_for_kafka(self, max_wait=120, interval=5):
        """Espera adaptativa hasta que Kafka esté listo"""
        logger.info("⏳ Esperando que Kafka esté disponible...")
        start = time.time()
        while time.time() - start < max_wait:
            try:
                consumer = KafkaConsumer(**self.kafka_config)
                # Prueba simple de conexión
                topics = consumer.topics()
                if topics:
                    logger.info("✅ Kafka está listo")
                    return consumer
            except Exception as e:
                logger.info(f"⏳ Kafka no disponible aún: {e}")
                time.sleep(interval)
        logger.error("❌ Kafka no se pudo conectar tras espera adaptativa")
        return None

    def process_topics(self, topic_pattern: str = "dbserver_default.archivos."):
        """Procesar tópicos de Kafka que coincidan con el patrón, con espera adaptativa y feedback de usuario"""
        logger.info("🔄 Fase 1: Esperando servicios...")
        if not self.wait_for_clickhouse():
            logger.error("❌ No se puede continuar: ClickHouse no está listo")
            return False
        consumer = self.wait_for_kafka()
        if not consumer:
            logger.error("❌ No se puede continuar: Kafka no está listo")
            return False

        logger.info("🔄 Fase 2: Descubriendo tópicos de Kafka...")
        try:
            topics = [t for t in consumer.topics() if topic_pattern in t]
            if not topics:
                all_topics = consumer.topics()
                matching_topics = [t for t in all_topics if topic_pattern in str(t)]
                if not matching_topics:
                    logger.error(f"No se encontraron tópicos con patrón: {topic_pattern}")
                    return False
                topics = matching_topics
            logger.info(f"🔄 Fase 3: Procesando tópicos: {topics}")
            consumer.subscribe(topics)
            processed_count = 0
            tables_created = set()
            logger.info("🔄 Fase 4: Procesando mensajes de Kafka...")
            for message in consumer:
                try:
                    topic_parts = message.topic.split('.')
                    if len(topic_parts) >= 3:
                        table_name = topic_parts[-1]
                    else:
                        table_name = message.topic.replace('.', '_')
                    if table_name not in tables_created:
                        if self.create_table_if_not_exists(table_name, message.value):
                            tables_created.add(table_name)
                    if self.insert_data(table_name, message.value):
                        processed_count += 1
                        if processed_count % 100 == 0:
                            logger.info(f"🔄 Procesados {processed_count} registros...")
                    if processed_count >= 1000:
                        logger.info("🔄 Límite de prueba alcanzado (1000 registros)")
                        break
                except Exception as e:
                    logger.error(f"Error procesando mensaje: {e}")
                    continue
            logger.info(f"✅ Procesamiento completado: {processed_count} registros")
            return True
        except Exception as e:
            logger.error(f"❌ Error en proceso principal: {e}")
            return False

def main():
    """Función principal"""
    loader = KafkaToClickHouseLoader()
    
    if len(sys.argv) > 1:
        topic_pattern = sys.argv[1]
    else:
        topic_pattern = "dbserver_default.archivos."
    
    logger.info(f"Iniciando carga de datos desde Kafka a ClickHouse...")
    logger.info(f"Patrón de tópicos: {topic_pattern}")
    
    success = loader.process_topics(topic_pattern)
    
    if success:
        logger.info("🎉 Proceso completado exitosamente")
        sys.exit(0)
    else:
        logger.error("❌ Proceso falló")
        sys.exit(1)

if __name__ == "__main__":
    main()