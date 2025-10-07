#!/usr/bin/env python3
"""
🔍 DETECTOR AUTOMÁTICO DE TABLAS MYSQL
Conecta a MySQL, detecta todas las tablas y genera configuraciones automáticamente:
- Detecta estructura de tablas
- Genera conectores Debezium
- Crea esquemas ClickHouse
- Configura permisos necesarios
"""

import json
import logging
import pymysql
import os
from datetime import datetime
from typing import Dict, List, Tuple, Any

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/discovery.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MySQLTableDiscovery:
    def __init__(self):
        # Configuración MySQL desde variables de entorno o valores por defecto
        self.mysql_config = {
            'host': os.getenv('MYSQL_HOST', '172.21.61.53'),
            'port': int(os.getenv('MYSQL_PORT', 3306)),
            'user': os.getenv('MYSQL_USER', 'debezium'),
            'password': os.getenv('MYSQL_PASSWORD', 'dbz'),
            'database': os.getenv('MYSQL_DATABASE', 'archivos'),
            'charset': 'utf8mb4'
        }
        
        # Configuración de salida
        self.output_dir = '/app/generated/default'
        self.schemas_dir = f'{self.output_dir}/schemas'
        
        # Mapeo de tipos MySQL a ClickHouse
        self.type_mapping = {
            # Números enteros
            'tinyint': 'Int8',
            'smallint': 'Int16', 
            'mediumint': 'Int32',
            'int': 'Int32',
            'integer': 'Int32',
            'bigint': 'Int64',
            
            # Números decimales
            'decimal': 'Decimal64',
            'numeric': 'Decimal64', 
            'float': 'Float32',
            'double': 'Float64',
            'real': 'Float64',
            
            # Cadenas de texto
            'char': 'FixedString',
            'varchar': 'String',
            'text': 'String',
            'tinytext': 'String',
            'mediumtext': 'String',
            'longtext': 'String',
            
            # Fechas y tiempo
            'date': 'Date',
            'datetime': 'DateTime64',
            'timestamp': 'DateTime64',
            'time': 'String',
            'year': 'Int16',
            
            # Binarios
            'binary': 'String',
            'varbinary': 'String',
            'blob': 'String',
            'tinyblob': 'String',
            'mediumblob': 'String',
            'longblob': 'String',
            
            # Otros
            'json': 'String',
            'enum': 'String',
            'set': 'String'
        }
        
    def connect_mysql(self) -> pymysql.Connection:
        """Conectar a MySQL con reintentos"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                logger.info(f"🔌 Conectando a MySQL {self.mysql_config['host']}:{self.mysql_config['port']} (intento {attempt + 1})")
                connection = pymysql.connect(**self.mysql_config)
                logger.info("✅ Conexión MySQL establecida")
                return connection
            except Exception as e:
                logger.error(f"❌ Error conectando a MySQL (intento {attempt + 1}): {str(e)}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(2)
    
    def get_database_tables(self, connection: pymysql.Connection) -> List[str]:
        """Obtener lista de tablas en la base de datos"""
        logger.info("📋 Obteniendo lista de tablas...")
        
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]
            
        logger.info(f"📊 Encontradas {len(tables)} tablas: {', '.join(tables)}")
        return tables
    
    def get_table_structure(self, connection: pymysql.Connection, table_name: str) -> Dict[str, Any]:
        """Obtener estructura detallada de una tabla"""
        logger.info(f"🔍 Analizando estructura de tabla: {table_name}")
        
        table_info = {
            'name': table_name,
            'columns': [],
            'primary_keys': [],
            'indexes': [],
            'row_count': 0
        }
        
        with connection.cursor() as cursor:
            # Obtener información de columnas
            cursor.execute(f"DESCRIBE {table_name}")
            columns_info = cursor.fetchall()
            
            for col_info in columns_info:
                field_name = col_info[0]
                field_type = col_info[1].lower()
                is_nullable = col_info[2] == 'YES'
                is_key = col_info[3] == 'PRI'
                default_value = col_info[4]
                extra = col_info[5]
                
                # Mapear tipo MySQL a ClickHouse
                base_type = field_type.split('(')[0]  # quitar paréntesis y parámetros
                clickhouse_type = self.type_mapping.get(base_type, 'String')
                
                # Aplicar nullable si es necesario
                if is_nullable and not is_key:
                    clickhouse_type = f"Nullable({clickhouse_type})"
                
                column = {
                    'name': field_name,
                    'mysql_type': field_type,
                    'clickhouse_type': clickhouse_type,
                    'nullable': is_nullable,
                    'primary_key': is_key,
                    'default': default_value,
                    'extra': extra
                }
                
                table_info['columns'].append(column)
                
                if is_key:
                    table_info['primary_keys'].append(field_name)
            
            # Obtener conteo de filas
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            table_info['row_count'] = cursor.fetchone()[0]
            
            # Obtener información de índices
            cursor.execute(f"SHOW INDEX FROM {table_name}")
            indexes_info = cursor.fetchall()
            
            for index_info in indexes_info:
                index = {
                    'name': index_info[2],
                    'column': index_info[4],
                    'unique': index_info[1] == 0
                }
                table_info['indexes'].append(index)
        
        logger.info(f"✅ Tabla {table_name}: {len(table_info['columns'])} columnas, {table_info['row_count']} filas")
        return table_info
    
    def generate_debezium_connector_config(self, tables: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generar configuración del conector Debezium"""
        logger.info("⚙️  Generando configuración de conector Debezium...")
        
        table_names = [table['name'] for table in tables]
        table_include_list = ','.join([f"{self.mysql_config['database']}.{table}" for table in table_names])
        
        config = {
            "name": "mysql_source_auto",
            "config": {
                "connector.class": "io.debezium.connector.mysql.MySqlConnector",
                "tasks.max": "1",
                "database.hostname": self.mysql_config['host'],
                "database.port": str(self.mysql_config['port']),
                "database.user": self.mysql_config['user'],
                "database.password": self.mysql_config['password'],
                "database.server.id": "184054",
                "topic.prefix": "mysql_auto",
                "database.include.list": self.mysql_config['database'],
                "table.include.list": table_include_list,
                "database.history.kafka.bootstrap.servers": "kafka:9092",
                "database.history.kafka.topic": "mysql_auto.history",
                "include.schema.changes": "true",
                "snapshot.mode": "initial",
                "snapshot.locking.mode": "minimal",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter", 
                "value.converter.schemas.enable": "false",
                "transforms": "unwrap",
                "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
                "transforms.unwrap.drop.tombstones": "false",
                "transforms.unwrap.delete.handling.mode": "rewrite"
            }
        }
        
        return config
    
    def generate_clickhouse_schemas(self, tables: List[Dict[str, Any]]) -> Dict[str, str]:
        """Generar esquemas DDL para ClickHouse"""
        logger.info("🏗️  Generando esquemas de ClickHouse...")
        
        schemas = {}
        
        for table in tables:
            table_name = table['name']
            columns = table['columns']
            
            # Construir DDL
            column_definitions = []
            for col in columns:
                col_def = f"{col['name']} {col['clickhouse_type']}"
                column_definitions.append(col_def)
            
            # Determinar clave de ordenamiento
            if table['primary_keys']:
                order_by = ', '.join(table['primary_keys'])
            else:
                # Usar primera columna como orden por defecto
                order_by = columns[0]['name'] if columns else 'tuple()'
            
            # Crear DDL completo
            ddl = f"""CREATE TABLE IF NOT EXISTS {table_name} (
    {',\\n    '.join(column_definitions)}
) ENGINE = MergeTree()
ORDER BY ({order_by})
SETTINGS index_granularity = 8192"""
            
            schemas[table_name] = ddl
            
            logger.info(f"📄 Schema generado para {table_name}")
        
        return schemas
    
    def save_configurations(self, connector_config: Dict[str, Any], schemas: Dict[str, str], tables: List[Dict[str, Any]]):
        """Guardar todas las configuraciones generadas"""
        logger.info("💾 Guardando configuraciones...")
        
        # Crear directorios si no existen
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.schemas_dir, exist_ok=True)
        
        # Guardar configuración del conector
        connector_file = f"{self.output_dir}/mysql_connector_auto.json"
        with open(connector_file, 'w') as f:
            json.dump(connector_config, f, indent=2)
        logger.info(f"✅ Conector guardado: {connector_file}")
        
        # Guardar esquemas ClickHouse
        for table_name, ddl in schemas.items():
            schema_file = f"{self.schemas_dir}/{table_name}_clickhouse.sql"
            with open(schema_file, 'w') as f:
                f.write(ddl)
            logger.info(f"✅ Schema guardado: {schema_file}")
        
        # Guardar metadatos de tablas
        metadata_file = f"{self.output_dir}/tables_metadata.json"
        with open(metadata_file, 'w') as f:
            json.dump(tables, f, indent=2, default=str)
        logger.info(f"✅ Metadatos guardados: {metadata_file}")
        
        # Crear archivo de resumen
        summary = {
            'discovery_timestamp': datetime.now().isoformat(),
            'mysql_config': {k: v for k, v in self.mysql_config.items() if k != 'password'},
            'tables_discovered': len(tables),
            'table_names': [t['name'] for t in tables],
            'total_rows': sum(t['row_count'] for t in tables),
            'files_generated': {
                'connector_config': connector_file,
                'schemas_directory': self.schemas_dir,
                'metadata_file': metadata_file
            }
        }
        
        summary_file = f"{self.output_dir}/discovery_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        logger.info(f"✅ Resumen guardado: {summary_file}")
    
    def run_discovery(self) -> bool:
        """Ejecutar proceso completo de descubrimiento"""
        start_time = datetime.now()
        logger.info("🚀 === INICIANDO DESCUBRIMIENTO AUTOMÁTICO DE TABLAS ===")
        logger.info(f"⏰ Inicio: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # Conectar a MySQL
            connection = self.connect_mysql()
            
            # Obtener lista de tablas
            table_names = self.get_database_tables(connection)
            
            if not table_names:
                logger.warning("⚠️  No se encontraron tablas en la base de datos")
                return False
            
            # Analizar estructura de cada tabla
            tables = []
            for table_name in table_names:
                try:
                    table_info = self.get_table_structure(connection, table_name)
                    tables.append(table_info)
                except Exception as e:
                    logger.error(f"❌ Error analizando tabla {table_name}: {str(e)}")
                    continue
            
            connection.close()
            
            if not tables:
                logger.error("❌ No se pudo analizar ninguna tabla")
                return False
            
            # Generar configuraciones
            connector_config = self.generate_debezium_connector_config(tables)
            clickhouse_schemas = self.generate_clickhouse_schemas(tables)
            
            # Guardar configuraciones
            self.save_configurations(connector_config, clickhouse_schemas, tables)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info(f"\n🏁 === DESCUBRIMIENTO COMPLETADO ===")
            logger.info(f"⏰ Duración: {duration:.1f} segundos")
            logger.info(f"📊 Tablas procesadas: {len(tables)}")
            logger.info(f"📄 Archivos generados: {len(clickhouse_schemas) + 3}")
            
            return True
            
        except Exception as e:
            logger.error(f"💥 Error crítico en descubrimiento: {str(e)}")
            return False

def main():
    """Función principal"""
    try:
        discovery = MySQLTableDiscovery()
        success = discovery.run_discovery()
        
        print(f"\n{'='*60}")
        if success:
            print("🎉 DESCUBRIMIENTO AUTOMÁTICO EXITOSO")
            print("✅ Configuraciones generadas automáticamente")
            print("📁 Archivos disponibles en /app/generated/default/")
        else:
            print("❌ DESCUBRIMIENTO FALLÓ")
            print("💡 Revisar logs para detalles de errores")
        print(f"{'='*60}")
        
        return 0 if success else 1
        
    except KeyboardInterrupt:
        logger.info("🛑 Descubrimiento interrumpido por el usuario")
        return 1
    except Exception as e:
        logger.error(f"💥 Error crítico: {str(e)}")
        return 1

if __name__ == "__main__":
    import time
    exit(main())