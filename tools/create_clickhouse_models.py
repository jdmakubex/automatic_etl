#!/usr/bin/env python3
"""
🏗️ AUTOMATIZADOR DE MODELOS CLICKHOUSE
Crea automáticamente tablas en ClickHouse basadas en esquemas detectados:
- Lee metadatos de tablas MySQL
- Crea tablas ClickHouse con tipos optimizados
- Configura engines y particionado apropiado
- Valida creación exitosa
"""

import json
import logging
import clickhouse_connect
import os
from datetime import datetime
from typing import Dict, List, Any, Optional

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/clickhouse_setup.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ClickHouseModelsManager:
    def __init__(self):
        # Configuración ClickHouse
        self.clickhouse_config = {
            'host': 'clickhouse',
            'port': 8123,
            'database': 'fgeo_analytics'
        }
        
        # Archivos de entrada
        self.metadata_file = '/app/generated/default/tables_metadata.json'
        self.schemas_dir = '/app/generated/default/schemas'
        self.env_file = '/app/generated/default/.env_auto'
        
        # Cliente ClickHouse
        self.client = None
        
        # Configuración avanzada de engines por tipo de tabla
        self.engine_config = {
            'default': {
                'engine': 'MergeTree',
                'settings': {
                    'index_granularity': 8192,
                    'ttl_only_drop_parts': 1
                }
            },
            'log': {
                'engine': 'MergeTree',
                'partition_by': 'toYYYYMM(fcreacion)',
                'settings': {
                    'index_granularity': 8192
                }
            },
            'reference': {
                'engine': 'ReplacingMergeTree',
                'settings': {
                    'index_granularity': 8192
                }
            }
        }
    
    def load_credentials(self) -> Dict[str, str]:
        """Cargar credenciales del archivo .env_auto"""
        credentials = {}
        
        if os.path.exists(self.env_file):
            with open(self.env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        credentials[key.lower()] = value
        
        return credentials
    
    def connect_clickhouse(self) -> bool:
        """Conectar a ClickHouse con credenciales apropiadas"""
        try:
            logger.info("🔌 Conectando a ClickHouse...")
            
            # Cargar credenciales
            credentials = self.load_credentials()
            
            # Configurar conexión
            connect_params = {
                'host': self.clickhouse_config['host'],
                'port': self.clickhouse_config['port'],
                'database': self.clickhouse_config['database']
            }
            
            # Usar credenciales si están disponibles
            if 'clickhouse_user' in credentials:
                connect_params['username'] = credentials['clickhouse_user']
                connect_params['password'] = credentials['clickhouse_password']
                logger.info(f"🔐 Usando credenciales: {credentials['clickhouse_user']}")
            else:
                logger.info("🔓 Usando conexión default")
            
            self.client = clickhouse_connect.get_client(**connect_params)
            
            # Probar conexión
            result = self.client.command("SELECT 1")
            logger.info("✅ Conexión ClickHouse establecida")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error conectando a ClickHouse: {str(e)}")
            return False
    
    def load_table_metadata(self) -> List[Dict[str, Any]]:
        """Cargar metadatos de tablas desde archivo JSON"""
        try:
            logger.info("📋 Cargando metadatos de tablas...")
            
            if not os.path.exists(self.metadata_file):
                logger.error(f"❌ Archivo de metadatos no encontrado: {self.metadata_file}")
                return []
            
            with open(self.metadata_file, 'r') as f:
                tables = json.load(f)
            
            logger.info(f"✅ {len(tables)} tablas cargadas desde metadatos")
            return tables
            
        except Exception as e:
            logger.error(f"❌ Error cargando metadatos: {str(e)}")
            return []
    
    def determine_table_type(self, table: Dict[str, Any]) -> str:
        """Determinar tipo de tabla para optimizar engine"""
        table_name = table['name'].lower()
        columns = table['columns']
        
        # Buscar patrones comunes
        has_timestamps = any(col['name'].lower() in ['fcreacion', 'factualizacion', 'created_at', 'updated_at'] 
                           for col in columns)
        
        has_log_pattern = any(word in table_name for word in ['log', 'audit', 'history', 'events'])
        
        # Tablas de referencia (pequeñas, actualizadas ocasionalmente)
        has_reference_pattern = any(word in table_name for word in ['catalogo', 'tipo', 'status', 'config'])
        
        if has_log_pattern and has_timestamps:
            return 'log'
        elif has_reference_pattern:
            return 'reference'
        else:
            return 'default'
    
    def generate_optimized_ddl(self, table: Dict[str, Any]) -> str:
        """Generar DDL optimizado para ClickHouse"""
        table_name = table['name']
        columns = table['columns']
        table_type = self.determine_table_type(table)
        
        logger.info(f"🔧 Generando DDL optimizado para {table_name} (tipo: {table_type})")
        
        # Construir definiciones de columnas
        column_definitions = []
        for col in columns:
            col_name = col['name']
            col_type = col['clickhouse_type']
            
            # Optimizaciones específicas por tipo
            if 'timestamp' in col_name.lower() or 'fecha' in col_name.lower():
                if 'Nullable' not in col_type:
                    col_type = 'DateTime64(3)'  # Precisión de milisegundos
            
            column_definitions.append(f"    {col_name} {col_type}")
        
        # Configuración del engine
        engine_config = self.engine_config[table_type]
        engine = engine_config['engine']
        
        # Determinar ORDER BY
        primary_keys = table.get('primary_keys', [])
        if primary_keys:
            order_by = ', '.join(primary_keys)
        elif table_type == 'log':
            # Para logs, ordenar por timestamp si existe
            timestamp_cols = [col['name'] for col in columns 
                            if any(ts_name in col['name'].lower() 
                                  for ts_name in ['fcreacion', 'created_at', 'timestamp'])]
            if timestamp_cols:
                order_by = timestamp_cols[0]
            else:
                order_by = columns[0]['name'] if columns else 'tuple()'
        else:
            order_by = columns[0]['name'] if columns else 'tuple()'
        
        # Construir DDL base
        ddl_parts = [
            f"CREATE TABLE IF NOT EXISTS {table_name} (",
            ',\n'.join(column_definitions),
            f") ENGINE = {engine}()"
        ]
        
        # Agregar PARTITION BY si aplica
        if 'partition_by' in engine_config:
            # Verificar si la columna de partición existe
            partition_col = engine_config['partition_by']
            if any('fcreacion' in col['name'] for col in columns):
                ddl_parts.append(f"PARTITION BY {partition_col}")
        
        # Agregar ORDER BY
        ddl_parts.append(f"ORDER BY ({order_by})")
        
        # Agregar SETTINGS
        if 'settings' in engine_config:
            settings_list = [f"{k} = {v}" for k, v in engine_config['settings'].items()]
            ddl_parts.append(f"SETTINGS {', '.join(settings_list)}")
        
        ddl = '\n'.join(ddl_parts)
        return ddl
    
    def create_table(self, table: Dict[str, Any]) -> bool:
        """Crear tabla individual en ClickHouse"""
        try:
            table_name = table['name']
            logger.info(f"🏗️  Creando tabla: {table_name}")
            
            # Generar DDL optimizado
            ddl = self.generate_optimized_ddl(table)
            
            logger.debug(f"DDL para {table_name}:")
            logger.debug(ddl)
            
            # Ejecutar DDL
            self.client.command(ddl)
            
            # Verificar creación
            result = self.client.command(f"EXISTS TABLE {table_name}")
            if result:
                logger.info(f"✅ Tabla {table_name} creada exitosamente")
                
                # Obtener información de la tabla creada
                table_info = self.client.command(f"DESCRIBE TABLE {table_name}")
                col_count = len(table_info) if isinstance(table_info, list) else 0
                logger.info(f"📊 {table_name}: {col_count} columnas configuradas")
                
                return True
            else:
                logger.error(f"❌ Tabla {table_name} no fue creada correctamente")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error creando tabla {table_name}: {str(e)}")
            return False
    
    def validate_tables(self, tables: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validar todas las tablas creadas"""
        logger.info("🔍 Validando tablas creadas...")
        
        validation_results = {
            'total_tables': len(tables),
            'created_successfully': 0,
            'failed': 0,
            'table_details': {}
        }
        
        for table in tables:
            table_name = table['name']
            try:
                # Verificar existencia
                exists = self.client.command(f"EXISTS TABLE {table_name}")
                
                if exists:
                    # Obtener estadísticas básicas
                    row_count = self.client.command(f"SELECT COUNT(*) FROM {table_name}")
                    table_size = self.client.command(f"SELECT formatReadableSize(total_bytes) FROM system.tables WHERE name = '{table_name}' AND database = '{self.clickhouse_config['database']}'")
                    
                    validation_results['created_successfully'] += 1
                    validation_results['table_details'][table_name] = {
                        'exists': True,
                        'row_count': row_count,
                        'size': table_size[0] if table_size else '0 B'
                    }
                    
                    logger.info(f"✅ {table_name}: {row_count} filas, {validation_results['table_details'][table_name]['size']}")
                    
                else:
                    validation_results['failed'] += 1
                    validation_results['table_details'][table_name] = {
                        'exists': False,
                        'error': 'Table does not exist'
                    }
                    logger.error(f"❌ {table_name}: no existe")
                    
            except Exception as e:
                validation_results['failed'] += 1
                validation_results['table_details'][table_name] = {
                    'exists': False,
                    'error': str(e)
                }
                logger.error(f"❌ {table_name}: error de validación - {str(e)}")
        
        return validation_results
    
    def save_validation_results(self, results: Dict[str, Any]):
        """Guardar resultados de validación"""
        try:
            results_file = f"{self.schemas_dir}/../clickhouse_validation.json"
            
            # Agregar timestamp
            results['validation_timestamp'] = datetime.now().isoformat()
            results['database'] = self.clickhouse_config['database']
            
            with open(results_file, 'w') as f:
                json.dump(results, f, indent=2)
            
            logger.info(f"💾 Resultados de validación guardados: {results_file}")
            
        except Exception as e:
            logger.error(f"❌ Error guardando validación: {str(e)}")
    
    def run_models_creation(self) -> bool:
        """Ejecutar creación completa de modelos ClickHouse"""
        start_time = datetime.now()
        logger.info("🚀 === INICIANDO CREACIÓN DE MODELOS CLICKHOUSE ===")
        logger.info(f"⏰ Inicio: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # 1. Conectar a ClickHouse
            if not self.connect_clickhouse():
                return False
            
            # 2. Cargar metadatos de tablas
            tables = self.load_table_metadata()
            if not tables:
                logger.error("❌ No hay tablas para procesar")
                return False
            
            # 3. Crear cada tabla
            logger.info(f"\n📋 CREANDO {len(tables)} TABLAS")
            created_count = 0
            
            for i, table in enumerate(tables, 1):
                logger.info(f"\n--- Tabla {i}/{len(tables)}: {table['name']} ---")
                if self.create_table(table):
                    created_count += 1
            
            # 4. Validar resultados
            logger.info(f"\n🔍 VALIDANDO RESULTADOS")
            validation_results = self.validate_tables(tables)
            
            # 5. Guardar resultados
            self.save_validation_results(validation_results)
            
            # 6. Resumen final
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info(f"\n🏁 === CREACIÓN DE MODELOS COMPLETADA ===")
            logger.info(f"⏰ Duración: {duration:.1f} segundos")
            logger.info(f"📊 Tablas procesadas: {validation_results['total_tables']}")
            logger.info(f"✅ Creadas exitosamente: {validation_results['created_successfully']}")
            logger.info(f"❌ Fallidas: {validation_results['failed']}")
            
            success = validation_results['failed'] == 0
            if success:
                logger.info("🎉 TODAS LAS TABLAS CREADAS EXITOSAMENTE")
            else:
                logger.warning("⚠️  ALGUNAS TABLAS FALLARON - Revisar logs")
            
            return success
            
        except Exception as e:
            logger.error(f"💥 Error crítico en creación de modelos: {str(e)}")
            return False
        
        finally:
            if self.client:
                self.client.close()

def main():
    """Función principal"""
    try:
        manager = ClickHouseModelsManager()
        success = manager.run_models_creation()
        
        print(f"\n{'='*60}")
        if success:
            print("🎉 CREACIÓN DE MODELOS CLICKHOUSE EXITOSA")
            print("✅ Todas las tablas fueron creadas correctamente")
            print("🏗️  Esquemas optimizados aplicados")
        else:
            print("❌ CREACIÓN DE MODELOS FALLÓ")
            print("💡 Revisar logs para detalles de errores")
        print(f"{'='*60}")
        
        return 0 if success else 1
        
    except KeyboardInterrupt:
        logger.info("🛑 Creación interrumpida por el usuario")
        return 1
    except Exception as e:
        logger.error(f"💥 Error crítico: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())