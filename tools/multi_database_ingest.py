#!/usr/bin/env python3
"""
🏭 MULTI-DATABASE INGEST ORCHESTRATOR
===================================

Este script procesa TODAS las conexiones definidas en DB_CONNECTIONS,
ejecutando la ingesta completa para cada base de datos por separado:

1. Parse DB_CONNECTIONS del .env
2. Para cada conexión:
   - Descubre tablas MySQL
   - Ejecuta ingest_runner.py  
   - Ejecuta auditoría
   - Configura Superset
3. Genera reporte consolidado

Uso:
    python tools/multi_database_ingest.py
"""

import os
import json
import sys
import logging
import subprocess
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

# Importar e integrar limpiador robusto
try:
    from integrate_robust_cleaning import patch_ingest_runner
    # Aplicar parches inmediatamente al importar
    if patch_ingest_runner():
        print("✅ Limpieza robusta de datos activada")
    else:
        print("⚠ No se pudo activar limpieza robusta, continuando con sistema básico")
except Exception as e:
    print(f"⚠ Error activando limpieza robusta: {e}")

# Importar corrector de esquemas ClickHouse
try:
    from fix_clickhouse_schemas import ClickHouseSchemaFixer
    print("✅ Corrector de esquemas ClickHouse cargado")
except Exception as e:
    print(f"⚠ Error cargando corrector de esquemas: {e}")
    ClickHouseSchemaFixer = None

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/multi_database_ingest.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MultiDatabaseIngest:
    def __init__(self):
        """Inicializa el orquestador multi-base de datos"""
        self.db_connections_json = os.getenv("DB_CONNECTIONS", "[]")
        self.results = {}
        self.start_time = datetime.now()
        
        logger.info("🏭 Iniciando Multi-Database Ingest Orchestrator")
        
    def parse_connections(self) -> List[Dict[str, Any]]:
        """Parse las conexiones desde DB_CONNECTIONS"""
        try:
            connections = json.loads(self.db_connections_json)
            if isinstance(connections, dict):
                connections = [connections]
            
            logger.info(f"📊 Encontradas {len(connections)} conexiones de base de datos")
            for i, conn in enumerate(connections):
                db_name = conn.get('db', 'unknown')
                conn_name = conn.get('name', f'conn_{i}')
                logger.info(f"   {i+1}. {conn_name} → {db_name}")
            
            return connections
        except json.JSONDecodeError as e:
            logger.error(f"❌ Error parseando DB_CONNECTIONS: {e}")
            return []
    
    def build_source_url(self, conn: Dict[str, Any]) -> str:
        """Construye URL de conexión MySQL desde parámetros"""
        if 'url' in conn:
            return conn['url']
            
        # Construir URL desde componentes
        host = conn.get('host', 'localhost')
        port = conn.get('port', 3306)
        user = conn.get('user', 'root')
        password = conn.get('pass', '')
        database = conn.get('db', '')
        
        return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    
    def run_ingest_for_connection(self, conn: Dict[str, Any]) -> Dict[str, Any]:
        """Ejecuta ingesta completa para una conexión específica"""
        conn_name = conn.get('name', 'default')
        db_name = conn.get('db', 'unknown')
        
        logger.info(f"🚀 Iniciando ingesta para: {conn_name} ({db_name})")
        
        result = {
            'connection_name': conn_name,
            'database': db_name,
            'start_time': datetime.now().isoformat(),
            'status': 'PENDING',
            'steps': {},
            'total_records': 0,
            'errors': []
        }
        
        try:
            # Construir URL de conexión
            source_url = self.build_source_url(conn)
            
            # Paso 1: Ejecutar ingest_runner
            logger.info(f"📥 Ejecutando ingesta de datos para {db_name}...")
            ingest_cmd = [
                'python', '/app/tools/ingest_runner.py',
                '--source-url', source_url,
                '--source-name', conn_name,
                '--ch-database', f"{db_name}_analytics",
                '--ch-prefix', f"{db_name}_",
                '--schemas', db_name,
                '--chunksize', '50000',
                '--truncate-before-load',
                '--dedup', 'none'
            ]
            
            ingest_result = subprocess.run(
                ingest_cmd, 
                capture_output=True, 
                text=True, 
                timeout=1800  # 30 minutos
            )
            
            if ingest_result.returncode == 0:
                result['steps']['ingestion'] = 'SUCCESS'
                # Extraer número de registros del output
                output_lines = ingest_result.stdout.split('\n')
                for line in output_lines:
                    if 'registros totales' in line.lower():
                        try:
                            import re
                            numbers = re.findall(r'\d+', line)
                            if numbers:
                                result['total_records'] = int(numbers[-1])
                        except:
                            pass
                logger.info(f"✅ Ingesta completada para {db_name}: {result['total_records']} registros")
            else:
                result['steps']['ingestion'] = 'FAILED'
                result['errors'].append(f"Ingesta falló: {ingest_result.stderr}")
                logger.error(f"❌ Ingesta falló para {db_name}: {ingest_result.stderr}")
            
            # Paso 1.5: Corregir esquemas ClickHouse (especialmente para problemas de nullable)
            schema_fix_success = False
            if result['steps'].get('ingestion') in ['SUCCESS', 'FAILED']:  # Intentar fix incluso si falló
                logger.info(f"🔧 Corrigiendo esquemas ClickHouse para {db_name}...")
                
                # Obtener primary keys de las tablas
                table_primary_keys = self.get_table_primary_keys(source_url, db_name)
                
                # Aplicar correcciones de esquema
                schema_fix_success = self.fix_clickhouse_schemas(db_name, table_primary_keys)
                
                if schema_fix_success:
                    result['steps']['schema_fix'] = 'SUCCESS'
                    logger.info(f"✅ Esquemas ClickHouse corregidos para {db_name}")
                else:
                    result['steps']['schema_fix'] = 'FAILED'
                    result['errors'].append("Falló corrección de esquemas ClickHouse")
                    logger.warning(f"⚠️ Falló corrección de esquemas para {db_name}")
            
            # Paso 1.75: Reintento de ingesta si hubo problemas iniciales pero se corrigieron esquemas
            if (result['steps'].get('ingestion') == 'FAILED' and 
                result['steps'].get('schema_fix') == 'SUCCESS'):
                
                logger.info(f"🔄 Reintentando ingesta para {db_name} con esquemas corregidos...")
                
                retry_result = subprocess.run(
                    ingest_cmd, 
                    capture_output=True, 
                    text=True, 
                    timeout=1800  # 30 minutos
                )
                
                if retry_result.returncode == 0:
                    result['steps']['ingestion'] = 'SUCCESS_ON_RETRY'
                    # Actualizar número de registros
                    output_lines = retry_result.stdout.split('\n')
                    for line in output_lines:
                        if 'registros totales' in line.lower():
                            try:
                                import re
                                numbers = re.findall(r'\d+', line)
                                if numbers:
                                    result['total_records'] = int(numbers[-1])
                            except:
                                pass
                    logger.info(f"✅ Reintento exitoso para {db_name}: {result['total_records']} registros")
                else:
                    logger.warning(f"⚠️ Reintento falló para {db_name}: {retry_result.stderr}")
            
            # Paso 2: Ejecutar auditoría
            if result['steps'].get('ingestion') in ['SUCCESS', 'SUCCESS_ON_RETRY']:
                logger.info(f"🔍 Ejecutando auditoría para {db_name}...")
                audit_cmd = [
                    'python', '/app/tools/audit_mysql_clickhouse.py',
                    source_url,
                    f"{db_name}_analytics"
                ]
                
                audit_result = subprocess.run(
                    audit_cmd,
                    capture_output=True,
                    text=True,
                    timeout=600  # 10 minutos
                )
                
                if audit_result.returncode == 0:
                    result['steps']['audit'] = 'SUCCESS'
                    logger.info(f"✅ Auditoría completada para {db_name}")
                else:
                    result['steps']['audit'] = 'FAILED'
                    result['errors'].append(f"Auditoría falló: {audit_result.stderr}")
                    logger.warning(f"⚠️ Auditoría falló para {db_name}: {audit_result.stderr}")
            
            # Determinar status final
            if result['steps'].get('ingestion') in ['SUCCESS', 'SUCCESS_ON_RETRY']:
                if result['steps'].get('audit') == 'SUCCESS':
                    result['status'] = 'COMPLETED'
                else:
                    result['status'] = 'COMPLETED_WITH_WARNINGS'
            else:
                result['status'] = 'FAILED'
                
        except subprocess.TimeoutExpired:
            result['status'] = 'TIMEOUT'
            result['errors'].append('Proceso excedió tiempo límite')
            logger.error(f"⏰ Timeout en ingesta para {db_name}")
        except Exception as e:
            result['status'] = 'ERROR'
            result['errors'].append(str(e))
            logger.error(f"💥 Error inesperado en {db_name}: {e}")
        
        result['end_time'] = datetime.now().isoformat()
        return result
    
    def fix_clickhouse_schemas(self, db_name: str, table_primary_keys: Dict[str, List[str]] = None) -> bool:
        """
        Corregir esquemas de ClickHouse para hacer columnas nullable cuando sea necesario.
        
        Args:
            db_name: Nombre de la base de datos
            table_primary_keys: Diccionario con primary keys por tabla
            
        Returns:
            True si fue exitoso
        """
        if not ClickHouseSchemaFixer:
            logger.warning("⚠ Corrector de esquemas ClickHouse no disponible")
            return False
            
        try:
            # Crear fixer usando configuración del entorno
            fixer = ClickHouseSchemaFixer(
                clickhouse_host=os.getenv('CLICKHOUSE_HOST', 'clickhouse'),
                clickhouse_port=int(os.getenv('CLICKHOUSE_HTTP_PORT', '8123')),
                clickhouse_user=os.getenv('CLICKHOUSE_ETL_USER', 'etl'),
                clickhouse_password=os.getenv('CLICKHOUSE_ETL_PASSWORD', 'Et1Ingest!')
            )
            
            database_name = f"{db_name}_analytics"
            logger.info(f"🔧 Corrigiendo esquemas ClickHouse para: {database_name}")
            
            # Ejecutar corrección de esquemas
            result = fixer.fix_database_schemas(database_name, table_primary_keys)
            
            if 'error' in result:
                logger.error(f"❌ Error corrigiendo esquemas: {result['error']}")
                return False
            
            logger.info(f"✅ Esquemas corregidos: {result['total_columns_modified']} columnas modificadas")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error en corrección de esquemas: {e}")
            return False

    def get_table_primary_keys(self, connection_url: str, db_name: str) -> Dict[str, List[str]]:
        """
        Obtener primary keys de todas las tablas de una base de datos.
        
        Args:
            connection_url: URL de conexión MySQL
            db_name: Nombre de la base de datos
            
        Returns:
            Diccionario con primary keys por tabla
        """
        try:
            from sqlalchemy import create_engine, text
            engine = create_engine(connection_url)
            
            table_pks = {}
            
            # Obtener lista de tablas
            with engine.connect() as connection:
                tables_result = connection.execute(text("SHOW TABLES"))
                tables = [row[0] for row in tables_result]
                
                # Para cada tabla, obtener sus primary keys
                for table in tables:
                    pk_query = text(f"""
                        SELECT COLUMN_NAME 
                        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
                        WHERE TABLE_SCHEMA = :schema 
                        AND TABLE_NAME = :table 
                        AND CONSTRAINT_NAME = 'PRIMARY'
                        ORDER BY ORDINAL_POSITION
                    """)
                    
                    pk_result = connection.execute(pk_query, {
                        'schema': db_name,
                        'table': table
                    })
                    
                    primary_keys = [row[0] for row in pk_result]
                    if primary_keys:
                        table_pks[table] = primary_keys
            
            engine.dispose()
            logger.info(f"📊 Primary keys detectadas para {len(table_pks)} tablas")
            return table_pks
            
        except Exception as e:
            logger.error(f"❌ Error obteniendo primary keys: {e}")
            return {}

    def generate_consolidated_report(self) -> Dict[str, Any]:
        """Genera reporte consolidado de todas las ingestas"""
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        
        successful = sum(1 for r in self.results.values() if r['status'] in ['COMPLETED', 'COMPLETED_WITH_WARNINGS'])
        failed = sum(1 for r in self.results.values() if r['status'] in ['FAILED', 'ERROR', 'TIMEOUT'])
        total_records = sum(r['total_records'] for r in self.results.values())
        
        report = {
            'timestamp': end_time.isoformat(),
            'duration_seconds': round(duration, 2),
            'summary': {
                'total_connections': len(self.results),
                'successful': successful,
                'failed': failed,
                'total_records_processed': total_records
            },
            'databases': self.results,
            'status': 'SUCCESS' if failed == 0 else 'PARTIAL_SUCCESS' if successful > 0 else 'FAILED'
        }
        
        return report
    
    def run(self) -> bool:
        """Ejecuta ingesta multi-base de datos completa"""
        connections = self.parse_connections()
        
        if not connections:
            logger.error("❌ No se encontraron conexiones válidas en DB_CONNECTIONS")
            return False
        
        logger.info(f"🎯 Procesando {len(connections)} bases de datos...")
        
        # Procesar cada conexión
        for conn in connections:
            conn_name = conn.get('name', 'default')
            result = self.run_ingest_for_connection(conn)
            self.results[conn_name] = result
        
        # Generar reporte consolidado
        report = self.generate_consolidated_report()
        
        # Guardar reporte
        report_path = '/app/logs/multi_database_ingest_report.json'
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        # Log resumen
        logger.info("📊 RESUMEN MULTI-DATABASE INGEST:")
        logger.info(f"   ✅ Exitosos: {report['summary']['successful']}")
        logger.info(f"   ❌ Fallidos: {report['summary']['failed']}")
        logger.info(f"   📊 Total registros: {report['summary']['total_records_processed']:,}")
        logger.info(f"   ⏱️ Duración: {report['duration_seconds']:.1f}s")
        logger.info(f"📋 Reporte guardado en: {report_path}")
        
        return report['status'] in ['SUCCESS', 'PARTIAL_SUCCESS']

def main():
    """Función principal"""
    try:
        orchestrator = MultiDatabaseIngest()
        success = orchestrator.run()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"💥 Error crítico: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()