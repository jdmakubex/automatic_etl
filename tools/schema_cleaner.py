#!/usr/bin/env python3
"""
Sistema de depuración automática de esquemas para el pipeline ETL
Elimina esquemas innecesarios y configura automáticamente Superset y Metabase
con solo las tablas útiles
"""

import subprocess
import os
import json
import sys
import time
import requests
from datetime import datetime
from collections import defaultdict

class SchemaCleaner:
    def __init__(self):
        self.clickhouse_user = "default"
        self.clickhouse_password = os.getenv("CLICKHOUSE_DEFAULT_PASSWORD", "ClickHouse123!")
        self.useful_tables = []
        self.cleanup_log = []
        
    def run_clickhouse_query(self, query, timeout=30):
        """Ejecutar consulta en ClickHouse"""
        try:
            result = subprocess.run([
                'docker', 'exec', 'clickhouse', 
                'clickhouse-client', 
                '--user', self.clickhouse_user, 
                '--password', self.clickhouse_password,
                '--query', query
            ], capture_output=True, text=True, timeout=timeout)
            
            if result.returncode == 0:
                return result.stdout.strip()
            else:
                print(f"❌ Error en consulta: {result.stderr}")
                return None
        except Exception as e:
            print(f"❌ Error ejecutando consulta: {e}")
            return None
    
    def identify_schemas_to_clean(self):
        """Identificar esquemas y tablas a limpiar"""
        print("🔍 FASE 1: IDENTIFICANDO ESQUEMAS A DEPURAR")
        print("="*50)
        
        # Obtener todas las bases de datos
        databases_result = self.run_clickhouse_query("SHOW DATABASES")
        if not databases_result:
            return [], []
        
        databases = [db for db in databases_result.split('\n') 
                    if db and not db.startswith('INFORMATION_SCHEMA') 
                    and db not in ['information_schema', 'system']]
        
        useful_tables = []
        tables_to_remove = []
        databases_to_remove = []
        
        for db in databases:
            # Obtener tablas
            tables_result = self.run_clickhouse_query(f"SHOW TABLES FROM {db}")
            if not tables_result:
                if db not in ['default', 'ext']:  # Mantener estas bases especiales
                    databases_to_remove.append(db)
                continue
                
            tables = [t for t in tables_result.split('\n') if t]
            useful_count = 0
            
            for table in tables:
                # Contar registros
                count_result = self.run_clickhouse_query(f"SELECT COUNT(*) FROM {db}.{table}")
                if count_result and int(count_result) > 0:
                    count = int(count_result)
                    
                    # Criterios para mantener una tabla
                    if (count >= 10 and  # Al menos 10 registros
                        (table.startswith('src__') or  # Tablas principales
                         table.endswith('_v') or  # Vistas
                         db in ['archivos', 'fiscalizacion', 'fiscalizacion_analytics'])):
                        useful_tables.append({
                            'database': db,
                            'table': table,
                            'records': count,
                            'full_name': f"{db}.{table}"
                        })
                        useful_count += 1
                    else:
                        tables_to_remove.append(f"{db}.{table}")
                else:
                    # Tabla vacía o con errores
                    tables_to_remove.append(f"{db}.{table}")
            
            # Si la base de datos no tiene tablas útiles, marcarla para eliminación
            if useful_count == 0 and db not in ['default', 'ext']:
                databases_to_remove.append(db)
        
        print(f"✅ Tablas útiles identificadas: {len(useful_tables)}")
        print(f"🗑️  Tablas a eliminar: {len(tables_to_remove)}")
        print(f"🗑️  Bases de datos a eliminar: {len(databases_to_remove)}")
        
        return useful_tables, tables_to_remove, databases_to_remove
    
    def clean_empty_tables(self, tables_to_remove):
        """Eliminar tablas vacías o innecesarias"""
        print("\n🧹 FASE 2: ELIMINANDO TABLAS INNECESARIAS")
        print("="*50)
        
        removed_count = 0
        for table_full_name in tables_to_remove:
            try:
                # Intentar eliminar la tabla
                drop_query = f"DROP TABLE IF EXISTS {table_full_name}"
                result = self.run_clickhouse_query(drop_query)
                if result is not None:  # Sin errores
                    print(f"🗑️  Eliminada: {table_full_name}")
                    self.cleanup_log.append(f"ELIMINADA TABLA: {table_full_name}")
                    removed_count += 1
                else:
                    print(f"⚠️  No se pudo eliminar: {table_full_name}")
                    
            except Exception as e:
                print(f"❌ Error eliminando {table_full_name}: {e}")
        
        print(f"✅ Tablas eliminadas: {removed_count}")
        return removed_count
    
    def clean_empty_databases(self, databases_to_remove):
        """Eliminar bases de datos vacías"""
        print("\n🧹 FASE 3: ELIMINANDO BASES DE DATOS VACÍAS")
        print("="*50)
        
        removed_count = 0
        for db in databases_to_remove:
            try:
                # Verificar que esté realmente vacía
                tables_result = self.run_clickhouse_query(f"SHOW TABLES FROM {db}")
                if not tables_result:  # No hay tablas
                    drop_query = f"DROP DATABASE IF EXISTS {db}"
                    result = self.run_clickhouse_query(drop_query)
                    if result is not None:
                        print(f"🗑️  Base de datos eliminada: {db}")
                        self.cleanup_log.append(f"ELIMINADA BD: {db}")
                        removed_count += 1
                    else:
                        print(f"⚠️  No se pudo eliminar BD: {db}")
                else:
                    print(f"⚠️  BD {db} aún tiene tablas, saltando...")
                    
            except Exception as e:
                print(f"❌ Error eliminando BD {db}: {e}")
        
        print(f"✅ Bases de datos eliminadas: {removed_count}")
        return removed_count
    
    def configure_superset_datasets(self, useful_tables):
        """Configurar automáticamente datasets en Superset"""
        print("\n📊 FASE 4: CONFIGURANDO SUPERSET CON TABLAS ÚTILES")
        print("="*50)
        
        try:
            # Obtener token de autenticación de Superset
            login_data = {
                "username": "admin",
                "password": "admin",
                "provider": "db",
                "refresh": True
            }
            
            # Intentar hacer login
            login_response = requests.post(
                "http://localhost:8088/api/v1/security/login",
                json=login_data,
                timeout=10
            )
            
            if login_response.status_code == 200:
                access_token = login_response.json().get("access_token")
                headers = {"Authorization": f"Bearer {access_token}"}
                
                # Obtener bases de datos configuradas
                db_response = requests.get(
                    "http://localhost:8088/api/v1/database/",
                    headers=headers,
                    timeout=10
                )
                
                if db_response.status_code == 200:
                    databases = db_response.json().get("result", [])
                    clickhouse_db_id = None
                    
                    for db in databases:
                        if "clickhouse" in db.get("database_name", "").lower():
                            clickhouse_db_id = db["id"]
                            break
                    
                    if clickhouse_db_id:
                        # Configurar datasets para tablas útiles
                        configured_count = 0
                        for table in useful_tables:
                            if table['records'] >= 100:  # Solo tablas con datos significativos
                                dataset_data = {
                                    "database": clickhouse_db_id,
                                    "table_name": table['table'],
                                    "schema": table['database'],
                                    "sql": f"SELECT * FROM {table['full_name']}",
                                    "description": f"Dataset automático - {table['records']} registros"
                                }
                                
                                dataset_response = requests.post(
                                    "http://localhost:8088/api/v1/dataset/",
                                    json=dataset_data,
                                    headers=headers,
                                    timeout=10
                                )
                                
                                if dataset_response.status_code in [200, 201]:
                                    print(f"✅ Dataset configurado: {table['full_name']}")
                                    configured_count += 1
                                elif dataset_response.status_code == 422:
                                    print(f"ℹ️  Dataset ya existe: {table['full_name']}")
                                else:
                                    print(f"⚠️  Error configurando: {table['full_name']}")
                        
                        print(f"✅ Datasets configurados en Superset: {configured_count}")
                        return configured_count
                    else:
                        print("❌ No se encontró base de datos ClickHouse en Superset")
                else:
                    print(f"❌ Error obteniendo bases de datos: {db_response.status_code}")
            else:
                print(f"❌ Error de login en Superset: {login_response.status_code}")
                
        except Exception as e:
            print(f"⚠️  Superset no disponible o error: {e}")
            print("ℹ️  Configuración manual requerida")
        
        return 0
    
    def create_metabase_guide(self, useful_tables):
        """Crear guía automática para Metabase"""
        print("\n📈 FASE 5: GENERANDO GUÍA PARA METABASE")
        print("="*50)
        
        # Agrupar por base de datos
        by_database = defaultdict(list)
        for table in useful_tables:
            if table['records'] >= 10:  # Solo tablas con datos
                by_database[table['database']].append(table)
        
        guide_content = "# GUÍA AUTOMÁTICA METABASE - TABLAS DEPURADAS\n\n"
        guide_content += f"Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        
        for db, tables in by_database.items():
            if tables:
                total_records = sum(t['records'] for t in tables)
                guide_content += f"## Base de datos: {db}\n"
                guide_content += f"- Total registros: {total_records:,}\n"
                guide_content += f"- Tablas disponibles: {len(tables)}\n\n"
                
                for table in sorted(tables, key=lambda x: x['records'], reverse=True):
                    guide_content += f"### {table['table']}\n"
                    guide_content += f"- Registros: {table['records']:,}\n"
                    guide_content += f"- Ruta: {table['full_name']}\n\n"
        
        # Guardar guía
        with open('/mnt/c/proyectos/etl_prod/logs/metabase_clean_guide.md', 'w') as f:
            f.write(guide_content)
        
        print("✅ Guía para Metabase generada: logs/metabase_clean_guide.md")
        return len(by_database)
    
    def generate_cleanup_report(self, useful_tables, removed_tables, removed_dbs):
        """Generar reporte de limpieza"""
        print("\n📋 FASE 6: GENERANDO REPORTE DE DEPURACIÓN")
        print("="*50)
        
        report = {
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "useful_tables_kept": len(useful_tables),
                "tables_removed": removed_tables,
                "databases_removed": removed_dbs,
                "total_records_kept": sum(t['records'] for t in useful_tables)
            },
            "useful_tables": useful_tables,
            "cleanup_actions": self.cleanup_log
        }
        
        # Guardar reporte JSON
        with open('/mnt/c/proyectos/etl_prod/logs/schema_cleanup_report.json', 'w') as f:
            json.dump(report, f, indent=2)
        
        print("✅ Reporte guardado: logs/schema_cleanup_report.json")
        
        # Mostrar resumen
        print(f"\n🎯 RESUMEN DE DEPURACIÓN:")
        print(f"   ✅ Tablas útiles mantenidas: {len(useful_tables)}")
        print(f"   🗑️  Tablas eliminadas: {removed_tables}")
        print(f"   🗑️  Bases de datos eliminadas: {removed_dbs}")
        print(f"   💾 Total registros conservados: {report['summary']['total_records_kept']:,}")
        
        return report
    
    def run_complete_cleanup(self):
        """Ejecutar proceso completo de depuración"""
        print("🧹 INICIANDO DEPURACIÓN AUTOMÁTICA DE ESQUEMAS")
        print("="*60)
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # Fase 1: Identificar qué limpiar
            useful_tables, tables_to_remove, databases_to_remove = self.identify_schemas_to_clean()
            
            if not useful_tables:
                print("❌ No se encontraron tablas útiles. Abortando limpieza.")
                return False
            
            # Fase 2: Limpiar tablas innecesarias
            removed_tables = self.clean_empty_tables(tables_to_remove)
            
            # Fase 3: Limpiar bases de datos vacías
            removed_dbs = self.clean_empty_databases(databases_to_remove)
            
            # Fase 4: Configurar Superset
            configured_datasets = self.configure_superset_datasets(useful_tables)
            
            # Fase 5: Guía para Metabase
            metabase_dbs = self.create_metabase_guide(useful_tables)
            
            # Fase 6: Reporte final
            report = self.generate_cleanup_report(useful_tables, removed_tables, removed_dbs)
            
            print(f"\n🎉 DEPURACIÓN COMPLETADA EXITOSAMENTE")
            print(f"✅ Sistema limpio y configurado para análisis")
            print(f"📊 Acceso: Superset (http://localhost:8088) | Metabase (http://localhost:3000)")
            
            return True
            
        except Exception as e:
            print(f"❌ Error durante la depuración: {e}")
            return False

def main():
    """Función principal"""
    cleaner = SchemaCleaner()
    success = cleaner.run_complete_cleanup()
    
    if success:
        print("\n🚀 DEPURACIÓN INTEGRADA AL PIPELINE COMPLETADA")
        sys.exit(0)
    else:
        print("\n❌ FALLÓ LA DEPURACIÓN")
        sys.exit(1)

if __name__ == "__main__":
    main()