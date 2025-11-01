#!/usr/bin/env python3
"""
Script para verificar el estado de la ingesta de datos y explicar las diferencias
entre los esquemas src y __raw en Superset y Metabase
"""

import subprocess
import json
import sys
import re
from datetime import datetime

class DataIngestionChecker:
    def __init__(self):
        self.clickhouse_user = "default"
        self.clickhouse_password = "ClickHouse123!"
        
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
    
    def get_database_summary(self):
        """Obtener resumen de todas las bases de datos y tablas"""
        print("🔍 ANÁLISIS COMPLETO DE DATOS EN CLICKHOUSE")
        print("="*60)
        
        # Obtener todas las bases de datos
        databases_result = self.run_clickhouse_query("SHOW DATABASES")
        if not databases_result:
            return
        
        databases = [db for db in databases_result.split('\n') 
                    if db and not db.startswith('INFORMATION_SCHEMA') 
                    and db not in ['information_schema', 'system']]
        
        total_records = 0
        
        for db in databases:
            print(f"\n📊 BASE DE DATOS: {db}")
            print("-" * 40)
            
            # Obtener tablas
            tables_result = self.run_clickhouse_query(f"SHOW TABLES FROM {db}")
            if not tables_result:
                print("   📭 No hay tablas")
                continue
                
            tables = [t for t in tables_result.split('\n') if t]
            
            if not tables:
                print("   📭 No hay tablas")
                continue
            
            db_total = 0
            for table in tables:
                # Contar registros
                count_result = self.run_clickhouse_query(f"SELECT COUNT(*) FROM {db}.{table}")
                if count_result:
                    count = int(count_result)
                    db_total += count
                    total_records += count
                    
                    # Indicador visual del contenido
                    if count > 0:
                        status = "✅" if count > 1000 else "⚠️" if count > 0 else "❌"
                        print(f"   {status} {table}: {count:,} registros")
                    else:
                        print(f"   📭 {table}: Sin datos")
                else:
                    print(f"   ❓ {table}: Error al contar")
            
            print(f"   💾 Total BD '{db}': {db_total:,} registros")
        
        print(f"\n🎯 TOTAL GENERAL: {total_records:,} registros")
        return total_records
    
    def explain_schema_differences(self):
        """Explicar las diferencias entre esquemas src y __raw"""
        print("\n" + "="*60)
        print("📚 EXPLICACIÓN DE ESQUEMAS Y PREFIJOS")
        print("="*60)
        
        print("""
🔹 PREFIJO 'src__':
   • Indica tablas de origen (source) creadas por dbt o herramientas ETL
   • Estructura: src__<database>__<schema>__<table>
   • Son las tablas RAW importadas desde las fuentes originales
   • Ejemplo: src__archivos__archivos__archivos
            ↳ base: archivos, esquema: archivos, tabla: archivos

🔹 SUFIJO '__raw' o sin prefijo:
   • Tablas sin transformaciones adicionales
   • Datos tal como vienen de la fuente original
   • Pueden aparecer en diferentes formatos según la herramienta

🔹 SUFIJO '_v':
   • Vistas (views) creadas sobre las tablas principales
   • Pueden incluir transformaciones o filtros
   • Ejemplo: src__fiscalizacion__fiscalizacion__altoimpacto_v

🔹 BASES DE DATOS '_analytics':
   • Contienen vistas y datos procesados
   • Están optimizadas para análisis y reporting
   • Pueden incluir agregaciones y métricas calculadas
        """)
    
    def check_sample_data(self):
        """Verificar datos de muestra de algunas tablas principales"""
        print("\n" + "="*60)
        print("🔎 MUESTRA DE DATOS DISPONIBLES")
        print("="*60)
        
        # Tablas principales con datos
        sample_tables = [
            ("archivos", "src__archivos__archivos__archivos"),
            ("fiscalizacion", "src__fiscalizacion__fiscalizacion__altoimpacto"),
        ]
        
        for db, table in sample_tables:
            print(f"\n📋 Muestra de {db}.{table}:")
            print("-" * 50)
            
            # Obtener estructura de la tabla
            describe_result = self.run_clickhouse_query(f"DESCRIBE {db}.{table}")
            if describe_result:
                lines = describe_result.split('\n')[:5]  # Primeras 5 columnas
                print("   Columnas principales:")
                for line in lines:
                    if line:
                        parts = line.split('\t')
                        if len(parts) >= 2:
                            print(f"     • {parts[0]}: {parts[1]}")
            
            # Obtener muestra de datos
            sample_result = self.run_clickhouse_query(
                f"SELECT * FROM {db}.{table} LIMIT 2",
            )
            if sample_result:
                print(f"   Primeros 2 registros disponibles ✅")
            else:
                print(f"   No se pudieron obtener datos de muestra ❌")
    
    def check_superset_metabase_connection(self):
        """Verificar si Superset y Metabase pueden conectar correctamente"""
        print("\n" + "="*60)
        print("🔗 VERIFICACIÓN DE CONEXIONES")
        print("="*60)
        
        # Verificar servicios
        try:
            # Superset
            superset_result = subprocess.run([
                'curl', '-s', '-o', '/dev/null', '-w', '%{http_code}', 
                'http://localhost:8088/health'
            ], capture_output=True, text=True, timeout=10)
            
            if superset_result.returncode == 0 and superset_result.stdout == '200':
                print("✅ Superset: Disponible en http://localhost:8088")
            else:
                print("❌ Superset: No disponible")
                
            # Metabase
            metabase_result = subprocess.run([
                'curl', '-s', '-o', '/dev/null', '-w', '%{http_code}', 
                'http://localhost:3000/api/health'
            ], capture_output=True, text=True, timeout=10)
            
            if metabase_result.returncode == 0 and metabase_result.stdout == '200':
                print("✅ Metabase: Disponible en http://localhost:3000")
            else:
                print("❌ Metabase: No disponible")
                
            # ClickHouse
            clickhouse_test = self.run_clickhouse_query("SELECT 1")
            if clickhouse_test:
                print("✅ ClickHouse: Conectado correctamente")
            else:
                print("❌ ClickHouse: Error de conexión")
                
        except Exception as e:
            print(f"❌ Error verificando conexiones: {e}")
    
    def provide_recommendations(self):
        """Proporcionar recomendaciones para acceder a los datos"""
        print("\n" + "="*60)
        print("💡 RECOMENDACIONES PARA VER LOS DATOS")
        print("="*60)
        
        print("""
🎯 Para SUPERSET:
   1. Ve a http://localhost:8088
   2. Login: admin / admin
   3. Ve a "Datasets" en el menú superior
   4. Busca las tablas que empiecen con 'src__'
   5. Haz clic en el lápiz (editar) de cualquier dataset
   6. Ve a la pestaña "Columns" para ver la estructura
   7. Crea un gráfico usando "Create Chart"

🎯 Para METABASE:
   1. Ve a http://localhost:3000
   2. Login con las credenciales configuradas
   3. Ve a "Browse Data" o "Examinar datos"
   4. Selecciona la base de datos ClickHouse
   5. Navega por las bases de datos y tablas disponibles

🎯 Bases de datos principales a revisar:
   • 'archivos': Contiene datos de archivos del sistema
   • 'fiscalizacion': Contiene datos de fiscalización
   • 'archivos_analytics' y 'fiscalizacion_analytics': Vistas procesadas

🎯 Para consultas directas en ClickHouse:
   docker exec clickhouse clickhouse-client --user default --password ClickHouse123!
        """)
    
    def run_complete_check(self):
        """Ejecutar verificación completa"""
        print("🚀 VERIFICACIÓN COMPLETA DE INGESTA DE DATOS")
        print("=" * 60)
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 1. Resumen de bases de datos
        total_records = self.get_database_summary()
        
        # 2. Explicar diferencias
        self.explain_schema_differences()
        
        # 3. Muestra de datos
        self.check_sample_data()
        
        # 4. Verificar conexiones
        self.check_superset_metabase_connection()
        
        # 5. Recomendaciones
        self.provide_recommendations()
        
        # Conclusión
        print(f"\n🎉 CONCLUSIÓN:")
        if total_records > 0:
            print(f"✅ La ingesta de datos SE COMPLETÓ EXITOSAMENTE")
            print(f"✅ Total de registros disponibles: {total_records:,}")
            print(f"✅ Los datos están listos para análisis en Superset y Metabase")
        else:
            print(f"❌ No se encontraron datos. La ingesta podría no haberse completado.")
        
        print(f"\n📊 Acceso directo a las herramientas:")
        print(f"   🔗 Superset: http://localhost:8088 (admin/admin)")
        print(f"   🔗 Metabase: http://localhost:3000")
        print(f"   🔗 ClickHouse: http://localhost:8123")

def main():
    checker = DataIngestionChecker()
    checker.run_complete_check()

if __name__ == "__main__":
    main()