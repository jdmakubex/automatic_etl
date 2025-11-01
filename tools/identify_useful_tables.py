#!/usr/bin/env python3
"""
Script para identificar exactamente qué tablas usar en Superset y Metabase
Filtra solo las tablas con datos reales y proporciona guía específica
"""

import subprocess
import json
import sys
from collections import defaultdict

class UsefulTablesIdentifier:
    def __init__(self):
        self.clickhouse_user = "default"
        self.clickhouse_password = "ClickHouse123!"
        self.useful_tables = []
        
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
                return None
        except Exception as e:
            return None
    
    def identify_useful_tables(self):
        """Identificar solo las tablas con datos útiles"""
        print("🔍 IDENTIFICANDO TABLAS ÚTILES CON DATOS REALES")
        print("="*60)
        
        # Obtener todas las bases de datos
        databases_result = self.run_clickhouse_query("SHOW DATABASES")
        if not databases_result:
            print("❌ No se pueden obtener las bases de datos")
            return
        
        databases = [db for db in databases_result.split('\n') 
                    if db and not db.startswith('INFORMATION_SCHEMA') 
                    and db not in ['information_schema', 'system', 'ext']]
        
        useful_by_category = {
            'principales': [],
            'analytics': [],
            'raw_data': []
        }
        
        for db in databases:
            # Obtener tablas
            tables_result = self.run_clickhouse_query(f"SHOW TABLES FROM {db}")
            if not tables_result:
                continue
                
            tables = [t for t in tables_result.split('\n') if t]
            
            for table in tables:
                # Contar registros
                count_result = self.run_clickhouse_query(f"SELECT COUNT(*) FROM {db}.{table}")
                if count_result and int(count_result) > 0:
                    count = int(count_result)
                    
                    table_info = {
                        'database': db,
                        'table': table,
                        'records': count,
                        'full_name': f"{db}.{table}"
                    }
                    
                    # Categorizar
                    if db.endswith('_analytics') and table.endswith('_v'):
                        useful_by_category['analytics'].append(table_info)
                    elif table.endswith('_raw'):
                        useful_by_category['raw_data'].append(table_info)
                    elif table.startswith('src__'):
                        useful_by_category['principales'].append(table_info)
                    else:
                        useful_by_category['principales'].append(table_info)
        
        return useful_by_category
    
    def create_superset_guide(self, useful_tables):
        """Crear guía específica para Superset"""
        print("\n🎯 GUÍA ESPECÍFICA PARA SUPERSET")
        print("="*60)
        
        print("📊 BASES DE DATOS Y TABLAS RECOMENDADAS:")
        print("-" * 40)
        
        # Principales tablas con datos
        if useful_tables['principales']:
            print("\n✅ TABLAS PRINCIPALES (usar estas en Superset):")
            principales_sorted = sorted(useful_tables['principales'], 
                                      key=lambda x: x['records'], reverse=True)
            
            for i, table in enumerate(principales_sorted[:10], 1):  # Top 10
                records_k = table['records'] // 1000 if table['records'] > 1000 else table['records']
                unit = "K" if table['records'] > 1000 else ""
                
                print(f"   {i:2d}. 📋 {table['database']}")
                print(f"       Tabla: {table['table']}")
                print(f"       Registros: {records_k}{unit}")
                print(f"       Uso en Superset: Datasets → {table['full_name']}")
                print()
        
        # Vistas analytics si existen
        if useful_tables['analytics']:
            print("📈 VISTAS ANALYTICS (para análisis avanzados):")
            for table in useful_tables['analytics'][:5]:
                records_k = table['records'] // 1000 if table['records'] > 1000 else table['records']
                unit = "K" if table['records'] > 1000 else ""
                print(f"   • {table['database']}.{table['table']} ({records_k}{unit} registros)")
    
    def create_metabase_guide(self, useful_tables):
        """Crear guía específica para Metabase"""
        print("\n🎯 GUÍA ESPECÍFICA PARA METABASE")
        print("="*60)
        
        # Agrupar por base de datos
        by_database = defaultdict(list)
        for category in useful_tables.values():
            for table in category:
                by_database[table['database']].append(table)
        
        print("📁 NAVEGACIÓN RECOMENDADA EN METABASE:")
        print("-" * 40)
        
        for db, tables in by_database.items():
            if not tables:
                continue
                
            total_records = sum(t['records'] for t in tables)
            records_display = f"{total_records//1000}K" if total_records > 1000 else str(total_records)
            
            print(f"\n📂 Base de datos: {db}")
            print(f"   Total registros: {records_display}")
            print(f"   Tablas útiles: {len(tables)}")
            
            # Mostrar top 3 tablas por base de datos
            top_tables = sorted(tables, key=lambda x: x['records'], reverse=True)[:3]
            for table in top_tables:
                records_k = table['records'] // 1000 if table['records'] > 1000 else table['records']
                unit = "K" if table['records'] > 1000 else ""
                print(f"     • {table['table']}: {records_k}{unit} registros")
    
    def create_connection_instructions(self):
        """Crear instrucciones específicas de conexión"""
        print("\n🔗 INSTRUCCIONES DE CONEXIÓN ESPECÍFICAS")
        print("="*60)
        
        print("""
🎯 SUPERSET - Pasos exactos:
   1. Ve a: http://localhost:8088
   2. Login: admin / admin
   3. Menú: Settings → Database Connections
   4. Verifica que ClickHouse esté conectado
   5. Ve a: Data → Datasets
   6. Busca las tablas que empiecen con 'src__'
   7. IGNORA las tablas con 0 registros

🎯 METABASE - Pasos exactos:
   1. Ve a: http://localhost:3000
   2. Menú: Browse Data
   3. Selecciona: ClickHouse database
   4. Navega SOLO a estas bases de datos:
      • archivos (34K registros)
      • fiscalizacion (726K registros) 
      • fiscalizacion_analytics (726K registros - vistas)
   5. IGNORA: fgeo_analytics (pocos datos), ext (tablas técnicas)

🚨 IMPORTANTE:
   • NO uses tablas con sufijo '_raw' que tengan pocos registros
   • SÍ usa tablas que empiecen con 'src__'
   • Las tablas con '_v' son vistas (buenas para análisis)
        """)
    
    def create_sample_queries(self, useful_tables):
        """Crear consultas de ejemplo para verificar datos"""
        print("\n📝 CONSULTAS DE PRUEBA RECOMENDADAS")
        print("="*60)
        
        if useful_tables['principales']:
            top_table = max(useful_tables['principales'], key=lambda x: x['records'])
            
            print(f"🔍 Para probar en SQL Lab (Superset) o Query (Metabase):")
            print(f"```sql")
            print(f"-- Contar todos los registros")
            print(f"SELECT COUNT(*) as total_registros")
            print(f"FROM {top_table['full_name']};")
            print()
            print(f"-- Ver muestra de datos")
            print(f"SELECT *")
            print(f"FROM {top_table['full_name']}")
            print(f"LIMIT 10;")
            print(f"```")
            
            # Obtener columnas de la tabla principal
            columns_result = self.run_clickhouse_query(f"DESCRIBE {top_table['full_name']}")
            if columns_result:
                columns = [line.split('\t')[0] for line in columns_result.split('\n')[:5]]
                print(f"\n📋 Columnas disponibles en {top_table['table']}:")
                for col in columns:
                    print(f"   • {col}")
    
    def run_analysis(self):
        """Ejecutar análisis completo"""
        print("🚀 IDENTIFICACIÓN DE TABLAS ÚTILES PARA SUPERSET Y METABASE")
        print("="*70)
        
        # 1. Identificar tablas útiles
        useful_tables = self.identify_useful_tables()
        
        if not any(useful_tables.values()):
            print("❌ No se encontraron tablas con datos")
            return
        
        # 2. Crear guías específicas
        self.create_superset_guide(useful_tables)
        self.create_metabase_guide(useful_tables)
        self.create_connection_instructions()
        self.create_sample_queries(useful_tables)
        
        # 3. Resumen final
        total_useful = sum(len(tables) for tables in useful_tables.values())
        total_records = sum(table['records'] for category in useful_tables.values() 
                           for table in category)
        
        print(f"\n🎉 RESUMEN FINAL:")
        print(f"✅ Tablas útiles encontradas: {total_useful}")
        print(f"✅ Total registros disponibles: {total_records:,}")
        print(f"✅ Usa las tablas identificadas arriba - ¡ignora las vacías!")

def main():
    identifier = UsefulTablesIdentifier()
    identifier.run_analysis()

if __name__ == "__main__":
    main()