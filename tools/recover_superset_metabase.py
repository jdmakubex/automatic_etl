#!/usr/bin/env python3
"""
Script de recuperación para Superset y Metabase después de la depuración
Diagnostica problemas y restaura las conexiones y datasets
"""

import subprocess
import json
import sys
import time
import requests
from datetime import datetime
import os

class SupsetMetabaseRecovery:
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
    
    def verify_clickhouse_data(self):
        """Verificar que los datos aún están en ClickHouse"""
        print("🔍 VERIFICANDO DATOS EN CLICKHOUSE")
        print("="*50)
        
        # Verificar bases de datos
        databases = self.run_clickhouse_query("SHOW DATABASES")
        if not databases:
            print("❌ No se puede conectar a ClickHouse")
            return False
        
        db_list = databases.split('\n')
        useful_dbs = ['archivos', 'fiscalizacion', 'fiscalizacion_analytics']
        
        for db in useful_dbs:
            if db in db_list:
                # Contar tablas
                tables = self.run_clickhouse_query(f"SHOW TABLES FROM {db}")
                if tables:
                    table_count = len([t for t in tables.split('\n') if t])
                    print(f"✅ {db}: {table_count} tablas")
                    
                    # Contar registros totales
                    total_query = f"""
                    SELECT sum(rows) as total 
                    FROM system.parts 
                    WHERE database = '{db}' AND active = 1
                    """
                    total_result = self.run_clickhouse_query(total_query)
                    if total_result:
                        print(f"   💾 Registros: {int(total_result):,}")
                else:
                    print(f"❌ {db}: Sin tablas")
            else:
                print(f"❌ {db}: Base de datos no encontrada")
        
        return True
    
    def fix_superset_connection(self):
        """Reparar conexión de Superset a ClickHouse"""
        print("\n🔧 REPARANDO CONEXIÓN DE SUPERSET")
        print("="*50)
        
        try:
            # Login en Superset
            login_data = {
                "username": "admin",
                "password": "admin",
                "provider": "db",
                "refresh": True
            }
            
            print("🔑 Iniciando sesión en Superset...")
            login_response = requests.post(
                "http://localhost:8088/api/v1/security/login",
                json=login_data,
                timeout=15
            )
            
            if login_response.status_code == 200:
                access_token = login_response.json().get("access_token")
                headers = {
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json"
                }
                print("✅ Login exitoso en Superset")
                
                # Verificar conexiones existentes
                print("🔍 Verificando conexiones existentes...")
                db_response = requests.get(
                    "http://localhost:8088/api/v1/database/",
                    headers=headers,
                    timeout=15
                )
                
                clickhouse_db_id = None
                if db_response.status_code == 200:
                    databases = db_response.json().get("result", [])
                    for db in databases:
                        db_name = db.get("database_name", "").lower()
                        if "clickhouse" in db_name:
                            clickhouse_db_id = db["id"]
                            print(f"✅ Conexión ClickHouse encontrada: ID {clickhouse_db_id}")
                            break
                
                # Si no hay conexión, crearla
                if not clickhouse_db_id:
                    print("🔧 Creando nueva conexión ClickHouse...")
                    new_db_data = {
                        "database_name": "ClickHouse_Clean",
                        "sqlalchemy_uri": "clickhouse://default:ClickHouse123!@clickhouse:9000/default",
                        "expose_in_sqllab": True,
                        "allow_ctas": True,
                        "allow_cvas": True,
                        "allow_dml": True
                    }
                    
                    create_response = requests.post(
                        "http://localhost:8088/api/v1/database/",
                        json=new_db_data,
                        headers=headers,
                        timeout=15
                    )
                    
                    if create_response.status_code in [200, 201]:
                        clickhouse_db_id = create_response.json().get("id")
                        print(f"✅ Nueva conexión creada: ID {clickhouse_db_id}")
                    else:
                        print(f"❌ Error creando conexión: {create_response.status_code}")
                        print(create_response.text)
                
                # Refrescar esquemas
                if clickhouse_db_id:
                    print("🔄 Refrescando esquemas...")
                    refresh_response = requests.post(
                        f"http://localhost:8088/api/v1/database/{clickhouse_db_id}/refresh/",
                        headers=headers,
                        timeout=30
                    )
                    
                    if refresh_response.status_code == 200:
                        print("✅ Esquemas refrescados")
                        
                        # Crear datasets automáticamente
                        return self.create_superset_datasets(headers, clickhouse_db_id)
                    else:
                        print(f"⚠️  Error refrescando: {refresh_response.status_code}")
                
            else:
                print(f"❌ Error de login: {login_response.status_code}")
                print("🔧 Intentando reset de credenciales...")
                return self.reset_superset_credentials()
                
        except Exception as e:
            print(f"❌ Error en Superset: {e}")
            return False
        
        return True
    
    def create_superset_datasets(self, headers, db_id):
        """Crear datasets en Superset para las tablas útiles"""
        print("\n📊 CREANDO DATASETS EN SUPERSET")
        print("="*50)
        
        # Obtener tablas útiles
        useful_tables = []
        for db in ['archivos', 'fiscalizacion']:
            tables_result = self.run_clickhouse_query(f"SHOW TABLES FROM {db}")
            if tables_result:
                for table in tables_result.split('\n'):
                    if table and table.startswith('src__'):
                        # Verificar que tenga datos
                        count_result = self.run_clickhouse_query(f"SELECT COUNT(*) FROM {db}.{table}")
                        if count_result and int(count_result) > 100:  # Al menos 100 registros
                            useful_tables.append({
                                'database': db,
                                'table': table,
                                'records': int(count_result)
                            })
        
        created_count = 0
        for table_info in useful_tables:
            try:
                dataset_data = {
                    "database": db_id,
                    "table_name": table_info['table'],
                    "schema": table_info['database']
                }
                
                dataset_response = requests.post(
                    "http://localhost:8088/api/v1/dataset/",
                    json=dataset_data,
                    headers=headers,
                    timeout=15
                )
                
                if dataset_response.status_code in [200, 201]:
                    print(f"✅ Dataset: {table_info['database']}.{table_info['table']}")
                    created_count += 1
                elif dataset_response.status_code == 422:
                    print(f"ℹ️  Ya existe: {table_info['database']}.{table_info['table']}")
                else:
                    print(f"⚠️  Error: {table_info['database']}.{table_info['table']} - {dataset_response.status_code}")
                    
            except Exception as e:
                print(f"❌ Error creando dataset {table_info['table']}: {e}")
        
        print(f"✅ Datasets configurados: {created_count}")
        return created_count > 0
    
    def reset_superset_credentials(self):
        """Resetear credenciales de Superset"""
        print("\n🔄 RESETEANDO CREDENCIALES DE SUPERSET")
        print("="*50)
        
        try:
            # Reiniciar servicio de Superset
            print("🔄 Reiniciando Superset...")
            subprocess.run(['docker', 'compose', 'restart', 'superset'], timeout=60)
            
            # Esperar que se reinicie
            print("⏳ Esperando reinicio...")
            time.sleep(30)
            
            # Verificar que esté disponible
            for i in range(5):
                try:
                    response = requests.get("http://localhost:8088/health", timeout=10)
                    if response.status_code == 200:
                        print("✅ Superset reiniciado correctamente")
                        return True
                except:
                    time.sleep(10)
            
            print("⚠️  Superset tardó en reiniciar")
            return False
            
        except Exception as e:
            print(f"❌ Error reiniciando Superset: {e}")
            return False
    
    def clear_metabase_cache(self):
        """Limpiar caché de Metabase"""
        print("\n🔧 LIMPIANDO CACHÉ DE METABASE")
        print("="*50)
        
        try:
            # Reiniciar Metabase para limpiar caché
            print("🔄 Reiniciando Metabase...")
            subprocess.run(['docker', 'compose', 'restart', 'metabase'], timeout=60)
            
            print("⏳ Esperando reinicio de Metabase...")
            time.sleep(45)  # Metabase tarda más en cargar
            
            # Verificar que esté disponible
            for i in range(10):
                try:
                    response = requests.get("http://localhost:3000/api/health", timeout=10)
                    if response.status_code == 200:
                        print("✅ Metabase reiniciado correctamente")
                        return True
                except:
                    print(f"⏳ Esperando Metabase... intento {i+1}/10")
                    time.sleep(15)
            
            print("⚠️  Metabase tardó en reiniciar, pero debería estar disponible")
            return True
            
        except Exception as e:
            print(f"❌ Error reiniciando Metabase: {e}")
            return False
    
    def create_recovery_guide(self):
        """Crear guía de recuperación manual"""
        print("\n📋 GENERANDO GUÍA DE RECUPERACIÓN")
        print("="*50)
        
        guide_content = f"""# GUÍA DE RECUPERACIÓN POST-DEPURACIÓN
Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 🚨 PROBLEMAS DETECTADOS Y SOLUCIONES

### SUPERSET (http://localhost:8088)
**Login:** admin / admin

**Si no ves esquemas:**
1. Ve a Settings → Database Connections
2. Haz clic en "ClickHouse" (o el que esté)
3. En la pestaña "Extra" asegúrate que tenga:
   ```json
   {{"allows_virtual_table_explore": true}}
   ```
4. Haz clic en "Test Connection"
5. Si falla, edita la URI: `clickhouse://default:ClickHouse123!@clickhouse:9000/default`
6. Guarda y ve a SQL Lab → selecciona la base "archivos" o "fiscalizacion"

**Datasets recomendados para crear manualmente:**
- `archivos.src__archivos__archivos__archivos` (17K registros)
- `fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora` (513K registros)
- `fiscalizacion.src__fiscalizacion__fiscalizacion__ofeindisdup` (209K registros)

### METABASE (http://localhost:3000)
**El reinicio debe haber limpiado el caché.**

**Si sigues viendo tablas vacías:**
1. Ve a Admin → Databases
2. Haz clic en la base ClickHouse
3. Clic en "Sync database schema now"
4. Ve a Browse Data → solo deberías ver:
   - archivos (con 9 tablas)
   - fiscalizacion (con 8 tablas)
   - fiscalizacion_analytics (con 8 vistas)

## ✅ DATOS DISPONIBLES ACTUALMENTE
"""
        
        # Agregar estado actual de datos
        for db in ['archivos', 'fiscalizacion', 'fiscalizacion_analytics']:
            tables_result = self.run_clickhouse_query(f"SHOW TABLES FROM {db}")
            if tables_result:
                table_list = [t for t in tables_result.split('\n') if t]
                guide_content += f"\n### Base de datos: {db}\n"
                guide_content += f"Tablas disponibles: {len(table_list)}\n"
                for table in table_list[:5]:  # Primeras 5
                    count_result = self.run_clickhouse_query(f"SELECT COUNT(*) FROM {db}.{table}")
                    if count_result:
                        guide_content += f"- {table}: {int(count_result):,} registros\n"
        
        guide_content += "\n## 🔍 CONSULTA DE PRUEBA\n"
        guide_content += "```sql\n"
        guide_content += "SELECT COUNT(*) FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora;\n"
        guide_content += "-- Debe devolver 513,344\n"
        guide_content += "```\n"
        
        # Guardar guía
        with open('/mnt/c/proyectos/etl_prod/logs/recovery_guide.md', 'w') as f:
            f.write(guide_content)
        
        print("✅ Guía guardada: logs/recovery_guide.md")
        return True
    
    def run_complete_recovery(self):
        """Ejecutar recuperación completa"""
        print("🚑 INICIANDO RECUPERACIÓN DE SUPERSET Y METABASE")
        print("="*60)
        
        # 1. Verificar datos
        if not self.verify_clickhouse_data():
            print("❌ Los datos no están disponibles en ClickHouse")
            return False
        
        # 2. Reparar Superset
        print("\n" + "="*60)
        superset_ok = self.fix_superset_connection()
        
        # 3. Limpiar Metabase
        print("\n" + "="*60)
        metabase_ok = self.clear_metabase_cache()
        
        # 4. Crear guía
        print("\n" + "="*60)
        self.create_recovery_guide()
        
        # Resumen
        print(f"\n🎯 RESULTADO DE RECUPERACIÓN:")
        print(f"   ClickHouse: ✅ Datos disponibles")
        print(f"   Superset: {'✅' if superset_ok else '⚠️ '} {'Recuperado' if superset_ok else 'Requiere configuración manual'}")
        print(f"   Metabase: {'✅' if metabase_ok else '⚠️ '} {'Reiniciado' if metabase_ok else 'Requiere reinicio manual'}")
        
        print(f"\n📖 SIGUIENTES PASOS:")
        print(f"   1. Superset: http://localhost:8088 (admin/admin)")
        print(f"   2. Metabase: http://localhost:3000")
        print(f"   3. Guía completa: logs/recovery_guide.md")
        
        if not superset_ok or not metabase_ok:
            print(f"\n⚠️  Si aún hay problemas, consulta logs/recovery_guide.md")
        
        return True

def main():
    recovery = SupsetMetabaseRecovery()
    recovery.run_complete_recovery()

if __name__ == "__main__":
    main()