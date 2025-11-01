#!/usr/bin/env python3
"""
Script para verificar y configurar correctamente ClickHouse en Metabase
"""

import requests
import json
import time

def login_to_metabase():
    """Login a Metabase con las credenciales configuradas"""
    metabase_url = "http://localhost:3000"
    
    credentials = {
        "username": "admin@etl.local",
        "password": "AdminETL2024!"
    }
    
    try:
        response = requests.post(
            f"{metabase_url}/api/session",
            json=credentials,
            timeout=15
        )
        
        if response.status_code == 200:
            session_id = response.json().get("id")
            print(f"✅ Login exitoso")
            return session_id
        else:
            print(f"❌ Error login: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"❌ Error login: {e}")
        return None

def get_databases(session_id):
    """Obtener todas las bases de datos configuradas"""
    metabase_url = "http://localhost:3000"
    headers = {"X-Metabase-Session": session_id}
    
    try:
        response = requests.get(
            f"{metabase_url}/api/database",
            headers=headers,
            timeout=15
        )
        
        if response.status_code == 200:
            databases = response.json().get("data", [])
            print(f"📊 Bases de datos encontradas: {len(databases)}")
            
            for db in databases:
                print(f"   - {db['name']} (ID: {db['id']}, Engine: {db.get('engine', 'N/A')})")
            
            return databases
        else:
            print(f"❌ Error obteniendo bases de datos: {response.status_code}")
            return []
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return []

def create_clickhouse_database(session_id):
    """Crear nueva base de datos ClickHouse"""
    metabase_url = "http://localhost:3000"
    headers = {"X-Metabase-Session": session_id}
    
    print("🔧 Creando nueva base de datos ClickHouse...")
    
    db_config = {
        "name": "ClickHouse ETL",
        "engine": "clickhouse",
        "details": {
            "host": "clickhouse",
            "port": 8123,
            "dbname": "default",
            "user": "default",
            "password": "ClickHouse123!",
            "ssl": False,
            "additional-options": "socket_timeout=300000&connection_timeout=30000&max_execution_time=300"
        },
        "auto_run_queries": True,
        "is_full_sync": True,
        "cache_ttl": None
    }
    
    try:
        response = requests.post(
            f"{metabase_url}/api/database",
            json=db_config,
            headers=headers,
            timeout=60
        )
        
        if response.status_code == 200:
            db_data = response.json()
            db_id = db_data.get("id")
            print(f"✅ Base de datos ClickHouse creada (ID: {db_id})")
            return db_id
        else:
            print(f"❌ Error creando base de datos: {response.status_code}")
            print(f"Response: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def sync_database_schema(session_id, db_id):
    """Sincronizar esquema de base de datos"""
    metabase_url = "http://localhost:3000"
    headers = {"X-Metabase-Session": session_id}
    
    print("🔄 Sincronizando esquema de base de datos...")
    
    try:
        response = requests.post(
            f"{metabase_url}/api/database/{db_id}/sync_schema",
            headers=headers,
            timeout=120
        )
        
        if response.status_code in [200, 202]:
            print("✅ Sincronización iniciada")
            
            # Esperar sincronización
            print("⏳ Esperando sincronización...")
            time.sleep(30)
            
            return True
        else:
            print(f"❌ Error sincronizando: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_clickhouse_query(session_id, db_id):
    """Probar consultas ClickHouse"""
    metabase_url = "http://localhost:3000"
    headers = {"X-Metabase-Session": session_id}
    
    print("🧪 Probando consultas ClickHouse...")
    
    test_queries = [
        "SELECT 1 as test_connection",
        "SHOW DATABASES",
        "SELECT COUNT(*) as total_records FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora"
    ]
    
    for i, query in enumerate(test_queries, 1):
        print(f"   Consulta {i}: {query[:50]}...")
        
        try:
            query_data = {
                "type": "native",
                "native": {"query": query},
                "database": db_id
            }
            
            response = requests.post(
                f"{metabase_url}/api/dataset",
                json=query_data,
                headers=headers,
                timeout=45
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get("data") and data["data"].get("rows"):
                    rows = data["data"]["rows"]
                    if "COUNT" in query:
                        count = rows[0][0]
                        print(f"   ✅ {count:,} registros encontrados")
                    elif "SHOW DATABASES" in query:
                        databases = [row[0] for row in rows]
                        print(f"   ✅ Bases de datos: {', '.join(databases[:5])}...")
                    else:
                        print(f"   ✅ Consulta exitosa: {len(rows)} filas")
                else:
                    print(f"   ⚠️  Sin resultados")
            else:
                print(f"   ❌ Error: {response.status_code}")
                print(f"      {response.text[:200]}...")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    return True

def main():
    print("🔧 VERIFICACIÓN Y CONFIGURACIÓN DE CLICKHOUSE EN METABASE")
    print("="*70)
    
    # 1. Login
    print("🔐 Iniciando sesión en Metabase...")
    session_id = login_to_metabase()
    if not session_id:
        return False
    
    # 2. Verificar bases de datos existentes
    print("\n📊 VERIFICANDO BASES DE DATOS EXISTENTES")
    print("="*50)
    databases = get_databases(session_id)
    
    # Buscar ClickHouse existente
    clickhouse_db = None
    for db in databases:
        if "clickhouse" in db.get("name", "").lower() or db.get("engine") == "clickhouse":
            clickhouse_db = db
            break
    
    # 3. Crear ClickHouse si no existe
    if not clickhouse_db:
        print("\n🔧 CONFIGURANDO CLICKHOUSE")
        print("="*50)
        db_id = create_clickhouse_database(session_id)
        if not db_id:
            return False
    else:
        print(f"\n✅ ClickHouse encontrado: {clickhouse_db['name']} (ID: {clickhouse_db['id']})")
        db_id = clickhouse_db["id"]
    
    # 4. Sincronizar esquema
    print("\n🔄 SINCRONIZACIÓN DE ESQUEMA")
    print("="*50)
    sync_database_schema(session_id, db_id)
    
    # 5. Probar consultas
    print("\n🧪 PRUEBAS DE CONSULTAS")
    print("="*50)
    test_clickhouse_query(session_id, db_id)
    
    print("\n🎉 CONFIGURACIÓN COMPLETADA")
    print("="*50)
    print("📊 ACCESO A METABASE:")
    print(f"   🔗 URL: http://localhost:3000")
    print(f"   👤 Usuario: admin@etl.local")
    print(f"   🔑 Contraseña: AdminETL2024!")
    print("\n📋 PARA VER DATOS:")
    print("   1. Ve a 'Browse Data' → 'ClickHouse ETL'")
    print("   2. Explora las tablas en 'fiscalizacion' y otros esquemas")
    print("\n🔍 PARA CONSULTAS SQL:")
    print("   1. 'New' → 'Question' → 'Native Query'")
    print("   2. Prueba: SELECT * FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora LIMIT 10")
    
    return True

if __name__ == "__main__":
    main()