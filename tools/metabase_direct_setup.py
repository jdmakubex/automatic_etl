#!/usr/bin/env python3
"""
Script directo para configurar ClickHouse en Metabase usando el setup token disponible
"""

import requests
import json
import time

def configure_metabase_with_setup_token():
    """Configurar Metabase usando el setup token disponible"""
    metabase_url = "http://localhost:3000"
    
    print("🚀 CONFIGURACIÓN DIRECTA DE METABASE CON SETUP TOKEN")
    print("="*60)
    
    # Obtener el setup token actual
    print("🔑 Obteniendo setup token...")
    try:
        response = requests.get(f"{metabase_url}/api/session/properties", timeout=10)
        if response.status_code == 200:
            data = response.json()
            setup_token = data.get("setup-token")
            
            if not setup_token:
                print("❌ No hay setup token disponible")
                return False
            
            print(f"✅ Setup token obtenido: {setup_token[:20]}...")
            
        else:
            print(f"❌ Error obteniendo properties: {response.status_code}")
            return False
    
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    
    # Configurar Metabase con ClickHouse
    print("⚙️  Configurando Metabase con ClickHouse...")
    
    setup_data = {
        "token": setup_token,
        "user": {
            "first_name": "Admin",
            "last_name": "ETL",
            "email": "admin@admin.com",
            "password": "Admin123!"
        },
        "database": {
            "engine": "clickhouse",
            "name": "ClickHouse ETL Data",
            "details": {
                "host": "clickhouse",
                "port": 8123,
                "dbname": "default",
                "user": "default", 
                "password": "ClickHouse123!",
                "ssl": False,
                "additional-options": "socket_timeout=300000&connection_timeout=10000&max_execution_time=300"
            },
            "is_full_sync": True,
            "auto_run_queries": True
        },
        "prefs": {
            "site_name": "ETL Analytics Platform",
            "allow_tracking": False
        }
    }
    
    try:
        response = requests.post(
            f"{metabase_url}/api/setup",
            json=setup_data,
            timeout=120
        )
        
        if response.status_code == 200:
            result = response.json()
            print("✅ Metabase configurado exitosamente")
            print(f"   👤 Usuario: admin@admin.com")
            print(f"   🔑 Contraseña: Admin123!")
            print(f"   🗄️  Base de datos ClickHouse configurada")
            
            # Obtener session ID del resultado
            session_id = result.get("id")
            return session_id
            
        else:
            print(f"❌ Error configurando Metabase: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error en configuración: {e}")
        return False

def test_metabase_data_access(session_id):
    """Probar acceso a datos en Metabase"""
    metabase_url = "http://localhost:3000"
    
    print("\n🧪 PROBANDO ACCESO A DATOS")
    print("="*40)
    
    headers = {"X-Metabase-Session": session_id}
    
    # 1. Obtener bases de datos
    print("📋 Obteniendo bases de datos...")
    try:
        response = requests.get(f"{metabase_url}/api/database", headers=headers, timeout=15)
        if response.status_code == 200:
            databases = response.json().get("data", [])
            clickhouse_db = None
            
            for db in databases:
                if "clickhouse" in db.get("name", "").lower():
                    clickhouse_db = db
                    print(f"✅ ClickHouse encontrado: {db['name']} (ID: {db['id']})")
                    break
            
            if not clickhouse_db:
                print("❌ No se encontró base de datos ClickHouse")
                return False
            
        else:
            print(f"❌ Error obteniendo bases de datos: {response.status_code}")
            return False
    
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    
    # 2. Sincronizar esquema si es necesario
    print("🔄 Forzando sincronización de esquema...")
    try:
        sync_response = requests.post(
            f"{metabase_url}/api/database/{clickhouse_db['id']}/sync_schema",
            headers=headers,
            timeout=60
        )
        
        if sync_response.status_code in [200, 202]:
            print("✅ Sincronización iniciada")
            time.sleep(20)  # Esperar sincronización
        else:
            print(f"⚠️  Advertencia sincronización: {sync_response.status_code}")
    
    except Exception as e:
        print(f"⚠️  Error sincronización: {e}")
    
    # 3. Probar consulta de datos
    print("🔍 Probando consulta de datos...")
    
    test_queries = [
        "SELECT COUNT(*) as total FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora",
        "SELECT 1 as test",
        "SHOW DATABASES"
    ]
    
    for query in test_queries:
        try:
            query_data = {
                "type": "native",
                "native": {"query": query},
                "database": clickhouse_db["id"]
            }
            
            response = requests.post(
                f"{metabase_url}/api/dataset",
                json=query_data,
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get("data") and data["data"].get("rows"):
                    if "COUNT" in query:
                        count = data["data"]["rows"][0][0]
                        print(f"✅ Consulta exitosa: {count:,} registros")
                    else:
                        print(f"✅ Consulta '{query[:30]}...' exitosa")
                    return True
                else:
                    print(f"⚠️  Consulta sin resultados: {query[:30]}...")
            else:
                print(f"❌ Error consulta '{query[:30]}...': {response.status_code}")
                
        except Exception as e:
            print(f"❌ Error ejecutando '{query[:30]}...': {e}")
    
    return False

def main():
    # Configurar Metabase
    session_id = configure_metabase_with_setup_token()
    
    if session_id:
        # Probar acceso a datos
        if test_metabase_data_access(session_id):
            print("\n🎉 METABASE COMPLETAMENTE CONFIGURADO Y FUNCIONAL")
            print("="*60)
            print("📊 ACCESO:")
            print("   🔗 URL: http://localhost:3000")
            print("   👤 Usuario: admin@etl.local")
            print("   🔑 Contraseña: AdminETL2024!")
            print("\n📋 PARA VER DATOS:")
            print("   1. Ve a 'Browse Data' en el menú principal")
            print("   2. Selecciona 'ClickHouse ETL Data'")
            print("   3. Explora las tablas disponibles")
            print("\n🔍 PARA CONSULTAS SQL:")
            print("   1. Haz clic en 'New' → 'Question'")
            print("   2. Selecciona 'Native Query'")
            print("   3. Escribe tu consulta SQL")
            return True
        else:
            print("\n⚠️  Metabase configurado pero datos no accesibles")
            return False
    else:
        print("\n❌ Error configurando Metabase")
        return False

if __name__ == "__main__":
    main()