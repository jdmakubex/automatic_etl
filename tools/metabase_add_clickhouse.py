#!/usr/bin/env python3
"""
Script para configurar ClickHouse después del setup inicial
"""

import requests
import json
import time
import os

def load_env_credentials():
    """Cargar credenciales desde .env"""
    credentials = {
        'METABASE_ADMIN': 'admin@admin.com',
        'METABASE_PASSWORD': 'Admin123!',
        'CLICKHOUSE_DEFAULT_USER': 'default',
        'CLICKHOUSE_DEFAULT_PASSWORD': 'ClickHouse123!'
    }
    
    try:
        with open('/mnt/c/proyectos/etl_prod/.env', 'r') as f:
            for line in f:
                line = line.strip()
                if '=' in line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    if key.strip() in credentials:
                        credentials[key.strip()] = value.strip()
    except Exception:
        pass
    
    return credentials


def configure_clickhouse_after_setup():
    """Configurar ClickHouse después de que Metabase ya esté inicializado"""
    
    # Cargar credenciales del .env
    env_creds = load_env_credentials()
    
    print("🔧 CONFIGURANDO CLICKHOUSE EN METABASE EXISTENTE")
    print("="*60)
    
    metabase_url = "http://localhost:3000"
    
    # Credenciales del .env
    credentials = {
        "username": env_creds["METABASE_ADMIN"],
        "password": env_creds["METABASE_PASSWORD"]
    }
    
    # 1. Login
    print("🔐 Iniciando sesión...")
    try:
        response = requests.post(f"{metabase_url}/api/session", json=credentials, timeout=15)
        if response.status_code == 200:
            session_id = response.json().get("id")
            print("✅ Login exitoso")
        else:
            print(f"❌ Error login: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    
    headers = {"X-Metabase-Session": session_id}
    
    # 2. Verificar bases de datos existentes
    print("\n📊 Verificando bases de datos existentes...")
    try:
        db_response = requests.get(f"{metabase_url}/api/database", headers=headers, timeout=15)
        if db_response.status_code == 200:
            databases = db_response.json().get("data", [])
            
            print(f"   Bases de datos encontradas: {len(databases)}")
            for db in databases:
                print(f"   - {db['name']} (ID: {db['id']}, Engine: {db.get('engine', 'N/A')})")
            
            # Verificar si ClickHouse ya existe
            clickhouse_exists = any("clickhouse" in db.get("name", "").lower() or db.get("engine") == "clickhouse" 
                                  for db in databases)
            
            if clickhouse_exists:
                print("ℹ️  ClickHouse ya configurado")
                clickhouse_db = next(db for db in databases 
                                   if "clickhouse" in db.get("name", "").lower() or db.get("engine") == "clickhouse")
                return test_clickhouse_connection(metabase_url, session_id, clickhouse_db["id"])
        else:
            print(f"❌ Error obteniendo bases de datos: {db_response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    
    # 3. Crear base de datos ClickHouse
    print("\n🗄️  Creando base de datos ClickHouse...")
    
    clickhouse_config = {
        "name": "ClickHouse ETL",
        "engine": "clickhouse",
        "details": {
            "host": "clickhouse",
            "port": 8123,
            "dbname": "default",
            "user": env_creds["CLICKHOUSE_DEFAULT_USER"],
            "password": env_creds["CLICKHOUSE_DEFAULT_PASSWORD"],
            "ssl": False,
            "additional-options": "socket_timeout=300000&connection_timeout=30000&max_execution_time=300"
        },
        "auto_run_queries": True,
        "is_full_sync": True,
        "cache_ttl": None
    }
    
    try:
        create_response = requests.post(
            f"{metabase_url}/api/database",
            json=clickhouse_config,
            headers=headers,
            timeout=60
        )
        
        if create_response.status_code == 200:
            db_data = create_response.json()
            db_id = db_data.get("id")
            print(f"✅ ClickHouse creado exitosamente (ID: {db_id})")
        else:
            print(f"❌ Error creando ClickHouse: {create_response.status_code}")
            print(f"Response: {create_response.text}")
            return False
    
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    
    # 4. Sincronizar esquema
    print("\n🔄 Sincronizando esquema de ClickHouse...")
    try:
        sync_response = requests.post(
            f"{metabase_url}/api/database/{db_id}/sync_schema",
            headers=headers,
            timeout=120
        )
        
        if sync_response.status_code in [200, 202]:
            print("✅ Sincronización iniciada")
            print("⏳ Esperando que se complete la sincronización...")
            time.sleep(30)
        else:
            print(f"⚠️  Advertencia sincronización: {sync_response.status_code}")
    
    except Exception as e:
        print(f"⚠️  Error sincronización: {e}")
    
    # 5. Probar conexión
    return test_clickhouse_connection(metabase_url, session_id, db_id)

def test_clickhouse_connection(metabase_url, session_id, db_id):
    """Probar la conexión y consultas a ClickHouse"""
    
    print("\n🧪 PROBANDO CONEXIÓN CLICKHOUSE")
    print("="*50)
    
    headers = {"X-Metabase-Session": session_id}
    
    test_queries = [
        {
            "name": "Test básico",
            "query": "SELECT 1 as test_number",
            "expected": "number"
        },
        {
            "name": "Bases de datos disponibles",
            "query": "SHOW DATABASES",
            "expected": "list"
        },
        {
            "name": "Conteo de registros principales",
            "query": "SELECT COUNT(*) as total_records FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora",
            "expected": "count"
        },
        {
            "name": "Muestra de datos",
            "query": "SELECT * FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora LIMIT 3",
            "expected": "data"
        }
    ]
    
    successful_tests = 0
    
    for test in test_queries:
        print(f"   🔍 {test['name']}...")
        
        try:
            query_data = {
                "type": "native",
                "native": {"query": test["query"]},
                "database": db_id
            }
            
            response = requests.post(
                f"{metabase_url}/api/dataset",
                json=query_data,
                headers=headers,
                timeout=45
            )
            
            if response.status_code in [200, 202]:
                data = response.json()
                
                if data.get("data") and data["data"].get("rows"):
                    rows = data["data"]["rows"]
                    
                    if test["expected"] == "count":
                        count = rows[0][0]
                        print(f"      ✅ {count:,} registros encontrados")
                    elif test["expected"] == "list":
                        items = [row[0] for row in rows[:5]]
                        print(f"      ✅ Elementos: {', '.join(items)}...")
                    elif test["expected"] == "data":
                        print(f"      ✅ {len(rows)} filas de datos obtenidas")
                    else:
                        print(f"      ✅ Consulta exitosa")
                    
                    successful_tests += 1
                else:
                    print(f"      ⚠️  Consulta sin resultados")
            else:
                print(f"      ❌ Error HTTP: {response.status_code}")
                if response.status_code != 202:
                    print(f"         {response.text[:200]}...")
        
        except Exception as e:
            print(f"      ❌ Error: {e}")
    
    # Resultado final
    print(f"\n🎯 RESULTADO: {successful_tests}/{len(test_queries)} pruebas exitosas")
    
    if successful_tests >= 3:
        print("\n🎉 ¡CLICKHOUSE COMPLETAMENTE CONFIGURADO!")
        print("="*60)
        print("✅ Conexión: Exitosa")
        print("✅ Esquemas: Visibles") 
        print("✅ Datos: Accesibles")
        
        print("\n📱 ACCESO A METABASE:")
        print("   🔗 URL: http://localhost:3000")
        print(f"   👤 Usuario: {env_creds['METABASE_ADMIN']}")
        print(f"   🔑 Contraseña: {env_creds['METABASE_PASSWORD']}")
        
        print("\n📊 PARA VISUALIZAR TUS DATOS:")
        print("   1. Ve a 'New' → 'Question'")
        print("   2. Selecciona 'Native Query'")
        print("   3. Elige 'ClickHouse ETL' como base de datos")
        print("   4. Escribe tu consulta SQL, por ejemplo:")
        print("      SELECT * FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora")
        print("      WHERE created_at >= today() - 30 LIMIT 100")
        
        print("\n🎯 ¡LISTO! Ahora puedes visualizar todos tus datos en Metabase")
        return True
    else:
        print("\n⚠️  CONFIGURACIÓN INCOMPLETA")
        print(f"Solo {successful_tests} de {len(test_queries)} pruebas fueron exitosas")
        return False

def main():
    success = configure_clickhouse_after_setup()
    exit(0 if success else 1)

if __name__ == "__main__":
    main()