#!/usr/bin/env python3
"""
Script unificado para configurar Metabase usando credenciales del .env
"""

import requests
import json
import time
import os
import sys

def load_env_credentials():
    """Cargar credenciales desde el archivo .env"""
    
    # Valores por defecto del .env
    env_values = {
        'METABASE_ADMIN': 'admin@admin.com',
        'METABASE_PASSWORD': 'Admin123!',
        'CLICKHOUSE_DEFAULT_USER': 'default',
        'CLICKHOUSE_DEFAULT_PASSWORD': 'ClickHouse123!'
    }
    
    # Intentar leer del archivo .env
    try:
        with open('/mnt/c/proyectos/etl_prod/.env', 'r') as f:
            for line in f:
                line = line.strip()
                if '=' in line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip()
                    
                    if key in env_values:
                        env_values[key] = value
    
    except Exception as e:
        print(f"⚠️  Advertencia leyendo .env: {e}")
        print("   Usando valores por defecto")
    
    return env_values

def configure_metabase_from_env():
    """Configurar Metabase usando credenciales del .env"""
    
    print("🚀 CONFIGURACIÓN DE METABASE CON CREDENCIALES .ENV")
    print("="*70)
    
    # Cargar credenciales del .env
    env_creds = load_env_credentials()
    
    print("🔑 Credenciales cargadas:")
    print(f"   👤 Usuario: {env_creds['METABASE_ADMIN']}")
    print(f"   🔑 Contraseña: {env_creds['METABASE_PASSWORD'][:4]}***")
    print(f"   🗄️  ClickHouse User: {env_creds['CLICKHOUSE_DEFAULT_USER']}")
    
    metabase_url = "http://localhost:3000"
    
    # 1. Verificar disponibilidad
    print("\n⏳ Verificando disponibilidad de Metabase...")
    for attempt in range(10):
        try:
            response = requests.get(f"{metabase_url}/api/health", timeout=5)
            if response.status_code == 200:
                print("✅ Metabase disponible")
                break
        except:
            pass
        
        print(f"   Intento {attempt + 1}/10...")
        time.sleep(3)
    else:
        print("❌ Metabase no disponible")
        return False
    
    # 2. Verificar si necesita setup inicial
    print("\n🔍 Verificando estado de configuración...")
    
    try:
        props_response = requests.get(f"{metabase_url}/api/session/properties", timeout=10)
        if props_response.status_code == 200:
            data = props_response.json()
            setup_token = data.get("setup-token")
            has_user_setup = data.get("has-user-setup", False)
            
            if setup_token and not has_user_setup:
                print("📝 Setup inicial requerido")
                return complete_initial_setup(metabase_url, setup_token, env_creds)
            else:
                print("🔐 Metabase ya configurado, intentando login...")
                return login_and_configure(metabase_url, env_creds)
        else:
            print(f"⚠️  Error verificando estado: {props_response.status_code}")
            return False
    
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def complete_initial_setup(metabase_url, setup_token, env_creds):
    """Completar setup inicial usando credenciales del .env"""
    
    print("⚙️  Completando setup inicial con credenciales .env...")
    
    setup_data = {
        "token": setup_token,
        "user": {
            "first_name": "Admin",
            "last_name": "ETL",
            "email": env_creds['METABASE_ADMIN'],
            "password": env_creds['METABASE_PASSWORD']
        },
        "database": {
            "engine": "clickhouse",
            "name": "ClickHouse ETL",
            "details": {
                "host": "clickhouse",
                "port": 8123,
                "dbname": "default",
                "user": env_creds['CLICKHOUSE_DEFAULT_USER'],
                "password": env_creds['CLICKHOUSE_DEFAULT_PASSWORD'],
                "ssl": False,
                "additional-options": "socket_timeout=300000&connection_timeout=30000&max_execution_time=300"
            },
            "is_full_sync": True,
            "auto_run_queries": True
        },
        "prefs": {
            "site_name": "ETL Analytics",
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
            session_id = result.get("id")
            
            print("✅ Setup inicial completado")
            print(f"   👤 Usuario: {env_creds['METABASE_ADMIN']}")
            print(f"   🗄️  ClickHouse configurado")
            
            # Esperar sincronización
            print("⏳ Esperando sincronización inicial...")
            time.sleep(30)
            
            return test_data_access(metabase_url, session_id)
        else:
            print(f"❌ Error setup: {response.status_code}")
            print(f"Response: {response.text}")
            return False
    
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def login_and_configure(metabase_url, env_creds):
    """Login con credenciales existentes y configurar ClickHouse si es necesario"""
    
    print("🔐 Intentando login con credenciales .env...")
    
    credentials = {
        "username": env_creds['METABASE_ADMIN'],
        "password": env_creds['METABASE_PASSWORD']
    }
    
    try:
        response = requests.post(
            f"{metabase_url}/api/session",
            json=credentials,
            timeout=15
        )
        
        if response.status_code == 200:
            session_id = response.json().get("id")
            print("✅ Login exitoso")
            
            # Verificar/configurar ClickHouse
            return configure_clickhouse_database(metabase_url, session_id, env_creds)
        else:
            print(f"❌ Error login: {response.status_code}")
            
            # Si las credenciales fallan, intentar reset
            print("💡 Las credenciales no funcionan, se requiere reset completo")
            return False
    
    except Exception as e:
        print(f"❌ Error login: {e}")
        return False

def configure_clickhouse_database(metabase_url, session_id, env_creds):
    """Configurar o verificar base de datos ClickHouse"""
    
    print("🔧 Verificando configuración de ClickHouse...")
    
    headers = {"X-Metabase-Session": session_id}
    
    # Obtener bases de datos existentes
    try:
        db_response = requests.get(f"{metabase_url}/api/database", headers=headers, timeout=15)
        if db_response.status_code == 200:
            databases = db_response.json().get("data", [])
            
            # Buscar ClickHouse
            clickhouse_db = None
            for db in databases:
                if "clickhouse" in db.get("name", "").lower() or db.get("engine") == "clickhouse":
                    clickhouse_db = db
                    print(f"✅ ClickHouse encontrado: {db['name']} (ID: {db['id']})")
                    break
            
            if not clickhouse_db:
                print("📝 Creando nueva configuración ClickHouse...")
                db_id = create_clickhouse_database(metabase_url, session_id, env_creds)
            else:
                db_id = clickhouse_db["id"]
            
            if db_id:
                return test_data_access(metabase_url, session_id, db_id)
            else:
                return False
        else:
            print(f"❌ Error obteniendo bases de datos: {db_response.status_code}")
            return False
    
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def create_clickhouse_database(metabase_url, session_id, env_creds):
    """Crear nueva base de datos ClickHouse"""
    
    headers = {"X-Metabase-Session": session_id}
    
    db_config = {
        "name": "ClickHouse ETL",
        "engine": "clickhouse",
        "details": {
            "host": "clickhouse",
            "port": 8123,
            "dbname": "default",
            "user": env_creds['CLICKHOUSE_DEFAULT_USER'],
            "password": env_creds['CLICKHOUSE_DEFAULT_PASSWORD'],
            "ssl": False,
            "additional-options": "socket_timeout=300000&connection_timeout=30000&max_execution_time=300"
        },
        "auto_run_queries": True,
        "is_full_sync": True
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
            print(f"✅ ClickHouse creado (ID: {db_id})")
            
            # Sincronizar esquema
            print("🔄 Sincronizando esquema...")
            sync_response = requests.post(
                f"{metabase_url}/api/database/{db_id}/sync_schema",
                headers=headers,
                timeout=60
            )
            
            if sync_response.status_code in [200, 202]:
                print("✅ Sincronización iniciada")
                time.sleep(20)
            
            return db_id
        else:
            print(f"❌ Error creando ClickHouse: {response.status_code}")
            return None
    
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def test_data_access(metabase_url, session_id, db_id=None):
    """Probar acceso a datos"""
    
    print("\n🧪 PROBANDO ACCESO A DATOS")
    print("="*50)
    
    headers = {"X-Metabase-Session": session_id}
    
    # Si no tenemos db_id, buscar ClickHouse
    if not db_id:
        try:
            db_response = requests.get(f"{metabase_url}/api/database", headers=headers, timeout=15)
            if db_response.status_code == 200:
                databases = db_response.json().get("data", [])
                for db in databases:
                    if "clickhouse" in db.get("name", "").lower():
                        db_id = db["id"]
                        break
        except:
            pass
    
    if not db_id:
        print("❌ No se encontró base de datos ClickHouse")
        return False
    
    # Probar consultas
    test_queries = [
        ("Conectividad", "SELECT 1 as test"),
        ("Conteo principal", "SELECT COUNT(*) as total FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora"),
        ("Esquemas", "SELECT name FROM system.databases WHERE name NOT LIKE '%schema%' ORDER BY name")
    ]
    
    success_count = 0
    
    for name, query in test_queries:
        print(f"   🔍 {name}...")
        
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
                timeout=30
            )
            
            if response.status_code in [200, 202]:
                data = response.json()
                if data.get("data") and data["data"].get("rows"):
                    rows = data["data"]["rows"]
                    
                    if "COUNT" in query:
                        count = rows[0][0]
                        print(f"      ✅ {count:,} registros")
                    elif "databases" in query.lower():
                        dbs = [row[0] for row in rows]
                        print(f"      ✅ Esquemas: {', '.join(dbs)}")
                    else:
                        print(f"      ✅ Exitoso")
                    
                    success_count += 1
                else:
                    print(f"      ⚠️  Sin resultados")
            else:
                print(f"      ❌ Error: {response.status_code}")
        
        except Exception as e:
            print(f"      ❌ Error: {e}")
    
    return success_count >= 2

def show_final_status(env_creds, success):
    """Mostrar estado final"""
    
    if success:
        print("\n🎉 ¡METABASE CONFIGURADO EXITOSAMENTE!")
        print("="*60)
        print("✅ Conexión ClickHouse: Funcional")
        print("✅ Acceso a datos: Confirmado")
        print("✅ Esquemas: Visibles")
        
        print(f"\n📱 ACCESO:")
        print(f"   🔗 URL: http://localhost:3000")
        print(f"   👤 Usuario: {env_creds['METABASE_ADMIN']}")
        print(f"   🔑 Contraseña: {env_creds['METABASE_PASSWORD']}")
        
        print(f"\n📊 PARA VER TUS DATOS:")
        print(f"   1. Ve a 'New' → 'Question'")
        print(f"   2. Selecciona 'Native Query'")
        print(f"   3. Prueba: SELECT * FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora LIMIT 10")
        
        return True
    else:
        print("\n⚠️  CONFIGURACIÓN INCOMPLETA")
        print("="*40)
        print("📋 Posibles soluciones:")
        print("   1. Ejecutar reset completo: ./tools/metabase_complete_reset.sh")
        print("   2. Verificar que ClickHouse esté corriendo")
        print("   3. Revisar credenciales en .env")
        
        return False

def main():
    success = configure_metabase_from_env()
    env_creds = load_env_credentials()
    show_final_status(env_creds, success)
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()