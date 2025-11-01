#!/usr/bin/env python3
"""
Script inteligente para configurar Metabase - con reset automático si es necesario
Usa credenciales del .env y maneja todos los casos posibles
"""

import requests
import json
import time
import subprocess
import sys
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

def wait_for_metabase(max_attempts=15):
    """Esperar que Metabase esté disponible"""
    print("⏳ Esperando Metabase...")
    
    for attempt in range(max_attempts):
        try:
            response = requests.get("http://localhost:3000/api/health", timeout=5)
            if response.status_code == 200:
                print("✅ Metabase disponible")
                return True
        except:
            pass
        
        if attempt < max_attempts - 1:
            time.sleep(3)
    
    return False

def check_metabase_status():
    """Verificar el estado actual de Metabase"""
    try:
        response = requests.get("http://localhost:3000/api/session/properties", timeout=10)
        if response.status_code == 200:
            data = response.json()
            has_setup_token = bool(data.get("setup-token"))
            has_user_setup = data.get("has-user-setup", False)
            
            if has_setup_token and not has_user_setup:
                return "needs_setup"
            elif has_user_setup:
                return "configured"
            else:
                return "unknown"
        else:
            return "error"
    except Exception:
        return "error"

def try_login(credentials):
    """Intentar login con credenciales del .env"""
    try:
        login_data = {
            "username": credentials['METABASE_ADMIN'],
            "password": credentials['METABASE_PASSWORD']
        }
        
        response = requests.post(
            "http://localhost:3000/api/session",
            json=login_data,
            timeout=15
        )
        
        if response.status_code == 200:
            return response.json().get("id")
        else:
            return None
    except Exception:
        return None

def reset_metabase_if_needed():
    """Reset Metabase si las credenciales no funcionan"""
    print("🔄 Ejecutando reset automático de Metabase...")
    
    try:
        # Ejecutar script de reset
        result = subprocess.run([
            "/mnt/c/proyectos/etl_prod/tools/metabase_complete_reset.sh"
        ], capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0:
            print("✅ Reset completado")
            # Esperar que Metabase esté disponible nuevamente
            return wait_for_metabase(20)
        else:
            print("❌ Error en reset")
            return False
            
    except Exception as e:
        print(f"❌ Error ejecutando reset: {e}")
        return False

def setup_metabase_initial(credentials):
    """Configurar Metabase desde cero"""
    print("⚙️  Configurando Metabase desde setup inicial...")
    
    # Obtener setup token
    try:
        response = requests.get("http://localhost:3000/api/session/properties", timeout=10)
        if response.status_code != 200:
            return False
        
        setup_token = response.json().get("setup-token")
        if not setup_token:
            return False
        
    except Exception:
        return False
    
    # Configurar con ClickHouse incluido
    setup_data = {
        "token": setup_token,
        "user": {
            "first_name": "Admin",
            "last_name": "ETL",
            "email": credentials['METABASE_ADMIN'],
            "password": credentials['METABASE_PASSWORD']
        },
        "database": {
            "engine": "clickhouse",
            "name": "ClickHouse ETL",
            "details": {
                "host": "clickhouse",
                "port": 8123,
                "dbname": "default",
                "user": credentials['CLICKHOUSE_DEFAULT_USER'],
                "password": credentials['CLICKHOUSE_DEFAULT_PASSWORD'],
                "ssl": False,
                "additional-options": "socket_timeout=300000&connection_timeout=30000"
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
            "http://localhost:3000/api/setup",
            json=setup_data,
            timeout=120
        )
        
        if response.status_code == 200:
            session_id = response.json().get("id")
            print("✅ Setup inicial completado")
            time.sleep(20)  # Esperar sincronización
            return session_id
        else:
            print(f"❌ Error setup: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def add_clickhouse_to_existing(session_id, credentials):
    """Agregar ClickHouse a Metabase ya configurado"""
    print("🔧 Agregando ClickHouse a Metabase existente...")
    
    headers = {"X-Metabase-Session": session_id}
    
    # Verificar si ClickHouse ya existe
    try:
        db_response = requests.get("http://localhost:3000/api/database", headers=headers, timeout=15)
        if db_response.status_code == 200:
            databases = db_response.json().get("data", [])
            
            for db in databases:
                if "clickhouse" in db.get("name", "").lower() or db.get("engine") == "clickhouse":
                    print("ℹ️  ClickHouse ya configurado")
                    return db["id"]
    except:
        pass
    
    # Crear ClickHouse
    clickhouse_config = {
        "name": "ClickHouse ETL",
        "engine": "clickhouse",
        "details": {
            "host": "clickhouse",
            "port": 8123,
            "dbname": "default",
            "user": credentials['CLICKHOUSE_DEFAULT_USER'],
            "password": credentials['CLICKHOUSE_DEFAULT_PASSWORD'],
            "ssl": False,
            "additional-options": "socket_timeout=300000&connection_timeout=30000"
        },
        "auto_run_queries": True,
        "is_full_sync": True
    }
    
    try:
        response = requests.post(
            "http://localhost:3000/api/database",
            json=clickhouse_config,
            headers=headers,
            timeout=60
        )
        
        if response.status_code == 200:
            db_id = response.json().get("id")
            print(f"✅ ClickHouse creado (ID: {db_id})")
            
            # Sincronizar
            requests.post(
                f"http://localhost:3000/api/database/{db_id}/sync_schema",
                headers=headers,
                timeout=60
            )
            
            time.sleep(15)
            return db_id
        else:
            print(f"❌ Error creando ClickHouse: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def test_data_access(session_id, db_id):
    """Probar acceso básico a datos"""
    if not session_id or not db_id:
        return False
    
    headers = {"X-Metabase-Session": session_id}
    
    try:
        query_data = {
            "type": "native",
            "native": {"query": "SELECT COUNT(*) FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora"},
            "database": db_id
        }
        
        response = requests.post(
            "http://localhost:3000/api/dataset",
            json=query_data,
            headers=headers,
            timeout=30
        )
        
        if response.status_code in [200, 202]:
            data = response.json()
            if data.get("data") and data["data"].get("rows"):
                count = data["data"]["rows"][0][0]
                print(f"✅ {count:,} registros accesibles")
                return True
        
        return False
        
    except Exception:
        return False

def smart_metabase_config():
    """Configuración inteligente de Metabase con manejo automático de casos"""
    
    print("🤖 CONFIGURACIÓN INTELIGENTE DE METABASE")
    print("="*60)
    
    # Cargar credenciales
    credentials = load_env_credentials()
    print(f"🔑 Usuario: {credentials['METABASE_ADMIN']}")
    
    # Esperar Metabase
    if not wait_for_metabase():
        print("❌ Metabase no disponible")
        return False
    
    # Verificar estado
    status = check_metabase_status()
    print(f"📊 Estado Metabase: {status}")
    
    session_id = None
    db_id = None
    
    if status == "needs_setup":
        # Caso 1: Setup inicial necesario
        print("📝 Configuración inicial requerida")
        session_id = setup_metabase_initial(credentials)
        
        if session_id:
            # ClickHouse ya debería estar configurado desde el setup
            try:
                db_response = requests.get("http://localhost:3000/api/database", 
                                         headers={"X-Metabase-Session": session_id}, timeout=15)
                if db_response.status_code == 200:
                    databases = db_response.json().get("data", [])
                    for db in databases:
                        if "clickhouse" in db.get("name", "").lower():
                            db_id = db["id"]
                            break
            except:
                pass
    
    elif status == "configured":
        # Caso 2: Ya configurado, intentar login
        print("🔐 Intentando login...")
        session_id = try_login(credentials)
        
        if session_id:
            print("✅ Login exitoso")
            db_id = add_clickhouse_to_existing(session_id, credentials)
        else:
            # Caso 3: Credenciales incorrectas, reset necesario
            print("⚠️  Credenciales incorrectas, ejecutando reset...")
            if reset_metabase_if_needed():
                session_id = setup_metabase_initial(credentials)
                if session_id:
                    try:
                        db_response = requests.get("http://localhost:3000/api/database", 
                                                 headers={"X-Metabase-Session": session_id}, timeout=15)
                        if db_response.status_code == 200:
                            databases = db_response.json().get("data", [])
                            for db in databases:
                                if "clickhouse" in db.get("name", "").lower():
                                    db_id = db["id"]
                                    break
                    except:
                        pass
    
    else:
        print("❌ Estado desconocido, intentando reset...")
        if reset_metabase_if_needed():
            session_id = setup_metabase_initial(credentials)
    
    # Probar acceso a datos
    if session_id and db_id:
        print("\n🧪 Probando acceso a datos...")
        if test_data_access(session_id, db_id):
            print("\n🎉 ¡METABASE COMPLETAMENTE CONFIGURADO!")
            print("="*50)
            print(f"🔗 URL: http://localhost:3000")
            print(f"👤 Usuario: {credentials['METABASE_ADMIN']}")
            print(f"🔑 Contraseña: {credentials['METABASE_PASSWORD']}")
            print("📊 ClickHouse ETL conectado y funcional")
            return True
        else:
            print("⚠️  Configuración incompleta - datos no accesibles")
            return False
    else:
        print("❌ No se pudo completar la configuración")
        return False

def main():
    success = smart_metabase_config()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()