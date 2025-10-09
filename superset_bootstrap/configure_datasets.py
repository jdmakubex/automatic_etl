#!/usr/bin/env python3
"""
Script para configurar automáticamente los datasets de ClickHouse en Superset
"""
import requests
import time
import json
import os
import sys

def wait_for_superset(url, timeout=300):
    """Esperar a que Superset esté disponible"""
    print(f"🔄 Esperando a que Superset esté disponible en {url}...")
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            response = requests.get(f"{url}/health", timeout=5)
            if response.status_code == 200:
                print("✅ Superset está disponible")
                return True
        except requests.exceptions.RequestException:
            pass
        
        time.sleep(5)
    
    print("❌ Timeout esperando a Superset")
    return False

def get_auth_token(url, username, password):
    """Obtener token de autenticación de Superset"""
    print("🔑 Obteniendo token de autenticación...")
    
    login_data = {
        "username": username,
        "password": password,
        "provider": "db",
        "refresh": True
    }
    
    try:
        response = requests.post(
            f"{url}/api/v1/security/login",
            json=login_data,
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 200:
            token = response.json().get("access_token")
            if token:
                print("✅ Token obtenido exitosamente")
                return token
            else:
                print("❌ No se encontró token en la respuesta")
                return None
        else:
            print(f"❌ Error al obtener token: {response.status_code} - {response.text}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Error de conexión al obtener token: {e}")
        return None

def get_databases(url, token):
    """Obtener lista de bases de datos configuradas"""
    print("📊 Obteniendo bases de datos configuradas...")
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(f"{url}/api/v1/database/", headers=headers)
        if response.status_code == 200:
            databases = response.json().get("result", [])
            print(f"✅ Se encontraron {len(databases)} bases de datos")
            return databases
        else:
            print(f"❌ Error al obtener bases de datos: {response.status_code}")
            return []
    except requests.exceptions.RequestException as e:
        print(f"❌ Error de conexión: {e}")
        return []

def sync_database_schemas(url, token, database_id):
    """Sincronizar esquemas de una base de datos"""
    print(f"🔄 Sincronizando esquemas para base de datos ID: {database_id}...")
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    try:
        # Primero intentar obtener los esquemas existentes
        response = requests.get(
            f"{url}/api/v1/database/{database_id}/schemas",
            headers=headers
        )
        
        if response.status_code == 200:
            schemas = response.json().get("result", [])
            print(f"✅ Esquemas encontrados: {schemas}")
            return True
        else:
            print(f"⚠️  No se pudieron obtener esquemas: {response.status_code}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Error al sincronizar esquemas: {e}")
        return False

def get_database_tables(url, token, database_id, schema_name="fgeo_analytics"):
    """Obtener tablas de una base de datos"""
    print(f"🔄 Obteniendo tablas para base de datos ID: {database_id}, esquema: {schema_name}...")
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    try:
        # Obtener tablas del esquema
        response = requests.get(
            f"{url}/api/v1/database/{database_id}/tables?q={schema_name}",
            headers=headers
        )
        
        if response.status_code == 200:
            tables = response.json().get("result", [])
            print(f"✅ Se encontraron {len(tables)} tablas: {[t.get('value', t) for t in tables[:5]]}...")
            return tables
        else:
            print(f"⚠️  No se pudieron obtener tablas: {response.status_code} - {response.text}")
            return []
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Error al obtener tablas: {e}")
        return []

def create_dataset(url, token, database_id, table_name, schema_name="fgeo_analytics"):
    """Crear un dataset en Superset"""
    print(f"📊 Creando dataset para tabla: {table_name}...")
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    dataset_data = {
        "database": database_id,
        "schema": schema_name,
        "table_name": table_name,
        "sql": "",
        "is_sqllab_view": False
    }
    
    try:
        response = requests.post(
            f"{url}/api/v1/dataset/",
            json=dataset_data,
            headers=headers
        )
        
        if response.status_code in [200, 201]:
            print(f"✅ Dataset creado para {table_name}")
            return True
        else:
            print(f"⚠️  No se pudo crear dataset para {table_name}: {response.status_code}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Error al crear dataset para {table_name}: {e}")
        return False

def main():
    # Configuración
    SUPERSET_URL = os.getenv("SUPERSET_URL", "http://localhost:8088")
    SUPERSET_ADMIN = os.getenv("SUPERSET_ADMIN", "admin")
    SUPERSET_PASSWORD = os.getenv("SUPERSET_PASSWORD", "Admin123!")
    
    print("🚀 Iniciando configuración automática de datasets en Superset")
    print(f"📍 URL: {SUPERSET_URL}")
    print(f"👤 Usuario: {SUPERSET_ADMIN}")
    
    # Esperar a que Superset esté disponible
    if not wait_for_superset(SUPERSET_URL):
        sys.exit(1)
    
    # Obtener token de autenticación
    token = get_auth_token(SUPERSET_URL, SUPERSET_ADMIN, SUPERSET_PASSWORD)
    if not token:
        sys.exit(1)
    
    # Obtener bases de datos
    databases = get_databases(SUPERSET_URL, token)
    
    clickhouse_db = None
    for db in databases:
        if "clickhouse" in db.get("database_name", "").lower() or "fgeo_analytics" in db.get("database_name", "").lower():
            clickhouse_db = db
            break
    
    if not clickhouse_db:
        print("❌ No se encontró la base de datos de ClickHouse")
        sys.exit(1)
    
    print(f"✅ Base de datos ClickHouse encontrada: {clickhouse_db['database_name']} (ID: {clickhouse_db['id']})")
    
    # Sincronizar esquemas
    sync_database_schemas(SUPERSET_URL, token, clickhouse_db['id'])
    time.sleep(2)  # Esperar un poco
    
    # Obtener tablas y crear datasets
    tables = get_database_tables(SUPERSET_URL, token, clickhouse_db['id'], "fgeo_analytics")
    
    if tables:
        print(f"🔨 Creando datasets para {len(tables)} tablas...")
        created_count = 0
        for table in tables:
            table_name = table.get('value', table) if isinstance(table, dict) else table
            if create_dataset(SUPERSET_URL, token, clickhouse_db['id'], table_name, "fgeo_analytics"):
                created_count += 1
                time.sleep(1)  # Pequeña pausa entre creaciones
        
        print(f"✅ Se crearon {created_count} datasets de {len(tables)} tablas")
    else:
        print("⚠️  No se encontraron tablas para crear datasets")
    
    print("🎉 Configuración de datasets completada!")
    print("📋 Ve a Superset > Data > Datasets para ver las tablas disponibles")

if __name__ == "__main__":
    main()