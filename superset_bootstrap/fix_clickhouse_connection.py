#!/usr/bin/env python3
"""
Script para verificar y corregir la configuración de la conexión ClickHouse
"""
import requests
import json
import os

SUPERSET_URL = os.getenv("SUPERSET_URL", "http://localhost:8088")
SUPERSET_ADMIN = os.getenv("SUPERSET_ADMIN", "admin")
SUPERSET_PASSWORD = os.getenv("SUPERSET_PASSWORD", "Admin123!")

def get_token():
    response = requests.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json={"username": SUPERSET_ADMIN, "password": SUPERSET_PASSWORD, "provider": "db", "refresh": True},
        headers={"Content-Type": "application/json"}
    )
    if response.status_code == 200:
        return response.json().get("access_token")
    return None

def get_clickhouse_database(token):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    
    response = requests.get(f"{SUPERSET_URL}/api/v1/database/", headers=headers)
    if response.status_code == 200:
        databases = response.json().get("result", [])
        for db in databases:
            if "clickhouse" in db.get("database_name", "").lower():
                return db
    return None

def fix_clickhouse_connection(token, db_id):
    """Actualizar la conexión ClickHouse con configuración correcta para evitar el bug 'dict has no attribute set'"""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    
    # Primero obtener la configuración actual
    response = requests.get(f"{SUPERSET_URL}/api/v1/database/{db_id}", headers=headers)
    if response.status_code != 200:
        print(f"❌ Error obteniendo database: {response.status_code}")
        return False
    
    db = response.json().get("result", {})
    current_sqlalchemy_uri = db.get("sqlalchemy_uri", "")
    current_extra = db.get("extra", "")
    
    print(f"📊 Base de datos actual:")
    print(f"   ID: {db_id}")
    print(f"   Nombre: {db.get('database_name')}")
    print(f"   SQLAlchemy URI: {current_sqlalchemy_uri}")
    print(f"   Extra: {current_extra}")
    
    # Parsear extra
    try:
        extra_obj = json.loads(current_extra) if current_extra else {}
    except:
        extra_obj = {}
    
    # Configuración correcta para evitar el bug
    # El bug ocurre cuando Superset intenta usar cursor.set() en queries desde SQL Lab
    # La solución es asegurar que use el driver correcto y deshabilitar ciertas features
    
    extra_obj.update({
        "engine_params": {
            "connect_args": {
                "http_session_timeout": 300,
                "settings": {
                    "max_execution_time": 300
                }
            }
        },
        "metadata_params": {},
        "metadata_cache_timeout": {},
        "schemas_allowed_for_csv_upload": [],
        # Importante: asegurar que no intente usar features no soportadas
        "disable_data_preview": False,
        "allows_virtual_table_explore": True,
        "allows_subquery": True
    })
    
    # Actualizar la base de datos
    update_payload = {
        "extra": json.dumps(extra_obj, ensure_ascii=False)
    }
    
    print(f"\n🔧 Actualizando configuración...")
    response = requests.put(
        f"{SUPERSET_URL}/api/v1/database/{db_id}",
        json=update_payload,
        headers=headers
    )
    
    if response.status_code in [200, 201]:
        print("✅ Configuración actualizada correctamente")
        return True
    else:
        print(f"❌ Error actualizando: {response.status_code}")
        print(f"   Response: {response.text[:500]}")
        return False

def test_simple_query(token, db_id):
    """Probar una query simple para verificar si funciona"""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    
    payload = {
        "database_id": db_id,
        "sql": "SELECT 1 as test",
        "schema": "fiscalizacion",
        "runAsync": False,  # Sync para obtener resultado inmediato
        "queryLimit": 10
    }
    
    print("\n🧪 Probando query simple...")
    response = requests.post(
        f"{SUPERSET_URL}/api/v1/sqllab/execute/",
        json=payload,
        headers=headers
    )
    
    if response.status_code in [200, 201]:
        print("✅ Query ejecutada correctamente")
        return True
    else:
        print(f"❌ Error: {response.status_code}")
        print(f"   Response: {response.text[:500]}")
        return False

if __name__ == "__main__":
    print("🔍 Diagnosticando problema 'dict has no attribute set'...\n")
    
    token = get_token()
    if not token:
        print("❌ No se pudo autenticar")
        exit(1)
    
    db = get_clickhouse_database(token)
    if not db:
        print("❌ No se encontró base de datos ClickHouse")
        exit(1)
    
    db_id = db.get("id")
    
    # Intentar corregir la configuración
    if fix_clickhouse_connection(token, db_id):
        # Probar query simple
        test_simple_query(token, db_id)
    
    print("\n" + "="*80)
    print("NOTA IMPORTANTE:")
    print("="*80)
    print("""
El error 'dict has no attribute set' es un bug conocido de clickhouse-sqlalchemy
cuando Superset intenta acceder a atributos de cursor que no existen.

Posibles soluciones:
1. Usar runAsync=True siempre (queries procesadas por worker)
2. Actualizar clickhouse-sqlalchemy a versión más reciente
3. Usar SQL Lab con "Run Async" habilitado en settings

Para verificar si funciona ahora:
1. Ve a SQL Lab: http://localhost:8088/sqllab
2. En Settings (⚙️), asegúrate que "Run Async" esté ACTIVADO
3. Ejecuta tu query de nuevo
""")
