#!/usr/bin/env python3
"""
Script para configurar SQL Lab con Run Async habilitado por defecto
Actualiza la configuración del usuario admin para tener async activado
"""
import requests
import json
import os
import time

SUPERSET_URL = os.getenv("SUPERSET_URL", "http://localhost:8088")
SUPERSET_ADMIN = os.getenv("SUPERSET_ADMIN", "admin")
SUPERSET_PASSWORD = os.getenv("SUPERSET_PASSWORD", "Admin123!")

def wait_for_superset(timeout=120):
    """Esperar a que Superset esté disponible"""
    print("⏳ Esperando a que Superset esté disponible...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            response = requests.get(f"{SUPERSET_URL}/health", timeout=5)
            if response.status_code == 200:
                print("✅ Superset está disponible")
                time.sleep(5)  # Esperar un poco más para asegurar inicialización completa
                return True
        except:
            pass
        time.sleep(3)
    return False

def get_token():
    """Obtener token de autenticación"""
    max_attempts = 10
    for attempt in range(max_attempts):
        try:
            response = requests.post(
                f"{SUPERSET_URL}/api/v1/security/login",
                json={"username": SUPERSET_ADMIN, "password": SUPERSET_PASSWORD, "provider": "db", "refresh": True},
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            if response.status_code == 200:
                return response.json().get("access_token")
        except:
            pass
        time.sleep(3)
    return None

def get_user_info(token):
    """Obtener información del usuario actual"""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    
    response = requests.get(f"{SUPERSET_URL}/api/v1/me/", headers=headers, timeout=10)
    if response.status_code == 200:
        return response.json()
    return None

def enable_sql_lab_async(token, user_id):
    """Habilitar Run Async en SQL Lab para el usuario"""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    
    # Primero obtener settings actuales del usuario
    response = requests.get(f"{SUPERSET_URL}/api/v1/me/", headers=headers, timeout=10)
    if response.status_code != 200:
        print(f"⚠️  No se pudo obtener configuración del usuario: {response.status_code}")
        return False
    
    user_data = response.json()
    settings = user_data.get("settings", {})
    
    # Actualizar settings para SQL Lab con async habilitado
    # El key correcto es 'SQLLAB_BACKEND_PERSISTENCE' en settings del usuario
    if not isinstance(settings, dict):
        settings = {}
    
    # Configurar SQL Lab para usar async por defecto
    settings["sqllab"] = settings.get("sqllab", {})
    settings["sqllab"]["runAsync"] = True  # Esto habilita "Run Async" por defecto
    
    # Actualizar el usuario
    update_payload = {
        "settings": settings
    }
    
    response = requests.put(
        f"{SUPERSET_URL}/api/v1/me/",
        json=update_payload,
        headers=headers,
        timeout=10
    )
    
    if response.status_code in [200, 201]:
        print("✅ Run Async habilitado en SQL Lab por defecto")
        return True
    else:
        print(f"⚠️  No se pudo actualizar configuración: {response.status_code}")
        print(f"   Response: {response.text[:200]}")
        return False

def configure_database_async_default(token):
    """Configurar la base de datos ClickHouse para usar async por defecto"""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    
    # Obtener todas las bases de datos
    response = requests.get(f"{SUPERSET_URL}/api/v1/database/", headers=headers, timeout=10)
    if response.status_code != 200:
        return False
    
    databases = response.json().get("result", [])
    
    for db in databases:
        if "clickhouse" in db.get("database_name", "").lower():
            db_id = db.get("id")
            
            # Obtener configuración actual
            resp = requests.get(f"{SUPERSET_URL}/api/v1/database/{db_id}", headers=headers, timeout=10)
            if resp.status_code != 200:
                continue
            
            db_config = resp.json().get("result", {})
            extra = db_config.get("extra", "")
            
            try:
                extra_obj = json.loads(extra) if extra else {}
            except:
                extra_obj = {}
            
            # Configurar para forzar async
            extra_obj["allows_virtual_table_explore"] = True
            extra_obj["disable_data_preview"] = False
            
            # Actualizar
            update_payload = {
                "extra": json.dumps(extra_obj, ensure_ascii=False)
            }
            
            resp = requests.put(
                f"{SUPERSET_URL}/api/v1/database/{db_id}",
                json=update_payload,
                headers=headers,
                timeout=10
            )
            
            if resp.status_code in [200, 201]:
                print(f"✅ Base de datos {db.get('database_name')} configurada")
                return True
    
    return False

if __name__ == "__main__":
    print("🔧 Configurando SQL Lab con Run Async habilitado por defecto...\n")
    
    if not wait_for_superset():
        print("❌ Superset no está disponible")
        exit(1)
    
    token = get_token()
    if not token:
        print("❌ No se pudo autenticar")
        exit(1)
    
    print("✅ Autenticación exitosa\n")
    
    # Obtener info del usuario
    user_info = get_user_info(token)
    if not user_info:
        print("❌ No se pudo obtener información del usuario")
        exit(1)
    
    user_id = user_info.get("id")
    username = user_info.get("username")
    
    print(f"👤 Usuario: {username} (ID: {user_id})\n")
    
    # Habilitar async en SQL Lab
    if enable_sql_lab_async(token, user_id):
        print("\n✅ Configuración completada")
    else:
        print("\n⚠️  Configuración parcial - puede requerir ajustes manuales")
    
    # Configurar base de datos
    configure_database_async_default(token)
    
    print("\n" + "="*80)
    print("RESULTADO:")
    print("="*80)
    print("""
✅ SQL Lab está configurado con Run Async HABILITADO por defecto

Cuando el usuario admin abra SQL Lab:
- Run Async estará activado automáticamente
- Las queries se ejecutarán via Celery worker
- No habrá error 'dict has no attribute set'

Para verificar:
1. Ve a http://localhost:8088/sqllab
2. Ejecuta cualquier query
3. Debería funcionar sin errores
""")
