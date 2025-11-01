#!/usr/bin/env python3
"""
Script para verificar que el pipeline funcione con credenciales personalizadas
"""

import os
import requests
import subprocess
import time

def load_env_credentials():
    """Cargar todas las credenciales del .env"""
    credentials = {}
    
    try:
        with open('/mnt/c/proyectos/etl_prod/.env', 'r') as f:
            for line in f:
                line = line.strip()
                if '=' in line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    credentials[key.strip()] = value.strip()
    except Exception as e:
        print(f"⚠️ Error leyendo .env: {e}")
    
    return credentials

def test_superset_credentials(creds):
    """Probar credenciales de Superset"""
    print("🧪 Probando Superset...")
    
    try:
        # Intentar login
        login_data = {
            "username": creds.get('SUPERSET_ADMIN', 'admin'),
            "password": creds.get('SUPERSET_PASSWORD', 'Admin123!'),
            "provider": "db"
        }
        
        response = requests.post(
            "http://localhost:8088/api/v1/security/login",
            json=login_data,
            timeout=10
        )
        
        if response.status_code == 200:
            print("✅ Superset: Credenciales funcionan")
            return True
        else:
            print(f"❌ Superset: Error {response.status_code}")
            return False
            
    except Exception as e:
        print(f"⚠️ Superset: No disponible o error - {e}")
        return None

def test_metabase_credentials(creds):
    """Probar credenciales de Metabase"""
    print("🧪 Probando Metabase...")
    
    try:
        # Intentar login
        login_data = {
            "username": creds.get('METABASE_ADMIN', 'admin@admin.com'),
            "password": creds.get('METABASE_PASSWORD', 'Admin123!')
        }
        
        response = requests.post(
            "http://localhost:3000/api/session",
            json=login_data,
            timeout=10
        )
        
        if response.status_code == 200:
            print("✅ Metabase: Credenciales funcionan")
            return True
        else:
            print(f"❌ Metabase: Error {response.status_code}")
            return False
            
    except Exception as e:
        print(f"⚠️ Metabase: No disponible o error - {e}")
        return None

def test_clickhouse_credentials(creds):
    """Probar credenciales de ClickHouse"""
    print("🧪 Probando ClickHouse...")
    
    try:
        user = creds.get('CLICKHOUSE_DEFAULT_USER', 'default')
        password = creds.get('CLICKHOUSE_DEFAULT_PASSWORD', 'ClickHouse123!')
        
        # Usar curl para probar HTTP interface
        cmd = [
            'curl', '-s', 
            f'http://{user}:{password}@localhost:8123',
            '-d', 'SELECT 1'
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0 and '1' in result.stdout:
            print("✅ ClickHouse: Credenciales funcionan")
            return True
        else:
            print(f"❌ ClickHouse: Error en conexión")
            return False
            
    except Exception as e:
        print(f"⚠️ ClickHouse: Error - {e}")
        return None

def main():
    """Función principal de verificación"""
    
    print("🔐 VERIFICACIÓN DE CREDENCIALES PERSONALIZADAS")
    print("="*70)
    
    # Cargar credenciales
    creds = load_env_credentials()
    
    print("📋 CREDENCIALES CARGADAS:")
    important_creds = [
        'SUPERSET_ADMIN', 'SUPERSET_PASSWORD',
        'METABASE_ADMIN', 'METABASE_PASSWORD', 
        'CLICKHOUSE_DEFAULT_USER', 'CLICKHOUSE_DEFAULT_PASSWORD'
    ]
    
    for key in important_creds:
        value = creds.get(key, 'NO DEFINIDO')
        if 'PASSWORD' in key:
            print(f"   {key}: {value[:4]}***")
        else:
            print(f"   {key}: {value}")
    
    print("\n🧪 PROBANDO CONEXIONES:")
    
    # Probar cada servicio
    superset_ok = test_superset_credentials(creds)
    metabase_ok = test_metabase_credentials(creds)
    clickhouse_ok = test_clickhouse_credentials(creds)
    
    # Resultado final
    print("\n🎯 RESULTADO FINAL:")
    print("="*40)
    
    total_services = 0
    working_services = 0
    
    for service, status in [("Superset", superset_ok), ("Metabase", metabase_ok), ("ClickHouse", clickhouse_ok)]:
        if status is not None:
            total_services += 1
            if status:
                working_services += 1
                print(f"✅ {service}: Funcionando con credenciales personalizadas")
            else:
                print(f"❌ {service}: Problemas con credenciales")
        else:
            print(f"⏳ {service}: Servicio no disponible para prueba")
    
    print(f"\n📊 Servicios funcionando: {working_services}/{total_services}")
    
    if working_services == total_services and total_services > 0:
        print("\n🎉 ¡TODAS LAS CREDENCIALES FUNCIONAN CORRECTAMENTE!")
        print("✅ Puedes cambiar las credenciales en .env sin problemas")
        return True
    else:
        print("\n⚠️  ALGUNAS CREDENCIALES REQUIEREN ATENCIÓN")
        print("💡 Verifica que los servicios estén corriendo y configurados correctamente")
        return False

if __name__ == "__main__":
    main()
