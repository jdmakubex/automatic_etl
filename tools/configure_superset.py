#!/usr/bin/env python3
"""
Script para configurar automáticamente Superset con las conexiones correctas
después de la depuración
"""

import requests
import json
import time
import sys

def wait_for_superset(max_attempts=10):
    """Esperar a que Superset esté disponible"""
    print("⏳ Esperando que Superset esté disponible...")
    
    for attempt in range(max_attempts):
        try:
            response = requests.get("http://localhost:8088/health", timeout=10)
            if response.status_code == 200:
                print("✅ Superset disponible")
                return True
        except:
            pass
        
        print(f"   Intento {attempt + 1}/{max_attempts}...")
        time.sleep(5)
    
    return False

def login_superset():
    """Hacer login en Superset"""
    print("🔑 Iniciando sesión en Superset...")
    
    login_data = {
        "username": "admin",
        "password": "admin",
        "provider": "db",
        "refresh": True
    }
    
    try:
        response = requests.post(
            "http://localhost:8088/api/v1/security/login",
            json=login_data,
            timeout=15
        )
        
        if response.status_code == 200:
            token = response.json().get("access_token")
            print("✅ Login exitoso")
            return token
        else:
            print(f"❌ Error de login: {response.status_code}")
            print(response.text)
            return None
            
    except Exception as e:
        print(f"❌ Error conectando: {e}")
        return None

def configure_clickhouse_connection(token):
    """Configurar conexión a ClickHouse"""
    print("🔧 Configurando conexión ClickHouse...")
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Verificar conexiones existentes
    try:
        db_response = requests.get(
            "http://localhost:8088/api/v1/database/",
            headers=headers,
            timeout=15
        )
        
        clickhouse_db_id = None
        if db_response.status_code == 200:
            databases = db_response.json().get("result", [])
            for db in databases:
                if "clickhouse" in db.get("database_name", "").lower():
                    clickhouse_db_id = db["id"]
                    print(f"✅ Conexión existente encontrada: ID {clickhouse_db_id}")
                    break
        
        # Crear nueva conexión si no existe
        if not clickhouse_db_id:
            print("📝 Creando nueva conexión...")
            
            db_config = {
                "database_name": "ClickHouse_ETL_Clean",
                "sqlalchemy_uri": "clickhouse://default:ClickHouse123!@clickhouse:9000/default",
                "expose_in_sqllab": True,
                "allow_ctas": True,
                "allow_cvas": True,
                "allow_dml": False,
                "extra": json.dumps({
                    "allows_virtual_table_explore": True,
                    "schemas_allowed_for_csv_upload": ["archivos", "fiscalizacion"]
                })
            }
            
            create_response = requests.post(
                "http://localhost:8088/api/v1/database/",
                json=db_config,
                headers=headers,
                timeout=20
            )
            
            if create_response.status_code in [200, 201]:
                result = create_response.json()
                clickhouse_db_id = result.get("id")
                print(f"✅ Nueva conexión creada: ID {clickhouse_db_id}")
            else:
                print(f"❌ Error creando conexión: {create_response.status_code}")
                print(create_response.text)
                return None
        
        # Refrescar esquemas
        if clickhouse_db_id:
            print("🔄 Refrescando esquemas...")
            refresh_response = requests.post(
                f"http://localhost:8088/api/v1/database/{clickhouse_db_id}/refresh/",
                headers=headers,
                timeout=30
            )
            
            if refresh_response.status_code == 200:
                print("✅ Esquemas refrescados correctamente")
                return clickhouse_db_id
            else:
                print(f"⚠️ Error refrescando esquemas: {refresh_response.status_code}")
                return clickhouse_db_id
        
    except Exception as e:
        print(f"❌ Error configurando conexión: {e}")
        return None

def create_essential_datasets(token, db_id):
    """Crear datasets esenciales"""
    print("📊 Creando datasets esenciales...")
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Datasets esenciales
    essential_datasets = [
        {
            "schema": "archivos",
            "table_name": "src__archivos__archivos__archivos",
            "description": "Tabla principal de archivos (17K registros)"
        },
        {
            "schema": "fiscalizacion", 
            "table_name": "src__fiscalizacion__fiscalizacion__bitacora",
            "description": "Bitácora de fiscalización (513K registros)"
        },
        {
            "schema": "fiscalizacion",
            "table_name": "src__fiscalizacion__fiscalizacion__ofeindisdup",
            "description": "Datos de ofendidos fiscalización (209K registros)"
        }
    ]
    
    created_count = 0
    for dataset in essential_datasets:
        try:
            dataset_config = {
                "database": db_id,
                "schema": dataset["schema"],
                "table_name": dataset["table_name"],
                "description": dataset["description"]
            }
            
            response = requests.post(
                "http://localhost:8088/api/v1/dataset/",
                json=dataset_config,
                headers=headers,
                timeout=15
            )
            
            if response.status_code in [200, 201]:
                print(f"✅ Dataset: {dataset['schema']}.{dataset['table_name']}")
                created_count += 1
            elif response.status_code == 422:
                print(f"ℹ️ Ya existe: {dataset['schema']}.{dataset['table_name']}")
            else:
                print(f"⚠️ Error: {dataset['table_name']} - {response.status_code}")
                
        except Exception as e:
            print(f"❌ Error creando dataset {dataset['table_name']}: {e}")
    
    print(f"✅ Datasets procesados. Nuevos creados: {created_count}")
    return created_count

def main():
    """Función principal"""
    print("🚀 CONFIGURADOR AUTOMÁTICO DE SUPERSET")
    print("="*50)
    
    # 1. Esperar Superset
    if not wait_for_superset():
        print("❌ Superset no está disponible")
        sys.exit(1)
    
    # 2. Login
    token = login_superset()
    if not token:
        print("❌ No se pudo hacer login")
        sys.exit(1)
    
    # 3. Configurar conexión
    db_id = configure_clickhouse_connection(token)
    if not db_id:
        print("❌ No se pudo configurar la conexión")
        sys.exit(1)
    
    # 4. Crear datasets
    datasets_created = create_essential_datasets(token, db_id)
    
    # 5. Resultado
    print(f"\n🎉 CONFIGURACIÓN COMPLETADA")
    print(f"✅ Conexión ClickHouse: ID {db_id}")
    print(f"✅ Datasets disponibles en Superset")
    print(f"\n📊 ACCESO:")
    print(f"   🔗 Superset: http://localhost:8088")
    print(f"   👤 Login: admin / admin")
    print(f"   📋 Ve a 'Data' → 'Datasets' para ver las tablas")
    print(f"   🔍 Usa SQL Lab para consultas directas")

if __name__ == "__main__":
    main()