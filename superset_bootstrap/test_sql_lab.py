#!/usr/bin/env python3
"""
Script para probar SQL Lab programáticamente
"""
import requests
import json
import os
import time

SUPERSET_URL = os.getenv("SUPERSET_URL", "http://localhost:8088")
SUPERSET_ADMIN = os.getenv("SUPERSET_ADMIN", "admin")
SUPERSET_PASSWORD = os.getenv("SUPERSET_PASSWORD", "Admin123!")

def get_token():
    """Obtener token de autenticación"""
    response = requests.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json={"username": SUPERSET_ADMIN, "password": SUPERSET_PASSWORD, "provider": "db", "refresh": True},
        headers={"Content-Type": "application/json"}
    )
    if response.status_code == 200:
        return response.json().get("access_token")
    return None

def get_database_id(token):
    """Obtener ID de la base de datos ClickHouse"""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    
    response = requests.get(f"{SUPERSET_URL}/api/v1/database/", headers=headers)
    if response.status_code == 200:
        databases = response.json().get("result", [])
        for db in databases:
            if "clickhouse" in db.get("database_name", "").lower():
                return db.get("id")
    return None

def execute_sql_query(token, database_id, sql):
    """Ejecutar una query SQL en SQL Lab"""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    
    payload = {
        "database_id": database_id,
        "sql": sql,
        "schema": "fiscalizacion",
        "runAsync": True,  # Usar modo async
        "queryLimit": 1000
    }
    
    print(f"📤 Enviando query: {sql[:100]}...")
    response = requests.post(
        f"{SUPERSET_URL}/api/v1/sqllab/execute/",
        json=payload,
        headers=headers
    )
    
    if response.status_code in [200, 201]:
        result = response.json()
        print(f"✅ Query aceptada")
        return result
    else:
        print(f"❌ Error ejecutando query: {response.status_code}")
        print(f"   Response: {response.text}")
        return None

def check_query_status(token, client_id):
    """Verificar el estado de una query asíncrona usando client_id"""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    
    # Esperar a que el worker procese (async)
    max_attempts = 10
    for attempt in range(max_attempts):
        time.sleep(1)
        
        response = requests.get(
            f"{SUPERSET_URL}/api/v1/sqllab/results/?key={client_id}",
            headers=headers
        )
        
        if response.status_code == 200:
            result = response.json()
            status = result.get("status")
            
            if status == "success":
                data = result.get("data", [])
                print(f"✅ Query completada - Registros: {len(data)}")
                if data and len(data) > 0:
                    print(f"   Primera fila: {data[0]}")
                return result
            elif status == "failed":
                error = result.get("error", "Unknown error")
                print(f"❌ Query falló: {error}")
                return result
            elif status in ["pending", "running"]:
                if attempt < max_attempts - 1:
                    print(f"⏳ Esperando... (intento {attempt + 1}/{max_attempts})")
                    continue
                else:
                    print(f"⚠️  Timeout esperando resultados")
                    return result
            else:
                print(f"📊 Estado: {status}")
                return result
        else:
            print(f"⚠️  Error obteniendo resultados: {response.status_code} - {response.text[:200]}")
            return None
    
    return None

if __name__ == "__main__":
    print("🧪 Probando SQL Lab...")
    
    token = get_token()
    if not token:
        print("❌ No se pudo obtener token")
        exit(1)
    
    db_id = get_database_id(token)
    if not db_id:
        print("❌ No se encontró base de datos ClickHouse")
        exit(1)
    
    print(f"✅ Database ID: {db_id}")
    
    # Test 1: Query simple sin fechas
    print("\n📝 Test 1: Query simple (COUNT)")
    result = execute_sql_query(token, db_id, 
        "SELECT count(*) as total FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora")
    
    if result and result.get("query", {}).get("id"):
        check_query_status(token, result["query"]["id"])
    
    # Test 2: Query con fecha directa (sin transformaciones)
    print("\n📝 Test 2: Query con fecha (sin transformaciones)")
    result = execute_sql_query(token, db_id,
        "SELECT fecha, count(*) as total FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora GROUP BY fecha ORDER BY fecha DESC LIMIT 10")
    
    if result and result.get("query", {}).get("id"):
        check_query_status(token, result["query"]["id"])
    
    # Test 3: JOIN entre dos tablas
    print("\n📝 Test 3: JOIN entre tablas")
    result = execute_sql_query(token, db_id,
        """SELECT 
            b.id, 
            b.fecha,
            ai.registro
        FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora b
        LEFT JOIN fiscalizacion.src__fiscalizacion__fiscalizacion__altoimpacto ai 
            ON b.expedientes_id = ai.expedientes_id
        LIMIT 10""")
    
    if result and result.get("query", {}).get("id"):
        check_query_status(token, result["query"]["id"])
