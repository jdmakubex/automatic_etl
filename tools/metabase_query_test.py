#!/usr/bin/env python3
"""
Valida que Metabase pueda ejecutar consultas directas sobre ClickHouse y ver los datos.
"""
import os
import requests
from dotenv import load_dotenv

load_dotenv('/app/.env')

METABASE_URL = os.getenv("METABASE_URL")
METABASE_ADMIN = os.getenv("METABASE_ADMIN")
METABASE_PASSWORD = os.getenv("METABASE_PASSWORD")

LOGIN_ENDPOINT = f"{METABASE_URL}/api/session"
DATASET_ENDPOINT = f"{METABASE_URL}/api/dataset"

def login():
    resp = requests.post(LOGIN_ENDPOINT, json={"username": METABASE_ADMIN, "password": METABASE_PASSWORD})
    if resp.status_code == 200:
        return resp.json()["id"]
    return None

def execute_query(session_id, database_id, sql_query):
    """Ejecuta una consulta SQL directa en Metabase"""
    payload = {
        "type": "native",
        "native": {
            "query": sql_query
        },
        "database": database_id
    }
    headers = {"X-Metabase-Session": session_id}
    
    resp = requests.post(DATASET_ENDPOINT, json=payload, headers=headers)
    if resp.status_code == 200:
        result = resp.json()
        print(f"✅ Consulta ejecutada exitosamente")
        print(f"   Filas retornadas: {len(result.get('data', {}).get('rows', []))}")
        print(f"   Columnas: {[col['display_name'] for col in result.get('data', {}).get('cols', [])]}")
        if result.get('data', {}).get('rows'):
            print(f"   Primeras 3 filas: {result['data']['rows'][:3]}")
        return result
    else:
        print(f"❌ Error en consulta: {resp.status_code} - {resp.text}")
        return None

if __name__ == "__main__":
    print("=== VALIDACIÓN DE CONSULTAS EN METABASE ===")
    
    session_id = login()
    if not session_id:
        print("No se pudo autenticar")
        exit(1)
    
    # ID de la base ClickHouse (según el diagnóstico anterior es 2)
    clickhouse_db_id = 2
    
    # Consulta 1: Verificar datos en test_table
    print("\n1. Consultando test_table:")
    execute_query(session_id, clickhouse_db_id, "SELECT * FROM fgeo_analytics.test_table LIMIT 5")
    
    # Consulta 2: Verificar conteo de tablas con datos
    print("\n2. Consultando conteos de tablas:")
    execute_query(session_id, clickhouse_db_id, 
                 "SELECT name, total_rows FROM system.tables WHERE database = 'fgeo_analytics' AND total_rows > 0")
    
    # Consulta 3: Verificar estructura de una tabla vacía
    print("\n3. Verificando estructura de tabla vacía:")
    execute_query(session_id, clickhouse_db_id, 
                 "SELECT * FROM fgeo_analytics.archivos_archivos_raw LIMIT 1")
    
    print("\n=== FIN DE VALIDACIÓN ===")