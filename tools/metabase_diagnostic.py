#!/usr/bin/env python3
"""
Script para diagnosticar la configuración de Metabase y ClickHouse.
Verifica conexión, permisos, tablas y datos disponibles.
"""
import os
import requests
from dotenv import load_dotenv

load_dotenv('/app/.env')

METABASE_URL = os.getenv("METABASE_URL")
METABASE_ADMIN = os.getenv("METABASE_ADMIN")
METABASE_PASSWORD = os.getenv("METABASE_PASSWORD")

LOGIN_ENDPOINT = f"{METABASE_URL}/api/session"
DB_ENDPOINT = f"{METABASE_URL}/api/database"
TABLE_ENDPOINT = f"{METABASE_URL}/api/table"

LOG_PATH = "/app/logs/metabase_diagnostic.log"

def log(msg):
    print(msg)
    try:
        with open(LOG_PATH, "a") as f:
            f.write(msg + "\n")
    except Exception:
        pass

def login():
    resp = requests.post(LOGIN_ENDPOINT, json={"username": METABASE_ADMIN, "password": METABASE_PASSWORD})
    if resp.status_code == 200:
        log("Login exitoso en Metabase.")
        return resp.json()["id"]
    else:
        log(f"Error login: {resp.status_code} - {resp.text}")
        return None

def get_databases(session_id):
    headers = {"X-Metabase-Session": session_id}
    resp = requests.get(DB_ENDPOINT, headers=headers)
    if resp.status_code == 200:
        databases = resp.json()["data"]
        log(f"Bases de datos encontradas: {len(databases)}")
        for db in databases:
            log(f"  - ID: {db['id']}, Nombre: {db['name']}, Engine: {db['engine']}")
        return databases
    else:
        log(f"Error al obtener bases de datos: {resp.status_code} - {resp.text}")
        return []

def get_database_tables(session_id, db_id):
    headers = {"X-Metabase-Session": session_id}
    resp = requests.get(f"{DB_ENDPOINT}/{db_id}/metadata", headers=headers)
    if resp.status_code == 200:
        metadata = resp.json()
        tables = metadata.get("tables", [])
        log(f"Tablas en base ID {db_id}: {len(tables)}")
        for table in tables:
            log(f"  - Tabla: {table['name']}, Schema: {table.get('schema', 'N/A')}, Filas: {table.get('rows', 'N/A')}")
        return tables
    else:
        log(f"Error al obtener tablas de base {db_id}: {resp.status_code} - {resp.text}")
        return []

def test_database_connection(session_id, db_id):
    headers = {"X-Metabase-Session": session_id}
    resp = requests.post(f"{DB_ENDPOINT}/{db_id}/test", headers=headers)
    if resp.status_code == 200:
        log(f"Conexión a base {db_id}: EXITOSA")
        return True
    else:
        log(f"Error al probar conexión a base {db_id}: {resp.status_code} - {resp.text}")
        return False

def sync_database_schema(session_id, db_id):
    headers = {"X-Metabase-Session": session_id}
    resp = requests.post(f"{DB_ENDPOINT}/{db_id}/sync_schema", headers=headers)
    if resp.status_code == 200:
        log(f"Sincronización de esquemas para base {db_id}: EXITOSA")
        return True
    else:
        log(f"Error al sincronizar esquemas de base {db_id}: {resp.status_code} - {resp.text}")
        return False

if __name__ == "__main__":
    log("=== DIAGNÓSTICO DE METABASE ===")
    
    session_id = login()
    if not session_id:
        log("No se pudo autenticar. Abortando diagnóstico.")
        exit(1)
    
    # Obtener todas las bases de datos
    databases = get_databases(session_id)
    
    # Buscar ClickHouse
    clickhouse_db = None
    for db in databases:
        if db["engine"] == "clickhouse":
            clickhouse_db = db
            break
    
    if clickhouse_db:
        log(f"=== ANÁLISIS DE CLICKHOUSE (ID: {clickhouse_db['id']}) ===")
        
        # Probar conexión
        test_database_connection(session_id, clickhouse_db['id'])
        
        # Sincronizar esquemas
        sync_database_schema(session_id, clickhouse_db['id'])
        
        # Obtener tablas
        tables = get_database_tables(session_id, clickhouse_db['id'])
        
        if not tables:
            log("WARNING: No se encontraron tablas en ClickHouse. Esto puede ser un problema de permisos o configuración.")
        else:
            log(f"Se encontraron {len(tables)} tablas en ClickHouse.")
            
    else:
        log("ERROR: No se encontró ninguna base de datos ClickHouse configurada en Metabase.")
    
    log("=== FIN DEL DIAGNÓSTICO ===")