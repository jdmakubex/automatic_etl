#!/usr/bin/env python3
"""
Script para automatizar la conexión de Metabase a ClickHouse vía API REST.
Requiere que Metabase esté corriendo y el usuario admin creado.
"""
import time
import requests
import os
from dotenv import load_dotenv

load_dotenv('/app/.env')

METABASE_URL = os.getenv("METABASE_URL")
LOGIN_ENDPOINT = f"{METABASE_URL}/api/session"
DB_ENDPOINT = f"{METABASE_URL}/api/database"
LOG_PATH = "/app/logs/metabase_clickhouse.log"

METABASE_ADMIN = os.getenv("METABASE_ADMIN")
METABASE_PASSWORD = os.getenv("METABASE_PASSWORD")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CH_USER", "etl")
CLICKHOUSE_PASSWORD = os.getenv("CH_PASSWORD", "Et1Ingest!")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DATABASE", "fgeo_analytics")

def log(msg):
    print(msg)
    try:
        with open(LOG_PATH, "a") as f:
            f.write(msg + "\n")
    except Exception:
        pass

def wait_for_metabase():
    for _ in range(30):
        try:
            r = requests.get(f"{METABASE_URL}/api/health", timeout=3)
            if r.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(5)
    return False

def login_admin():
    resp = requests.post(LOGIN_ENDPOINT, json={"username": METABASE_ADMIN, "password": METABASE_PASSWORD})
    if resp.status_code == 200:
        log("Login de administrador exitoso.")
        return resp.json()["id"]
    else:
        log(f"Error al autenticar admin: {resp.status_code} - {resp.text}")
        return None

def create_clickhouse_db(session_id):
    payload_db = {
        "name": "ClickHouse ETL",
        "engine": "clickhouse",
        "details": {
            "host": CLICKHOUSE_HOST,
            "port": CLICKHOUSE_PORT,
            "user": CLICKHOUSE_USER,
            "password": CLICKHOUSE_PASSWORD,
            "dbname": CLICKHOUSE_DB,
            "ssl": False,
            "additional_options": "",
            "let_user_control_scheduling": True,
            "cache_field_values_schedule": "0 * * * *",  # cada hora
            "metadata_sync_schedule": "0 * * * *"        # cada hora
        },
        "is_full_sync": True,
        "is_on_demand": False,
        "cache_ttl": None
    }
    headers = {"X-Metabase-Session": session_id}
    resp = requests.post(DB_ENDPOINT, json=payload_db, headers=headers)
    if resp.status_code == 200:
        log("Conexión ClickHouse creada correctamente en Metabase.")
        # Forzar sincronización de esquemas
        db_id = resp.json().get("id")
        if db_id:
            sync_url = f"{DB_ENDPOINT}/{db_id}/sync_schema"
            sync_resp = requests.post(sync_url, headers=headers)
            if sync_resp.status_code == 200:
                log(f"Sincronización forzada de esquemas para la base {db_id} exitosa.")
            else:
                log(f"Error al forzar sincronización: {sync_resp.status_code} - {sync_resp.text}")
        else:
            log("No se pudo obtener el ID de la base creada para sincronizar esquemas.")
        return True
    else:
        log(f"Error al crear conexión: {resp.status_code} - {resp.text}")
        return False

if __name__ == "__main__":
    log("Esperando que Metabase esté disponible...")
    if wait_for_metabase():
        log("Metabase disponible. Autenticando admin...")
        session_id = login_admin()
        if session_id:
            log("Creando conexión a ClickHouse...")
            create_clickhouse_db(session_id)
        else:
            log("No se pudo autenticar el usuario admin.")
    else:
        log("Metabase no respondió a tiempo. Verifica el despliegue.")
