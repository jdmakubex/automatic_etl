#!/usr/bin/env python3
"""
Automatiza la creación de dashboards en Metabase vía API REST.
Lee credenciales y configuración desde .env.
"""
import os
import requests
from dotenv import load_dotenv

load_dotenv('/app/.env')

METABASE_URL = os.getenv("METABASE_URL")
METABASE_ADMIN = os.getenv("METABASE_ADMIN")
METABASE_PASSWORD = os.getenv("METABASE_PASSWORD")

LOGIN_ENDPOINT = f"{METABASE_URL}/api/session"
DASHBOARD_ENDPOINT = f"{METABASE_URL}/api/dashboard"

# Ejemplo: crea un dashboard vacío y lo retorna

def login():
    resp = requests.post(LOGIN_ENDPOINT, json={"username": METABASE_ADMIN, "password": METABASE_PASSWORD})
    if resp.status_code == 200:
        return resp.json()["id"]
    else:
        print(f"Error login: {resp.status_code} - {resp.text}")
        return None

def create_dashboard(session_id, name="ETL Dashboard", description="Dashboard automático del pipeline ETL"):
    payload = {
        "name": name,
        "description": description
    }
    headers = {"X-Metabase-Session": session_id}
    resp = requests.post(DASHBOARD_ENDPOINT, json=payload, headers=headers)
    if resp.status_code == 200:
        print(f"Dashboard '{name}' creado correctamente. ID: {resp.json()['id']}")
        return resp.json()['id']
    else:
        print(f"Error al crear dashboard: {resp.status_code} - {resp.text}")
        return None

if __name__ == "__main__":
    session_id = login()
    if session_id:
        create_dashboard(session_id)
    else:
        print("No se pudo autenticar en Metabase.")
