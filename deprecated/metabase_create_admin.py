#!/usr/bin/env python3
"""
Script para automatizar la creación del usuario administrador en Metabase vía API REST.
Se puede ejecutar desde cualquier contenedor con acceso a la red interna y Metabase corriendo.
"""
import time
import requests
import json
import os
from dotenv import load_dotenv

load_dotenv('/app/.env')

METABASE_URL = os.getenv("METABASE_URL")
SETUP_ENDPOINT = f"{METABASE_URL}/api/setup"
SESSION_ENDPOINT = f"{METABASE_URL}/api/session"
SETUP_TOKEN_ENDPOINT = f"{METABASE_URL}/api/session/properties"
LOG_PATH = "/app/logs/metabase_admin.log"

ADMIN_EMAIL = os.getenv("METABASE_ADMIN")
ADMIN_PASSWORD = os.getenv("METABASE_PASSWORD")

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
    return False

def is_setup_required():
    try:
        r = requests.get(SETUP_TOKEN_ENDPOINT, timeout=5)
        if r.status_code == 200:
            props = r.json()
            return props.get("setup-token") is not None
    except Exception:
        pass
    return False

def get_setup_token():
    try:
        r = requests.get(SETUP_TOKEN_ENDPOINT, timeout=5)
        if r.status_code == 200:
            props = r.json()
            return props.get("setup-token")
    except Exception:
        pass
    return None

def create_admin():
    token = get_setup_token()
    if not token:
        log("No se pudo obtener el token de configuración. Metabase probablemente ya está configurado. No se intentará crear el usuario admin.")
        return False
    else:
        payload = {
            "token": token,
            "user": {
                "first_name": "Admin",
                "last_name": "User",
                "email": ADMIN_EMAIL,
                "password": ADMIN_PASSWORD
            },
            "prefs": {
                "site_name": "Mi Metabase",
                "site_locale": "es",
                "allow_tracking": False
            }
        }
        resp = requests.post(SETUP_ENDPOINT, json=payload)
        if resp.status_code == 200:
            log("Usuario administrador creado correctamente.")
            return True
        else:
            log(f"Error al crear usuario: {resp.status_code} - {resp.text}")
            return False

def login_admin():
    resp = requests.post(SESSION_ENDPOINT, json={"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD})
    if resp.status_code == 200:
        log("Login de administrador exitoso.")
        return True
    else:
        log(f"Error al autenticar admin: {resp.status_code} - {resp.text}")
        return False

if __name__ == "__main__":
    log("Esperando que Metabase esté disponible...")
    if wait_for_metabase():
        log("Metabase disponible.")
        if is_setup_required():
            log("Metabase requiere configuración inicial. Creando usuario admin...")
            resultado = create_admin()
            if not resultado:
                log("No se pudo crear el usuario admin. Verifica si Metabase ya está configurado o revisa los errores anteriores.")
        else:
            log("Metabase ya está configurado. Intentando login...")
            login_admin()
    else:
        log("Metabase no respondió a tiempo. Verifica el despliegue.")

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


