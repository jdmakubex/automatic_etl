#!/usr/bin/env python3
"""
Script de limpieza automática de datasets del esquema base en Superset.
Elimina todos los datasets cuyo esquema sea 'fiscalizacion' (o cualquier otro especificado)
para evitar duplicados y asegurar que solo los datasets de analytics sean visibles.

Este script está diseñado para ser llamado desde el orquestador, sin intervención manual.

Variables de entorno:
- SUPERSET_URL: URL de Superset (ej: http://superset:8088)
- SUPERSET_ADMIN: usuario admin
- SUPERSET_PASSWORD: contraseña admin
- BASE_SCHEMA: esquema base a eliminar (por defecto: fiscalizacion)

Uso:
  python3 tools/cleanup_superset_base_datasets.py
"""
import os
import sys
import requests
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SUPERSET_URL = os.getenv("SUPERSET_URL", "http://superset:8088")
SUPERSET_ADMIN = os.getenv("SUPERSET_ADMIN", "admin")
SUPERSET_PASSWORD = os.getenv("SUPERSET_PASSWORD", "Admin123!")
BASE_SCHEMA = os.getenv("BASE_SCHEMA", "fiscalizacion")

session = requests.Session()


def get_csrf_token():
    resp = session.get(f"{SUPERSET_URL}/api/v1/security/csrf_token")
    resp.raise_for_status()
    return resp.json()["result"]['csrf_token']


def login(retry=True):
    """Login a Superset con reintentos."""
    try:
        resp = session.post(f"{SUPERSET_URL}/api/v1/security/login", json={
            "username": SUPERSET_ADMIN,
            "password": SUPERSET_PASSWORD,
            "provider": "db",
            "refresh": True
        })
        resp.raise_for_status()
        data = resp.json()
        
        # Guardar access token si está disponible
        if "access_token" in data:
            session.headers.update({"Authorization": f"Bearer {data['access_token']}"})
        
        logger.info("🔑 Login exitoso en Superset")
        return True
    except requests.HTTPError as e:
        if retry and e.response.status_code == 401:
            logger.warning("⚠️ Error 401 en login, reintentando...")
            import time
            time.sleep(2)
            return login(retry=False)
        logger.error(f"❌ Error en login: {e}")
        raise


def list_datasets(retry_on_auth_error=True):
    """Lista datasets con reintentos en caso de error de autenticación."""
    # Paginación para obtener todos los datasets
    datasets = []
    page = 0
    page_size = 100
    try:
        while True:
            resp = session.get(f"{SUPERSET_URL}/api/v1/dataset/?q={{\"page\":{page},\"page_size\":{page_size}}}")
            
            # Si es 401, re-autenticar y reintentar
            if resp.status_code == 401 and retry_on_auth_error:
                logger.warning("⚠️ Token expirado, re-autenticando...")
                login()
                return list_datasets(retry_on_auth_error=False)
            
            resp.raise_for_status()
            result = resp.json()
            items = result.get("result", [])
            if not items:
                break
            datasets.extend(items)
            if len(items) < page_size:
                break
            page += 1
        return datasets
    except Exception as e:
        logger.error(f"Error listando datasets: {e}")
        raise


def delete_dataset(dataset_id):
    csrf_token = get_csrf_token()
    headers = {"X-CSRFToken": csrf_token}
    resp = session.delete(f"{SUPERSET_URL}/api/v1/dataset/{dataset_id}", headers=headers)
    if resp.status_code == 200:
        logger.info(f"🗑️ Dataset {dataset_id} eliminado correctamente")
    else:
        logger.warning(f"⚠️ Error eliminando dataset {dataset_id}: {resp.text}")


def main():
    try:
        login()
        datasets = list_datasets()
        
        # Verificar tipo de datos
        if not isinstance(datasets, list):
            logger.error(f"❌ Error: datasets no es una lista, es {type(datasets)}")
            sys.exit(1)
        
        # Filtrar datasets del esquema base
        base_datasets = []
        for d in datasets:
            if not isinstance(d, dict):
                logger.warning(f"⚠️ Ignorando item no-dict: {type(d)}")
                continue
            if d.get("schema") == BASE_SCHEMA:
                base_datasets.append(d)
        
        logger.info(f"Encontrados {len(base_datasets)} datasets en el esquema base '{BASE_SCHEMA}' para eliminar.")
        
        for d in base_datasets:
            delete_dataset(d["id"])
        
        logger.info("✅ Limpieza de datasets del esquema base completada.")
    except Exception as e:
        logger.error(f"❌ Error en limpieza de datasets: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()
