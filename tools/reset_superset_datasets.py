#!/usr/bin/env python3
"""
Script para limpiar TODOS los datasets de Superset y reconfigurarlos desde cero.
Elimina todos los datasets existentes y luego reconfigura los datasets de analytics.

Uso:
  python3 tools/reset_superset_datasets.py
"""
import os
import sys
import time
import requests
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

SUPERSET_URL = os.getenv("SUPERSET_URL", "http://superset:8088")
SUPERSET_ADMIN = os.getenv("SUPERSET_ADMIN", "admin")
SUPERSET_PASSWORD = os.getenv("SUPERSET_PASSWORD", "Admin123!")

session = requests.Session()


def get_csrf_token():
    """Obtiene el CSRF token para operaciones de escritura"""
    try:
        resp = session.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/", timeout=10)
        resp.raise_for_status()
        return resp.json()["result"]
    except Exception as e:
        logger.error(f"Error obteniendo CSRF token: {e}")
        return None


def login():
    """Login en Superset y obtiene el access token"""
    logger.info("🔐 Autenticando en Superset...")
    try:
        resp = session.post(
            f"{SUPERSET_URL}/api/v1/security/login",
            json={
                "username": SUPERSET_ADMIN,
                "password": SUPERSET_PASSWORD,
                "provider": "db",
                "refresh": True
            },
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        resp.raise_for_status()
        data = resp.json()
        
        if "access_token" in data:
            session.headers.update({"Authorization": f"Bearer {data['access_token']}"})
            logger.info("✅ Login exitoso")
            return True
        else:
            logger.error("❌ No se obtuvo access token")
            return False
    except Exception as e:
        logger.error(f"❌ Error en login: {e}")
        return False


def list_all_datasets():
    """Lista todos los datasets en Superset con paginación"""
    logger.info("📋 Listando todos los datasets...")
    all_datasets = []
    page = 0
    page_size = 100
    
    try:
        while True:
            resp = session.get(
                f"{SUPERSET_URL}/api/v1/dataset/",
                params={"q": f'{{"page":{page},"page_size":{page_size}}}'},
                timeout=10
            )
            
            if resp.status_code == 401:
                logger.warning("⚠️ Token expirado, re-autenticando...")
                if not login():
                    return []
                continue
            
            resp.raise_for_status()
            result = resp.json()
            items = result.get("result", [])
            
            if not items:
                break
            
            all_datasets.extend(items)
            
            if len(items) < page_size:
                break
            
            page += 1
        
        logger.info(f"✅ Encontrados {len(all_datasets)} datasets en total")
        return all_datasets
    
    except Exception as e:
        logger.error(f"❌ Error listando datasets: {e}")
        return []


def delete_dataset(dataset_id, schema, table):
    """Elimina un dataset por ID"""
    try:
        csrf_token = get_csrf_token()
        if not csrf_token:
            logger.error(f"❌ No se pudo obtener CSRF token para eliminar {schema}.{table}")
            return False
        
        headers = {
            "X-CSRFToken": csrf_token,
            "Referer": SUPERSET_URL
        }
        
        resp = session.delete(
            f"{SUPERSET_URL}/api/v1/dataset/{dataset_id}",
            headers=headers,
            timeout=10
        )
        
        if resp.status_code in [200, 204]:
            logger.info(f"  ✅ Eliminado: {schema}.{table} (ID: {dataset_id})")
            return True
        else:
            logger.warning(f"  ⚠️ Error eliminando {schema}.{table}: {resp.status_code} - {resp.text[:200]}")
            return False
    
    except Exception as e:
        logger.error(f"  ❌ Error eliminando {schema}.{table}: {e}")
        return False


def delete_all_datasets():
    """Elimina todos los datasets de Superset"""
    datasets = list_all_datasets()
    
    if not datasets:
        logger.info("ℹ️  No hay datasets para eliminar")
        return True
    
    logger.info(f"🗑️  Eliminando {len(datasets)} datasets...")
    deleted = 0
    failed = 0
    
    for ds in datasets:
        dataset_id = ds.get("id")
        schema = ds.get("schema", "")
        table = ds.get("table_name", "")
        
        if delete_dataset(dataset_id, schema, table):
            deleted += 1
        else:
            failed += 1
        
        # Pequeña pausa para no saturar la API
        time.sleep(0.1)
    
    logger.info(f"✅ Eliminados: {deleted} datasets")
    if failed > 0:
        logger.warning(f"⚠️ Fallidos: {failed} datasets")
    
    return failed == 0


def refresh_database_schemas():
    """Refresca los esquemas de las bases de datos en Superset"""
    logger.info("🔄 Refrescando esquemas de bases de datos...")
    try:
        # Obtener lista de bases de datos
        resp = session.get(f"{SUPERSET_URL}/api/v1/database/", timeout=10)
        resp.raise_for_status()
        databases = resp.json().get("result", [])
        
        for db in databases:
            db_id = db.get("id")
            db_name = db.get("database_name", "")
            
            logger.info(f"  🔄 Refrescando: {db_name} (ID: {db_id})")
            
            # Endpoint para refrescar esquemas
            csrf_token = get_csrf_token()
            if not csrf_token:
                continue
            
            headers = {
                "X-CSRFToken": csrf_token,
                "Content-Type": "application/json"
            }
            
            # Forzar refresh de esquemas
            resp = session.post(
                f"{SUPERSET_URL}/api/v1/database/{db_id}/schemas/",
                headers=headers,
                timeout=10
            )
            
            if resp.status_code in [200, 201]:
                logger.info(f"    ✅ Esquemas refrescados para {db_name}")
            else:
                logger.warning(f"    ⚠️ No se pudo refrescar {db_name}: {resp.status_code}")
        
        return True
    
    except Exception as e:
        logger.error(f"❌ Error refrescando esquemas: {e}")
        return False


def main():
    logger.info("=" * 60)
    logger.info("🧹 LIMPIEZA COMPLETA DE DATASETS DE SUPERSET")
    logger.info("=" * 60)
    logger.info("")
    
    # 1. Login
    if not login():
        logger.error("❌ No se pudo autenticar en Superset")
        sys.exit(1)
    
    logger.info("")
    
    # 2. Listar y eliminar todos los datasets
    if not delete_all_datasets():
        logger.warning("⚠️ Algunos datasets no se pudieron eliminar, pero continuando...")
    
    logger.info("")
    
    # 3. Refrescar esquemas
    refresh_database_schemas()
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("✅ LIMPIEZA COMPLETADA")
    logger.info("=" * 60)
    logger.info("")
    logger.info("ℹ️  Ahora puedes ejecutar la reconfiguración de datasets:")
    logger.info("   docker compose exec etl-orchestrator python3 /app/configure_datasets.py")
    logger.info("")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\n⚠️ Operación cancelada por el usuario")
        sys.exit(130)
    except Exception as e:
        logger.error(f"❌ Error inesperado: {e}", exc_info=True)
        sys.exit(1)
