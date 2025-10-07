#!/usr/bin/env python3
"""
📊 PROVISION SUPERSET AUTOMÁTICO INTEGRADO
Configura Superset automáticamente con logging detallado
"""

import os, time, json, sys
import requests
import logging
from datetime import datetime
# from tenacity import retry, stop_after_delay, wait_fixed  # Disabled for simplicity

# Determinar la ruta base para logs
if os.path.exists('/app/logs'):
    log_dir = '/app/logs'  # En contenedor
else:
    log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')  # En host

logging.basicConfig(
    level=logging.DEBUG,  # Debug para más detalles
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'superset_setup.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuración Superset - detectar si estamos en contenedor o host
if os.path.exists('/app/logs'):
    # En contenedor
    SUP_URL = os.getenv('SUPERSET_URL', 'http://superset:8088')
else:
    # En host
    SUP_URL = os.getenv('SUPERSET_URL', 'http://localhost:8088')

ADMIN = os.getenv('SUPERSET_ADMIN', 'admin')
PASSWORD = os.getenv('SUPERSET_PASSWORD', 'Admin123!')
# Configuración ClickHouse - detectar si estamos en contenedor o host
if os.path.exists('/app/logs'):
    # En contenedor
    CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
else:
    # En host
    CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")

CH_DB     = os.getenv("CLICKHOUSE_DATABASE", "fgeo_analytics")
CH_USER   = os.getenv("CLICKHOUSE_USER", "default")
CH_PASS   = os.getenv("CLICKHOUSE_PASSWORD", "")
CH_PORT   = int(os.getenv("CLICKHOUSE_PORT", "8123"))
MAKE_DEMO = os.getenv("PROVISION_SAMPLE", "true").lower() in ("1","true","yes","y")

SESSION = requests.Session()
SESSION.headers.update({"Content-Type": "application/json"})

def uri():
    pwd = f":{CH_PASS}" if CH_PASS else ""
    # Para la URI de conexión, Superset siempre necesita el nombre del contenedor
    # porque el script corre desde el host pero la conexión la hace Superset desde su contenedor
    ch_host_for_superset = "clickhouse"  # Siempre usar nombre del servicio
    return f"clickhousedb+connect://{CH_USER}{pwd}@{ch_host_for_superset}:{CH_PORT}/{CH_DB}"

def wait_superset_ready(max_retries=40):
    """Esperar a que Superset esté listo con reintentos"""
    logger.info("⏳ Esperando a que Superset esté disponible...")
    
    for i in range(max_retries):
        try:
            r = SESSION.get(f"{SUP_URL}/health", timeout=10)
            r.raise_for_status()
            
            # El endpoint /health devuelve texto plano "OK", no JSON
            if r.text.strip() == "OK":
                logger.info("✅ Superset está disponible")
                return
            else:
                logger.debug(f"Respuesta health: {r.text}")
                
        except Exception as e:
            logger.debug(f"Intento {i+1}/{max_retries}: {str(e)}")
            if i < max_retries - 1:
                time.sleep(3)
    
    raise RuntimeError(f"Superset no disponible después de {max_retries * 3}s")

def login():
    # /api/v1/security/login
    payload = {
        "username": ADMIN,
        "password": PASSWORD,
        "provider": "db",
        "refresh": True
    }
    r = SESSION.post(f"{SUP_URL}/api/v1/security/login", json=payload)
    r.raise_for_status()
    access = r.json()["access_token"]
    SESSION.headers.update({"Authorization": f"Bearer {access}"})
    
    # Obtener CSRF token
    csrf_r = SESSION.get(f"{SUP_URL}/api/v1/security/csrf_token/")
    csrf_r.raise_for_status()
    csrf_token = csrf_r.json()["result"]
    SESSION.headers.update({"X-CSRFToken": csrf_token})
    logger.debug(f"🔐 CSRF token obtenido")

def get_database_id_by_name(name):
    """Buscar base de datos por nombre, fallback a búsqueda completa si falla el filtro"""
    try:
        # Intentar búsqueda directa primero
        r = SESSION.get(f"{SUP_URL}/api/v1/database/")
        r.raise_for_status()
        res = r.json()
        for item in res.get("result", []):
            if item.get("database_name") == name:
                return item["id"]
        return None
    except Exception as e:
        logger.warning(f"⚠️  Error buscando base de datos: {e}")
        return None

def create_or_update_database(name, sqlalchemy_uri):
    db_id = get_database_id_by_name(name)
    payload = {
        "database_name": name,
        "sqlalchemy_uri": sqlalchemy_uri,
        "expose_in_sqllab": True,
        "allow_ctas": True,
        "allow_cvas": True
    }
    
    logger.debug(f"📝 Payload DB: {payload}")
    
    if db_id:
        logger.info(f"🔄 Actualizando DB existente ID: {db_id}")
        r = SESSION.put(f"{SUP_URL}/api/v1/database/{db_id}", json=payload)
        r.raise_for_status()
        return db_id
        
    logger.info(f"➕ Creando nueva DB: {name}")
    r = SESSION.post(f"{SUP_URL}/api/v1/database/", json=payload)
    try:
        r.raise_for_status()
        return r.json()["id"]
    except Exception as e:
        logger.error(f"❌ Error creando DB. Response: {r.text}")
        raise

def get_dataset_id(db_id, schema, table):
    q = f"(filters:!((col:database,opr:eq,value:{db_id}),(col:schema,opr:eq,value:'{schema}'),(col:table_name,opr:eq,value:'{table}')))"
    r = SESSION.get(f"{SUP_URL}/api/v1/dataset/?q={q}")
    r.raise_for_status()
    res = r.json().get("result", [])
    return res[0]["id"] if res else None

def create_dataset(db_id, schema, table):
    payload = {
        "database": db_id,
        "schema": schema,
        "table_name": table
    }
    r = SESSION.post(f"{SUP_URL}/api/v1/dataset/", json=payload)
    r.raise_for_status()
    return r.json()["id"]

def create_datasets_for_etl_tables(db_id):
    """Crear datasets para las tablas del ETL"""
    try:
        # Cargar metadatos de tablas si existen
        metadata_file = '/app/generated/default/tables_metadata.json'
        tables_to_create = ['archivos']  # Default fallback
        
        if os.path.exists(metadata_file):
            try:
                with open(metadata_file, 'r') as f:
                    tables_data = json.load(f)
                tables_to_create = [t['name'] for t in tables_data]
                logger.info(f"📋 Usando {len(tables_to_create)} tablas desde metadatos")
            except Exception as e:
                logger.warning(f"⚠️  Error leyendo metadatos: {e}")
        
        created_count = 0
        for table_name in tables_to_create:
            try:
                logger.info(f"📊 Creando dataset para: {table_name}")
                ds_id = get_dataset_id(db_id, CH_DB, table_name)
                
                if ds_id:
                    logger.info(f"ℹ️  Dataset {table_name} ya existe (ID: {ds_id})")
                else:
                    ds_id = create_dataset(db_id, CH_DB, table_name)
                    logger.info(f"✅ Dataset {table_name} creado (ID: {ds_id})")
                
                created_count += 1
                
            except requests.HTTPError as e:
                logger.warning(f"⚠️  No se pudo crear dataset {table_name}: {e}")
            except Exception as e:
                logger.error(f"❌ Error con dataset {table_name}: {e}")
        
        return created_count
        
    except Exception as e:
        logger.error(f"💥 Error creando datasets: {e}")
        return 0

def main():
    """Función principal con logging mejorado"""
    start_time = datetime.now()
    logger.info("🚀 === INICIANDO CONFIGURACIÓN DE SUPERSET ===")
    logger.info(f"⏰ Inicio: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # 1. Esperar Superset
        logger.info("⏳ Esperando Superset...")
        wait_superset_ready()
        
        # 2. Login
        logger.info("🔐 Autenticando...")
        login()
        logger.info("✅ Autenticación exitosa")
        
        # 3. Crear conexión ClickHouse
        db_name = f"ClickHouse_ETL_{CH_HOST}_{CH_DB}"
        uri_string = uri()
        logger.info(f"🏠 Creando/actualizando DB: {db_name}")
        db_id = create_or_update_database(db_name, uri_string)
        logger.info(f"✅ Database configurada (ID: {db_id})")
        
        # 4. Crear datasets para tablas ETL
        logger.info("📊 Creando datasets para tablas ETL...")
        datasets_created = create_datasets_for_etl_tables(db_id)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info(f"\n🏁 === CONFIGURACIÓN DE SUPERSET COMPLETADA ===")
        logger.info(f"⏰ Duración: {duration:.1f} segundos")
        logger.info(f"🏠 Database ID: {db_id}")
        logger.info(f"📊 Datasets creados: {datasets_created}")
        logger.info(f"🌐 URL: {SUP_URL}")
        logger.info(f"👤 Usuario: {ADMIN}")
        
        if db_id and datasets_created > 0:
            logger.info("🎉 SUPERSET CONFIGURADO EXITOSAMENTE")
            print(f"\n{'='*60}")
            print("🎉 CONFIGURACIÓN DE SUPERSET EXITOSA")
            print(f"🌐 Acceso: {SUP_URL}")
            print(f"👤 Usuario: {ADMIN} / {PASSWORD}")
            print(f"📊 Datasets: {datasets_created} creados")
            print(f"{'='*60}")
            return True
        else:
            logger.warning("⚠️  CONFIGURACIÓN PARCIAL")
            return False
        
    except Exception as e:
        logger.error(f"💥 Error crítico en configuración: {str(e)}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
