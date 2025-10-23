#!/usr/bin/env python3
"""
🏗️ CONFIGURADOR AUTOMÁTICO DE CLICKHOUSE
Script que se ejecuta automáticamente al levantar el contenedor para:
- Crear bases de datos necesarias
- Configurar usuarios y permisos
- Crear tablas iniciales con datos de prueba
- Validar configuración
"""

import os
import sys
import time
import json
import logging
import requests
import subprocess
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables de entorno
ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

# Configurar logging
# Logging a archivo y consola con formato uniforme
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/clickhouse_setup.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def wait_for_clickhouse():
    """Esperar a que ClickHouse esté disponible"""
    logger.info("⏳ Esperando ClickHouse...")
    max_wait = 60
    start_time = time.time()
    
    while (time.time() - start_time) < max_wait:
        try:
            response = requests.get("http://clickhouse:8123/ping", timeout=5)
            if response.status_code == 200:
                logger.info("✅ ClickHouse disponible")
                return True
        except:
            time.sleep(2)
    
    logger.error("❌ ClickHouse no disponible")
    return False

def _ch_auth_headers():
    """Construye headers de autenticación para ClickHouse HTTP API."""
    user = os.getenv('CH_USER') or os.getenv('CLICKHOUSE_USER') or os.getenv('CLICKHOUSE_DEFAULT_USER', 'default')
    password = os.getenv('CH_PASSWORD') or os.getenv('CLICKHOUSE_PASSWORD') or os.getenv('CLICKHOUSE_DEFAULT_PASSWORD', '')
    headers = {}
    if user:
        headers['X-ClickHouse-User'] = user
    if password:
        headers['X-ClickHouse-Key'] = password
    return headers

def execute_clickhouse_query(query, database=None, retries=3, backoff=2):
    """Ejecutar query en ClickHouse con reintentos y backoff exponencial"""
    url = "http://clickhouse:8123/"
    if database:
        url += f"?database={database}"
    attempt = 0
    last_err = ""
    headers = _ch_auth_headers()
    while attempt <= retries:
        attempt += 1
        try:
            t0 = time.time()
            response = requests.post(url, data=query, timeout=30, headers=headers)
            dt = time.time() - t0
            if response.status_code == 200:
                if dt > 1.0:
                    logger.info(f"Query OK en {dt:.2f}s: {query.split()[0]} ...")
                return True, response.text.strip()
            last_err = response.text
            logger.warning(f"Intento {attempt}/{retries}: fallo ejecutando query ({dt:.2f}s): {last_err[:200]}")
        except Exception as e:
            last_err = str(e)
            logger.warning(f"Intento {attempt}/{retries}: excepción ejecutando query: {last_err}")
        if attempt <= retries:
            sleep_s = backoff ** attempt
            time.sleep(min(sleep_s, 10))
    return False, last_err

def preflight_auth_check():
    """Valida autenticación antes de configurar bases/usuarios."""
    ok, out = execute_clickhouse_query("SELECT 1")
    if not ok:
        logger.error(f"❌ Autenticación ClickHouse falló: {out}")
        return False
    logger.info("🔐 Autenticación ClickHouse verificada correctamente")
    return True

def create_databases():
    """Crear bases de datos necesarias con medición de tiempo"""
    logger.info("Creando bases de datos...")
    t0 = time.time()
    databases = [
        os.getenv("CLICKHOUSE_DATABASE", "fgeo_analytics"),
        "ext"  # Para tablas externas/temporales
    ]
    for db in databases:
        success, result = execute_clickhouse_query(f"CREATE DATABASE IF NOT EXISTS {db}")
        if success:
            logger.info(f"Base de datos '{db}' creada/verificada")
        else:
            logger.error(f"Error creando base de datos '{db}': {result}")
            return False
    logger.info(f"Bases de datos listas en {time.time()-t0:.2f}s")
    return True

def create_users():
    """Crear usuarios necesarios con medición de tiempo"""
    logger.info("Configurando usuarios y permisos...")
    t0 = time.time()
    
    # Importante: en este entorno los usuarios están definidos vía XML (almacenamiento readonly).
    # Por ello, evitar intentos de crear usuarios adicionales (superset_ro, superset_conn) que fallan.
    users = [
        {
            'name': os.getenv('CLICKHOUSE_ETL_USER', 'etl'),
            'password': os.getenv('CLICKHOUSE_ETL_PASSWORD', 'Et1Ingest!'),
            'grants': ['ALL']
        },
        {
            'name': os.getenv('CLICKHOUSE_USER', 'superset'), 
            'password': os.getenv('CLICKHOUSE_PASSWORD', 'Sup3rS3cret!'),
            'grants': ['SELECT']
        }
    ]
    
    for user in users:
        # Crear usuario
        query = f"CREATE USER IF NOT EXISTS {user['name']} IDENTIFIED BY '{user['password']}'"
        success, result = execute_clickhouse_query(query)
        if success:
            logger.info(f"Usuario '{user['name']}' creado/verificado")
        else:
            logger.warning(f"Usuario '{user['name']}': {result}")
        
        # Otorgar permisos base mínimos por usuario
        db_name = os.getenv("CLICKHOUSE_DATABASE", "fgeo_analytics")

        if user['name'] == os.getenv('CLICKHOUSE_ETL_USER', 'etl'):
            # Dar permisos para crear databases (necesario para analytics)
            query = f"GRANT CREATE DATABASE ON *.* TO {user['name']}"
            success, result = execute_clickhouse_query(query)
            if success:
                logger.info(f"Permiso 'CREATE DATABASE' otorgado a '{user['name']}'")
            else:
                logger.warning(f"Permiso 'CREATE DATABASE' para '{user['name']}': {result}")
            
            for grant in user['grants']:
                query = f"GRANT {grant} ON {db_name}.* TO {user['name']}"
                success, result = execute_clickhouse_query(query)
                if success:
                    logger.info(f"Permiso '{grant}' otorgado a '{user['name']}' en {db_name}.*")
                else:
                    logger.warning(f"Permiso '{grant}' para '{user['name']}' en {db_name}.*: {result}")
        elif user['name'] == os.getenv('CLICKHOUSE_USER', 'superset'):
            for grant in user['grants']:
                query = f"GRANT {grant} ON {db_name}.* TO {user['name']}"
                success, result = execute_clickhouse_query(query)
                if success:
                    logger.info(f"Permiso '{grant}' otorgado a '{user['name']}' en {db_name}.*")
                else:
                    logger.warning(f"Permiso '{grant}' para '{user['name']}' en {db_name}.*: {result}")
        else:
            # No crear usuarios adicionales; los permisos estrictos se aplicarán más abajo al usuario objetivo
            logger.info("Posponiendo permisos específicos para asignarlos dinámicamente por DB_CONNECTIONS...")
    logger.info(f"Usuarios y permisos listos en {time.time()-t0:.2f}s")
    return True

def parse_allowed_schemas_from_env() -> list:
    """Devuelve la lista de bases de datos (schemas) permitidas para Superset, derivada de DB_CONNECTIONS.
    Excluye schemas de sistema y patrones técnicos (_analytics, ext, default, system, INFORMATION_SCHEMA).
    """
    system_schemas = {"information_schema", "mysql", "performance_schema", "sys"}
    excluded_patterns = ["_analytics", "ext", "default", "system", "INFORMATION_SCHEMA"]
    try:
        raw = os.getenv("DB_CONNECTIONS", "").strip()
        if raw.startswith("[") and "]" in raw:
            conns = json.loads(raw)
            if isinstance(conns, dict):
                conns = [conns]
            schemas = []
            for c in conns or []:
                dbn = (c or {}).get("db")
                if dbn and dbn not in system_schemas and not any(p in dbn for p in excluded_patterns):
                    schemas.append(dbn)
            return sorted(set(schemas))
    except Exception as e:
        logger.warning(f"No se pudo parsear DB_CONNECTIONS: {e}")
    # Fallback: al menos la base principal
    return [os.getenv("CLICKHOUSE_DATABASE", "fgeo_analytics")]

def enforce_superset_strict_permissions() -> bool:
    """Restringe el usuario de conexión de Superset (SQL-managed) a solo las bases declaradas en DB_CONNECTIONS.
    Aplica REVOKE/GRANT directos sobre el usuario gestionado por SQL (evita users_xml readonly).
    También otorga permisos CREATE TEMPORARY TABLE para que Superset pueda crear gráficas avanzadas.
    """
    try:
        # Usar el usuario existente (definido en XML) para la conexión de Superset
        superset_user = os.getenv('CLICKHOUSE_USER', 'superset')
        allowed = parse_allowed_schemas_from_env()
        logger.info(f"Aplicando permisos estrictos para {superset_user}. Esquemas permitidos: {allowed}")
        # No intentar crear usuario (XML storage es readonly)
        # Limpiar permisos previos y otorgar solo los necesarios
        execute_clickhouse_query(f"REVOKE ALL ON *.* FROM {superset_user}")
        
        # Otorgar SELECT en cada schema permitido
        for db in allowed:
            ok, out = execute_clickhouse_query(f"GRANT SELECT ON {db}.* TO {superset_user}")
            if ok:
                logger.info(f"GRANT SELECT ON {db}.* TO {superset_user}")
            else:
                logger.warning(f"Fallo GRANT en {db}.*: {out}")
        
        # Otorgar CREATE TEMPORARY TABLE para que Superset pueda crear charts/dashboards
        # Esto permite a Superset crear tablas temporales necesarias para ciertos tipos de visualización
        ok, out = execute_clickhouse_query(f"GRANT CREATE TEMPORARY TABLE ON *.* TO {superset_user}")
        if ok:
            logger.info(f"✅ Permiso CREATE TEMPORARY TABLE otorgado a {superset_user}")
        else:
            logger.warning(f"⚠️  Fallo otorgando CREATE TEMPORARY TABLE: {out}")

        logger.info(f"Permisos estrictos aplicados a {superset_user}")
        return True
    except Exception as e:
        logger.error(f"Error aplicando permisos estrictos: {e}")
        return False

def create_initial_tables():
    """Crear tablas iniciales con datos de prueba y medir tiempo"""
    logger.info("Creando tablas iniciales...")
    t0 = time.time()
    
    db_name = os.getenv("CLICKHOUSE_DATABASE", "fgeo_analytics")
    
    # Tabla de prueba con datos
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {db_name}.test_table (
        id Int32,
        name String,
        created_at DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY id
    """
    
    success, result = execute_clickhouse_query(create_table_query)
    if success:
        logger.info("Tabla test_table creada")
        
        # Insertar datos de prueba
        insert_query = f"""
        INSERT INTO {db_name}.test_table (id, name) VALUES 
        (1, 'Pipeline Test 1'),
        (2, 'Pipeline Test 2'), 
        (3, 'Pipeline Test 3')
        """
        
        success, result = execute_clickhouse_query(insert_query)
        if success:
            logger.info("Datos de prueba insertados")
        else:
            logger.warning(f"Error insertando datos de prueba: {result}")
    else:
        logger.error(f"Error creando tabla test_table: {result}")
        return False
    logger.info(f"Tablas iniciales listas en {time.time()-t0:.2f}s")
    return True

def generate_analytics_views():
    """Generar vistas analytics con columnas *_date de forma automática y repetible con métricas"""
    try:
        t0 = time.time()
        # Bases de datos fuente a procesar (por defecto: fiscalizacion)
        src_dbs = os.getenv("ANALYTICS_SOURCE_DATABASES", "fiscalizacion").split(",")
        src_dbs = [d.strip() for d in src_dbs if d.strip()]

        if not src_dbs:
            logger.info("ℹ️ No hay bases fuente para generar vistas analytics (ANALYTICS_SOURCE_DATABASES vacío)")
            return True

        logger.info(f"Generando vistas analytics para: {src_dbs}")
        # Conteo estimado de tablas a procesar
        estimates = {}
        for db in src_dbs:
            ok, out = execute_clickhouse_query(
                "SELECT count() FROM system.tables WHERE database = '" + db + "' AND name NOT LIKE '%\\_v'"
            )
            if ok and out.strip().isdigit():
                estimates[db] = int(out.strip())
        if estimates:
            logger.info("Estimación de tablas a procesar: " + ", ".join(f"{k}: {v}" for k, v in estimates.items()))
        # Ejecuta el generador que usa docker exec clickhouse internamente
        cmd = [
            "python3", "/app/tools/generate_analytics_views.py",
            "--databases", *src_dbs
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=1200)
        if result.returncode == 0:
            logger.info("Vistas analytics generadas correctamente")
            # Resumen de views creadas
            created = sum(1 for line in result.stdout.splitlines() if line.strip().startswith("[OK] View"))
            logger.info(f"Total de vistas creadas/actualizadas: {created}")
            logger.info(result.stdout.strip()[-1000:])
            logger.info(f"Vistas analytics listas en {time.time()-t0:.2f}s")
            return True
        else:
            logger.warning("Generación de vistas analytics retornó código != 0")
            logger.warning(result.stdout.strip()[-2000:])
            logger.warning(result.stderr.strip()[-1000:])
            return False
    except Exception as e:
        logger.warning(f"Error generando vistas analytics: {e}")
        return False

def validate_setup():
    """Validar la configuración completa con detalles"""
    logger.info("Validando configuración...")
    t0 = time.time()
    
    db_name = os.getenv("CLICKHOUSE_DATABASE", "fgeo_analytics")
    
    # Verificar bases de datos
    success, result = execute_clickhouse_query("SHOW DATABASES")
    if success:
        databases = result.split('\n')
    logger.info(f"Bases de datos disponibles: {', '.join(databases)}")
    
    # Verificar usuarios  
    success, result = execute_clickhouse_query("SHOW USERS")
    if success:
        users = result.split('\n')
    logger.info(f"Usuarios disponibles: {', '.join(users)}")
    
    # Verificar tablas
    success, result = execute_clickhouse_query(f"SHOW TABLES FROM {db_name}")
    if success:
        tables = result.split('\n') if result else []
    logger.info(f"Tablas en {db_name}: {', '.join(tables)}")
    
    # Contar registros en tabla de prueba
    success, result = execute_clickhouse_query(f"SELECT count() FROM {db_name}.test_table")
    if success:
        count = result.strip()
        logger.info(f"Registros en test_table: {count}")
    logger.info(f"Validación completada en {time.time()-t0:.2f}s")
    return True

def main():
    """Función principal"""
    logger.info("=== CONFIGURACIÓN AUTOMÁTICA DE CLICKHOUSE ===")
    
    try:
        # 1. Esperar ClickHouse (con desglose de tiempo)
        t_wait0 = time.time()
        if not wait_for_clickhouse():
            logger.error("ClickHouse no disponible, abortando")
            return 1
        logger.info(f"ClickHouse disponible en {time.time()-t_wait0:.2f}s")

        # 1.1 Validar autenticación antes de continuar
        if not preflight_auth_check():
            logger.error("Fallo de autenticación HTTP a ClickHouse. Verifica CLICKHOUSE_* o CH_* en .env")
            return 1
        
        # 2. Crear bases de datos
        if not create_databases():
            logger.error("Error creando bases de datos")
            return 1
        
        # 3. Crear usuarios
        if not create_users():
            logger.error("Error configurando usuarios")
            return 1
        # 3.1. Restringir usuario de Superset a DB_CONNECTIONS
        enforce_superset_strict_permissions()
        
        # 4. Crear tablas iniciales
        if not create_initial_tables():
            logger.error("Error creando tablas iniciales")
            return 1
        
        # 5. Validar configuración
        if not validate_setup():
            logger.error("Error en validación")
            return 1

        # 6. Generar vistas analytics (idempotente)
        generate_analytics_views()
        logger.info("ClickHouse configurado exitosamente")
        return 0
        
    except Exception as e:
        logger.error(f"Error crítico: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())