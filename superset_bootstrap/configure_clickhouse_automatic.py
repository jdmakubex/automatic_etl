#!/usr/bin/env python3
"""
Configuración automática de ClickHouse en Superset
Integra automáticamente la base de datos ClickHouse después del inicio de contenedores
"""

import os
import sys
import time
import json
import logging
import requests
from urllib.parse import quote_plus

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuración de conexión
SUPERSET_URL = os.environ.get("SUPERSET_URL", "http://superset:8088")
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_HTTP_PORT = os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")
CLICKHOUSE_TCP_PORT = os.environ.get("CLICKHOUSE_TCP_PORT", "9000")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "superset_ro")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "Sup3rS3cret!")
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "fgeo_analytics")

# Configuración de Superset (admin por defecto)
SUPERSET_USERNAME = os.environ.get("SUPERSET_USERNAME", "admin")
SUPERSET_PASSWORD = os.environ.get("SUPERSET_PASSWORD", "admin")

def wait_for_service(url, service_name, max_retries=30):
    """Espera a que un servicio esté disponible"""
    for i in range(max_retries):
        try:
            response = requests.get(f"{url}/health", timeout=5)
            if response.status_code == 200:
                logger.info(f"{service_name} está disponible")
                return True
        except requests.exceptions.RequestException:
            pass
        
        logger.info(f"Esperando {service_name}... intento {i+1}/{max_retries}")
        time.sleep(5)
    
    logger.error(f"{service_name} no está disponible después de {max_retries} intentos")
    return False

def get_csrf_token(session):
    """Obtiene el token CSRF de Superset usando múltiples métodos"""
    try:
        # Método 1: Intentar desde /login/
        response = session.get(f"{SUPERSET_URL}/login/", timeout=10)
        if response.status_code == 200:
            content = response.text
            if 'csrf_token' in content:
                import re
                # Buscar diferentes patrones de CSRF token
                patterns = [
                    r'name="csrf_token".*?value="([^"]+)"',
                    r'csrf_token["\']?\s*:\s*["\']([^"\']+)["\']',
                    r'window\.CSRF_TOKEN\s*=\s*["\']([^"\']+)["\']'
                ]
                for pattern in patterns:
                    match = re.search(pattern, content)
                    if match:
                        logger.info("Token CSRF obtenido desde /login/")
                        return match.group(1)
        
        # Método 2: Intentar desde la página principal
        response = session.get(f"{SUPERSET_URL}/", timeout=10)
        if response.status_code == 200:
            content = response.text
            import re
            patterns = [
                r'csrf_token["\']?\s*:\s*["\']([^"\']+)["\']',
                r'window\.CSRF_TOKEN\s*=\s*["\']([^"\']+)["\']'
            ]
            for pattern in patterns:
                match = re.search(pattern, content)
                if match:
                    logger.info("Token CSRF obtenido desde página principal")
                    return match.group(1)
        
        logger.warning("No se pudo obtener token CSRF usando métodos tradicionales")
        return None
        
    except Exception as e:
        logger.error(f"Error obteniendo CSRF token: {e}")
        return None

def login_to_superset(session):
    """Realiza login a Superset usando múltiples métodos"""
    try:
        # Método 1: Login tradicional con CSRF
        csrf_token = get_csrf_token(session)
        if csrf_token:
            login_data = {
                'username': SUPERSET_USERNAME,
                'password': SUPERSET_PASSWORD,
                'csrf_token': csrf_token
            }
            
            response = session.post(
                f"{SUPERSET_URL}/login/",
                data=login_data,
                headers={'X-CSRFToken': csrf_token},
                timeout=10
            )
            
            if response.status_code == 200 and 'login' not in response.url:
                logger.info("Login exitoso a Superset (método tradicional)")
                return True
        
        # Método 2: Login API directo
        logger.info("Intentando login API directo...")
        api_login_data = {
            'username': SUPERSET_USERNAME,
            'password': SUPERSET_PASSWORD,
            'provider': 'db',
            'refresh': True
        }
        
        response = session.post(
            f"{SUPERSET_URL}/api/v1/security/login",
            json=api_login_data,
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get('access_token'):
                logger.info("Login exitoso a Superset (método API)")
                # Guardar token para uso posterior
                session.headers.update({'Authorization': f"Bearer {result['access_token']}"})
                return True
        
        logger.error(f"Todos los métodos de login fallaron")
        return False
            
    except Exception as e:
        logger.error(f"Error en login: {e}")
        return False

def get_api_token(session):
    """Obtiene token de API de Superset usando múltiples métodos"""
    try:
        # Método 1: API CSRF token endpoint
        response = session.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/", timeout=10)
        if response.status_code == 200:
            data = response.json()
            token = data.get('result')
            if token:
                logger.info("Token API obtenido desde endpoint CSRF")
                return token
        
        # Método 2: Obtener desde cookies o headers existentes
        if 'X-CSRFToken' in session.headers:
            logger.info("Usando token CSRF existente en headers")
            return session.headers['X-CSRFToken']
        
        # Método 3: Usar cualquier CSRF token disponible en cookies
        for cookie in session.cookies:
            if 'csrf' in cookie.name.lower():
                logger.info(f"Usando token desde cookie: {cookie.name}")
                return cookie.value
        
        # Método 4: Intentar sin token (para APIs que no lo requieren)
        logger.warning("No se pudo obtener token API, continuando sin token")
        return "no-token-available"
        
    except Exception as e:
        logger.error(f"Error obteniendo API token: {e}")
        return None

def cleanup_duplicate_databases(session, token):
    """Elimina bases de datos ClickHouse duplicadas o legacy, dejando solo la oficial"""
    try:
        headers = {
            'X-CSRFToken': token,
            'Content-Type': 'application/json'
        }
        
        response = session.get(f"{SUPERSET_URL}/api/v1/database/", headers=headers)
        if response.status_code == 200:
            databases = response.json().get('result', [])
            official_db_name = "ClickHouse ETL Database"
            official_db_id = None
            
            # Identificar la BD oficial y eliminar todas las demás que contengan 'clickhouse'
            for db in databases:
                db_name = db.get('database_name', '')
                db_id = db.get('id')
                
                if db_name == official_db_name:
                    official_db_id = db_id
                    logger.info(f"BD oficial encontrada: {db_name} (ID {db_id})")
                elif 'clickhouse' in db_name.lower():
                    # Eliminar BD duplicada/legacy
                    logger.info(f"Eliminando BD duplicada: {db_name} (ID {db_id})")
                    del_resp = session.delete(f"{SUPERSET_URL}/api/v1/database/{db_id}", headers=headers)
                    if del_resp.ok:
                        logger.info(f"✅ BD eliminada: {db_name}")
                    else:
                        logger.warning(f"⚠️  No se pudo eliminar {db_name}: {del_resp.status_code}")
            
            return official_db_id is not None
        return False
    except Exception as e:
        logger.error(f"Error limpiando bases de datos duplicadas: {e}")
        return False

def create_clickhouse_database(session, token):
    """Crea la conexión a ClickHouse en Superset"""
    try:
        headers = {
            'X-CSRFToken': token,
            'Content-Type': 'application/json'
        }
        
        # Construcción de la URI de conexión ClickHouse usando credenciales ETL
        ch_user = os.environ.get("CLICKHOUSE_ETL_USER", "etl")
        ch_password = os.environ.get("CLICKHOUSE_ETL_PASSWORD", "Et1Ingest!")
        encoded_password = quote_plus(ch_password)
        sqlalchemy_uri = f"clickhouse+http://{ch_user}:{encoded_password}@{CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}/{CLICKHOUSE_DATABASE}"
        
        database_config = {
            "database_name": "ClickHouse ETL Database",
            "sqlalchemy_uri": sqlalchemy_uri,
            "expose_in_sqllab": True,
            "allow_run_async": True,
            "allow_dml": False,
            "allow_file_upload": False,
            "extra": json.dumps({
                "allows_virtual_table_explore": True,
                "cancel_query_on_windows_unload": True,
                "metadata_params": {},
                "engine_params": {
                    "connect_args": {
                        "http_session_timeout": 30
                    }
                }
            })
        }
        
        response = session.post(
            f"{SUPERSET_URL}/api/v1/database/",
            headers=headers,
            json=database_config
        )
        
        if response.status_code == 201:
            logger.info("Base de datos ClickHouse creada exitosamente")
            return True
        else:
            logger.error(f"Error creando base de datos: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"Error creando base de datos ClickHouse: {e}")
        return False

def test_clickhouse_connection(session, token):
    """Prueba la conexión a ClickHouse"""
    try:
        headers = {
            'X-CSRFToken': token,
            'Content-Type': 'application/json'
        }
        
        ch_user = os.environ.get("CLICKHOUSE_ETL_USER", "etl")
        ch_password = os.environ.get("CLICKHOUSE_ETL_PASSWORD", "Et1Ingest!")
        encoded_password = quote_plus(ch_password)
        test_connection_data = {
            "sqlalchemy_uri": f"clickhouse+http://{ch_user}:{encoded_password}@{CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}/{CLICKHOUSE_DATABASE}",
            "database_name": "ClickHouse ETL Database Test",
            "impersonate_user": False
        }
        
        response = session.post(
            f"{SUPERSET_URL}/api/v1/database/test_connection",
            headers=headers,
            json=test_connection_data
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get('message') == 'OK':
                logger.info("Conexión a ClickHouse probada exitosamente")
                return True
            else:
                logger.warning(f"Prueba de conexión retornó: {result}")
                return False
        else:
            logger.error(f"Error probando conexión: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"Error probando conexión ClickHouse: {e}")
        return False

def main():
    """Función principal de configuración automática"""
    logger.info("🔧 Iniciando configuración automática de ClickHouse en Superset")
    logger.info(f"   - Superset URL: {SUPERSET_URL}")
    logger.info(f"   - ClickHouse Host: {CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}")
    logger.info(f"   - ClickHouse Database: {CLICKHOUSE_DATABASE}")
    logger.info(f"   - ClickHouse User: {CLICKHOUSE_USER}")
    
    # Esperar a que Superset esté disponible
    if not wait_for_service(SUPERSET_URL, "Superset"):
        logger.error("❌ Superset no está disponible")
        sys.exit(1)
    
    # Crear sesión
    session = requests.Session()
    
    # Login a Superset
    logger.info("🔐 Intentando login a Superset...")
    if not login_to_superset(session):
        logger.error("❌ No se pudo hacer login a Superset")
        sys.exit(1)
    logger.info("✅ Login exitoso a Superset")
    
    # Obtener token de API
    logger.info("🎫 Obteniendo token de API...")
    api_token = get_api_token(session)
    if not api_token:
        logger.error("❌ No se pudo obtener token de API")
        sys.exit(1)
    logger.info("✅ Token de API obtenido")
    
    # Limpiar bases de datos duplicadas y verificar si existe la oficial
    logger.info("🧹 Limpiando bases de datos ClickHouse duplicadas...")
    if cleanup_duplicate_databases(session, api_token):
        logger.info("✅ Base de datos ClickHouse ya está configurada (limpieza aplicada)")
        return
    
    # Probar conexión antes de crear
    logger.info("🔌 Probando conexión a ClickHouse...")
    if not test_clickhouse_connection(session, api_token):
        logger.error("❌ No se pudo conectar a ClickHouse")
        sys.exit(1)
    logger.info("✅ Conexión a ClickHouse verificada")
    
    # Crear base de datos ClickHouse
    logger.info("🏗️  Creando base de datos ClickHouse en Superset...")
    if create_clickhouse_database(session, api_token):
        logger.info("🎉 ✅ Configuración de ClickHouse completada exitosamente")
        logger.info("📊 Base de datos 'ClickHouse ETL Database' disponible en Superset")
    else:
        logger.error("❌ Error en la configuración de ClickHouse")
        sys.exit(1)

if __name__ == "__main__":
    main()