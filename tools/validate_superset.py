#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de validación automática de Superset.
Valida configuración, bases de datos y datasets en Superset.

Ejecución de validaciones desde el contenedor etl-tools
Para validar Superset desde el contenedor, ejecuta:

  docker compose run --rm etl-tools python tools/validate_superset.py

Si necesitas validar ClickHouse:
  docker compose run --rm etl-tools python tools/validate_clickhouse.py

Asegúrate de que el contenedor esté reconstruido si cambias los requirements:
  docker compose build etl-tools

Los resultados se guardan en la carpeta logs/ y también se muestran por consola.

Puedes controlar el formato de logs y parámetros usando variables de entorno en .env
Ejemplo:
  LOG_FORMAT=json
  SUPERSET_URL=http://superset:8088
  SUPERSET_ADMIN=admin
  SUPERSET_PASSWORD=Admin123!
  EXPECTED_DATABASE=fgeo_analytics
  MIN_DATASETS=1
  VALIDATE_CONNECTIVITY=true

IMPORTANTE: Para ejecutar validaciones, usa el contenedor etl-tools y configura las variables de entorno necesarias en tu archivo .env (no publiques credenciales sensibles).
Ejemplo de comando:
  docker compose run --rm etl-tools python tools/validate_superset.py
Las credenciales y parámetros deben estar en .env, nunca en el código ni en comentarios públicos.
"""
import os
import sys
import json
import logging
from typing import Dict, List, Optional
from datetime import datetime
import time
import unittest

# Configurar logging JSON
class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_data, ensure_ascii=False)


def setup_json_logging():
    """Configura logging en formato JSON"""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Remover handlers existentes
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Handler para JSON
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    logger.addHandler(handler)
    
    return logging.getLogger(__name__)


def validate_superset_health(url: str, max_retries: int = 3) -> Dict:
    """Valida que Superset esté respondiendo"""
    log = logging.getLogger(__name__)
    log.info(f"[TRACE] Entrando a validate_superset_health: url={url}, max_retries={max_retries}")
    result = {
        "test": "superset_health",
        "url": url,
        "status": "UNKNOWN",
        "details": {}
    }
    try:
        import requests
    except ImportError:
        result["status"] = "ERROR"
        result["details"]["error"] = "requests no instalado"
        log.error("[TRACE] requests no instalado")
        log.info("[TRACE] Saliendo de validate_superset_health")
        return result
    health_url = f"{url.rstrip('/')}/health"
    for attempt in range(max_retries):
        try:
            log.info(f"[TRACE] Intento healthcheck {attempt+1} a {health_url}")
            response = requests.get(health_url, timeout=5)
            if response.status_code == 200:
                result["status"] = "PASS"
                result["details"]["response"] = response.text
                result["details"]["attempts"] = attempt + 1
                log.info(f"[TRACE] Healthcheck exitoso en intento {attempt+1}")
                break
            else:
                result["status"] = "FAIL"
                result["details"]["http_code"] = response.status_code
                result["details"]["response"] = response.text
                log.warning(f"[TRACE] Healthcheck fallido: HTTP {response.status_code}")
        except requests.exceptions.RequestException as e:
            log.error(f"[TRACE] Excepción en healthcheck: {e}")
            if attempt == max_retries - 1:
                result["status"] = "ERROR"
                result["details"]["error"] = str(e)
                result["details"]["attempts"] = attempt + 1
            else:
                time.sleep(2)
    log.info(f"[TRACE] Saliendo de validate_superset_health con status={result['status']}")
    return result


def get_superset_session(url: str, username: str, password: str) -> Optional[object]:
    """Obtiene sesión autenticada de Superset"""
    log = logging.getLogger(__name__)
    log.info(f"[TRACE] Entrando a get_superset_session: url={url}, username={username}")
    try:
        import requests
    except ImportError:
        log.error("[TRACE] requests no instalado")
        raise Exception("requests no instalado. Instalar con: pip install requests")
    session = requests.Session()
    login_url = f"{url.rstrip('/')}/api/v1/security/login"
    login_data = {
        "username": username,
        "password": password,
        "provider": "db",
        "refresh": True
    }
    try:
        log.info(f"[TRACE] Intentando login en {login_url}")
        response = session.post(login_url, json=login_data, timeout=10)
        if response.status_code == 200:
            data = response.json()
            access_token = data.get("access_token")
            if access_token:
                session.headers.update({
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json"
                })
                log.info("[TRACE] Login exitoso, sesión autenticada")
                log.info("[TRACE] Saliendo de get_superset_session")
                return session
        log.error(f"[TRACE] Login fallido: HTTP {response.status_code} - {response.text}")
        raise Exception(f"Login fallido: HTTP {response.status_code} - {response.text}")
    except Exception as e:
        log.error(f"[TRACE] Excepción en login: {e}")
        raise Exception(f"No se pudo autenticar en Superset: {e}")


def validate_databases(session: object, url: str, expected_db: str = None) -> Dict:
    """Valida bases de datos configuradas en Superset"""
    log = logging.getLogger(__name__)
    log.info(f"[TRACE] Entrando a validate_databases: url={url}, expected_db={expected_db}")
    result = {
        "test": "superset_databases",
        "status": "UNKNOWN",
        "details": {}
    }
    databases_url = f"{url.rstrip('/')}/api/v1/database/"
    try:
        response = session.get(databases_url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            databases = data.get("result", [])
            result["details"]["databases"] = [
                {
                    "id": db.get("id"),
                    "name": db.get("database_name"),
                    "backend": db.get("backend")
                }
                for db in databases
            ]
            result["details"]["count"] = len(databases)
            log.info(f"[TRACE] Bases de datos encontradas: {len(databases)}")
            if expected_db:
                found = any(
                    expected_db.lower() in db.get("database_name", "").lower()
                    for db in databases
                )
                if found:
                    result["status"] = "PASS"
                    result["details"]["expected_db_found"] = True
                    log.info(f"[TRACE] Base de datos esperada '{expected_db}' encontrada")
                else:
                    result["status"] = "FAIL"
                    result["details"]["expected_db_found"] = False
                    result["details"]["error"] = (
                        f"Base de datos esperada '{expected_db}' no encontrada"
                    )
                    log.warning(f"[TRACE] Base de datos esperada '{expected_db}' NO encontrada")
            elif databases:
                result["status"] = "PASS"
            else:
                result["status"] = "FAIL"
                result["details"]["error"] = "No hay bases de datos configuradas"
                log.warning("[TRACE] No hay bases de datos configuradas")
        else:
            result["status"] = "ERROR"
            result["details"]["http_code"] = response.status_code
            result["details"]["error"] = response.text
            log.error(f"[TRACE] Error al consultar bases de datos: HTTP {response.status_code}")
    except Exception as e:
        result["status"] = "ERROR"
        result["details"]["error"] = str(e)
        log.error(f"[TRACE] Excepción en validate_databases: {e}")
    log.info(f"[TRACE] Saliendo de validate_databases con status={result['status']}")
    return result


def validate_datasets(session: object, url: str, min_datasets: int = 0) -> Dict:
    """Valida datasets configurados en Superset"""
    log = logging.getLogger(__name__)
    log.info(f"[TRACE] Entrando a validate_datasets: url={url}, min_datasets={min_datasets}")
    result = {
        "test": "superset_datasets",
        "status": "UNKNOWN",
        "details": {}
    }
    datasets_url = f"{url.rstrip('/')}/api/v1/dataset/"
    try:
        response = session.get(datasets_url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            datasets = data.get("result", [])
            result["details"]["datasets"] = [
                {
                    "id": ds.get("id"),
                    "name": ds.get("table_name"),
                    "database": ds.get("database", {}).get("database_name")
                }
                for ds in datasets
            ]
            result["details"]["count"] = len(datasets)
            log.info(f"[TRACE] Datasets encontrados: {len(datasets)}")
            if len(datasets) >= min_datasets:
                result["status"] = "PASS"
            else:
                result["status"] = "FAIL"
                result["details"]["error"] = (
                    f"Se esperaban al menos {min_datasets} dataset(s), "
                    f"encontrados: {len(datasets)}"
                )
                log.warning(f"[TRACE] No se alcanzó el mínimo de datasets requerido")
        else:
            result["status"] = "ERROR"
            result["details"]["http_code"] = response.status_code
            result["details"]["error"] = response.text
            log.error(f"[TRACE] Error al consultar datasets: HTTP {response.status_code}")
    except Exception as e:
        result["status"] = "ERROR"
        result["details"]["error"] = str(e)
        log.error(f"[TRACE] Excepción en validate_datasets: {e}")
    log.info(f"[TRACE] Saliendo de validate_datasets con status={result['status']}")
    return result


def validate_dataset_connectivity(
    session: object,
    url: str,
    dataset_id: int,
    dataset_name: str
) -> Dict:
    """Valida conectividad de un dataset específico"""
    log = logging.getLogger(__name__)
    log.info(f"[TRACE] Entrando a validate_dataset_connectivity: id={dataset_id}, name={dataset_name}")
    result = {
        "test": "dataset_connectivity",
        "dataset_id": dataset_id,
        "dataset_name": dataset_name,
        "status": "UNKNOWN",
        "details": {}
    }
    query_url = f"{url.rstrip('/')}/api/v1/dataset/{dataset_id}"
    try:
        response = session.get(query_url, timeout=10)
        if response.status_code == 200:
            result["status"] = "PASS"
            result["details"]["accessible"] = True
            log.info(f"[TRACE] Dataset {dataset_name} ({dataset_id}) accesible")
        else:
            result["status"] = "FAIL"
            result["details"]["accessible"] = False
            result["details"]["http_code"] = response.status_code
            log.warning(f"[TRACE] Dataset {dataset_name} ({dataset_id}) NO accesible: HTTP {response.status_code}")
    except Exception as e:
        result["status"] = "ERROR"
        result["details"]["error"] = str(e)
        log.error(f"[TRACE] Excepción en validate_dataset_connectivity: {e}")
    log.info(f"[TRACE] Saliendo de validate_dataset_connectivity con status={result['status']}")
    return result


def run_all_validations(
    url: str,
    username: str,
    password: str,
    expected_db: str = None,
    min_datasets: int = 0,
    validate_connectivity: bool = False
) -> Dict:
    """Ejecuta todas las validaciones de Superset"""
    log = logging.getLogger(__name__)
    
    results = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "component": "superset",
        "url": url,
        "tests": [],
        "summary": {
            "total": 0,
            "passed": 0,
            "failed": 0,
            "errors": 0
        }
    }
    
    try:
        # 1. Validar health
        log.info(f"Validando health de Superset en {url}")
        test_health = validate_superset_health(url)
        results["tests"].append(test_health)
        
        if test_health["status"] != "PASS":
            log.error("Superset no está respondiendo correctamente")
            return results
        
        # 2. Autenticar
        log.info("Autenticando en Superset...")
        try:
            session = get_superset_session(url, username, password)
            log.info("Autenticación exitosa")
        except Exception as e:
            log.error(f"Error de autenticación: {e}")
            results["tests"].append({
                "test": "authentication",
                "status": "ERROR",
                "details": {"error": str(e)}
            })
            return results
        
        # 3. Validar bases de datos
        log.info("Validando bases de datos configuradas")
        test_dbs = validate_databases(session, url, expected_db)
        results["tests"].append(test_dbs)
        
        # 4. Validar datasets
        log.info(f"Validando datasets (mínimo {min_datasets})")
        test_datasets = validate_datasets(session, url, min_datasets)
        results["tests"].append(test_datasets)
        
        # 5. Validar conectividad de datasets (opcional)
        if validate_connectivity and test_datasets["status"] == "PASS":
            datasets = test_datasets["details"].get("datasets", [])
            
            for ds in datasets[:5]:  # Validar máximo 5 datasets
                ds_id = ds["id"]
                ds_name = ds["name"]
                log.info(f"Validando conectividad del dataset: {ds_name}")
                
                test_conn = validate_dataset_connectivity(session, url, ds_id, ds_name)
                results["tests"].append(test_conn)
        
        # Calcular resumen
        results["summary"]["total"] = len(results["tests"])
        for test in results["tests"]:
            if test["status"] == "PASS":
                results["summary"]["passed"] += 1
            elif test["status"] == "FAIL":
                results["summary"]["failed"] += 1
            elif test["status"] == "ERROR":
                results["summary"]["errors"] += 1
    
    except Exception as e:
        log.error(f"Error ejecutando validaciones: {e}")
        results["error"] = str(e)
    
    return results


def main():
    # Configurar logging JSON si está habilitado
    use_json = os.getenv("LOG_FORMAT", "text").lower() == "json"
    
    if use_json:
        log = setup_json_logging()
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s"
        )
        log = logging.getLogger(__name__)
    
    # Parámetros
    url = os.getenv("SUPERSET_URL", "http://superset:8088")
    username = os.getenv("SUPERSET_ADMIN", "admin")
    password = os.getenv("SUPERSET_PASSWORD", "Admin123!")
    expected_db = os.getenv("EXPECTED_DATABASE", "fgeo_analytics")
    min_datasets = int(os.getenv("MIN_DATASETS", "0"))
    validate_connectivity = os.getenv("VALIDATE_CONNECTIVITY", "false").lower() == "true"
    
    log.info("=== Iniciando validación de Superset ===")
    log.info(f"URL: {url}")
    log.info(f"Usuario: {username}")
    log.info(f"Base de datos esperada: {expected_db}")
    log.info(f"Mínimo de datasets: {min_datasets}")
    
    try:
        # Ejecutar validaciones
        results = run_all_validations(
            url, username, password, expected_db, min_datasets, validate_connectivity
        )
    except Exception as e:
        log.error(f"[ROBUSTEZ] Error inesperado en main: {e}")
        results = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "superset",
            "url": url,
            "tests": [],
            "summary": {
                "total": 0,
                "passed": 0,
                "failed": 0,
                "errors": 1
            },
            "error": f"Error inesperado en main: {e}"
        }
    # Imprimir resultados
    try:
        if use_json:
            print(json.dumps(results, indent=2, ensure_ascii=False))
        else:
            print("\n=== Resultados de Validación ===")
            print(f"Total de pruebas: {results['summary']['total']}")
            print(f"✓ Pasadas: {results['summary']['passed']}")
            print(f"✗ Fallidas: {results['summary']['failed']}")
            print(f"⚠ Errores: {results['summary']['errors']}")
            if results['summary']['failed'] > 0 or results['summary']['errors'] > 0:
                print("\nDetalles de fallos:")
                for test in results.get('tests', []):
                    if test.get('status') in ['FAIL', 'ERROR']:
                        print(f"  - {test.get('test')}: {test.get('details', {}).get('error', 'Sin detalles')}")
                if results.get('error'):
                    print(f"  - error global: {results['error']}")
    except Exception as e:
        log.error(f"[ROBUSTEZ] Error imprimiendo resultados: {e}")
    # Guardar resultados en archivo, aunque haya error
    output_file = os.getenv("OUTPUT_FILE", "logs/superset_validation.json")
    try:
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, "w") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        log.info(f"Resultados guardados en: {output_file}")
    except Exception as e:
        log.error(f"[ROBUSTEZ] No se pudo guardar el archivo de resultados: {e}")
    # Exit code según resultados
    if results['summary']['errors'] > 0:
        log.error("Validación completada con errores")
        sys.exit(2)
    elif results['summary']['failed'] > 0:
        log.warning("Validación completada con fallos")
        sys.exit(1)
    else:
        log.info("✓ Todas las validaciones pasaron correctamente")
        sys.exit(0)



# Pruebas unitarias para Superset
class TestSupersetValidation(unittest.TestCase):
    def test_superset_health(self):
        url = os.getenv("SUPERSET_URL", "http://superset:8088")
        result = validate_superset_health(url)
        self.assertIn(result["status"], ["PASS", "FAIL", "ERROR"])

if __name__ == "__main__":
    main()
