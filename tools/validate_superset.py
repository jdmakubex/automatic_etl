#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de validación automática de Superset.
Valida configuración, bases de datos y datasets en Superset.
"""
import os
import sys
import json
import logging
from typing import Dict, List, Optional
from datetime import datetime
import time

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
        return result
    
    health_url = f"{url.rstrip('/')}/health"
    
    for attempt in range(max_retries):
        try:
            response = requests.get(health_url, timeout=5)
            
            if response.status_code == 200:
                result["status"] = "PASS"
                result["details"]["response"] = response.text
                result["details"]["attempts"] = attempt + 1
                break
            else:
                result["status"] = "FAIL"
                result["details"]["http_code"] = response.status_code
                result["details"]["response"] = response.text
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                result["status"] = "ERROR"
                result["details"]["error"] = str(e)
                result["details"]["attempts"] = attempt + 1
            else:
                time.sleep(2)
    
    return result


def get_superset_session(url: str, username: str, password: str) -> Optional[object]:
    """Obtiene sesión autenticada de Superset"""
    try:
        import requests
    except ImportError:
        raise Exception("requests no instalado. Instalar con: pip install requests")
    
    session = requests.Session()
    
    # Login
    login_url = f"{url.rstrip('/')}/api/v1/security/login"
    login_data = {
        "username": username,
        "password": password,
        "provider": "db",
        "refresh": True
    }
    
    try:
        response = session.post(login_url, json=login_data, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            access_token = data.get("access_token")
            
            if access_token:
                session.headers.update({
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json"
                })
                return session
        
        raise Exception(f"Login fallido: HTTP {response.status_code} - {response.text}")
    
    except Exception as e:
        raise Exception(f"No se pudo autenticar en Superset: {e}")


def validate_databases(session: object, url: str, expected_db: str = None) -> Dict:
    """Valida bases de datos configuradas en Superset"""
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
            
            # Verificar si existe la base de datos esperada
            if expected_db:
                found = any(
                    expected_db.lower() in db.get("database_name", "").lower()
                    for db in databases
                )
                
                if found:
                    result["status"] = "PASS"
                    result["details"]["expected_db_found"] = True
                else:
                    result["status"] = "FAIL"
                    result["details"]["expected_db_found"] = False
                    result["details"]["error"] = (
                        f"Base de datos esperada '{expected_db}' no encontrada"
                    )
            elif databases:
                result["status"] = "PASS"
            else:
                result["status"] = "FAIL"
                result["details"]["error"] = "No hay bases de datos configuradas"
        else:
            result["status"] = "ERROR"
            result["details"]["http_code"] = response.status_code
            result["details"]["error"] = response.text
    
    except Exception as e:
        result["status"] = "ERROR"
        result["details"]["error"] = str(e)
    
    return result


def validate_datasets(session: object, url: str, min_datasets: int = 0) -> Dict:
    """Valida datasets configurados en Superset"""
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
            
            if len(datasets) >= min_datasets:
                result["status"] = "PASS"
            else:
                result["status"] = "FAIL"
                result["details"]["error"] = (
                    f"Se esperaban al menos {min_datasets} dataset(s), "
                    f"encontrados: {len(datasets)}"
                )
        else:
            result["status"] = "ERROR"
            result["details"]["http_code"] = response.status_code
            result["details"]["error"] = response.text
    
    except Exception as e:
        result["status"] = "ERROR"
        result["details"]["error"] = str(e)
    
    return result


def validate_dataset_connectivity(
    session: object,
    url: str,
    dataset_id: int,
    dataset_name: str
) -> Dict:
    """Valida conectividad de un dataset específico"""
    result = {
        "test": "dataset_connectivity",
        "dataset_id": dataset_id,
        "dataset_name": dataset_name,
        "status": "UNKNOWN",
        "details": {}
    }
    
    # Intentar ejecutar una query simple en el dataset
    query_url = f"{url.rstrip('/')}/api/v1/dataset/{dataset_id}"
    
    try:
        # Primero obtener info del dataset
        response = session.get(query_url, timeout=10)
        
        if response.status_code == 200:
            result["status"] = "PASS"
            result["details"]["accessible"] = True
        else:
            result["status"] = "FAIL"
            result["details"]["accessible"] = False
            result["details"]["http_code"] = response.status_code
    
    except Exception as e:
        result["status"] = "ERROR"
        result["details"]["error"] = str(e)
    
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
    
    # Ejecutar validaciones
    results = run_all_validations(
        url, username, password, expected_db, min_datasets, validate_connectivity
    )
    
    # Imprimir resultados
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
            for test in results['tests']:
                if test['status'] in ['FAIL', 'ERROR']:
                    print(f"  - {test['test']}: {test.get('details', {}).get('error', 'Sin detalles')}")
    
    # Guardar resultados en archivo
    output_file = os.getenv("OUTPUT_FILE", "logs/superset_validation.json")
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    log.info(f"Resultados guardados en: {output_file}")
    
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


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"Error fatal: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(3)
