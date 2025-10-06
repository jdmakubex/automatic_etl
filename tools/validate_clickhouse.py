#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de validación automática de ClickHouse.
Valida tablas, datos y estructura en la base de datos ClickHouse.
"""
import os
import sys
import json
import logging
from typing import Dict, List, Optional
from datetime import datetime
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


def get_clickhouse_client():
    """Obtiene cliente de ClickHouse con manejo de errores"""
    try:
        import clickhouse_connect
    except ImportError:
        raise Exception(
            "clickhouse_connect no instalado. Instalar con: pip install clickhouse-connect"
        )
    
    host = os.getenv("CLICKHOUSE_HTTP_HOST", os.getenv("CH_HOST", "clickhouse"))
    port = int(os.getenv("CLICKHOUSE_HTTP_PORT", os.getenv("CH_PORT", "8123")))
    user = os.getenv("CH_USER", "default")
    password = os.getenv("CH_PASSWORD", "")
    database = os.getenv("CLICKHOUSE_DATABASE", "fgeo_analytics")
    
    try:
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password,
            database=database
        )
        return client, database
    except Exception as e:
        raise Exception(f"No se pudo conectar a ClickHouse en {host}:{port}: {e}")


def validate_database_exists(client, database: str) -> Dict:
    """Valida que la base de datos exista"""
    result = {
        "test": "database_exists",
        "database": database,
        "status": "UNKNOWN",
        "details": {}
    }
    
    try:
        query = "SELECT name FROM system.databases WHERE name = %(db)s"
        rows = client.query(query, parameters={"db": database}).result_rows
        
        if rows:
            result["status"] = "PASS"
            result["details"]["found"] = True
        else:
            result["status"] = "FAIL"
            result["details"]["found"] = False
            result["details"]["error"] = f"Base de datos '{database}' no existe"
    except Exception as e:
        result["status"] = "ERROR"
        result["details"]["error"] = str(e)
    
    return result


def validate_tables_exist(client, database: str, min_tables: int = 1) -> Dict:
    """Valida que existan tablas en la base de datos"""
    result = {
        "test": "tables_exist",
        "database": database,
        "status": "UNKNOWN",
        "details": {}
    }
    
    try:
        query = f"SELECT name, engine, total_rows FROM system.tables WHERE database = %(db)s"
        rows = client.query(query, parameters={"db": database}).result_rows
        
        tables = [{"name": r[0], "engine": r[1], "rows": r[2]} for r in rows]
        result["details"]["tables"] = tables
        result["details"]["count"] = len(tables)
        
        if len(tables) >= min_tables:
            result["status"] = "PASS"
        else:
            result["status"] = "FAIL"
            result["details"]["error"] = (
                f"Se esperaban al menos {min_tables} tabla(s), "
                f"encontradas: {len(tables)}"
            )
    except Exception as e:
        result["status"] = "ERROR"
        result["details"]["error"] = str(e)
    
    return result


def validate_table_data(client, database: str, table: str, min_rows: int = 0) -> Dict:
    """Valida que una tabla tenga datos"""
    result = {
        "test": "table_data",
        "database": database,
        "table": table,
        "status": "UNKNOWN",
        "details": {}
    }
    
    try:
        # Contar filas
        count_query = f"SELECT count() FROM `{database}`.`{table}`"
        count_result = client.query(count_query).result_rows
        row_count = count_result[0][0] if count_result else 0
        
        result["details"]["row_count"] = row_count
        
        if row_count >= min_rows:
            result["status"] = "PASS"
            
            # Obtener muestra de datos
            sample_query = f"SELECT * FROM `{database}`.`{table}` LIMIT 3"
            sample_rows = client.query(sample_query).result_rows
            result["details"]["sample_count"] = len(sample_rows)
        else:
            result["status"] = "FAIL"
            result["details"]["error"] = (
                f"Se esperaban al menos {min_rows} fila(s), "
                f"encontradas: {row_count}"
            )
    except Exception as e:
        result["status"] = "ERROR"
        result["details"]["error"] = str(e)
    
    return result


def validate_table_schema(client, database: str, table: str) -> Dict:
    """Valida el esquema de una tabla"""
    result = {
        "test": "table_schema",
        "database": database,
        "table": table,
        "status": "UNKNOWN",
        "details": {}
    }
    
    try:
        query = f"""
        SELECT name, type, default_kind
        FROM system.columns
        WHERE database = %(db)s AND table = %(table)s
        ORDER BY position
        """
        rows = client.query(
            query,
            parameters={"db": database, "table": table}
        ).result_rows
        
        columns = [
            {"name": r[0], "type": r[1], "default_kind": r[2]}
            for r in rows
        ]
        
        result["details"]["columns"] = columns
        result["details"]["column_count"] = len(columns)
        
        if columns:
            result["status"] = "PASS"
        else:
            result["status"] = "FAIL"
            result["details"]["error"] = "No se encontraron columnas"
    except Exception as e:
        result["status"] = "ERROR"
        result["details"]["error"] = str(e)
    
    return result


def run_all_validations(
    database: str,
    min_tables: int = 1,
    validate_data: bool = True
) -> Dict:
    """Ejecuta todas las validaciones de ClickHouse"""
    log = logging.getLogger(__name__)
    
    results = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "component": "clickhouse",
        "database": database,
        "tests": [],
        "summary": {
            "total": 0,
            "passed": 0,
            "failed": 0,
            "errors": 0
        }
    }
    
    try:
        # Obtener cliente
        log.info("Conectando a ClickHouse...")
        client, db = get_clickhouse_client()
        log.info("Conexión a ClickHouse exitosa")
        
        # 1. Validar base de datos existe
        log.info(f"Validando base de datos: {database}")
        test_db = validate_database_exists(client, database)
        results["tests"].append(test_db)
        
        if test_db["status"] != "PASS":
            log.error(f"Base de datos '{database}' no encontrada")
            return results
        
        # 2. Validar tablas existen
        log.info(f"Validando existencia de tablas (mínimo {min_tables})")
        test_tables = validate_tables_exist(client, database, min_tables)
        results["tests"].append(test_tables)
        
        if test_tables["status"] != "PASS":
            log.warning("No se encontraron suficientes tablas")
        
        # 3. Validar datos en cada tabla
        if validate_data and test_tables["status"] == "PASS":
            tables = test_tables["details"].get("tables", [])
            
            for table_info in tables:
                table_name = table_info["name"]
                log.info(f"Validando tabla: {table_name}")
                
                # Esquema
                test_schema = validate_table_schema(client, database, table_name)
                results["tests"].append(test_schema)
                
                # Datos
                test_data = validate_table_data(client, database, table_name, min_rows=0)
                results["tests"].append(test_data)
        
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
    database = os.getenv("CLICKHOUSE_DATABASE", "fgeo_analytics")
    min_tables = int(os.getenv("MIN_TABLES", "1"))
    validate_data = os.getenv("VALIDATE_DATA", "true").lower() == "true"
    
    log.info("=== Iniciando validación de ClickHouse ===")
    log.info(f"Base de datos: {database}")
    log.info(f"Mínimo de tablas: {min_tables}")
    log.info(f"Validar datos: {validate_data}")
    
    try:
        # Ejecutar validaciones
        results = run_all_validations(database, min_tables, validate_data)
    except Exception as e:
        log.error(f"[ROBUSTEZ] Error inesperado en main: {e}")
        results = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component": "clickhouse",
            "database": database,
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
    output_file = os.getenv("OUTPUT_FILE", "logs/clickhouse_validation.json")
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


if __name__ == "__main__":
    main()
