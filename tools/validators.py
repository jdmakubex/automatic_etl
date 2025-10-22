#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Módulo de validación de variables de entorno y dependencias.
Provee funciones para validar configuración antes de ejecutar scripts ETL.
"""
import os
import sys
import json
import logging
from typing import Dict, List, Optional, Tuple
from pathlib import Path

log = logging.getLogger(__name__)


class ValidationError(Exception):
    """Error de validación (recuperable - debe documentarse)"""
    pass


class FatalError(Exception):
    """Error fatal (no recuperable - debe abortar)"""
    pass


def validate_env_var(
    name: str,
    required: bool = True,
    default: Optional[str] = None,
    validator: Optional[callable] = None
) -> Optional[str]:
    """
    Valida una variable de entorno.
    
    Args:
        name: Nombre de la variable
        required: Si es requerida (True) o opcional (False)
        default: Valor por defecto si no está definida
        validator: Función de validación opcional (debe retornar True si válido)
    
    Returns:
        Valor de la variable o default
    
    Raises:
        ValidationError: Si es requerida y no está definida
        FatalError: Si el valor no pasa la validación
    """
    value = os.getenv(name, default)
    
    if value is None and required:
        raise ValidationError(
            f"Variable de entorno requerida no definida: {name}. "
            f"Debe definirse en el archivo .env o como variable de entorno."
        )
    
    if value is not None and validator is not None:
        try:
            if not validator(value):
                raise FatalError(
                    f"Variable de entorno {name} tiene un valor inválido: {value}"
                )
        except Exception as e:
            raise FatalError(
                f"Error validando variable de entorno {name}: {e}"
            )
    
    return value


def validate_db_connections(db_connections_json: str) -> List[Dict]:
    """
    Valida la estructura de DB_CONNECTIONS.
    
    Args:
        db_connections_json: String JSON con las conexiones
    
    Returns:
        Lista de diccionarios con las conexiones validadas
    
    Raises:
        ValidationError: Si el JSON es inválido o la estructura incorrecta
    """
    try:
        connections = json.loads(db_connections_json)
    except json.JSONDecodeError as e:
        raise ValidationError(
            f"DB_CONNECTIONS no es un JSON válido: {e}. "
            f"Ejemplo válido: [{{'name':'db1','host':'localhost','port':3306,'user':'root','pass':'pwd','db':'mydb'}}]"
        )
    
    if not isinstance(connections, list):
        raise ValidationError(
            f"DB_CONNECTIONS debe ser una lista de diccionarios, recibido: {type(connections).__name__}"
        )
    
    # Validar estructura de cada conexión
    # 'type' debe ser opcional; por compatibilidad asumimos 'mysql' si no se especifica
    required_fields = ['name', 'host', 'port', 'user', 'pass', 'db']
    for i, conn in enumerate(connections):
        if not isinstance(conn, dict):
            raise ValidationError(f"Conexión {i}: debe ser un diccionario, recibido: {type(conn).__name__}")
        
        # Campos estrictamente requeridos
        for field in required_fields:
            if field not in conn:
                raise ValidationError(f"Conexión {i}: campo requerido '{field}' faltante")

        # Ajustes de compatibilidad: completar valores por defecto si faltan
        # type: por defecto 'mysql'
        if 'type' not in conn or not conn.get('type'):
            conn['type'] = 'mysql'
        # Normalizar puerto a int si viene como string
        try:
            conn['port'] = int(conn['port'])
        except Exception:
            raise ValidationError(f"Conexión {i}: campo 'port' debe ser numérico")
    
    return connections


def get_db_connections():
    """
    Lee DB_CONNECTIONS directamente del archivo .env.
    
    Returns:
        Lista de diccionarios con las conexiones
        
    Raises:
        FatalError: Si no se puede leer o parsear DB_CONNECTIONS
    """
    import re, json
    try:
        with open('/app/.env') as f:
            env = f.read()
        # Buscar la línea DB_CONNECTIONS y limpiar espacios
        value = None
        for line in env.splitlines():
            if line.strip().startswith('DB_CONNECTIONS='):
                value = line.strip()[len('DB_CONNECTIONS='):].strip()
                # Eliminar comentarios al final de la línea
                value = value.split('#')[0].strip()
                print(f"[DEBUG] Valor extraído de DB_CONNECTIONS: {value}")
                break
        if value is None:
            raise FatalError('DB_CONNECTIONS no encontrada en .env')
        conns = json.loads(value)
        print(f"[DEBUG] Conexiones extraídas: {conns}")
        return conns
    except Exception as e:
        raise FatalError(f'DB_CONNECTIONS no es un JSON válido: {e}')


def validate_mysql_connection(conn: Dict) -> None:
    """
    Valida una conexión MySQL específica.
    
    Args:
        conn: Diccionario con datos de conexión
        
    Raises:
        ValidationError: Si la conexión es inválida
    """
    required_fields = ["host", "user", "pass", "db"]
    missing = [f for f in required_fields if f not in conn]
    if missing:
        raise ValidationError(
            f"Conexión MySQL '{conn.get('name', 'sin nombre')}' "
            f"falta campos requeridos: {', '.join(missing)}. "
            f"Campos requeridos: {', '.join(required_fields)}"
        )


def validate_python_dependencies(required_packages: List[str]) -> Tuple[List[str], List[str]]:
    """
    Valida que los paquetes Python requeridos estén instalados.
    
    Args:
        required_packages: Lista de nombres de paquetes
    
    Returns:
        Tupla (instalados, faltantes)
    """
    installed = []
    missing = []
    
    for package in required_packages:
        try:
            __import__(package.replace("-", "_"))
            installed.append(package)
        except ImportError:
            missing.append(package)
    
    if missing:
        log.warning(f"⚠ Paquetes Python faltantes: {', '.join(missing)}")
    else:
        log.info(f"✓ Todas las dependencias Python están instaladas ({len(installed)})")
    
    return installed, missing


def validate_clickhouse_connection(
    host: str = None,
    port: int = None,
    user: str = None,
    password: str = None,
    timeout: int = 5
) -> bool:
    """
    Valida conectividad a ClickHouse.
    
    Args:
        host: Host de ClickHouse (default: desde env)
        port: Puerto HTTP de ClickHouse (default: desde env)
        user: Usuario (default: desde env)
        password: Contraseña (default: desde env)
        timeout: Timeout en segundos
    
    Returns:
        True si la conexión es exitosa
    
    Raises:
        FatalError: Si no se puede conectar
    """
    try:
        import requests
    except ImportError:
        raise FatalError(
            "Paquete 'requests' no instalado. Instalar con: pip install requests"
        )
    
    host = host or os.getenv("CLICKHOUSE_HTTP_HOST", "clickhouse")
    port = port or int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
    user = user or os.getenv("CH_USER", "default")
    password = password or os.getenv("CH_PASSWORD", "")
    
    url = f"http://{host}:{port}/ping"
    
    try:
        response = requests.get(url, auth=(user, password), timeout=timeout)
        if response.status_code == 200 and response.text.strip() == "Ok.":
            log.info(f"✓ ClickHouse conectado en {host}:{port}")
            return True
        else:
            raise FatalError(
                f"ClickHouse respondió con código {response.status_code}: {response.text}"
            )
    except requests.exceptions.RequestException as e:
        raise FatalError(
            f"No se pudo conectar a ClickHouse en {host}:{port}: {e}. "
            f"Asegúrate de que el servicio esté iniciado y accesible."
        )


def validate_kafka_connection(
    bootstrap_servers: str = None,
    timeout: int = 5
) -> bool:
    """
    Valida conectividad a Kafka.
    
    Args:
        bootstrap_servers: Lista de servidores Kafka (default: desde env)
        timeout: Timeout en segundos
    
    Returns:
        True si la conexión es exitosa
    
    Raises:
        FatalError: Si no se puede conectar
    """
    try:
        import requests
    except ImportError:
        raise FatalError(
            "Paquete 'requests' no instalado. Instalar con: pip install requests"
        )
    
    # Para Kafka Connect, verificamos el endpoint REST
    connect_url = os.getenv("CONNECT_URL", "http://connect:8083")
    
    try:
        response = requests.get(f"{connect_url}/connectors", timeout=timeout)
        if response.status_code == 200:
            log.info(f"✓ Kafka Connect conectado en {connect_url}")
            return True
        else:
            raise FatalError(
                f"Kafka Connect respondió con código {response.status_code}"
            )
    except requests.exceptions.RequestException as e:
        raise FatalError(
            f"No se pudo conectar a Kafka Connect en {connect_url}: {e}. "
            f"Asegúrate de que el servicio esté iniciado y accesible."
        )


def validate_file_exists(filepath: str, description: str = "Archivo") -> Path:
    """
    Valida que un archivo exista.
    
    Args:
        filepath: Ruta del archivo
        description: Descripción del archivo para mensajes
    
    Returns:
        Path del archivo
    
    Raises:
        ValidationError: Si el archivo no existe
    """
    path = Path(filepath)
    if not path.exists():
        raise ValidationError(
            f"{description} no encontrado: {filepath}. "
            f"Verifica que el archivo exista y la ruta sea correcta."
        )
    
    if not path.is_file():
        raise ValidationError(
            f"{description} no es un archivo: {filepath}"
        )
    
    log.info(f"✓ {description} encontrado: {filepath}")
    return path


def validate_directory_exists(dirpath: str, description: str = "Directorio") -> Path:
    """
    Valida que un directorio exista.
    
    Args:
        dirpath: Ruta del directorio
        description: Descripción del directorio para mensajes
    
    Returns:
        Path del directorio
    
    Raises:
        ValidationError: Si el directorio no existe
    """
    path = Path(dirpath)
    if not path.exists():
        raise ValidationError(
            f"{description} no encontrado: {dirpath}. "
            f"Verifica que el directorio exista y la ruta sea correcta."
        )
    
    if not path.is_dir():
        raise ValidationError(
            f"{description} no es un directorio: {dirpath}"
        )
    
    log.info(f"✓ {description} encontrado: {dirpath}")
    return path


def run_validations(
    validate_env: bool = True,
    validate_deps: bool = True,
    validate_connections: bool = True
) -> Dict:
    """
    Ejecuta todas las validaciones configuradas.
    
    Args:
        validate_env: Validar variables de entorno
        validate_deps: Validar dependencias Python
        validate_connections: Validar conexiones a servicios
    
    Returns:
        Diccionario con resultados de validación
    
    Raises:
        FatalError: Si hay errores fatales
    """
    results = {
        "env_vars": {},
        "db_connections": [],
        "dependencies": {"installed": [], "missing": []},
        "connections": {},
        "errors": [],
        "warnings": []
    }
    
    # 1. Variables de entorno
    if validate_env:
        try:
            # Variables críticas
            results["env_vars"]["DB_CONNECTIONS"] = validate_env_var(
                "DB_CONNECTIONS",
                required=False,
                default="[]"
            )
            
            results["env_vars"]["CLICKHOUSE_DATABASE"] = validate_env_var(
                "CLICKHOUSE_DATABASE",
                required=False,
                default="fgeo_analytics"
            )
            
            # Validar DB_CONNECTIONS si está definido
            if results["env_vars"]["DB_CONNECTIONS"] != "[]":
                try:
                    results["db_connections"] = validate_db_connections(
                        results["env_vars"]["DB_CONNECTIONS"]
                    )
                except ValidationError as conn_error:
                    results["warnings"].append(f"DB_CONNECTIONS: {conn_error}")
                    log.warning(f"⚠ DB_CONNECTIONS: {conn_error}")
                    results["db_connections"] = []  # Mantener como lista vacía en caso de error
            else:
                # Si DB_CONNECTIONS está vacío, usar get_db_connections() como fallback
                try:
                    results["db_connections"] = get_db_connections()
                    log.info("✓ DB_CONNECTIONS leído directamente desde .env")
                except Exception as fallback_error:
                    results["warnings"].append(f"No se pudo leer DB_CONNECTIONS: {fallback_error}")
                    results["db_connections"] = []
        except ValidationError as e:
            results["warnings"].append(str(e))
            log.warning(f"⚠ {e}")
            results["db_connections"] = []  # Asegurar que siempre sea una lista
        except FatalError as e:
            results["errors"].append(str(e))
            results["db_connections"] = []  # Asegurar que siempre sea una lista
            raise
    
    # 2. Dependencias Python
    if validate_deps:
        common_packages = [
            "requests",
            "clickhouse_connect",
            "sqlalchemy",
            "pandas",
            "pymysql"
        ]
        installed, missing = validate_python_dependencies(common_packages)
        results["dependencies"]["installed"] = installed
        results["dependencies"]["missing"] = missing
        
        if missing:
            results["warnings"].append(
                f"Paquetes Python faltantes: {', '.join(missing)}"
            )
    
    # 3. Conexiones a servicios
    if validate_connections:
        try:
            # ClickHouse
            validate_clickhouse_connection()
            results["connections"]["clickhouse"] = "OK"
        except FatalError as e:
            results["connections"]["clickhouse"] = str(e)
            results["warnings"].append(f"ClickHouse: {e}")
        
        try:
            # Kafka Connect
            validate_kafka_connection()
            results["connections"]["kafka"] = "OK"
        except FatalError as e:
            results["connections"]["kafka"] = str(e)
            results["warnings"].append(f"Kafka: {e}")
    
    return results


if __name__ == "__main__":
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    
    # DEPURACIÓN: Forzar y mostrar el valor extraído de DB_CONNECTIONS
    try:
        conns = get_db_connections()
        print(f"[DEBUG] Conexiones extraídas: {conns}")
    except Exception as e:
        print(f"[DEBUG] Error extrayendo DB_CONNECTIONS: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(2)

    try:
        results = run_validations()
        print("\n=== Resultados de Validación ===")
        
        # Validar variables de entorno
        env_vars_count = len(results['env_vars']) if results.get('env_vars') else 0
        print(f"✓ Variables de entorno: {env_vars_count}")
        
        # Validar conexiones DB con manejo robusto de None
        db_connections = results.get('db_connections')
        if db_connections is not None:
            print(f"✓ Conexiones DB: {len(db_connections)}")
            if len(db_connections) > 0:
                print(f"  - Bases de datos configuradas: {', '.join([conn.get('db', 'unknown') for conn in db_connections])}")
        else:
            print("✗ Conexiones DB: No configuradas o error en parsing")
        
        # Validar dependencias
        dependencies = results.get('dependencies', {})
        installed_count = len(dependencies.get('installed', []))
        missing_count = len(dependencies.get('missing', []))
        print(f"✓ Dependencias instaladas: {installed_count}")
        
        if missing_count > 0:
            print(f"⚠ Dependencias faltantes: {missing_count}")
            for dep in dependencies.get('missing', []):
                print(f"  - {dep}")
        
        # Mostrar advertencias si existen
        warnings = results.get("warnings", [])
        if warnings:
            print(f"\n⚠ Advertencias ({len(warnings)}):")
            for w in warnings:
                print(f"  - {w}")
        if results["errors"]:
            print(f"\n✗ Errores fatales ({len(results['errors'])}):")
            for e in results["errors"]:
                print(f"  - {e}")
            sys.exit(1)
        print("\n✓ Todas las validaciones pasaron correctamente")
        sys.exit(0)
    except FatalError as e:
        print(f"\n✗ Error fatal: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Error inesperado: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(2)
