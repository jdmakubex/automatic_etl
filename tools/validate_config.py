#!/usr/bin/env python3
"""
validate_config.py
Valida la configuración global y las variables de entorno requeridas para el pipeline ETL.
Ejecución recomendada: Docker o local.
📋 VALIDADOR DE CONFIGURACIÓN DEL PIPELINE ETL
Valida que todas las variables de entorno estén correctamente configuradas antes de ejecutar el pipeline completo.
"""

import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables de entorno
ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

def print_section(title: str):
    """Imprimir sección con formato"""
    print(f"\n{'='*60}")
    print(f"🔍 {title}")
    print('='*60)

def check_required_vars(vars_dict: dict, section: str) -> bool:
    """Verificar variables requeridas"""
    print(f"\n📋 Verificando {section}:")
    all_ok = True
    
    for var_name, description in vars_dict.items():
        value = os.getenv(var_name)
        if value:
            # Censurar contraseñas
            display_value = "***" if "password" in var_name.lower() or "pass" in var_name.lower() else value
            print(f"✅ {var_name}: {display_value}")
        else:
            print(f"❌ {var_name}: NO CONFIGURADA - {description}")
            all_ok = False
    
    return all_ok

def validate_db_connections() -> bool:
    """Validar formato de DB_CONNECTIONS"""
    print(f"\n📋 Verificando DB_CONNECTIONS:")
    
    connections = os.getenv("DB_CONNECTIONS")
    if not connections:
        print("❌ DB_CONNECTIONS no está configurada")
        return False
    
    try:
        parsed = json.loads(connections)
        if not isinstance(parsed, list) or len(parsed) == 0:
            print("❌ DB_CONNECTIONS debe ser un array JSON con al menos una conexión")
            return False
        
        print(f"✅ DB_CONNECTIONS: {len(parsed)} conexiones configuradas")
        for i, conn in enumerate(parsed):
            required_fields = ["name", "host", "port", "user", "pass", "db"]
            missing = [field for field in required_fields if field not in conn]
            if missing:
                print(f"❌ Conexión {i+1} falta campos: {missing}")
                return False
            print(f"   - {conn['name']}: {conn['host']}:{conn['port']}/{conn['db']}")
        
        return True
    except json.JSONDecodeError as e:
        print(f"❌ DB_CONNECTIONS tiene formato JSON inválido: {e}")
        return False

def validate_ports() -> bool:
    """Validar que los puertos sean numéricos"""
    print(f"\n📋 Verificando puertos:")
    
    port_vars = {
        "MYSQL_PORT": "Puerto de MySQL",
        "CLICKHOUSE_HTTP_PORT": "Puerto HTTP de ClickHouse", 
        "CLICKHOUSE_NATIVE_PORT": "Puerto nativo de ClickHouse",
        "SUPERSET_PORT": "Puerto de Superset",
        "KAFKA_EXTERNAL_PORT": "Puerto externo de Kafka"
    }
    
    all_ok = True
    for var_name, description in port_vars.items():
        value = os.getenv(var_name)
        if value:
            try:
                port = int(value)
                if 1 <= port <= 65535:
                    print(f"✅ {var_name}: {port}")
                else:
                    print(f"❌ {var_name}: {port} (fuera de rango 1-65535)")
                    all_ok = False
            except ValueError:
                print(f"❌ {var_name}: '{value}' no es un número válido")
                all_ok = False
        else:
            print(f"⚠️  {var_name}: No configurado (usando default)")
    
    return all_ok

def main():
    """Función principal de validación"""
    print_section("VALIDACIÓN DE CONFIGURACIÓN DEL PIPELINE ETL")
    
    all_valid = True
    
    # Variables requeridas por sección
    mysql_vars = {
        "MYSQL_HOST": "Dirección IP/hostname del servidor MySQL",
        "MYSQL_PORT": "Puerto del servidor MySQL (usualmente 3306)",
        "MYSQL_USER": "Usuario para conexión a MySQL",
        "MYSQL_PASSWORD": "Contraseña para conexión a MySQL", 
        "MYSQL_DATABASE": "Base de datos de origen"
    }
    
    kafka_vars = {
        "KAFKA_CLUSTER_ID": "ID único del clúster Kafka",
        "KAFKA_BROKERS": "Lista de brokers Kafka internos",
        "KAFKA_EXTERNAL_PORT": "Puerto externo de Kafka"
    }
    
    debezium_vars = {
        "CONNECT_URL": "URL del servicio Kafka Connect",
        "DBZ_SERVER_NAME_PREFIX": "Prefijo para nombres de servidor Debezium",
        "DBZ_SNAPSHOT_MODE": "Modo de snapshot (initial/schema_only)"
    }
    
    clickhouse_vars = {
        "CLICKHOUSE_HOST": "Hostname del servidor ClickHouse",
        "CLICKHOUSE_HTTP_PORT": "Puerto HTTP de ClickHouse",
        "CLICKHOUSE_DATABASE": "Base de datos destino en ClickHouse",
        "CLICKHOUSE_ETL_USER": "Usuario ETL para ClickHouse",
        "CLICKHOUSE_ETL_PASSWORD": "Contraseña del usuario ETL"
    }
    
    superset_vars = {
        "SUPERSET_URL": "URL del servicio Superset",
        "SUPERSET_ADMIN": "Usuario administrador de Superset",
        "SUPERSET_PASSWORD": "Contraseña del administrador",
        "SUPERSET_SECRET_KEY": "Clave secreta de Superset"
    }
    
    # Verificar cada sección
    all_valid &= check_required_vars(mysql_vars, "MYSQL")
    all_valid &= check_required_vars(kafka_vars, "KAFKA") 
    all_valid &= check_required_vars(debezium_vars, "DEBEZIUM")
    all_valid &= check_required_vars(clickhouse_vars, "CLICKHOUSE")
    all_valid &= check_required_vars(superset_vars, "SUPERSET")
    
    # Validaciones especiales
    all_valid &= validate_db_connections()
    all_valid &= validate_ports()
    
    # Resultado final
    print_section("RESULTADO DE LA VALIDACIÓN")
    
    if all_valid:
        print("🎉 ¡TODAS LAS CONFIGURACIONES SON VÁLIDAS!")
        print("✅ El pipeline puede ejecutarse correctamente")
        print("\n💡 Para ejecutar el pipeline completo:")
        print("   docker compose down && docker compose up -d")
        return 0
    else:
        print("❌ HAY ERRORES EN LA CONFIGURACIÓN")
        print("⚠️  Corrije los errores antes de ejecutar el pipeline")
        print("\n📝 Edita el archivo .env y vuelve a ejecutar:")
        print("   python3 tools/validate_config.py")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)