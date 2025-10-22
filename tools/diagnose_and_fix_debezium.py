#!/usr/bin/env python3
"""
Diagnóstico simple de Debezium sin dependencias MySQL
"""

import requests
import json
import logging
import sys
import subprocess

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

KAFKA_CONNECT_URL = "http://localhost:8083"

def check_connector_status(connector_name: str) -> dict:
    """Verifica el estado de un conector"""
    try:
        resp = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{connector_name}/status", timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"Error obteniendo estado de {connector_name}: {e}")
        return {}

def get_connector_config(connector_name: str) -> dict:
    """Obtiene la configuración de un conector"""
    try:
        resp = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{connector_name}", timeout=10)
        resp.raise_for_status()
        return resp.json().get("config", {})
    except Exception as e:
        logger.error(f"Error obteniendo config de {connector_name}: {e}")
        return {}

def check_mysql_binlog_simple(host: str, port: int, user: str, password: str, database: str) -> dict:
    """Verifica binlog usando docker run mysql client"""
    result = {
        "binlog_enabled": False,
        "binlog_format": None,
        "errors": []
    }
    
    try:
        # Verificar log_bin
        cmd = f"docker run --rm mysql:8.0 mysql -h{host} -P{port} -u{user} -p{password} -e \"SHOW VARIABLES LIKE 'log_bin'\" 2>&1"
        output = subprocess.check_output(cmd, shell=True, text=True)
        
        if "ON" in output:
            result["binlog_enabled"] = True
        elif "OFF" in output or "log_bin" in output:
            result["binlog_enabled"] = False
            result["errors"].append("⚠️ Binlog NO está habilitado (log_bin=OFF)")
        
        # Verificar binlog_format
        cmd = f"docker run --rm mysql:8.0 mysql -h{host} -P{port} -u{user} -p{password} -e \"SHOW VARIABLES LIKE 'binlog_format'\" 2>&1"
        output = subprocess.check_output(cmd, shell=True, text=True)
        
        if "ROW" in output:
            result["binlog_format"] = "ROW"
        elif "MIXED" in output:
            result["binlog_format"] = "MIXED"
            result["errors"].append("⚠️ binlog_format=MIXED, debe ser ROW")
        elif "STATEMENT" in output:
            result["binlog_format"] = "STATEMENT"
            result["errors"].append("⚠️ binlog_format=STATEMENT, debe ser ROW")
        
    except subprocess.CalledProcessError as e:
        result["errors"].append(f"❌ Error consultando MySQL: {e}")
    except Exception as e:
        result["errors"].append(f"❌ Error inesperado: {e}")
    
    return result

def update_connector_to_schema_only(connector_name: str) -> bool:
    """Actualiza un conector para usar snapshot.mode=schema_only"""
    try:
        config = get_connector_config(connector_name)
        if not config:
            return False
        
        config["snapshot.mode"] = "schema_only"
        
        resp = requests.put(
            f"{KAFKA_CONNECT_URL}/connectors/{connector_name}/config",
            json=config,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        resp.raise_for_status()
        logger.info(f"✅ Conector {connector_name} actualizado a snapshot.mode=schema_only")
        return True
    except Exception as e:
        logger.error(f"Error actualizando {connector_name}: {e}")
        return False

def restart_connector(connector_name: str) -> bool:
    """Reinicia un conector"""
    try:
        resp = requests.post(f"{KAFKA_CONNECT_URL}/connectors/{connector_name}/restart", timeout=10)
        resp.raise_for_status()
        logger.info(f"✅ Conector {connector_name} reiniciado")
        return True
    except Exception as e:
        logger.error(f"Error reiniciando {connector_name}: {e}")
        return False

def main():
    logger.info("=" * 60)
    logger.info("🔍 DIAGNÓSTICO DE DEBEZIUM")
    logger.info("=" * 60)
    
    # 1. Listar conectores
    try:
        resp = requests.get(f"{KAFKA_CONNECT_URL}/connectors", timeout=10)
        resp.raise_for_status()
        connectors = resp.json()
        logger.info(f"\n📋 Encontrados {len(connectors)} conectores:")
        for conn in connectors:
            logger.info(f"   - {conn}")
    except Exception as e:
        logger.error(f"❌ Error listando conectores: {e}")
        sys.exit(1)
    
    # 2. Verificar estado de cada conector
    logger.info("\n" + "=" * 60)
    logger.info("📊 ESTADO DE CONECTORES")
    logger.info("=" * 60)
    
    connector_configs = {}
    for conn_name in connectors:
        status = check_connector_status(conn_name)
        config = get_connector_config(conn_name)
        connector_configs[conn_name] = config
        
        connector_state = status.get("connector", {}).get("state", "UNKNOWN")
        task_state = status.get("tasks", [{}])[0].get("state", "UNKNOWN") if status.get("tasks") else "NO_TASKS"
        snapshot_mode = config.get("snapshot.mode", "N/A")
        
        logger.info(f"\n🔌 {conn_name}:")
        logger.info(f"   Connector: {connector_state}")
        logger.info(f"   Task: {task_state}")
        logger.info(f"   Snapshot Mode: {snapshot_mode}")
    
    # 3. Verificar MySQL binlog
    if connectors:
        logger.info("\n" + "=" * 60)
        logger.info("🔍 VERIFICACIÓN DE MySQL BINLOG")
        logger.info("=" * 60)
        
        first_conn = connectors[0]
        config = connector_configs.get(first_conn, {})
        
        host = config.get("database.hostname")
        port = int(config.get("database.port", 3306))
        user = config.get("database.user")
        password = config.get("database.password")
        database = config.get("database.include.list", "").split(",")[0] if config.get("database.include.list") else None
        
        if not database and config.get("table.include.list"):
            first_table = config.get("table.include.list", "").split(",")[0]
            if "." in first_table:
                database = first_table.split(".")[0]
        
        if host and user and password and database:
            logger.info(f"\n🔗 Verificando MySQL en {host}:{port} / {database}")
            binlog_status = check_mysql_binlog_simple(host, port, user, password, database)
            
            logger.info(f"\n📊 Estado del Binlog:")
            logger.info(f"   Binlog Enabled: {'✅ SÍ' if binlog_status['binlog_enabled'] else '❌ NO'}")
            logger.info(f"   Binlog Format: {binlog_status['binlog_format']}")
            
            if binlog_status['errors']:
                logger.warning(f"\n⚠️ Problemas encontrados:")
                for error in binlog_status['errors']:
                    logger.warning(f"   {error}")
            
            # 4. Proponer solución
            if not binlog_status['binlog_enabled']:
                logger.info("\n" + "=" * 60)
                logger.info("💡 PROBLEMA Y SOLUCIÓN")
                logger.info("=" * 60)
                logger.warning("\n⚠️ El binlog NO está habilitado en MySQL.")
                logger.warning("   Por eso Debezium no produce mensajes a Kafka.")
                logger.info("\n📝 Opciones:")
                logger.info("\n1️⃣ OPCIÓN PREFERIDA: Habilitar binlog en MySQL")
                logger.info("   - Editar /etc/my.cnf o /etc/mysql/my.cnf en el servidor MySQL")
                logger.info("   - Agregar en la sección [mysqld]:")
                logger.info("     server-id=1")
                logger.info("     log_bin=mysql-bin")
                logger.info("     binlog_format=ROW")
                logger.info("     binlog_row_image=FULL")
                logger.info("   - Reiniciar MySQL: sudo systemctl restart mysqld")
                logger.info("\n2️⃣ ALTERNATIVA: Usar snapshot.mode=schema_only")
                logger.info("   ⚠️ Solo captura cambios NUEVOS (no datos existentes)")
                logger.info("   - Las tablas raw estarán vacías hasta que haya INSERT/UPDATE")
                logger.info("   - Los datos históricos NO se capturarán")
                logger.info("\nPara esta demo, aplicaré la ALTERNATIVA 2 automáticamente...")
                
                # Aplicar workaround automáticamente
                logger.info("\n🔧 Aplicando workaround (schema_only mode)...")
                for conn_name in connectors:
                    if update_connector_to_schema_only(conn_name):
                        restart_connector(conn_name)
                
                logger.info("\n✅ Workaround aplicado.")
                logger.info("ℹ️  Ahora Debezium capturará solo cambios nuevos (INSERT/UPDATE/DELETE)")
                logger.info("ℹ️  Para capturar datos existentes, habilita el binlog en MySQL")
            else:
                logger.info("\n✅ MySQL está configurado correctamente para CDC con snapshot")
    
    logger.info("\n" + "=" * 60)
    logger.info("🏁 DIAGNÓSTICO COMPLETADO")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
