#!/usr/bin/env python3
"""
Diagnóstico y solución de problemas de Debezium MySQL CDC
"""

import requests
import json
import logging
import sys
import mysql.connector
from typing import Dict, List

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

KAFKA_CONNECT_URL = "http://localhost:8083"

def check_connector_status(connector_name: str) -> Dict:
    """Verifica el estado de un conector"""
    try:
        resp = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{connector_name}/status", timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"Error obteniendo estado de {connector_name}: {e}")
        return {}

def check_mysql_binlog(host: str, port: int, user: str, password: str, database: str) -> Dict:
    """Verifica si el binlog está habilitado en MySQL"""
    result = {
        "binlog_enabled": False,
        "binlog_format": None,
        "binlog_row_image": None,
        "server_id": None,
        "errors": []
    }
    
    try:
        conn = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
        cursor = conn.cursor()
        
        # Verificar si binlog está habilitado
        cursor.execute("SHOW VARIABLES LIKE 'log_bin'")
        log_bin = cursor.fetchone()
        if log_bin and log_bin[1] == 'ON':
            result["binlog_enabled"] = True
        else:
            result["errors"].append("⚠️ Binlog NO está habilitado (log_bin=OFF)")
        
        # Verificar binlog_format
        cursor.execute("SHOW VARIABLES LIKE 'binlog_format'")
        binlog_format = cursor.fetchone()
        if binlog_format:
            result["binlog_format"] = binlog_format[1]
            if binlog_format[1] != 'ROW':
                result["errors"].append(f"⚠️ binlog_format={binlog_format[1]}, debe ser ROW")
        
        # Verificar binlog_row_image
        cursor.execute("SHOW VARIABLES LIKE 'binlog_row_image'")
        binlog_row_image = cursor.fetchone()
        if binlog_row_image:
            result["binlog_row_image"] = binlog_row_image[1]
            if binlog_row_image[1] != 'FULL':
                result["errors"].append(f"ℹ️ binlog_row_image={binlog_row_image[1]}, recomendado FULL")
        
        # Verificar server_id
        cursor.execute("SHOW VARIABLES LIKE 'server_id'")
        server_id = cursor.fetchone()
        if server_id:
            result["server_id"] = server_id[1]
            if server_id[1] == '0':
                result["errors"].append("⚠️ server_id=0, debe ser > 0")
        
        cursor.close()
        conn.close()
        
    except mysql.connector.Error as e:
        result["errors"].append(f"❌ Error conectando a MySQL: {e}")
    
    return result

def get_connector_config(connector_name: str) -> Dict:
    """Obtiene la configuración de un conector"""
    try:
        resp = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{connector_name}", timeout=10)
        resp.raise_for_status()
        return resp.json().get("config", {})
    except Exception as e:
        logger.error(f"Error obteniendo config de {connector_name}: {e}")
        return {}

def update_connector_to_schema_only(connector_name: str) -> bool:
    """Actualiza un conector para usar snapshot.mode=schema_only"""
    try:
        config = get_connector_config(connector_name)
        if not config:
            return False
        
        # Cambiar snapshot.mode a schema_only
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
    
    # 3. Verificar configuración de MySQL (solo primer conector como ejemplo)
    if connectors:
        logger.info("\n" + "=" * 60)
        logger.info("🔍 VERIFICACIÓN DE MySQL")
        logger.info("=" * 60)
        
        first_conn = connectors[0]
        config = connector_configs.get(first_conn, {})
        
        host = config.get("database.hostname")
        port = int(config.get("database.port", 3306))
        user = config.get("database.user")
        password = config.get("database.password")
        database = config.get("database.include.list", "").split(",")[0] if config.get("database.include.list") else None
        
        # Si no hay database.include.list, usar la primera tabla de table.include.list
        if not database and config.get("table.include.list"):
            first_table = config.get("table.include.list", "").split(",")[0]
            if "." in first_table:
                database = first_table.split(".")[0]
        
        if host and user and password and database:
            logger.info(f"\n🔗 Verificando MySQL en {host}:{port} / {database}")
            binlog_status = check_mysql_binlog(host, port, user, password, database)
            
            logger.info(f"\n📊 Estado del Binlog:")
            logger.info(f"   Binlog Enabled: {'✅ SÍ' if binlog_status['binlog_enabled'] else '❌ NO'}")
            logger.info(f"   Binlog Format: {binlog_status['binlog_format']}")
            logger.info(f"   Binlog Row Image: {binlog_status['binlog_row_image']}")
            logger.info(f"   Server ID: {binlog_status['server_id']}")
            
            if binlog_status['errors']:
                logger.warning(f"\n⚠️ Problemas encontrados:")
                for error in binlog_status['errors']:
                    logger.warning(f"   {error}")
            
            # 4. Proponer solución
            if not binlog_status['binlog_enabled']:
                logger.info("\n" + "=" * 60)
                logger.info("💡 SOLUCIÓN RECOMENDADA")
                logger.info("=" * 60)
                logger.warning("\n⚠️ El binlog NO está habilitado en MySQL.")
                logger.info("\n📝 Opciones:")
                logger.info("\n1️⃣ OPCIÓN PREFERIDA: Habilitar binlog en MySQL")
                logger.info("   - Editar /etc/my.cnf o /etc/mysql/my.cnf")
                logger.info("   - Agregar:")
                logger.info("     [mysqld]")
                logger.info("     server-id=1")
                logger.info("     log_bin=mysql-bin")
                logger.info("     binlog_format=ROW")
                logger.info("     binlog_row_image=FULL")
                logger.info("   - Reiniciar MySQL")
                logger.info("\n2️⃣ WORKAROUND: Usar snapshot.mode=schema_only")
                logger.info("   - Solo captura cambios nuevos (no snapshot inicial)")
                logger.info("   - Las tablas raw estarán vacías hasta que haya INSERT/UPDATE")
                logger.info("\n¿Aplicar WORKAROUND? (y/N): ", end="")
                
                try:
                    choice = input().strip().lower()
                    if choice == 'y':
                        logger.info("\n🔧 Aplicando workaround...")
                        for conn_name in connectors:
                            update_connector_to_schema_only(conn_name)
                        logger.info("\n✅ Workaround aplicado. Reinicia los conectores:")
                        logger.info("   docker compose restart connect")
                except KeyboardInterrupt:
                    logger.info("\n\nOperación cancelada")
                    sys.exit(0)
            else:
                logger.info("\n✅ MySQL está configurado correctamente para CDC")
        else:
            logger.warning("⚠️ No se pudo extraer configuración de MySQL del conector")
    
    logger.info("\n" + "=" * 60)
    logger.info("🏁 DIAGNÓSTICO COMPLETADO")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
