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
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables de entorno
ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

# Configurar logging
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

def execute_clickhouse_query(query, database=None):
    """Ejecutar query en ClickHouse"""
    try:
        url = "http://clickhouse:8123/"
        if database:
            url += f"?database={database}"
        
        response = requests.post(url, data=query, timeout=30)
        if response.status_code == 200:
            return True, response.text.strip()
        else:
            return False, response.text
    except Exception as e:
        return False, str(e)

def create_databases():
    """Crear bases de datos necesarias"""
    logger.info("🏗️ Creando bases de datos...")
    
    databases = [
        os.getenv("CLICKHOUSE_DATABASE", "fgeo_analytics"),
        "ext"  # Para tablas externas/temporales
    ]
    
    for db in databases:
        success, result = execute_clickhouse_query(f"CREATE DATABASE IF NOT EXISTS {db}")
        if success:
            logger.info(f"✅ Base de datos '{db}' creada/verificada")
        else:
            logger.error(f"❌ Error creando base de datos '{db}': {result}")
            return False
    
    return True

def create_users():
    """Crear usuarios necesarios"""
    logger.info("👥 Configurando usuarios...")
    
    users = [
        {
            'name': 'etl',
            'password': os.getenv('CLICKHOUSE_ETL_PASSWORD', 'Et1Ingest!'),
            'grants': ['ALL']
        },
        {
            'name': 'superset', 
            'password': os.getenv('CLICKHOUSE_PASSWORD', 'Sup3rS3cret!'),
            'grants': ['SELECT']
        }
    ]
    
    for user in users:
        # Crear usuario
        query = f"CREATE USER IF NOT EXISTS {user['name']} IDENTIFIED BY '{user['password']}'"
        success, result = execute_clickhouse_query(query)
        if success:
            logger.info(f"✅ Usuario '{user['name']}' creado/verificado")
        else:
            logger.warning(f"⚠️ Usuario '{user['name']}': {result}")
        
        # Otorgar permisos
        db_name = os.getenv("CLICKHOUSE_DATABASE", "fgeo_analytics")
        for grant in user['grants']:
            if grant == 'ALL':
                query = f"GRANT ALL ON {db_name}.* TO {user['name']}"
            else:
                query = f"GRANT {grant} ON {db_name}.* TO {user['name']}"
            
            success, result = execute_clickhouse_query(query)
            if success:
                logger.info(f"✅ Permiso '{grant}' otorgado a '{user['name']}'")
            else:
                logger.warning(f"⚠️ Permiso '{grant}' para '{user['name']}': {result}")
    
    return True

def create_initial_tables():
    """Crear tablas iniciales con datos de prueba"""
    logger.info("📊 Creando tablas iniciales...")
    
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
        logger.info("✅ Tabla test_table creada")
        
        # Insertar datos de prueba
        insert_query = f"""
        INSERT INTO {db_name}.test_table (id, name) VALUES 
        (1, 'Pipeline Test 1'),
        (2, 'Pipeline Test 2'), 
        (3, 'Pipeline Test 3')
        """
        
        success, result = execute_clickhouse_query(insert_query)
        if success:
            logger.info("✅ Datos de prueba insertados")
        else:
            logger.warning(f"⚠️ Error insertando datos de prueba: {result}")
    else:
        logger.error(f"❌ Error creando tabla test_table: {result}")
        return False
    
    return True

def validate_setup():
    """Validar la configuración completa"""
    logger.info("✅ Validando configuración...")
    
    db_name = os.getenv("CLICKHOUSE_DATABASE", "fgeo_analytics")
    
    # Verificar bases de datos
    success, result = execute_clickhouse_query("SHOW DATABASES")
    if success:
        databases = result.split('\n')
        logger.info(f"📊 Bases de datos disponibles: {', '.join(databases)}")
    
    # Verificar usuarios  
    success, result = execute_clickhouse_query("SHOW USERS")
    if success:
        users = result.split('\n')
        logger.info(f"👥 Usuarios disponibles: {', '.join(users)}")
    
    # Verificar tablas
    success, result = execute_clickhouse_query(f"SHOW TABLES FROM {db_name}")
    if success:
        tables = result.split('\n') if result else []
        logger.info(f"📋 Tablas en {db_name}: {', '.join(tables)}")
    
    # Contar registros en tabla de prueba
    success, result = execute_clickhouse_query(f"SELECT count() FROM {db_name}.test_table")
    if success:
        count = result.strip()
        logger.info(f"📊 Registros en test_table: {count}")
    
    return True

def main():
    """Función principal"""
    logger.info("🚀 === CONFIGURACIÓN AUTOMÁTICA DE CLICKHOUSE ===")
    
    try:
        # 1. Esperar ClickHouse
        if not wait_for_clickhouse():
            logger.error("❌ ClickHouse no disponible, abortando")
            return 1
        
        # 2. Crear bases de datos
        if not create_databases():
            logger.error("❌ Error creando bases de datos")
            return 1
        
        # 3. Crear usuarios
        if not create_users():
            logger.error("❌ Error configurando usuarios")
            return 1
        
        # 4. Crear tablas iniciales
        if not create_initial_tables():
            logger.error("❌ Error creando tablas iniciales")
            return 1
        
        # 5. Validar configuración
        if not validate_setup():
            logger.error("❌ Error en validación")
            return 1
        
        logger.info("🎉 ClickHouse configurado exitosamente")
        return 0
        
    except Exception as e:
        logger.error(f"💥 Error crítico: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())