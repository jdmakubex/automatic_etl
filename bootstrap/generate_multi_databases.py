#!/usr/bin/env python3
"""
Dynamic ClickHouse Database Generator
====================================

Este script genera automáticamente bases de datos ClickHouse
basándose en el JSON de conexiones MySQL definido en DB_CONNECTIONS

Genera:
- Scripts SQL para crear bases de datos ClickHouse
- Configuración de usuarios y permisos
- Estructura adaptada a múltiples conexiones
"""

import os
import json
import sys
import logging
from pathlib import Path

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ClickHouseDatabaseGenerator:
    def __init__(self):
        """Inicializa el generador de bases de datos ClickHouse"""
        self.db_connections_json = os.getenv("DB_CONNECTIONS", "[]")
        self.output_dir = Path("/bootstrap")
        
        logger.info("🏗️  Inicializando ClickHouse Database Generator")
        
    def parse_database_connections(self) -> list:
        """Parse el JSON de conexiones de bases de datos"""
        try:
            connections = json.loads(self.db_connections_json)
            logger.info(f"📊 Encontradas {len(connections)} conexiones de base de datos")
            for i, conn in enumerate(connections):
                logger.info(f"   {i+1}. {conn.get('name', 'Sin nombre')} - {conn.get('db', 'Sin DB')}")
            return connections
        except json.JSONDecodeError as e:
            logger.error(f"❌ Error parseando DB_CONNECTIONS JSON: {e}")
            return []
    
    def generate_multi_database_sql(self) -> str:
        """Genera SQL para múltiples bases de datos ClickHouse con permisos granulares"""
        connections = self.parse_database_connections()
        
        if not connections:
            logger.warning("⚠️  No hay conexiones, usando configuración por defecto")
            connections = [{"name": "default", "db": "archivos"}]
        
        sql_parts = [
            "-- Auto-generated ClickHouse Multi-Database Setup with Granular Permissions",
            "-- Generated from DB_CONNECTIONS environment variable",
            "-- Each user gets specific permissions on each database",
            "",
            "-- Create users first with comprehensive permissions",
            "CREATE USER IF NOT EXISTS etl IDENTIFIED BY 'Et1Ingest!';",
            "CREATE USER IF NOT EXISTS superset IDENTIFIED BY 'Sup3rS3cret!';",
            "CREATE USER IF NOT EXISTS auditor IDENTIFIED BY 'Audit0r123!';",
            "",
            "-- Global system permissions for ETL user",
            "GRANT SHOW DATABASES ON *.* TO etl;",
            "GRANT SHOW TABLES ON *.* TO etl;",
            "GRANT CREATE DATABASE ON *.* TO etl;",
            "",
            "-- Global read permissions for auditor",
            "GRANT SHOW DATABASES ON *.* TO auditor;", 
            "GRANT SHOW TABLES ON *.* TO auditor;",
            "GRANT SELECT ON system.* TO auditor;",
            "",
            "-- Superset basic permissions",
            "GRANT SHOW DATABASES ON *.* TO superset;",
            "GRANT SHOW TABLES ON *.* TO superset;",
            ""
        ]
        
        # Generar bases de datos para cada conexión con permisos específicos
        for i, conn in enumerate(connections):
            db_name = conn.get('name', f'default_{i}')
            source_db = conn.get('db', 'unknown')
            source_host = conn.get('host', 'unknown')
            source_port = conn.get('port', 'unknown')
            clickhouse_db = f"fgeo_{db_name}"
            
            logger.info(f"🏗️  Generando configuración granular para: {clickhouse_db}")
            logger.info(f"    Fuente: {source_db}@{source_host}:{source_port}")
            
            sql_parts.extend([
                f"-- ================================================================",
                f"-- DATABASE: {clickhouse_db}",
                f"-- Source: {source_db} from {source_host}:{source_port}",
                f"-- Connection: {db_name}",
                f"-- ================================================================",
                "",
                f"-- Create database",
                f"CREATE DATABASE IF NOT EXISTS {clickhouse_db};",
                "",
                f"-- ETL User - Full permissions on {clickhouse_db}",
                f"GRANT ALL ON {clickhouse_db}.* TO etl WITH GRANT OPTION;",
                f"GRANT SELECT, INSERT, CREATE TABLE, CREATE VIEW, ALTER, DROP ON {clickhouse_db}.* TO etl;",
                "",
                f"-- Superset User - Read permissions on {clickhouse_db}",
                f"GRANT SELECT ON {clickhouse_db}.* TO superset;",
                f"GRANT SHOW TABLES ON {clickhouse_db}.* TO superset;",
                "",
                f"-- Auditor User - Read and validation permissions on {clickhouse_db}",
                f"GRANT SELECT ON {clickhouse_db}.* TO auditor;",
                f"GRANT SHOW TABLES ON {clickhouse_db}.* TO auditor;",
                "",
                f"-- Metadata table for connection tracking in {clickhouse_db}",
                f"CREATE TABLE IF NOT EXISTS {clickhouse_db}.connection_metadata (",
                "    connection_name String,",
                "    source_type String,",
                "    source_host String,",
                "    source_port UInt16,",
                "    source_database String,",
                "    clickhouse_database String,",
                "    created_at DateTime DEFAULT now(),",
                "    last_updated DateTime DEFAULT now(),",
                "    status String DEFAULT 'active'",
                ") ENGINE = MergeTree()",
                "ORDER BY (connection_name, created_at);",
                "",
                f"-- Insert connection metadata for {clickhouse_db}",
                f"INSERT INTO {clickhouse_db}.connection_metadata ",
                f"(connection_name, source_type, source_host, source_port, source_database, clickhouse_database)",
                f"VALUES ('{db_name}', 'mysql', '{source_host}', {source_port}, '{source_db}', '{clickhouse_db}');",
                "",
                f"-- Audit table for permission tracking in {clickhouse_db}",
                f"CREATE TABLE IF NOT EXISTS {clickhouse_db}.permission_audit (",
                "    username String,",
                "    permission_type String,",
                "    database_name String,",
                "    table_name String,",
                "    granted_at DateTime DEFAULT now(),",
                "    granted_by String DEFAULT 'system'",
                ") ENGINE = MergeTree()",
                "ORDER BY (username, granted_at);",
                "",
                f"-- Log permission grants for {clickhouse_db}",
                f"INSERT INTO {clickhouse_db}.permission_audit ",
                f"(username, permission_type, database_name, table_name) VALUES",
                f"('etl', 'ALL', '{clickhouse_db}', '*'),",
                f"('superset', 'SELECT', '{clickhouse_db}', '*'),",
                f"('auditor', 'SELECT', '{clickhouse_db}', '*');",
                "",
                f"-- Test table for validation in {clickhouse_db}",
                f"CREATE TABLE IF NOT EXISTS {clickhouse_db}.test_table (",
                "    id UInt32,",
                "    name String,",
                "    source_connection String,",
                "    created_at DateTime DEFAULT now()",
                ") ENGINE = MergeTree()",
                "ORDER BY id;",
                "",
                f"-- Test data for {clickhouse_db}",
                f"INSERT INTO {clickhouse_db}.test_table (id, name, source_connection) VALUES ",
                f"    (1, 'test_data_from_{db_name}', '{db_name}'), ",
                f"    (2, 'sample_row_{db_name}', '{db_name}'), ",
                f"    (3, 'validation_entry_{db_name}', '{db_name}');",
                "",
                ""
            ])
        
        # Agregar tabla global de auditoría
        sql_parts.extend([
            "-- ================================================================",
            "-- GLOBAL AUDIT AND MONITORING TABLES",
            "-- ================================================================",
            "",
            "-- Global database registry",
            "CREATE DATABASE IF NOT EXISTS etl_system;",
            "",
            "-- Global permissions for system database",
            "GRANT ALL ON etl_system.* TO etl WITH GRANT OPTION;",
            "GRANT SELECT ON etl_system.* TO auditor;",
            "GRANT SELECT ON etl_system.* TO superset;",
            "",
            "-- Database registry table",
            "CREATE TABLE IF NOT EXISTS etl_system.database_registry (",
            "    database_name String,",
            "    connection_name String,",
            "    source_type String,",
            "    source_info String,",
            "    status String,",
            "    created_at DateTime DEFAULT now(),",
            "    last_validated DateTime DEFAULT now()",
            ") ENGINE = MergeTree()",
            "ORDER BY (database_name, created_at);",
            "",
            "-- User permissions registry",
            "CREATE TABLE IF NOT EXISTS etl_system.user_permissions (",
            "    username String,",
            "    database_name String,",
            "    permission_type String,",
            "    granted_at DateTime DEFAULT now(),",
            "    validated_at DateTime DEFAULT now()",
            ") ENGINE = MergeTree()",
            "ORDER BY (username, database_name, granted_at);",
            ""
        ])
        
        # Insertar datos en el registro global
        for conn in connections:
            db_name = conn.get('name', 'default')
            source_db = conn.get('db', 'unknown')
            source_host = conn.get('host', 'unknown')
            source_port = conn.get('port', 'unknown')
            clickhouse_db = f"fgeo_{db_name}"
            
            sql_parts.extend([
                f"-- Register {clickhouse_db} in global registry",
                f"INSERT INTO etl_system.database_registry ",
                f"(database_name, connection_name, source_type, source_info, status) VALUES ",
                f"('{clickhouse_db}', '{db_name}', 'mysql', '{source_db}@{source_host}:{source_port}', 'active');",
                "",
                f"-- Register permissions for {clickhouse_db}",
                f"INSERT INTO etl_system.user_permissions ",
                f"(username, database_name, permission_type) VALUES ",
                f"('etl', '{clickhouse_db}', 'ALL'),",
                f"('superset', '{clickhouse_db}', 'SELECT'),",
                f"('auditor', '{clickhouse_db}', 'SELECT');",
                ""
            ])
        
        return "\n".join(sql_parts)
    
    def generate_superset_yaml_configs(self) -> dict:
        """Genera configuraciones YAML para Superset de cada base de datos"""
        connections = self.parse_database_connections()
        configs = {}
        
        if not connections:
            connections = [{"name": "default", "db": "archivos"}]
        
        for conn in connections:
            db_name = conn.get('name', 'default')
            clickhouse_db = f"fgeo_{db_name}"
            
            config = {
                "version": "1.0.0",
                "databases": [{
                    "database_name": f"ClickHouse {db_name}",
                    "sqlalchemy_uri": f"clickhousedb+connect://superset:Sup3rS3cret!@clickhouse:8123/{clickhouse_db}",
                    "extra": "{}"
                }]
            }
            
            configs[f"clickhouse_{db_name}_db.yaml"] = config
            
        return configs
    
    def write_files(self):
        """Escribe los archivos generados"""
        try:
            # Generar SQL multi-database
            sql_content = self.generate_multi_database_sql()
            sql_file = self.output_dir / "clickhouse_multi_init.sql"
            
            with open(sql_file, 'w', encoding='utf-8') as f:
                f.write(sql_content)
            
            logger.info(f"✅ Generado: {sql_file}")
            
            # Generar configuraciones YAML de Superset
            yaml_configs = self.generate_superset_yaml_configs()
            
            for filename, config in yaml_configs.items():
                yaml_file = self.output_dir / filename
                
                # Convertir a YAML simple (sin dependencias externas)
                yaml_content = f"""version: {config['version']}
databases:
  - database_name: {config['databases'][0]['database_name']}
    sqlalchemy_uri: {config['databases'][0]['sqlalchemy_uri']}
    extra: "{config['databases'][0]['extra']}"
"""
                
                with open(yaml_file, 'w', encoding='utf-8') as f:
                    f.write(yaml_content)
                
                logger.info(f"✅ Generado: {yaml_file}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error escribiendo archivos: {e}")
            return False
    
    def generate_all(self):
        """Genera toda la configuración multi-database"""
        logger.info("🚀 Iniciando generación de configuración multi-database")
        
        if self.write_files():
            logger.info("✅ Configuración multi-database generada exitosamente")
            return True
        else:
            logger.error("❌ Error generando configuración")
            return False

def main():
    """Función principal"""
    generator = ClickHouseDatabaseGenerator()
    
    logger.info("=" * 60)
    logger.info("🏗️  CLICKHOUSE MULTI-DATABASE GENERATOR")
    logger.info("=" * 60)
    
    if generator.generate_all():
        logger.info("🎉 Generación completada exitosamente!")
        return 0
    else:
        logger.error("❌ Error en la generación")
        return 1

if __name__ == "__main__":
    sys.exit(main())