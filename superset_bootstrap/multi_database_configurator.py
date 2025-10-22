#!/usr/bin/env python3
"""
Multi-Database Superset Configurator
====================================

Este script configura automáticamente múltiples bases de datos en Superset
basándose en el JSON de conexiones definido en DB_CONNECTIONS del .env

Capacidades:
- Parse JSON de múltiples conexiones MySQL
- Auto-descubrimiento de tablas por cada conexión
- Creación automática de datasets en Superset
- Configuración de conexiones ClickHouse correspondientes
- Manejo de errores por conexión individual
"""

import os
import json
import sys
import logging
import requests
from urllib.parse import quote_plus
import time
from typing import List, Dict, Any

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MultiDatabaseConfigurator:

    def __init__(self):
        """Inicializa el configurador multi-base de datos"""
        self.superset_url = os.getenv("SUPERSET_URL", "http://superset:8088")
        self.superset_admin = os.getenv("SUPERSET_ADMIN", "admin")
        self.superset_password = os.getenv("SUPERSET_PASSWORD", "Admin123!")
        self.db_connections_json = os.getenv("DB_CONNECTIONS", "[]")
        self.clickhouse_host = os.getenv("CLICKHOUSE_HTTP_HOST", "clickhouse")
        self.clickhouse_port = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
        self.clickhouse_user = os.getenv("CLICKHOUSE_USER", "superset_ro")
        self.clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "Sup3rS3cret!")
        self.session = requests.Session()
        self.csrf_token = None
        logger.info("🔧 Inicializando Multi-Database Configurator")
        
    def parse_database_connections(self) -> List[Dict[str, Any]]:
        """Parse el JSON de conexiones de bases de datos"""
        try:
            connections = json.loads(self.db_connections_json)
            logger.info(f"📊 Encontradas {len(connections)} conexiones de base de datos")
            for i, conn in enumerate(connections):
                logger.info(f"   {i+1}. {conn.get('name', 'Sin nombre')} - {conn.get('host', 'Sin host')}:{conn.get('port', 'Sin puerto')}")
            return connections
        except json.JSONDecodeError as e:
            logger.error(f"❌ Error parseando DB_CONNECTIONS JSON: {e}")
            return []
    
    def authenticate_superset(self) -> bool:
        """Autentica con Superset usando session y CSRF"""
        try:
            logger.info("🔐 Iniciando autenticación con Superset...")
            
            # Primero obtener la página de login para el CSRF token
            login_page = self.session.get(f"{self.superset_url}/login/")
            if login_page.status_code != 200:
                logger.error(f"❌ No se pudo acceder a página de login: {login_page.status_code}")
                return False
                
            # Buscar CSRF token en el HTML
            csrf_start = login_page.text.find('csrf_token" value="')
            if csrf_start == -1:
                logger.error("❌ No se encontró CSRF token en página de login")
                return False
                
            csrf_start += len('csrf_token" value="')
            csrf_end = login_page.text.find('"', csrf_start)
            self.csrf_token = login_page.text[csrf_start:csrf_end]
            
            # Realizar login
            login_data = {
                'username': self.superset_admin,
                'password': self.superset_password,
                'csrf_token': self.csrf_token
            }
            
            response = self.session.post(
                f"{self.superset_url}/login/",
                data=login_data,
                allow_redirects=False
            )
            
            if response.status_code in [200, 302] and 'session' in self.session.cookies:
                logger.info("✅ Autenticación exitosa con Superset")
                return True
            else:
                logger.error(f"❌ Falló autenticación: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error en autenticación: {e}")
            return False
    
    def create_clickhouse_database_connection(self, db_config: Dict[str, Any]) -> bool:
        """Crea una conexión de base de datos ClickHouse en Superset"""
        try:
            db_name = db_config.get('name', 'unknown')
            clickhouse_db_name = f"fgeo_{db_name}"
            
            logger.info(f"🔌 Creando conexión ClickHouse para: {db_name}")
            
            # URI de conexión ClickHouse
            uri = f"clickhousedb+connect://{self.clickhouse_user}:{quote_plus(self.clickhouse_password)}@{self.clickhouse_host}:{self.clickhouse_port}/{clickhouse_db_name}"
            
            # Datos para crear la conexión
            connection_data = {
                'database_name': f'ClickHouse {db_name}',
                'sqlalchemy_uri': uri,
                'expose_in_sqllab': True,
                'allow_run_async': True,
                'allow_ctas': True,
                'allow_cvas': True,
                'allow_dml': True,
                'force_ctas_schema': clickhouse_db_name,
                'extra': json.dumps({
                    'metadata_params': {},
                    'engine_params': {},
                    'metadata_cache_timeout': {},
                    'schemas_allowed_for_file_upload': []
                }),
                'csrf_token': self.csrf_token
            }
            
            # POST a la API de databases
            response = self.session.post(
                f"{self.superset_url}/databaseview/add",
                data=connection_data
            )
            
            if response.status_code in [200, 201, 302]:
                logger.info(f"✅ Conexión ClickHouse creada para {db_name}")
                return True
            else:
                logger.warning(f"⚠️  Posible duplicado o error en conexión {db_name}: {response.status_code}")
                return True  # Asumimos que puede ser duplicado
                
        except Exception as e:
            logger.error(f"❌ Error creando conexión ClickHouse para {db_config.get('name', 'unknown')}: {e}")
            return False
    
    def discover_tables_for_database(self, db_config: Dict[str, Any]) -> List[str]:
        """Descubre tablas en ClickHouse para una base de datos específica"""
        try:
            db_name = db_config.get('name', 'unknown')
            clickhouse_db_name = f"fgeo_{db_name}"
            
            logger.info(f"🔍 Descubriendo tablas en ClickHouse para: {db_name}")
            
            # Query para obtener tablas de la base de datos específica
            query = f"SHOW TABLES FROM {clickhouse_db_name}"
            
            response = requests.get(
                f"http://{self.clickhouse_host}:{self.clickhouse_port}/",
                params={
                    'query': query,
                    'user': self.clickhouse_user,
                    'password': self.clickhouse_password
                }
            )
            
            if response.status_code == 200:
                tables = [line.strip() for line in response.text.strip().split('\n') if line.strip()]
                logger.info(f"📋 Encontradas {len(tables)} tablas en {clickhouse_db_name}")
                for table in tables:
                    logger.info(f"   - {table}")
                return tables
            else:
                logger.warning(f"⚠️  No se pudieron obtener tablas de {clickhouse_db_name}: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"❌ Error descubriendo tablas para {db_config.get('name', 'unknown')}: {e}")
            return []
    
    def create_datasets_for_database(self, db_config: Dict[str, Any], tables: List[str]) -> int:
        """Crea datasets en Superset para las tablas de una base de datos"""
        created_count = 0
        db_name = db_config.get('name', 'unknown')
        
        logger.info(f"📊 Creando datasets para base de datos: {db_name}")
        
        for table in tables:
            try:
                # Datos del dataset
                dataset_data = {
                    'table_name': table,
                    'database': f'ClickHouse {db_name}',
                    'schema': f'fgeo_{db_name}',
                    'csrf_token': self.csrf_token
                }
                
                response = self.session.post(
                    f"{self.superset_url}/tablemodelview/add",
                    data=dataset_data
                )
                
                if response.status_code in [200, 201, 302]:
                    logger.info(f"✅ Dataset creado: {table}")
                    created_count += 1
                else:
                    logger.warning(f"⚠️  Posible duplicado dataset {table}: {response.status_code}")
                    
                # Pequeña pausa para no sobrecargar
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"❌ Error creando dataset {table}: {e}")
        
        return created_count
    
    def configure_all_databases(self) -> Dict[str, Any]:
        """Configura todas las bases de datos del JSON de conexiones"""
        logger.info("🚀 Iniciando configuración multi-base de datos")
        
        # Parse conexiones
        connections = self.parse_database_connections()
        if not connections:
            logger.error("❌ No hay conexiones válidas para configurar")
            return {"success": False, "error": "No connections found"}
        
        # Autenticar con Superset
        if not self.authenticate_superset():
            logger.error("❌ No fue posible autenticar con Superset")
            return {"success": False, "error": "Authentication failed"}
        
        # Configurar cada base de datos
        results = {
            "success": True,
            "databases_processed": 0,
            "databases_success": 0,
            "total_datasets_created": 0,
            "details": []
        }
        
        for db_config in connections:
            try:
                db_name = db_config.get('name', 'unknown')
                logger.info(f"\n🔧 Procesando base de datos: {db_name}")
                
                db_result = {
                    "name": db_name,
                    "connection_created": False,
                    "tables_discovered": 0,
                    "datasets_created": 0,
                    "success": False
                }
                
                # Crear conexión ClickHouse
                if self.create_clickhouse_database_connection(db_config):
                    db_result["connection_created"] = True
                    
                    # Descubrir tablas
                    tables = self.discover_tables_for_database(db_config)
                    db_result["tables_discovered"] = len(tables)
                    
                    if tables:
                        # Crear datasets
                        datasets_created = self.create_datasets_for_database(db_config, tables)
                        db_result["datasets_created"] = datasets_created
                        results["total_datasets_created"] += datasets_created
                        
                        if datasets_created > 0:
                            db_result["success"] = True
                            results["databases_success"] += 1
                    else:
                        logger.warning(f"⚠️  No se encontraron tablas para {db_name}")
                else:
                    logger.error(f"❌ No se pudo crear conexión para {db_name}")
                
                results["details"].append(db_result)
                results["databases_processed"] += 1
                
            except Exception as e:
                logger.error(f"❌ Error procesando base de datos {db_config.get('name', 'unknown')}: {e}")
                continue
        
        return results

def main():
    """Función principal"""
    configurator = MultiDatabaseConfigurator()
    
    logger.info("=" * 80)
    logger.info("🌟 MULTI-DATABASE SUPERSET CONFIGURATOR")
    logger.info("=" * 80)
    
    results = configurator.configure_all_databases()
    
    logger.info("\n" + "=" * 80)
    logger.info("📊 RESUMEN DE RESULTADOS")
    logger.info("=" * 80)
    
    if results["success"]:
        logger.info(f"✅ Procesamiento completado:")
        logger.info(f"   📊 Bases de datos procesadas: {results['databases_processed']}")
        logger.info(f"   ✅ Bases de datos exitosas: {results['databases_success']}")
        logger.info(f"   📋 Total datasets creados: {results['total_datasets_created']}")
        
        logger.info("\n📋 Detalle por base de datos:")
        for detail in results["details"]:
            status = "✅" if detail["success"] else "❌"
            logger.info(f"   {status} {detail['name']}: {detail['datasets_created']} datasets creados")
    else:
        logger.error(f"❌ Error general: {results.get('error', 'Unknown error')}")
        return 1
    
    logger.info("\n🎉 Configuración multi-base de datos completada!")
    return 0

if __name__ == "__main__":
    sys.exit(main())