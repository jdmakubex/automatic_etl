#!/usr/bin/env python3
"""
Metabase Schema Discovery Helper
===============================

Utilidades para el descubrimiento dinámico de esquemas y tablas de ClickHouse
para Metabase, basado en DB_CONNECTIONS del .env.

Funciones similares a las de Superset pero adaptadas para Metabase API.
"""
import os
import json
import logging
from typing import List, Dict, Any, Optional
from clickhouse_connect import get_client
from dotenv import load_dotenv

load_dotenv('/app/.env')
logger = logging.getLogger(__name__)

class MetabaseSchemaDiscovery:
    """Helper para descubrimiento de esquemas en ClickHouse para Metabase"""
    
    def __init__(self):
        """Inicializa el descubridor de esquemas"""
        self.clickhouse_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
        self.clickhouse_port = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
        self.clickhouse_user = os.getenv("CH_USER", "etl")
        self.clickhouse_password = os.getenv("CH_PASSWORD", "Et1Ingest!")
        self.clickhouse_database = os.getenv("CLICKHOUSE_DATABASE", "fgeo_analytics")
        
        self.client = None
    
    def connect_clickhouse(self) -> bool:
        """Conecta a ClickHouse para consultas directas"""
        try:
            self.client = get_client(
                host=self.clickhouse_host,
                port=self.clickhouse_port,
                username=self.clickhouse_user,
                password=self.clickhouse_password,
                database=self.clickhouse_database
            )
            logger.info("✅ Conectado a ClickHouse para descubrimiento")
            return True
        except Exception as e:
            logger.error(f"❌ Error conectando a ClickHouse: {e}")
            return False
    
    def parse_schemas_from_env(self) -> List[str]:
        """
        Descubre esquemas de ClickHouse dinámicamente desde DB_CONNECTIONS.
        Implementación similar a la función de Superset.
        """
        logger.info("🔍 Descubriendo esquemas desde DB_CONNECTIONS...")
        
        # 1) METABASE_SCHEMAS explícito (comma-separated)
        explicit_schemas = os.getenv("METABASE_SCHEMAS", "").strip()
        if explicit_schemas:
            schemas = [s.strip() for s in explicit_schemas.split(",") if s.strip()]
            if schemas:
                logger.info(f"✅ Esquemas explícitos desde METABASE_SCHEMAS: {schemas}")
                return sorted(set(schemas))
        
        # 2) Derivar desde DB_CONNECTIONS JSON
        system_schemas = {"information_schema", "mysql", "performance_schema", "sys"}
        excluded_patterns = ["_analytics", "ext", "default", "system", "INFORMATION_SCHEMA"]
        
        try:
            db_connections_str = os.getenv("DB_CONNECTIONS", "").strip()
            schemas = []
            
            if db_connections_str.startswith("[") and "]" in db_connections_str:
                connections = json.loads(db_connections_str)
                if isinstance(connections, dict):
                    connections = [connections]
                
                for conn in connections or []:
                    db_name = (conn or {}).get("db")
                    if db_name and db_name not in system_schemas:
                        # Excluir variantes técnicas
                        if not any(pattern in db_name for pattern in excluded_patterns):
                            # Generar nombre de esquema ClickHouse
                            conn_name = conn.get("name", db_name)
                            ch_schema = f"fgeo_{conn_name}"
                            schemas.append(ch_schema)
                            logger.info(f"   📊 {db_name} → {ch_schema}")
                        else:
                            logger.info(f"   ⚠️  Excluido: {db_name} (patrón técnico)")
            
            # Agregar esquema principal si no está incluido
            if self.clickhouse_database not in schemas:
                schemas.append(self.clickhouse_database)
            
            if schemas:
                result = sorted(set(schemas))
                logger.info(f"✅ Esquemas derivados: {result}")
                return result
                
        except json.JSONDecodeError as e:
            logger.warning(f"⚠️  Error parseando DB_CONNECTIONS: {e}")
        except Exception as e:
            logger.warning(f"⚠️  Error procesando esquemas: {e}")
        
        # 3) Fallback default
        default_schema = os.getenv("METABASE_SCHEMA", self.clickhouse_database)
        logger.info(f"✅ Esquema fallback: {default_schema}")
        return [default_schema]
    
    def discover_available_schemas(self) -> List[str]:
        """Descubre esquemas realmente disponibles en ClickHouse"""
        if not self.client:
            if not self.connect_clickhouse():
                return self.parse_schemas_from_env()
        
        try:
            logger.info("🔍 Consultando esquemas disponibles en ClickHouse...")
            
            # Consultar databases disponibles
            result = self.client.query("SHOW DATABASES")
            available_databases = [row[0] for row in result.result_rows]
            
            # Filtrar schemas relevantes
            relevant_schemas = []
            for db in available_databases:
                if db.startswith("fgeo_") or db == self.clickhouse_database:
                    relevant_schemas.append(db)
                    logger.info(f"   ✅ Schema disponible: {db}")
            
            if not relevant_schemas:
                logger.warning("⚠️  No se encontraron esquemas fgeo_*, usando configuración del .env")
                return self.parse_schemas_from_env()
            
            return relevant_schemas
            
        except Exception as e:
            logger.error(f"❌ Error consultando esquemas ClickHouse: {e}")
            return self.parse_schemas_from_env()
    
    def get_schema_table_info(self, schema: str) -> Dict[str, Any]:
        """Obtiene información detallada de tablas de un esquema"""
        if not self.client:
            if not self.connect_clickhouse():
                return {}
        
        try:
            logger.info(f"📊 Analizando tablas en esquema: {schema}")
            
            # Consultar tablas del esquema
            query = """
            SELECT 
                name,
                engine,
                total_rows,
                total_bytes
            FROM system.tables 
            WHERE database = %(schema)s
            AND engine NOT IN ('Merge', 'View')
            ORDER BY total_rows DESC
            """
            
            result = self.client.query(query, parameters={"schema": schema})
            
            tables_info = {
                "schema": schema,
                "tables": [],
                "total_tables": 0,
                "total_rows": 0
            }
            
            for row in result.result_rows:
                table_info = {
                    "name": row[0],
                    "engine": row[1],
                    "rows": row[2] or 0,
                    "bytes": row[3] or 0
                }
                tables_info["tables"].append(table_info)
                tables_info["total_rows"] += table_info["rows"]
            
            tables_info["total_tables"] = len(tables_info["tables"])
            
            logger.info(f"   📈 {tables_info['total_tables']} tablas, {tables_info['total_rows']:,} filas totales")
            
            return tables_info
            
        except Exception as e:
            logger.error(f"❌ Error analizando esquema {schema}: {e}")
            return {"schema": schema, "tables": [], "total_tables": 0, "total_rows": 0}
    
    def generate_sample_queries(self, schema: str, table_name: str) -> Dict[str, str]:
        """Genera consultas de muestra para una tabla específica"""
        queries = {
            "overview": f"SELECT * FROM {schema}.{table_name} LIMIT 10",
            "count": f"SELECT COUNT(*) as total_records FROM {schema}.{table_name}",
            "recent": f"SELECT * FROM {schema}.{table_name} ORDER BY ingested_at DESC LIMIT 5",
            "sample": f"SELECT * FROM {schema}.{table_name} SAMPLE 0.1 LIMIT 20"
        }
        
        # Si la tabla parece tener datos JSON, agregar consulta específica
        if "_raw" in table_name:
            queries["json_keys"] = f"""
            SELECT 
                JSONExtractKeys(value) as json_keys,
                COUNT(*) as frequency
            FROM {schema}.{table_name} 
            WHERE isValidJSON(value)
            GROUP BY json_keys
            LIMIT 10
            """
        
        return queries
    
    def create_dynamic_card_config(self, schema: str, table_name: str, query_type: str = "overview") -> Dict[str, Any]:
        """Crea configuración de tarjeta Metabase dinámica"""
        queries = self.generate_sample_queries(schema, table_name)
        
        display_names = {
            "overview": "Vista General",
            "count": "Conteo Total", 
            "recent": "Datos Recientes",
            "sample": "Muestra Aleatoria",
            "json_keys": "Estructura JSON"
        }
        
        icons = {
            "overview": "📊",
            "count": "🔢",
            "recent": "⏰", 
            "sample": "🎲",
            "json_keys": "🔍"
        }
        
        display_types = {
            "overview": "table",
            "count": "scalar",
            "recent": "table",
            "sample": "table", 
            "json_keys": "table"
        }
        
        clean_schema = schema.replace("fgeo_", "").title()
        clean_table = table_name.replace("_", " ").title()
        
        return {
            "name": f"{icons.get(query_type, '📊')} {clean_schema} - {clean_table} ({display_names.get(query_type, 'Vista')})",
            "description": f"{display_names.get(query_type, 'Vista')} automática para {table_name} en {schema}",
            "query": queries.get(query_type, queries["overview"]),
            "display": display_types.get(query_type, "table"),
            "visualization_settings": {}
        }


def parse_schemas_from_env() -> List[str]:
    """Función de conveniencia para compatibilidad con otros módulos"""
    discovery = MetabaseSchemaDiscovery()
    return discovery.parse_schemas_from_env()


def get_available_schemas() -> List[str]:
    """Función de conveniencia para obtener esquemas disponibles"""
    discovery = MetabaseSchemaDiscovery()
    return discovery.discover_available_schemas()


def analyze_all_schemas() -> Dict[str, Any]:
    """Analiza todos los esquemas disponibles y retorna información completa"""
    discovery = MetabaseSchemaDiscovery()
    schemas = discovery.discover_available_schemas()
    
    analysis = {
        "schemas_found": len(schemas),
        "schemas": {},
        "total_tables": 0,
        "total_rows": 0
    }
    
    for schema in schemas:
        schema_info = discovery.get_schema_table_info(schema)
        analysis["schemas"][schema] = schema_info
        analysis["total_tables"] += schema_info.get("total_tables", 0)
        analysis["total_rows"] += schema_info.get("total_rows", 0)
    
    return analysis


if __name__ == "__main__":
    # Prueba de las funciones
    logging.basicConfig(level=logging.INFO)
    
    print("🔍 DESCUBRIMIENTO DE ESQUEMAS PARA METABASE")
    print("=" * 50)
    
    analysis = analyze_all_schemas()
    
    print(f"📊 Esquemas encontrados: {analysis['schemas_found']}")
    print(f"📈 Tablas totales: {analysis['total_tables']}")
    print(f"📋 Filas totales: {analysis['total_rows']:,}")
    
    for schema, info in analysis["schemas"].items():
        print(f"\n🗄️  Esquema: {schema}")
        print(f"   Tablas: {info['total_tables']}")
        print(f"   Filas: {info['total_rows']:,}")
        
        for table in info["tables"][:3]:  # Mostrar primeras 3 tablas
            print(f"     - {table['name']} ({table['rows']:,} filas)")