#!/usr/bin/env python3
"""
Metabase Dynamic Configurator
============================

Configurador dinámico para Metabase que lee DB_CONNECTIONS del .env y:
- Conecta automáticamente a ClickHouse
- Descubre esquemas y tablas dinámicamente 
- Crea preguntas y dashboards automáticamente
- Se adapta a cualquier configuración sin hardcodear nombres

Similar al configurador de Superset pero para Metabase API.
"""
import os
import json
import time
import requests
import logging
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

# Importar helper de descubrimiento de esquemas
import sys
sys.path.append('/app/tools')

class MetabaseSchemaDiscovery:
    """Helper simplificado para descubrimiento cuando no se puede importar el módulo completo"""
    def __init__(self):
        self.clickhouse_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
        self.clickhouse_database = os.getenv("CLICKHOUSE_DATABASE", "fgeo_analytics")
    
    def discover_available_schemas(self) -> List[str]:
        """Descubrimiento básico de esquemas"""
        db_connections_str = os.getenv("DB_CONNECTIONS", "[]")
        try:
            connections = json.loads(db_connections_str)
            if isinstance(connections, dict):
                connections = [connections]
            
            schemas = []
            for conn in connections:
                db_name = conn.get("db", "")
                if db_name:
                    conn_name = conn.get("name", db_name)
                    ch_schema = f"fgeo_{conn_name}"
                    schemas.append(ch_schema)
            
            if not schemas:
                schemas.append(self.clickhouse_database)
            
            return sorted(set(schemas))
        except:
            return [self.clickhouse_database]
    
    def get_schema_table_info(self, schema: str) -> Dict[str, Any]:
        """Info básica del esquema"""
        return {"total_tables": 0, "total_rows": 0, "tables": []}
    
    def create_dynamic_card_config(self, schema: str, table: str, query_type: str) -> Dict[str, Any]:
        """Configuración básica de tarjeta"""
        queries = {
            "overview": f"SELECT * FROM {schema}.{table} LIMIT 10",
            "count": f"SELECT COUNT(*) as total FROM {schema}.{table}",
            "recent": f"SELECT * FROM {schema}.{table} ORDER BY ingested_at DESC LIMIT 5"
        }
        
        icons = {"overview": "📊", "count": "🔢", "recent": "⏰"}
        displays = {"overview": "table", "count": "scalar", "recent": "table"}
        
        clean_schema = schema.replace("fgeo_", "").title()
        clean_table = table.replace("_", " ").title()
        
        return {
            "name": f"{icons.get(query_type, '📊')} {clean_schema} - {clean_table}",
            "description": f"Vista {query_type} para {table} en {schema}",
            "query": queries.get(query_type, queries["overview"]),
            "display": displays.get(query_type, "table"),
            "visualization_settings": {}
        }

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv('/app/.env')

class MetabaseDynamicConfigurator:
    """Configurador dinámico completo para Metabase"""
    
    def __init__(self):
        """Inicializa el configurador con variables de entorno"""
        self.metabase_url = os.getenv("METABASE_URL", "http://metabase:3000")
        self.admin_user = os.getenv("METABASE_ADMIN", "admin")
        self.admin_password = os.getenv("METABASE_PASSWORD", "Metabase123!")
        
        # ClickHouse connection details
        self.clickhouse_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
        self.clickhouse_port = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
        self.clickhouse_user = os.getenv("CH_USER", "etl")
        self.clickhouse_password = os.getenv("CH_PASSWORD", "Et1Ingest!")
        self.clickhouse_database = os.getenv("CLICKHOUSE_DATABASE", "fgeo_analytics")
        
        # Parse DB_CONNECTIONS for dynamic schema discovery
        self.db_connections = self._parse_db_connections()
        
        # Inicializar helper de descubrimiento
        self.schema_discovery = MetabaseSchemaDiscovery()
        
        # API endpoints
        self.session_endpoint = f"{self.metabase_url}/api/session"
        self.database_endpoint = f"{self.metabase_url}/api/database"
        self.card_endpoint = f"{self.metabase_url}/api/card"
        self.dashboard_endpoint = f"{self.metabase_url}/api/dashboard"
        
        self.session_id = None
        self.clickhouse_db_id = None
        
        logger.info(f"🔧 Metabase Dynamic Configurator inicializado")
        logger.info(f"📍 URL: {self.metabase_url}")
        logger.info(f"👤 Usuario: {self.admin_user}")
        logger.info(f"🗄️  ClickHouse: {self.clickhouse_host}:{self.clickhouse_port}/{self.clickhouse_database}")
        logger.info(f"📊 Esquemas detectados desde DB_CONNECTIONS: {len(self.db_connections)}")
    
    def _parse_db_connections(self) -> List[Dict[str, Any]]:
        """Parse DB_CONNECTIONS para descubrir esquemas dinámicamente"""
        db_connections_str = os.getenv("DB_CONNECTIONS", "[]")
        
        try:
            connections = json.loads(db_connections_str)
            if isinstance(connections, dict):
                connections = [connections]
            
            # Filtrar schemas de sistema como en Superset
            system_schemas = {"information_schema", "mysql", "performance_schema", "sys"}
            excluded_patterns = ["_analytics", "ext", "default", "system"]
            
            valid_connections = []
            for conn in connections:
                db_name = conn.get("db", "")
                if db_name and db_name not in system_schemas:
                    # Excluir patrones técnicos
                    if not any(pattern in db_name for pattern in excluded_patterns):
                        valid_connections.append(conn)
                        logger.info(f"   ✅ Esquema: {db_name} ({conn.get('name', 'unnamed')})")
                    else:
                        logger.info(f"   ⚠️  Esquema excluido: {db_name} (patrón técnico)")
                else:
                    logger.info(f"   ❌ Esquema inválido: {db_name} (sistema)")
            
            return valid_connections
            
        except json.JSONDecodeError as e:
            logger.error(f"❌ Error parseando DB_CONNECTIONS: {e}")
            return []
    
    def get_dynamic_schemas(self) -> List[str]:
        """Obtiene lista de esquemas ClickHouse dinámicamente usando el helper"""
        return self.schema_discovery.discover_available_schemas()
    
    def wait_for_metabase(self, max_attempts: int = 30) -> bool:
        """Espera a que Metabase esté disponible"""
        logger.info("⏳ Esperando que Metabase esté disponible...")
        
        for attempt in range(max_attempts):
            try:
                response = requests.get(f"{self.metabase_url}/api/health", timeout=5)
                if response.status_code == 200:
                    logger.info("✅ Metabase está disponible")
                    return True
            except requests.exceptions.RequestException:
                pass
            
            if attempt < max_attempts - 1:
                time.sleep(5)
                logger.info(f"   Intento {attempt + 1}/{max_attempts}...")
        
        logger.error("❌ Metabase no respondió a tiempo")
        return False
    
    def authenticate(self) -> bool:
        """Autentica con el usuario admin de Metabase"""
        logger.info("🔐 Autenticando en Metabase...")
        
        try:
            payload = {
                "username": self.admin_user,
                "password": self.admin_password
            }
            
            response = requests.post(self.session_endpoint, json=payload)
            if response.status_code == 200:
                self.session_id = response.json()["id"]
                logger.info("✅ Autenticación exitosa")
                return True
            else:
                logger.error(f"❌ Error de autenticación: {response.status_code} - {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error de conexión durante autenticación: {e}")
            return False
    
    def create_or_get_clickhouse_connection(self) -> Optional[int]:
        """Crea o obtiene la conexión a ClickHouse dinámicamente"""
        logger.info("🔗 Configurando conexión a ClickHouse...")
        
        headers = {"X-Metabase-Session": self.session_id}
        
        try:
            # Verificar si ya existe una conexión ClickHouse
            response = requests.get(self.database_endpoint, headers=headers)
            if response.status_code == 200:
                databases = response.json()["data"]
                for db in databases:
                    if db.get("engine") == "clickhouse":
                        self.clickhouse_db_id = db["id"]
                        logger.info(f"✅ Conexión ClickHouse existente encontrada (ID: {self.clickhouse_db_id})")
                        return self.clickhouse_db_id
            
            # Crear nueva conexión
            logger.info("📝 Creando nueva conexión a ClickHouse...")
            payload = {
                "name": f"ClickHouse ETL ({self.clickhouse_database})",
                "engine": "clickhouse",
                "details": {
                    "host": self.clickhouse_host,
                    "port": self.clickhouse_port,
                    "user": self.clickhouse_user,
                    "password": self.clickhouse_password,
                    "dbname": self.clickhouse_database,
                    "ssl": False,
                    "additional_options": "",
                    "let_user_control_scheduling": True,
                    "cache_field_values_schedule": "0 * * * *",
                    "metadata_sync_schedule": "0 * * * *"
                },
                "is_full_sync": True,
                "is_on_demand": False,
                "cache_ttl": None
            }
            
            response = requests.post(self.database_endpoint, json=payload, headers=headers)
            if response.status_code == 200:
                self.clickhouse_db_id = response.json()["id"]
                logger.info(f"✅ Conexión ClickHouse creada (ID: {self.clickhouse_db_id})")
                
                # Forzar sincronización de esquemas
                self.sync_database_schema()
                return self.clickhouse_db_id
            else:
                logger.error(f"❌ Error creando conexión: {response.status_code} - {response.text}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error configurando conexión ClickHouse: {e}")
            return None
    
    def sync_database_schema(self) -> bool:
        """Sincroniza esquemas de ClickHouse"""
        if not self.clickhouse_db_id:
            return False
        
        logger.info("🔄 Sincronizando esquemas de ClickHouse...")
        headers = {"X-Metabase-Session": self.session_id}
        
        try:
            # Sincronización de esquemas
            sync_url = f"{self.database_endpoint}/{self.clickhouse_db_id}/sync_schema"
            response = requests.post(sync_url, headers=headers)
            
            if response.status_code == 200:
                logger.info("✅ Sincronización de esquemas exitosa")
                
                # Rescaneo de valores
                rescan_url = f"{self.database_endpoint}/{self.clickhouse_db_id}/rescan_values"
                rescan_response = requests.post(rescan_url, headers=headers)
                
                if rescan_response.status_code == 200:
                    logger.info("✅ Re-escaneo de valores exitoso")
                else:
                    logger.warning(f"⚠️  Re-escaneo falló: {rescan_response.status_code}")
                
                # Esperar que termine la sincronización
                time.sleep(10)
                return True
            else:
                logger.error(f"❌ Error en sincronización: {response.status_code} - {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error sincronizando esquemas: {e}")
            return False
    
    def discover_tables(self) -> Dict[str, List[str]]:
        """Descubre tablas disponibles en los esquemas dinámicamente"""
        if not self.clickhouse_db_id:
            return {}
        
        logger.info("🔍 Descubriendo tablas disponibles...")
        headers = {"X-Metabase-Session": self.session_id}
        
        try:
            # Obtener metadata completa de la base de datos
            metadata_url = f"{self.database_endpoint}/{self.clickhouse_db_id}/metadata"
            response = requests.get(metadata_url, headers=headers)
            
            if response.status_code == 200:
                metadata = response.json()
                tables_by_schema = {}
                
                for table in metadata.get("tables", []):
                    schema = table.get("schema", "default")
                    table_name = table.get("name", "")
                    
                    if schema not in tables_by_schema:
                        tables_by_schema[schema] = []
                    
                    if table_name:
                        tables_by_schema[schema].append(table_name)
                
                # Log resultados
                for schema, tables in tables_by_schema.items():
                    logger.info(f"   📊 Esquema '{schema}': {len(tables)} tablas")
                    for table in tables[:3]:  # Mostrar primeras 3
                        logger.info(f"      - {table}")
                    if len(tables) > 3:
                        logger.info(f"      ... y {len(tables) - 3} más")
                
                return tables_by_schema
            else:
                logger.error(f"❌ Error obteniendo metadata: {response.status_code}")
                return {}
                
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error descubriendo tablas: {e}")
            return {}
    
    def create_dynamic_questions(self, tables_by_schema: Dict[str, List[str]]) -> List[int]:
        """Crea preguntas dinámicas avanzadas para cada esquema/tabla"""
        logger.info("❓ Creando preguntas dinámicas avanzadas...")
        headers = {"X-Metabase-Session": self.session_id}
        created_cards = []
        
        for schema, tables in tables_by_schema.items():
            logger.info(f"   📝 Esquema: {schema}")
            
            # Obtener información detallada del esquema
            schema_info = self.schema_discovery.get_schema_table_info(schema)
            
            # Crear múltiples tipos de preguntas para cada tabla
            for table in tables[:3]:  # Limitar a 3 tablas por esquema para no saturar
                try:
                    query_types = ["overview", "count", "recent"]
                    
                    for query_type in query_types:
                        # Usar el helper para generar configuración dinámica
                        card_config = self.schema_discovery.create_dynamic_card_config(
                            schema, table, query_type
                        )
                        
                        payload = {
                            "name": card_config["name"],
                            "description": card_config["description"],
                            "dataset_query": {
                                "type": "native",
                                "native": {
                                    "query": card_config["query"]
                                },
                                "database": self.clickhouse_db_id
                            },
                            "display": card_config["display"],
                            "visualization_settings": card_config["visualization_settings"]
                        }
                        
                        response = requests.post(self.card_endpoint, json=payload, headers=headers)
                        if response.status_code == 200:
                            card_id = response.json()["id"]
                            created_cards.append(card_id)
                            logger.info(f"      ✅ {card_config['name']} (ID: {card_id})")
                        else:
                            logger.warning(f"      ⚠️  Error creando {query_type} para {table}: {response.status_code}")
                
                except Exception as e:
                    logger.warning(f"      ❌ Error procesando tabla {table}: {e}")
            
            # Crear pregunta de resumen del esquema con datos reales
            if schema_info.get("total_tables", 0) > 0:
                try:
                    clean_schema = schema.replace("fgeo_", "").title()
                    summary_name = f"📈 {clean_schema} - Dashboard Resumen"
                    summary_query = f"""
                    SELECT 
                        '{clean_schema}' as Esquema,
                        {schema_info['total_tables']} as Tablas,
                        {schema_info['total_rows']} as Filas_Totales,
                        'Análisis automático completado' as Estado
                    """
                    
                    summary_payload = {
                        "name": summary_name,
                        "description": f"Resumen ejecutivo del esquema {schema} con métricas reales",
                        "dataset_query": {
                            "type": "native",
                            "native": {
                                "query": summary_query
                            },
                            "database": self.clickhouse_db_id
                        },
                        "display": "table",
                        "visualization_settings": {
                            "table.pivot_column": "Esquema",
                            "table.cell_column": "Filas_Totales"
                        }
                    }
                    
                    response = requests.post(self.card_endpoint, json=summary_payload, headers=headers)
                    if response.status_code == 200:
                        card_id = response.json()["id"]
                        created_cards.append(card_id)
                        logger.info(f"   ✅ Resumen ejecutivo creado: {summary_name} (ID: {card_id})")
                
                except Exception as e:
                    logger.warning(f"   ❌ Error creando resumen ejecutivo para {schema}: {e}")
        
        logger.info(f"✅ {len(created_cards)} preguntas dinámicas avanzadas creadas")
        return created_cards
    
    def create_dynamic_dashboard(self, card_ids: List[int]) -> Optional[int]:
        """Crea dashboard dinámico con las preguntas generadas"""
        if not card_ids:
            return None
        
        logger.info("📊 Creando dashboard dinámico...")
        headers = {"X-Metabase-Session": self.session_id}
        
        try:
            # Crear dashboard
            dashboard_payload = {
                "name": "🚀 ETL Analytics - Dashboard Automático",
                "description": f"Dashboard generado automáticamente con {len(card_ids)} visualizaciones basadas en DB_CONNECTIONS"
            }
            
            response = requests.post(self.dashboard_endpoint, json=dashboard_payload, headers=headers)
            if response.status_code == 200:
                dashboard_id = response.json()["id"]
                logger.info(f"✅ Dashboard creado (ID: {dashboard_id})")
                
                # Agregar tarjetas al dashboard
                dashboard_cards = []
                row = 0
                col = 0
                
                for i, card_id in enumerate(card_ids[:12]):  # Máximo 12 tarjetas
                    dashboard_cards.append({
                        "id": card_id,
                        "card_id": card_id,
                        "row": row,
                        "col": col,
                        "sizeX": 6,
                        "sizeY": 4,
                        "series": [],
                        "visualization_settings": {},
                        "parameter_mappings": []
                    })
                    
                    col += 6
                    if col >= 12:  # Nueva fila cada 2 tarjetas
                        col = 0
                        row += 4
                
                # Actualizar dashboard con tarjetas
                update_payload = {
                    "ordered_cards": dashboard_cards
                }
                
                update_response = requests.put(
                    f"{self.dashboard_endpoint}/{dashboard_id}/cards", 
                    json=update_payload, 
                    headers=headers
                )
                
                if update_response.status_code == 200:
                    logger.info(f"✅ {len(dashboard_cards)} tarjetas agregadas al dashboard")
                    logger.info(f"🔗 Acceso: {self.metabase_url}/dashboard/{dashboard_id}")
                    return dashboard_id
                else:
                    logger.warning(f"⚠️  Error agregando tarjetas: {update_response.status_code}")
                    return dashboard_id
            else:
                logger.error(f"❌ Error creando dashboard: {response.status_code} - {response.text}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error creando dashboard: {e}")
            return None
    
    def run_configuration(self) -> bool:
        """Ejecuta la configuración completa de Metabase"""
        logger.info("🚀 INICIANDO CONFIGURACIÓN DINÁMICA DE METABASE")
        logger.info("=" * 60)
        
        try:
            # 1. Verificar disponibilidad
            if not self.wait_for_metabase():
                return False
            
            # 2. Autenticar
            if not self.authenticate():
                return False
            
            # 3. Configurar conexión ClickHouse
            if not self.create_or_get_clickhouse_connection():
                return False
            
            # 4. Descubrir tablas
            tables_by_schema = self.discover_tables()
            if not tables_by_schema:
                logger.warning("⚠️  No se encontraron tablas, continuando con configuración básica...")
                return True
            
            # 5. Crear preguntas dinámicas
            card_ids = self.create_dynamic_questions(tables_by_schema)
            
            # 6. Crear dashboard dinámico
            dashboard_id = self.create_dynamic_dashboard(card_ids)
            
            # Resumen final
            logger.info("\n" + "=" * 60)
            logger.info("✅ ¡CONFIGURACIÓN DINÁMICA COMPLETADA!")
            logger.info(f"🔗 URL de acceso: {self.metabase_url}")
            logger.info(f"👤 Usuario: {self.admin_user}")
            logger.info(f"🔑 Contraseña: {self.admin_password}")
            logger.info(f"📊 Esquemas configurados: {len(tables_by_schema)}")
            logger.info(f"❓ Preguntas creadas: {len(card_ids)}")
            if dashboard_id:
                logger.info(f"📈 Dashboard: {self.metabase_url}/dashboard/{dashboard_id}")
            logger.info("=" * 60)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error durante configuración: {e}")
            return False


def main():
    """Función principal"""
    configurator = MetabaseDynamicConfigurator()
    success = configurator.run_configuration()
    
    if success:
        logger.info("🎉 Configuración dinámica de Metabase completada exitosamente")
        return 0
    else:
        logger.error("💥 Falló la configuración dinámica de Metabase")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())