#!/usr/bin/env python3
"""
📊 CONFIGURADOR AUTOMÁTICO DE SUPERSET
Sistema completo que automatiza toda la configuración de Superset:
- Inicialización de BD y admin
- Configuración de conexión ClickHouse
- Creación automática de datasets
- Configuración de permisos y dashboards básicos
"""

import requests
import json
import os
import time
import logging
import subprocess
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables de entorno del archivo .env
ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

class SupersetAutoConfigurator:
    """Configurador automático completo de Superset"""
    
    def __init__(self):
        self.logger = self._setup_logging()
        self.superset_url = self._get_superset_url()
        self.admin_config = self._load_admin_config()
        self.clickhouse_config = self._load_clickhouse_config()
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        self.csrf_token = None
        
    def _setup_logging(self) -> logging.Logger:
        """Configurar logging detallado"""
        logger = logging.getLogger('SupersetAutoConfig')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def _get_superset_url(self) -> str:
        """Determinar URL de Superset según el contexto"""
        if os.path.exists('/app/logs'):
            return os.getenv('SUPERSET_URL', 'http://superset:8088')
        else:
            return os.getenv('SUPERSET_URL', 'http://localhost:8088')
    
    def _load_admin_config(self) -> Dict[str, str]:
        """Cargar configuración del usuario admin"""
        return {
            'username': os.getenv('SUPERSET_ADMIN', 'admin'),
            'password': os.getenv('SUPERSET_PASSWORD', 'Admin123!'),
            'firstname': 'Admin',
            'lastname': 'User',
            'email': 'admin@etl.local'
        }
    
    def _load_clickhouse_config(self) -> Dict[str, str]:
        """Cargar configuración de ClickHouse"""
        # IMPORTANTE: Superset siempre se ejecuta en contenedor, por lo que 
        # SIEMPRE debe usar 'clickhouse' como host, independientemente de 
        # desde dónde se ejecute este configurador
        # Nota clave: En este entorno los usuarios están definidos vía XML (lectura únicamente),
        # por lo que debemos usar el usuario existente 'superset' (o CLICKHOUSE_USER) y NO crear
        # usuarios nuevos dinámicamente. Si se desea usar un usuario dedicado distinto, deberá
        # venir explícito por variables de entorno.
        user = (
            os.getenv('CLICKHOUSE_USER')
            or os.getenv('CLICKHOUSE_SUPERSET_USER')
            or 'superset'
        )
        password = (
            os.getenv('CLICKHOUSE_PASSWORD')
            or os.getenv('CLICKHOUSE_SUPERSET_PASSWORD')
            or 'Sup3rS3cret!'
        )
        return {
            'host': 'clickhouse',
            'port': int(os.getenv('CLICKHOUSE_PORT', 8123)),
            'database': os.getenv('CLICKHOUSE_DATABASE', 'fgeo_analytics'),
            'user': user,
            'password': password,
        }
    
    def wait_for_superset(self, max_retries: int = 40) -> bool:
        """Esperar a que Superset esté disponible"""
        self.logger.info("⏳ Esperando a que Superset esté disponible...")
        
        for i in range(max_retries):
            try:
                response = self.session.get(f"{self.superset_url}/health", timeout=10)
                if response.status_code == 200 and response.text.strip() == "OK":
                    self.logger.info("✅ Superset está disponible")
                    return True
            except Exception as e:
                self.logger.debug(f"Intento {i+1}/{max_retries}: {str(e)}")
            
            if i < max_retries - 1:
                time.sleep(3)
        
        self.logger.error(f"❌ Superset no disponible después de {max_retries * 3}s")
        return False
    
    def initialize_superset_database(self) -> Tuple[bool, List[str]]:
        """Inicializar base de datos y usuario admin de Superset"""
        issues = []
        
        try:
            self.logger.info("🔧 Inicializando base de datos de Superset...")
            
            # Comandos de inicialización
            init_commands = [
                {
                    'name': 'crear admin',
                    'cmd': [
                        'docker', 'exec', 'superset', 'superset', 'fab', 'create-admin',
                        '--username', self.admin_config['username'],
                        '--firstname', self.admin_config['firstname'],
                        '--lastname', self.admin_config['lastname'],
                        '--email', self.admin_config['email'],
                        '--password', self.admin_config['password']
                    ],
                    'success_indicators': ['created', 'admin user'],
                    'ignore_errors': ['already exists']
                },
                {
                    'name': 'actualizar BD',
                    'cmd': ['docker', 'exec', 'superset', 'superset', 'db', 'upgrade'],
                    'success_indicators': ['upgraded', 'running upgrade'],
                    'ignore_errors': []
                },
                {
                    'name': 'inicializar Superset',
                    'cmd': ['docker', 'exec', 'superset', 'superset', 'init'],
                    'success_indicators': ['syncing', 'creating'],
                    'ignore_errors': []
                }
            ]
            
            for cmd_info in init_commands:
                try:
                    self.logger.info(f"🚀 Ejecutando: {cmd_info['name']}")
                    
                    result = subprocess.run(
                        cmd_info['cmd'], 
                        capture_output=True, 
                        text=True, 
                        timeout=180  # 3 minutos timeout
                    )
                    
                    output = result.stdout + result.stderr
                    
                    # Verificar éxito
                    success_found = any(indicator in output.lower() for indicator in cmd_info['success_indicators'])
                    ignore_error = any(ignore in output.lower() for ignore in cmd_info['ignore_errors'])
                    
                    if result.returncode == 0 or success_found or ignore_error:
                        if ignore_error:
                            self.logger.info(f"ℹ️  {cmd_info['name']}: ya ejecutado previamente")
                        else:
                            self.logger.info(f"✅ {cmd_info['name']}: completado exitosamente")
                    else:
                        error_msg = f"Error en {cmd_info['name']}: {output[:200]}"
                        self.logger.error(f"❌ {error_msg}")
                        issues.append(error_msg)
                
                except subprocess.TimeoutExpired:
                    error_msg = f"Timeout en {cmd_info['name']}"
                    self.logger.error(f"❌ {error_msg}")
                    issues.append(error_msg)
                
                except Exception as e:
                    error_msg = f"Error ejecutando {cmd_info['name']}: {str(e)}"
                    self.logger.error(f"❌ {error_msg}")
                    issues.append(error_msg)
            
            # Dar tiempo para que Superset se reinicie
            if len(issues) == 0:
                self.logger.info("⏳ Esperando reinicio de Superset...")
                time.sleep(10)
            
        except Exception as e:
            error_msg = f"Error general inicializando Superset: {str(e)}"
            self.logger.error(f"❌ {error_msg}")
            issues.append(error_msg)
        
        success = len(issues) == 0
        return success, issues
    
    def authenticate_superset(self) -> bool:
        """Autenticar con Superset y obtener tokens"""
        try:
            self.logger.info("🔐 Autenticando con Superset...")
            
            # Login
            login_payload = {
                "username": self.admin_config['username'],
                "password": self.admin_config['password'],
                "provider": "db",
                "refresh": True
            }
            
            response = self.session.post(f"{self.superset_url}/api/v1/security/login", json=login_payload)
            response.raise_for_status()
            
            access_token = response.json()["access_token"]
            self.session.headers.update({"Authorization": f"Bearer {access_token}"})
            
            # Obtener CSRF token
            csrf_response = self.session.get(f"{self.superset_url}/api/v1/security/csrf_token/")
            csrf_response.raise_for_status()
            
            self.csrf_token = csrf_response.json()["result"]
            self.session.headers.update({"X-CSRFToken": self.csrf_token})
            
            self.logger.info("✅ Autenticación exitosa")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error autenticando: {str(e)}")
            return False
    
    def cleanup_duplicate_databases(self) -> Optional[int]:
        """Eliminar BDs ClickHouse duplicadas/legadas dejando solo la oficial.
        Retorna el ID de la BD oficial si existe.
        """
        try:
            self.logger.info("🧹 Limpiando bases de datos ClickHouse duplicadas...")
            resp = self.session.get(f"{self.superset_url}/api/v1/database/")
            resp.raise_for_status()

            official_name = "ClickHouse ETL Database"
            official_id = None
            for db in resp.json().get("result", []):
                name = db.get("database_name", "")
                dbid = db.get("id")
                if name == official_name:
                    official_id = dbid
                    self.logger.info(f"✅ BD oficial encontrada: {name} (ID {dbid})")
                elif "clickhouse" in name.lower():
                    self.logger.info(f"🗑️  Eliminando BD duplicada: {name} (ID {dbid})")
                    del_resp = self.session.delete(f"{self.superset_url}/api/v1/database/{dbid}")
                    if del_resp.ok:
                        self.logger.info(f"✅ BD eliminada: {name}")
                    else:
                        self.logger.warning(f"⚠️  No se pudo eliminar {name}: {del_resp.status_code}")
            return official_id
        except Exception as e:
            self.logger.error(f"❌ Error limpiando BDs duplicadas: {e}")
            return None

    def create_clickhouse_database_connection(self) -> Tuple[bool, Optional[int]]:
        """Crear/asegurar conexión única a ClickHouse en Superset"""
        try:
            self.logger.info("🏠 Configurando conexión ClickHouse (única y oficial)...")
            # Nombre oficial único
            db_name = "ClickHouse ETL Database"

            # Buscar base de datos existente y limpiar duplicadas primero
            self.cleanup_duplicate_databases()

            response = self.session.get(f"{self.superset_url}/api/v1/database/")
            response.raise_for_status()
            databases = response.json().get("result", [])
            existing_db = next((db for db in databases if db.get("database_name") == db_name), None)

            # Construir URI de conexión
            # Usamos el driver HTTP estándar de Superset para ClickHouse
            ch_uri = (f"clickhouse+http://{self.clickhouse_config['user']}:"
                     f"{self.clickhouse_config['password']}@{self.clickhouse_config['host']}:"
                     f"{self.clickhouse_config['port']}/{self.clickhouse_config['database']}")
            
            # Configuración avanzada para ClickHouse con mejor manejo de fechas y temporales
            extra_config = {
                "metadata_params": {},
                "engine_params": {
                    "connect_args": {
                        "http_session_timeout": 60
                    }
                },
                "metadata_cache_timeout": {},
                "schemas_allowed_for_file_upload": [],
                # Configuración para permitir SQL arbitrario (necesario para charts avanzados)
                "allows_virtual_table_explore": True,
                "allows_subquery": True,
                "allows_cost_estimate": False,
                "disable_data_preview": False
            }
            
            payload = {
                "database_name": db_name,
                "sqlalchemy_uri": ch_uri,
                "expose_in_sqllab": True,
                "allow_ctas": True,
                "allow_cvas": True,
                "allow_run_async": False,
                "allow_dml": False,
                "extra": json.dumps(extra_config)
            }
            
            if existing_db:
                # Actualizar existente
                db_id = existing_db["id"]
                self.logger.info(f"🔄 Actualizando conexión existente (ID: {db_id})")
                
                response = self.session.put(f"{self.superset_url}/api/v1/database/{db_id}", json=payload)
                response.raise_for_status()
            else:
                # Crear nueva
                self.logger.info("➕ Creando nueva conexión ClickHouse")
                
                response = self.session.post(f"{self.superset_url}/api/v1/database/", json=payload)
                response.raise_for_status()
                
                db_id = response.json()["id"]
            
            self.logger.info(f"✅ Conexión ClickHouse configurada (ID: {db_id})")
            return True, db_id
            
        except Exception as e:
            self.logger.error(f"❌ Error configurando ClickHouse: {str(e)}")
            return False, None
    
    def discover_and_create_datasets(self, database_id: int) -> Tuple[bool, List[str]]:
        """Descubrir tablas de ClickHouse y crear datasets automáticamente"""
        created_datasets = []
        
        try:
            self.logger.info("📊 Descubriendo y creando datasets...")
            
            # 1. Obtener lista de tablas desde ClickHouse directamente
            tables_to_create = self._get_clickhouse_tables()
            
            if not tables_to_create:
                # Fallback: usar tablas conocidas del ETL
                tables_to_create = ['archivos', 'eventos', 'usuarios', 'logs']
                self.logger.info("📋 Usando tablas ETL por defecto")
            
            # 2. Crear datasets para cada tabla
            for table_name in tables_to_create:
                try:
                    dataset_created = self._create_single_dataset(database_id, table_name)
                    if dataset_created:
                        created_datasets.append(table_name)
                        self.logger.info(f"✅ Dataset creado: {table_name}")
                    else:
                        self.logger.warning(f"⚠️  No se pudo crear dataset: {table_name}")
                
                except Exception as e:
                    self.logger.warning(f"⚠️  Error creando dataset {table_name}: {str(e)}")
            
            success = len(created_datasets) > 0
            if success:
                self.logger.info(f"🎉 Datasets creados: {len(created_datasets)}")
            else:
                self.logger.warning("⚠️  No se pudieron crear datasets")
            
            return success, created_datasets
            
        except Exception as e:
            self.logger.error(f"❌ Error creando datasets: {str(e)}")
            return False, []
    
    def _get_clickhouse_tables(self) -> List[str]:
        """Obtener lista de tablas desde ClickHouse"""
        try:
            ch_host = self.clickhouse_config['host']
            ch_port = self.clickhouse_config['port']
            ch_db = self.clickhouse_config['database']
            
            # Consultar tablas
            query = f"SHOW TABLES FROM {ch_db}"
            response = requests.post(
                f"http://{ch_host}:{ch_port}/",
                data=query,
                timeout=15
            )
            
            if response.status_code == 200:
                tables = [table.strip() for table in response.text.strip().split('\n') if table.strip()]
                self.logger.info(f"📋 Encontradas {len(tables)} tablas en ClickHouse")
                return tables
            else:
                self.logger.warning(f"⚠️  No se pudieron obtener tablas: {response.status_code}")
                return []
                
        except Exception as e:
            self.logger.warning(f"⚠️  Error consultando tablas ClickHouse: {str(e)}")
            return []
    
    def _create_single_dataset(self, database_id: int, table_name: str) -> bool:
        """Crear un dataset individual"""
        try:
            schema = self.clickhouse_config['database']
            
            # Verificar si ya existe
            # Nota: La query de búsqueda es compleja, así que intentamos crear directamente
            
            payload = {
                "database": database_id,
                "schema": schema,
                "table_name": table_name
            }
            
            response = self.session.post(f"{self.superset_url}/api/v1/dataset/", json=payload)
            
            if response.status_code in [200, 201]:
                return True
            elif response.status_code == 409:
                # Ya existe
                self.logger.info(f"ℹ️  Dataset {table_name} ya existe")
                return True
            else:
                self.logger.warning(f"⚠️  Error creando dataset {table_name}: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            self.logger.warning(f"⚠️  Excepción creando dataset {table_name}: {str(e)}")
            return False
    
    def create_sample_dashboard(self, datasets: List[str]) -> bool:
        """Crear dashboard de muestra con los datasets disponibles"""
        try:
            self.logger.info("📈 Creando dashboard de muestra...")
            
            if not datasets:
                self.logger.warning("⚠️  No hay datasets para crear dashboard")
                return False
            
            dashboard_payload = {
                "dashboard_title": "ETL Pipeline Dashboard",
                "slug": "etl-pipeline-dashboard",
                "published": True,
                "position_json": json.dumps({
                    "DASHBOARD_VERSION_KEY": "v2",
                    "ROOT_ID": {
                        "type": "ROOT",
                        "id": "ROOT_ID",
                        "children": ["GRID_ID"]
                    },
                    "GRID_ID": {
                        "type": "GRID",
                        "id": "GRID_ID",
                        "children": [],
                        "parents": ["ROOT_ID"]
                    }
                })
            }
            
            response = self.session.post(f"{self.superset_url}/api/v1/dashboard/", json=dashboard_payload)
            
            if response.status_code in [200, 201]:
                self.logger.info("✅ Dashboard de muestra creado")
                return True
            else:
                self.logger.warning(f"⚠️  No se pudo crear dashboard: {response.status_code}")
                return False
                
        except Exception as e:
            self.logger.warning(f"⚠️  Error creando dashboard: {str(e)}")
            return False
    
    def comprehensive_superset_setup(self) -> Tuple[bool, Dict[str, Any]]:
        """Configuración completa y automática de Superset"""
        start_time = datetime.now()
        self.logger.info("📊 === INICIANDO CONFIGURACIÓN AUTOMÁTICA DE SUPERSET ===")
        
        results = {
            'timestamp': start_time.isoformat(),
            'superset_available': False,
            'database_initialized': False,
            'authentication': False,
            'clickhouse_connection': False,
            'datasets_created': [],
            'dashboard_created': False,
            'database_id': None,
            'issues': [],
            'duration_seconds': 0
        }
        
        try:
            # 1. Esperar disponibilidad
            if not self.wait_for_superset():
                results['issues'].append("Superset no disponible")
                return False, results
            results['superset_available'] = True
            
            # 2. Inicializar base de datos
            db_init_success, db_issues = self.initialize_superset_database()
            results['database_initialized'] = db_init_success
            results['issues'].extend(db_issues)
            
            if not db_init_success:
                self.logger.error("❌ Falló inicialización de BD, continuando...")
            
            # 3. Esperar que Superset esté listo después de la inicialización
            if not self.wait_for_superset():
                results['issues'].append("Superset no responde después de inicialización")
                return False, results
            
            # 4. Autenticar
            if not self.authenticate_superset():
                results['issues'].append("Falló autenticación")
                return False, results
            results['authentication'] = True
            
            # 5. Crear conexión ClickHouse (única, con limpieza previa)
            ch_success, db_id = self.create_clickhouse_database_connection()
            results['clickhouse_connection'] = ch_success
            results['database_id'] = db_id
            
            if not ch_success:
                results['issues'].append("Falló conexión ClickHouse")
                return False, results
            
            # 6. (Opcional) Crear datasets y dashboard se delega a 'superset-datasets'
            self.logger.info("ℹ️  Creación de datasets se delega al servicio superset-datasets (idempotente)")
            
        except Exception as e:
            error_msg = f"Error crítico en configuración: {str(e)}"
            self.logger.error(f"❌ {error_msg}")
            results['issues'].append(error_msg)
        
        # Calcular duración
        end_time = datetime.now()
        results['duration_seconds'] = (end_time - start_time).total_seconds()
        
        # Determinar éxito
        success = (results['superset_available'] and 
                  results['authentication'] and 
                  results['clickhouse_connection'])
        
        # Resumen final
        self.logger.info(f"🏁 Configuración Superset completada en {results['duration_seconds']:.1f}s")
        
        if success:
            self.logger.info("🎉 SUPERSET CONFIGURADO EXITOSAMENTE")
            self.logger.info(f"🌐 Acceso: {self.superset_url}")
            self.logger.info(f"👤 Usuario: {self.admin_config['username']} / {self.admin_config['password']}")
            self.logger.info(f"🏠 Base de datos ClickHouse: ID {results['database_id']}")
            self.logger.info(f"📊 Datasets: {len(results['datasets_created'])} creados")
        else:
            self.logger.error("❌ PROBLEMAS EN CONFIGURACIÓN DE SUPERSET")
            for issue in results['issues'][:3]:
                self.logger.error(f"   • {issue}")
        
        return success, results


def main():
    """Función principal para ejecución independiente"""
    configurator = SupersetAutoConfigurator()
    success, results = configurator.comprehensive_superset_setup()
    
    return 0 if success else 1


if __name__ == "__main__":
    exit(main())