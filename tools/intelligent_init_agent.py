#!/usr/bin/env python3
"""
🤖 AGENTE DE INICIALIZACIÓN INTELIGENTE DEL PIPELINE ETL
=====================================================

Este agente automatiza completamente la inicialización del pipeline ETL,
resolviendo todos los problemas identificados:

1. ✅ Validación robusta de salud de servicios
2. ✅ Inicialización automática de Superset con admin
3. ✅ Creación automática de usuarios con permisos correctos
4. ✅ Configuración automática de conexiones ClickHouse
5. ✅ Creación automática de datasets basada en metadatos
6. ✅ Manejo inteligente de tokens CSRF
7. ✅ Recuperación automática de errores

Uso:
    python tools/intelligent_init_agent.py [--retry-failed] [--debug]
"""

import os
import sys
import time
import json
import logging
import requests
import subprocess
import traceback
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
from enum import Enum
from dataclasses import dataclass, field

# Imports para bases de datos
try:
    import pymysql
    import clickhouse_connect
except ImportError as e:
    print(f"⚠️  Dependencias faltantes: {e}")
    print("💡 Instalar con: pip install pymysql clickhouse-connect")
    sys.exit(1)

class ServiceStatus(Enum):
    """Estados posibles de un servicio"""
    UNKNOWN = "unknown"
    STARTING = "starting"
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    FAILED = "failed"
    READY = "ready"

@dataclass
class ServiceInfo:
    """Información de un servicio"""
    name: str
    container_name: str
    health_check: str
    dependencies: List[str] = field(default_factory=list)
    status: ServiceStatus = ServiceStatus.UNKNOWN
    last_check: datetime = field(default_factory=datetime.now)
    retry_count: int = 0
    max_retries: int = 10

@dataclass
class InitializationStep:
    """Paso de inicialización"""
    name: str
    description: str
    function: str
    dependencies: List[str] = field(default_factory=list)
    completed: bool = False
    error: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3

class IntelligentInitAgent:
    """🤖 Agente Inteligente de Inicialización"""
    
    def __init__(self, debug: bool = False):
        self.debug = debug
        self.start_time = datetime.now()
        self.setup_logging()
        
        # Configuración desde variables de entorno
        self.load_environment()
        
        # Definir servicios críticos
        self.services = self.define_services()
        
        # Definir pasos de inicialización
        self.init_steps = self.define_initialization_steps()
        
        # Estado del agente
        self.session = requests.Session()
        self.superset_csrf_token = None
        self.superset_access_token = None
        
        logger.info("🤖 === AGENTE DE INICIALIZACIÓN INTELIGENTE ACTIVADO ===")
        logger.info(f"⏰ Inicio: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    def setup_logging(self):
        """Configurar sistema de logging"""
        log_level = logging.DEBUG if self.debug else logging.INFO
        log_dir = '/app/logs' if os.path.exists('/app/logs') else 'logs'
        os.makedirs(log_dir, exist_ok=True)
        
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - INTELLIGENT_AGENT - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'{log_dir}/intelligent_init.log'),
                logging.StreamHandler()
            ]
        )
        
        global logger
        logger = logging.getLogger(__name__)
    
    def load_environment(self):
        """Cargar configuración desde variables de entorno y .env"""
        # Cargar .env si existe
        env_file = '.env'
        if os.path.exists(env_file):
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        os.environ[key] = value
        
        # Configuración de servicios
        self.mysql_config = self.extract_mysql_config()
        self.clickhouse_config = self.extract_clickhouse_config()
        self.superset_config = self.extract_superset_config()
        
        logger.info("🔧 Configuración cargada exitosamente")
    
    def extract_mysql_config(self) -> Dict[str, Any]:
        """Extraer configuración MySQL desde DB_CONNECTIONS"""
        try:
            db_connections_str = os.getenv('DB_CONNECTIONS', '[]')
            db_connections = json.loads(db_connections_str)
            
            if db_connections:
                first_db = db_connections[0]
                return {
                    'host': first_db.get('host', 'localhost'),
                    'port': int(first_db.get('port', 3306)),
                    'user': first_db.get('user', 'root'),
                    'password': first_db.get('pass', ''),
                    'database': first_db.get('db', 'mysql'),
                    'charset': 'utf8mb4'
                }
            else:
                return {
                    'host': 'localhost',
                    'port': 3306,
                    'user': 'root',
                    'password': '',
                    'database': 'mysql',
                    'charset': 'utf8mb4'
                }
        except Exception as e:
            logger.warning(f"⚠️  Error extrayendo config MySQL: {e}")
            return {'host': 'localhost', 'port': 3306, 'user': 'root', 'password': '', 'database': 'mysql'}
    
    def extract_clickhouse_config(self) -> Dict[str, Any]:
        """Extraer configuración ClickHouse"""
        return {
            'host': 'clickhouse' if os.path.exists('/app/logs') else 'localhost',
            'port': int(os.getenv('CLICKHOUSE_HTTP_PORT', 8123)),
            'database': os.getenv('CLICKHOUSE_DATABASE', 'fgeo_analytics'),
            'user': os.getenv('CLICKHOUSE_USER', 'default'),
            'password': os.getenv('CLICKHOUSE_PASSWORD', '')
        }
    
    def extract_superset_config(self) -> Dict[str, Any]:
        """Extraer configuración Superset"""
        return {
            'url': 'http://superset:8088' if os.path.exists('/app/logs') else 'http://localhost:8088',
            'admin_user': os.getenv('SUPERSET_ADMIN', 'admin'),
            'admin_password': os.getenv('SUPERSET_PASSWORD', 'Admin123!'),
            'admin_email': 'admin@admin.com'
        }
    
    def define_services(self) -> Dict[str, ServiceInfo]:
        """Definir servicios críticos y sus dependencias"""
        return {
            'clickhouse': ServiceInfo(
                name='ClickHouse',
                container_name='clickhouse',
                health_check='http://localhost:8123/ping',
                dependencies=[]
            ),
            'kafka': ServiceInfo(
                name='Kafka',
                container_name='kafka',
                health_check='localhost:19092',
                dependencies=[]
            ),
            'connect': ServiceInfo(
                name='Kafka Connect',
                container_name='connect',
                health_check='http://localhost:8083/connectors',
                dependencies=['kafka']
            ),
            'superset': ServiceInfo(
                name='Superset',
                container_name='superset',
                health_check='http://localhost:8088/health',
                dependencies=['clickhouse']
            )
        }
    
    def define_initialization_steps(self) -> List[InitializationStep]:
        """Definir pasos de inicialización en orden"""
        return [
            InitializationStep(
                name="validate_services",
                description="🔍 Validar que todos los servicios estén saludables",
                function="validate_all_services",
                dependencies=[]
            ),
            InitializationStep(
                name="initialize_clickhouse_users",
                description="🏠 Crear usuarios ClickHouse con permisos",
                function="initialize_clickhouse_users",
                dependencies=["validate_services"]
            ),
            InitializationStep(
                name="initialize_mysql_users", 
                description="🐬 Crear usuarios MySQL para Debezium",
                function="initialize_mysql_users",
                dependencies=["validate_services"]
            ),
            InitializationStep(
                name="initialize_superset_database",
                description="📊 Inicializar base de datos de Superset",
                function="initialize_superset_database",
                dependencies=["validate_services"]
            ),
            InitializationStep(
                name="create_superset_admin",
                description="👤 Crear usuario administrador de Superset",
                function="create_superset_admin",
                dependencies=["initialize_superset_database"]
            ),
            InitializationStep(
                name="configure_superset_clickhouse",
                description="🔗 Configurar conexión ClickHouse en Superset",
                function="configure_superset_clickhouse",
                dependencies=["create_superset_admin", "initialize_clickhouse_users"]
            ),
            InitializationStep(
                name="create_superset_datasets",
                description="📋 Crear datasets automáticos en Superset",
                function="create_superset_datasets",
                dependencies=["configure_superset_clickhouse"]
            ),
            InitializationStep(
                name="start_etl_ingestion",
                description="⚡ Iniciar ingesta de datos ETL",
                function="start_etl_ingestion",
                dependencies=["initialize_mysql_users", "configure_superset_clickhouse"]
            )
        ]
    
    # =============== VALIDACIÓN DE SERVICIOS ===============
    
    def validate_all_services(self) -> bool:
        """🔍 Validar que todos los servicios críticos estén saludables"""
        logger.info("🔍 === VALIDANDO SERVICIOS CRÍTICOS ===")
        
        all_healthy = True
        for service_name, service in self.services.items():
            if not self.validate_service_health(service):
                all_healthy = False
        
        if all_healthy:
            logger.info("✅ Todos los servicios están saludables")
            return True
        else:
            logger.error("❌ Algunos servicios no están saludables")
            return False
    
    def validate_service_health(self, service: ServiceInfo) -> bool:
        """Validar salud de un servicio específico"""
        logger.info(f"🔍 Validando {service.name}...")
        
        # Verificar que el contenedor existe y está corriendo
        if not self.is_container_running(service.container_name):
            logger.error(f"❌ Contenedor {service.container_name} no está corriendo")
            service.status = ServiceStatus.FAILED
            return False
        
        # Verificar health check específico
        if not self.check_service_endpoint(service):
            logger.error(f"❌ {service.name} no responde correctamente")
            service.status = ServiceStatus.UNHEALTHY
            return False
        
        service.status = ServiceStatus.HEALTHY
        logger.info(f"✅ {service.name} está saludable")
        return True
    
    def is_container_running(self, container_name: str) -> bool:
        """Verificar si un contenedor está corriendo"""
        try:
            result = subprocess.run(
                ['docker', 'ps', '--filter', f'name={container_name}', '--format', '{{.Names}}'],
                capture_output=True, text=True
            )
            return container_name in result.stdout
        except Exception as e:
            logger.error(f"Error verificando contenedor {container_name}: {e}")
            return False
    
    def check_service_endpoint(self, service: ServiceInfo) -> bool:
        """Verificar endpoint de salud del servicio"""
        try:
            if service.health_check.startswith('http'):
                response = requests.get(service.health_check, timeout=10)
                return response.status_code == 200
            else:
                # Para servicios TCP como Kafka
                import socket
                host, port = service.health_check.split(':')
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                result = sock.connect_ex((host, int(port)))
                sock.close()
                return result == 0
        except Exception as e:
            logger.debug(f"Error verificando {service.name}: {e}")
            return False
    
    # =============== INICIALIZACIÓN DE CLICKHOUSE ===============
    
    def initialize_clickhouse_users(self) -> bool:
        """🏠 Crear usuarios ClickHouse con permisos apropiados"""
        logger.info("🏠 === INICIALIZANDO USUARIOS CLICKHOUSE ===")
        
        try:
            # Conectar como default (usuario inicial)
            client = clickhouse_connect.get_client(
                host=self.clickhouse_config['host'],
                port=self.clickhouse_config['port'],
                database=self.clickhouse_config['database'],
                username='default',
                password=''
            )
            
            # Crear usuario superset para lectura
            superset_user = os.getenv('CLICKHOUSE_USER', 'superset')
            superset_pass = os.getenv('CLICKHOUSE_PASSWORD', 'Sup3rS3cret!')
            
            logger.info(f"👤 Creando usuario: {superset_user}")
            
            # Crear usuario si no existe
            client.command(f"""
                CREATE USER IF NOT EXISTS {superset_user} 
                IDENTIFIED WITH plaintext_password BY '{superset_pass}'
            """)
            
            # Otorgar permisos de lectura
            client.command(f"""
                GRANT SELECT ON {self.clickhouse_config['database']}.* TO {superset_user}
            """)
            
            # Crear usuario ETL para escritura
            etl_user = os.getenv('CH_USER', 'etl')
            etl_pass = os.getenv('CH_PASSWORD', 'Et1Ingest!')
            
            logger.info(f"👤 Creando usuario ETL: {etl_user}")
            
            client.command(f"""
                CREATE USER IF NOT EXISTS {etl_user} 
                IDENTIFIED WITH plaintext_password BY '{etl_pass}'
            """)
            
            # Otorgar permisos completos para ETL
            client.command(f"""
                GRANT ALL ON {self.clickhouse_config['database']}.* TO {etl_user}
            """)
            
            logger.info("✅ Usuarios ClickHouse creados exitosamente")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creando usuarios ClickHouse: {e}")
            return False
    
    # =============== INICIALIZACIÓN DE MYSQL ===============
    
    def initialize_mysql_users(self) -> bool:
        """🐬 Crear usuarios MySQL para Debezium"""
        logger.info("🐬 === INICIALIZANDO USUARIOS MYSQL ===")
        
        try:
            # Conectar con usuario administrador
            conn = pymysql.connect(**self.mysql_config)
            cursor = conn.cursor()
            
            # Usuario para Debezium
            debezium_user = 'debezium_auto'
            debezium_pass = 'Dbz_Auto_2025!'
            
            logger.info(f"👤 Creando usuario Debezium: {debezium_user}")
            
            # Crear usuario
            cursor.execute(f"""
                CREATE USER IF NOT EXISTS '{debezium_user}'@'%' 
                IDENTIFIED BY '{debezium_pass}'
            """)
            
            # Otorgar permisos globales
            cursor.execute(f"""
                GRANT RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT 
                ON *.* TO '{debezium_user}'@'%'
            """)
            
            # Otorgar permisos SELECT en la base de datos específica
            db_name = self.mysql_config['database']
            cursor.execute(f"""
                GRANT SELECT ON `{db_name}`.* TO '{debezium_user}'@'%'
            """)
            
            cursor.execute("FLUSH PRIVILEGES")
            
            # Verificar conexión con el nuevo usuario
            test_conn = pymysql.connect(
                host=self.mysql_config['host'],
                port=self.mysql_config['port'],
                user=debezium_user,
                password=debezium_pass,
                database=db_name
            )
            test_conn.close()
            
            conn.close()
            logger.info("✅ Usuario MySQL Debezium creado y verificado")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creando usuarios MySQL: {e}")
            return False
    
    # =============== INICIALIZACIÓN DE SUPERSET ===============
    
    def initialize_superset_database(self) -> bool:
        """📊 Inicializar base de datos de Superset"""
        logger.info("📊 === INICIALIZANDO BASE DE DATOS SUPERSET ===")
        
        try:
            # Ejecutar comandos de inicialización en el contenedor
            commands = [
                "superset db upgrade",
                "superset init"
            ]
            
            for command in commands:
                logger.info(f"⚡ Ejecutando: {command}")
                result = subprocess.run(
                    ['docker', 'exec', 'superset'] + command.split(),
                    capture_output=True, text=True
                )
                
                if result.returncode != 0:
                    logger.error(f"❌ Error en comando: {command}")
                    logger.error(f"Salida: {result.stderr}")
                    return False
                else:
                    logger.info(f"✅ Comando exitoso: {command}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error inicializando BD Superset: {e}")
            return False
    
    def create_superset_admin(self) -> bool:
        """👤 Crear usuario administrador de Superset"""
        logger.info("👤 === CREANDO ADMINISTRADOR SUPERSET ===")
        
        try:
            admin_config = self.superset_config
            
            # Comando para crear admin
            create_admin_cmd = [
                "superset", "fab", "create-admin",
                "--username", admin_config['admin_user'],
                "--firstname", "Admin",
                "--lastname", "User", 
                "--email", admin_config['admin_email'],
                "--password", admin_config['admin_password']
            ]
            
            logger.info(f"👤 Creando usuario: {admin_config['admin_user']}")
            
            result = subprocess.run(
                ['docker', 'exec', 'superset'] + create_admin_cmd,
                capture_output=True, text=True
            )
            
            # El comando puede devolver error si el usuario ya existe
            if "already exists" in result.stdout or result.returncode == 0:
                logger.info("✅ Usuario administrador disponible")
                
                # Verificar login
                if self.test_superset_login():
                    return True
                else:
                    logger.error("❌ No se puede hacer login con admin")
                    return False
            else:
                logger.error(f"❌ Error creando admin: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error creando admin Superset: {e}")
            return False
    
    def test_superset_login(self) -> bool:
        """🧪 Probar login de Superset y obtener tokens"""
        try:
            # Obtener token CSRF
            csrf_response = self.session.get(f"{self.superset_config['url']}/api/v1/security/csrf_token/")
            csrf_response.raise_for_status()
            csrf_data = csrf_response.json()
            self.superset_csrf_token = csrf_data['result']
            
            # Login
            login_data = {
                'username': self.superset_config['admin_user'],
                'password': self.superset_config['admin_password'],
                'provider': 'db'
            }
            
            headers = {
                'Content-Type': 'application/json',
                'X-CSRFToken': self.superset_csrf_token
            }
            
            login_response = self.session.post(
                f"{self.superset_config['url']}/api/v1/security/login",
                json=login_data,
                headers=headers
            )
            login_response.raise_for_status()
            
            login_result = login_response.json()
            self.superset_access_token = login_result.get('access_token')
            
            logger.info("✅ Login Superset exitoso")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error en login Superset: {e}")
            return False
    
    def configure_superset_clickhouse(self) -> bool:
        """🔗 Configurar conexión ClickHouse en Superset"""
        logger.info("🔗 === CONFIGURANDO CONEXIÓN CLICKHOUSE ===")
        
        try:
            if not self.superset_csrf_token:
                if not self.test_superset_login():
                    return False
            
            # Configurar conexión ClickHouse
            ch_config = self.clickhouse_config
            
            # URI de conexión
            if os.path.exists('/app/logs'):
                # En contenedor
                ch_host = 'clickhouse'
            else:
                # En host
                ch_host = 'localhost'
            
            uri = f"clickhousedb+connect://{ch_config['user']}:{ch_config['password']}@{ch_host}:{ch_config['port']}/{ch_config['database']}"
            
            database_config = {
                "database_name": f"ClickHouse_ETL_{ch_config['database']}",
                "sqlalchemy_uri": uri,
                "extra": json.dumps({
                    "engine_params": {
                        "pool_pre_ping": True,
                        "pool_recycle": 3600
                    }
                })
            }
            
            headers = {
                'Content-Type': 'application/json',
                'X-CSRFToken': self.superset_csrf_token
            }
            
            # Crear base de datos
            response = self.session.post(
                f"{self.superset_config['url']}/api/v1/database/",
                json=database_config,
                headers=headers
            )
            
            if response.status_code in [200, 201]:
                result = response.json()
                db_id = result.get('id')
                logger.info(f"✅ Base de datos ClickHouse configurada (ID: {db_id})")
                return True
            else:
                logger.error(f"❌ Error configurando ClickHouse: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error configurando conexión ClickHouse: {e}")
            return False
    
    def create_superset_datasets(self) -> bool:
        """📋 Crear datasets automáticos en Superset"""
        logger.info("📋 === CREANDO DATASETS AUTOMÁTICOS ===")
        
        try:
            # Obtener lista de bases de datos
            response = self.session.get(f"{self.superset_config['url']}/api/v1/database/")
            response.raise_for_status()
            
            databases = response.json()['result']
            clickhouse_db = None
            
            for db in databases:
                if 'ClickHouse' in db['database_name']:
                    clickhouse_db = db
                    break
            
            if not clickhouse_db:
                logger.error("❌ No se encontró base de datos ClickHouse")
                return False
            
            # Cargar metadatos de tablas si existen
            metadata_file = os.path.join('generated', 'default', 'tables_metadata.json')
            tables_to_create = ['archivos']  # Fallback
            
            if os.path.exists(metadata_file):
                with open(metadata_file, 'r') as f:
                    tables_data = json.load(f)
                tables_to_create = [t['name'] for t in tables_data]
                logger.info(f"📋 Usando {len(tables_to_create)} tablas desde metadatos")
            
            created_count = 0
            for table_name in tables_to_create:
                try:
                    dataset_config = {
                        "database": clickhouse_db['id'],
                        "schema": self.clickhouse_config['database'],
                        "table_name": table_name
                    }
                    
                    headers = {
                        'Content-Type': 'application/json',
                        'X-CSRFToken': self.superset_csrf_token
                    }
                    
                    response = self.session.post(
                        f"{self.superset_config['url']}/api/v1/dataset/",
                        json=dataset_config,
                        headers=headers
                    )
                    
                    if response.status_code in [200, 201]:
                        created_count += 1
                        logger.info(f"✅ Dataset creado: {table_name}")
                    else:
                        logger.warning(f"⚠️  No se pudo crear dataset {table_name}: {response.status_code}")
                        
                except Exception as e:
                    logger.warning(f"⚠️  Error con dataset {table_name}: {e}")
            
            logger.info(f"✅ {created_count} datasets creados exitosamente")
            return created_count > 0
            
        except Exception as e:
            logger.error(f"❌ Error creando datasets: {e}")
            return False
    
    # =============== INICIALIZACIÓN DE ETL ===============
    
    def start_etl_ingestion(self) -> bool:
        """⚡ Iniciar ingesta de datos ETL"""
        logger.info("⚡ === INICIANDO INGESTA ETL ===")
        
        try:
            # Ejecutar descubrimiento de tablas
            logger.info("🔍 Ejecutando descubrimiento de tablas...")
            result = subprocess.run(
                ['python3', 'tools/discover_mysql_tables.py'],
                cwd=os.getcwd(),
                capture_output=True, text=True
            )
            
            if result.returncode != 0:
                logger.error(f"❌ Error en descubrimiento: {result.stderr}")
                return False
            
            # Crear modelos ClickHouse
            logger.info("🏗️  Creando modelos ClickHouse...")
            result = subprocess.run(
                ['python3', 'tools/create_clickhouse_models.py'],
                cwd=os.getcwd(),
                capture_output=True, text=True
            )
            
            if result.returncode != 0:
                logger.error(f"❌ Error creando modelos: {result.stderr}")
                return False
            
            # Aplicar conectores
            logger.info("🔌 Aplicando conectores Debezium...")
            result = subprocess.run(
                ['python3', 'tools/apply_connectors_auto.py'],
                cwd=os.getcwd(),
                capture_output=True, text=True
            )
            
            if result.returncode != 0:
                logger.error(f"❌ Error aplicando conectores: {result.stderr}")
                return False
            
            logger.info("✅ Ingesta ETL iniciada exitosamente")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error iniciando ingesta ETL: {e}")
            return False
    
    # =============== ORQUESTACIÓN PRINCIPAL ===============
    
    def execute_initialization_step(self, step: InitializationStep) -> bool:
        """Ejecutar un paso de inicialización"""
        logger.info(f"🚀 Ejecutando: {step.description}")
        
        try:
            # Obtener función por nombre
            func = getattr(self, step.function)
            result = func()
            
            if result:
                step.completed = True
                step.error = None
                logger.info(f"✅ Completado: {step.name}")
                return True
            else:
                step.error = "Función retornó False"
                logger.error(f"❌ Falló: {step.name}")
                return False
                
        except Exception as e:
            step.error = str(e)
            logger.error(f"💥 Error en {step.name}: {e}")
            if self.debug:
                logger.error(traceback.format_exc())
            return False
    
    def check_dependencies_completed(self, step: InitializationStep) -> bool:
        """Verificar si las dependencias de un paso están completadas"""
        for dep_name in step.dependencies:
            dep_step = next((s for s in self.init_steps if s.name == dep_name), None)
            if not dep_step or not dep_step.completed:
                return False
        return True
    
    def run_intelligent_initialization(self) -> bool:
        """🤖 Ejecutar inicialización inteligente completa"""
        logger.info("🤖 === INICIANDO INICIALIZACIÓN INTELIGENTE ===")
        
        total_steps = len(self.init_steps)
        completed_steps = 0
        
        while completed_steps < total_steps:
            made_progress = False
            
            for step in self.init_steps:
                if step.completed:
                    continue
                
                # Verificar dependencias
                if not self.check_dependencies_completed(step):
                    continue
                
                # Ejecutar paso
                if self.execute_initialization_step(step):
                    completed_steps += 1
                    made_progress = True
                else:
                    # Manejar reintento
                    step.retry_count += 1
                    if step.retry_count <= step.max_retries:
                        logger.warning(f"⚠️  Reintentando {step.name} ({step.retry_count}/{step.max_retries})")
                        time.sleep(5)  # Esperar antes de reintentar
                    else:
                        logger.error(f"💥 {step.name} falló después de {step.max_retries} intentos")
                        return False
            
            if not made_progress and completed_steps < total_steps:
                logger.error("💥 No se puede hacer progreso, dependencias bloqueadas")
                return False
        
        # Resumen final
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        
        logger.info(f"\n🏁 === INICIALIZACIÓN INTELIGENTE COMPLETADA ===")
        logger.info(f"⏰ Duración: {duration:.1f} segundos")
        logger.info(f"📊 Pasos completados: {completed_steps}/{total_steps}")
        logger.info(f"🎉 PIPELINE ETL COMPLETAMENTE AUTOMATIZADO Y FUNCIONAL")
        
        return True

def main():
    """Función principal"""
    import argparse
    
    parser = argparse.ArgumentParser(description='🤖 Agente Inteligente de Inicialización ETL')
    parser.add_argument('--debug', action='store_true', help='Activar modo debug')
    parser.add_argument('--retry-failed', action='store_true', help='Reintentar pasos fallidos')
    
    args = parser.parse_args()
    
    try:
        # Crear y ejecutar agente
        agent = IntelligentInitAgent(debug=args.debug)
        success = agent.run_intelligent_initialization()
        
        if success:
            print(f"\n{'='*60}")
            print("🎉 INICIALIZACIÓN AUTOMÁTICA EXITOSA")
            print(f"🌐 Superset: {agent.superset_config['url']}")
            print(f"👤 Usuario: {agent.superset_config['admin_user']} / {agent.superset_config['admin_password']}")
            print(f"🏠 ClickHouse: localhost:8123")
            print(f"📊 Pipeline ETL: Completamente operativo")
            print(f"{'='*60}")
            return 0
        else:
            print(f"\n{'='*60}")
            print("❌ INICIALIZACIÓN FALLÓ")
            print("💡 Revisar logs para más detalles")
            print(f"{'='*60}")
            return 1
            
    except KeyboardInterrupt:
        print("\n⚠️  Inicialización interrumpida por el usuario")
        return 1
    except Exception as e:
        print(f"\n💥 Error crítico: {e}")
        return 1

if __name__ == "__main__":
    exit(main())