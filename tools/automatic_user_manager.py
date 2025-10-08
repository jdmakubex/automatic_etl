#!/usr/bin/env python3
"""
👥 CREADOR AUTOMÁTICO DE USUARIOS ETL
Sistema inteligente que crea automáticamente todos los usuarios necesarios
con sus permisos correctos en MySQL, ClickHouse y Superset
"""

import pymysql
import clickhouse_connect
import requests
import json
import logging
import os
import time
import subprocess
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any

class AutomaticUserManager:
    """Gestor automático de usuarios para todo el stack ETL"""
    
    def __init__(self):
        self.logger = self._setup_logging()
        self.db_connections = self._load_db_connections()
        self.users_config = self._load_users_config()
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        
    def _setup_logging(self) -> logging.Logger:
        """Configurar logging con múltiples handlers"""
        logger = logging.getLogger('AutoUserManager')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            # Handler para consola
            console_handler = logging.StreamHandler()
            console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)
            
            # Handler para archivo (si es posible)
            try:
                log_dir = '/app/logs' if os.path.exists('/app/logs') else 'logs'
                os.makedirs(log_dir, exist_ok=True)
                file_handler = logging.FileHandler(f'{log_dir}/auto_users.log')
                file_handler.setFormatter(console_formatter)
                logger.addHandler(file_handler)
            except Exception:
                pass  # No crítico si no se puede crear el archivo de log
                
        return logger
    
    def _load_db_connections(self) -> List[Dict[str, Any]]:
        """Cargar configuraciones de conexiones de base de datos"""
        db_connections_str = os.getenv('DB_CONNECTIONS', '[]')
        try:
            return json.loads(db_connections_str)
        except Exception as e:
            self.logger.error(f"❌ Error parseando DB_CONNECTIONS: {e}")
            return []
    
    def _load_users_config(self) -> Dict[str, Any]:
        """Cargar configuración de usuarios necesarios"""
        return {
            'mysql': {
                'debezium_user': {
                    'username': 'debezium_auto',
                    'password': 'Dbz_Auto_2025!',
                    'host': '%',
                    'privileges': [
                        'SELECT',
                        'RELOAD', 
                        'SHOW DATABASES',
                        'REPLICATION SLAVE',
                        'REPLICATION CLIENT'
                    ]
                }
            },
            'clickhouse': {
                'superset_user': {
                    'username': 'superset',
                    'password': os.getenv('CLICKHOUSE_PASSWORD', 'Sup3rS3cret!'),
                    'database': os.getenv('CLICKHOUSE_DATABASE', 'fgeo_analytics'),
                    'privileges': ['SELECT', 'SHOW']
                },
                'etl_user': {
                    'username': 'etl',
                    'password': os.getenv('CH_PASSWORD', 'Et1Ingest!'),
                    'database': os.getenv('CLICKHOUSE_DATABASE', 'fgeo_analytics'),
                    'privileges': ['SELECT', 'INSERT', 'CREATE', 'DROP', 'SHOW']
                }
            },
            'superset': {
                'admin_user': {
                    'username': os.getenv('SUPERSET_ADMIN', 'admin'),
                    'password': os.getenv('SUPERSET_PASSWORD', 'Admin123!'),
                    'firstname': 'Admin',
                    'lastname': 'User',
                    'email': 'admin@etl.local'
                }
            }
        }
    
    def create_mysql_users(self) -> Tuple[bool, List[str]]:
        """Crear usuarios MySQL con permisos de replicación"""
        issues = []
        
        if not self.db_connections:
            issues.append("No hay conexiones MySQL configuradas")
            return False, issues
        
        for db_config in self.db_connections:
            if db_config.get('type') != 'mysql':
                continue
                
            try:
                self.logger.info(f"👤 Configurando usuarios MySQL en {db_config['host']}")
                
                # Conectar como admin usando las credenciales del config
                admin_connection = pymysql.connect(
                    host=db_config['host'],
                    port=int(db_config.get('port', 3306)),
                    user=db_config['user'],
                    password=db_config['pass'],
                    database=db_config['db'],
                    charset='utf8mb4'
                )
                
                with admin_connection.cursor() as cursor:
                    # Crear usuario Debezium
                    debezium_config = self.users_config['mysql']['debezium_user']
                    
                    # 1. Crear usuario si no existe
                    create_user_sql = f"""
                    CREATE USER IF NOT EXISTS '{debezium_config['username']}'@'{debezium_config['host']}' 
                    IDENTIFIED BY '{debezium_config['password']}'
                    """
                    cursor.execute(create_user_sql)
                    self.logger.info(f"✅ Usuario {debezium_config['username']} creado/verificado")
                    
                    # 2. Otorgar permisos globales
                    for privilege in debezium_config['privileges']:
                        if privilege in ['RELOAD', 'SHOW DATABASES', 'REPLICATION SLAVE', 'REPLICATION CLIENT']:
                            grant_sql = f"""
                            GRANT {privilege} ON *.* 
                            TO '{debezium_config['username']}'@'{debezium_config['host']}'
                            """
                            cursor.execute(grant_sql)
                            self.logger.info(f"🔐 Permiso {privilege} otorgado globalmente")
                    
                    # 3. Otorgar permisos específicos de base de datos
                    grant_db_sql = f"""
                    GRANT SELECT ON {db_config['db']}.* 
                    TO '{debezium_config['username']}'@'{debezium_config['host']}'
                    """
                    cursor.execute(grant_db_sql)
                    self.logger.info(f"🔐 Permiso SELECT otorgado en {db_config['db']}")
                    
                    # 4. Aplicar cambios
                    cursor.execute("FLUSH PRIVILEGES")
                    
                    # 5. Verificar permisos
                    cursor.execute(f"SHOW GRANTS FOR '{debezium_config['username']}'@'{debezium_config['host']}'")
                    grants = cursor.fetchall()
                    self.logger.info(f"✅ Usuario MySQL configurado con {len(grants)} permisos")
                
                admin_connection.close()
                
            except Exception as e:
                error_msg = f"Error configurando MySQL {db_config['host']}: {str(e)}"
                self.logger.error(f"❌ {error_msg}")
                issues.append(error_msg)
        
        success = len(issues) == 0
        if success:
            self.logger.info("✅ Usuarios MySQL configurados exitosamente")
        
        return success, issues
    
    def create_clickhouse_users(self) -> Tuple[bool, List[str]]:
        """Crear usuarios ClickHouse con permisos apropiados"""
        issues = []
        ch_host = os.getenv('CLICKHOUSE_HTTP_HOST', 'clickhouse')
        ch_port = int(os.getenv('CLICKHOUSE_HTTP_PORT', 8123))
        
        try:
            self.logger.info("🏠 Configurando usuarios ClickHouse...")
            
            for user_key, user_config in self.users_config['clickhouse'].items():
                try:
                    username = user_config['username']
                    password = user_config['password']
                    database = user_config['database']
                    
                    self.logger.info(f"👤 Creando usuario ClickHouse: {username}")
                    
                    # 1. Crear usuario
                    create_user_sql = f"CREATE USER IF NOT EXISTS {username} IDENTIFIED BY '{password}'"
                    response = requests.post(
                        f"http://{ch_host}:{ch_port}/",
                        data=create_user_sql,
                        timeout=15
                    )
                    
                    if response.status_code != 200:
                        raise Exception(f"Error creando usuario: {response.text}")
                    
                    # 2. Otorgar permisos según el tipo de usuario
                    if 'superset' in user_key:
                        # Usuario de lectura para Superset
                        grant_sql = f"GRANT SELECT ON {database}.* TO {username}"
                    else:
                        # Usuario ETL con permisos completos
                        grant_sql = f"GRANT SELECT, INSERT, CREATE, DROP ON {database}.* TO {username}"
                    
                    grant_response = requests.post(
                        f"http://{ch_host}:{ch_port}/",
                        data=grant_sql,
                        timeout=15
                    )
                    
                    if grant_response.status_code != 200:
                        self.logger.warning(f"⚠️  Advertencia otorgando permisos a {username}: {grant_response.text}")
                    else:
                        self.logger.info(f"🔐 Permisos otorgados a {username}")
                    
                    # 3. Verificar usuario creado
                    verify_sql = f"SELECT name FROM system.users WHERE name = '{username}'"
                    verify_response = requests.post(
                        f"http://{ch_host}:{ch_port}/",
                        data=verify_sql,
                        timeout=10
                    )
                    
                    if verify_response.status_code == 200 and username in verify_response.text:
                        self.logger.info(f"✅ Usuario {username} verificado en ClickHouse")
                    else:
                        self.logger.warning(f"⚠️  Usuario {username} no se pudo verificar")
                
                except Exception as e:
                    error_msg = f"Error creando usuario ClickHouse {user_config['username']}: {str(e)}"
                    self.logger.error(f"❌ {error_msg}")
                    issues.append(error_msg)
        
        except Exception as e:
            error_msg = f"Error general ClickHouse: {str(e)}"
            self.logger.error(f"❌ {error_msg}")
            issues.append(error_msg)
        
        success = len(issues) == 0
        if success:
            self.logger.info("✅ Usuarios ClickHouse configurados exitosamente")
        
        return success, issues
    
    def create_superset_admin(self) -> Tuple[bool, List[str]]:
        """Crear usuario administrador de Superset"""
        issues = []
        
        try:
            self.logger.info("📊 Configurando usuario admin de Superset...")
            
            admin_config = self.users_config['superset']['admin_user']
            
            # Usar docker exec para ejecutar comandos de Superset
            superset_commands = [
                # Crear usuario admin
                [
                    'docker', 'exec', 'superset', 'superset', 'fab', 'create-admin',
                    '--username', admin_config['username'],
                    '--firstname', admin_config['firstname'],
                    '--lastname', admin_config['lastname'],
                    '--email', admin_config['email'],
                    '--password', admin_config['password']
                ],
                # Actualizar base de datos
                ['docker', 'exec', 'superset', 'superset', 'db', 'upgrade'],
                # Inicializar Superset
                ['docker', 'exec', 'superset', 'superset', 'init']
            ]
            
            for i, cmd in enumerate(superset_commands):
                cmd_name = ['crear admin', 'actualizar BD', 'inicializar'][i]
                
                try:
                    self.logger.info(f"🔧 Ejecutando: {cmd_name}")
                    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
                    
                    if result.returncode == 0:
                        self.logger.info(f"✅ {cmd_name} completado")
                        if 'created' in result.stdout.lower() or 'admin user' in result.stdout.lower():
                            self.logger.info("👤 Usuario admin creado exitosamente")
                    else:
                        # Algunos comandos pueden fallar si ya se ejecutaron antes
                        if 'already exists' in result.stderr or 'already exists' in result.stdout:
                            self.logger.info(f"ℹ️  {cmd_name} ya ejecutado previamente")
                        else:
                            self.logger.warning(f"⚠️  {cmd_name} con advertencias: {result.stderr}")
                
                except subprocess.TimeoutExpired:
                    self.logger.warning(f"⚠️  Timeout en {cmd_name}")
                except Exception as e:
                    error_msg = f"Error en {cmd_name}: {str(e)}"
                    self.logger.error(f"❌ {error_msg}")
                    issues.append(error_msg)
            
            # Verificar que Superset responda
            try:
                time.sleep(5)  # Dar tiempo para que se reinicie
                superset_url = os.getenv('SUPERSET_URL', 'http://localhost:8088')
                response = requests.get(f"{superset_url}/health", timeout=30)
                
                if response.status_code == 200 and response.text.strip() == "OK":
                    self.logger.info("✅ Superset respondiendo correctamente")
                else:
                    self.logger.warning(f"⚠️  Superset responde pero con estado: {response.status_code}")
            
            except Exception as e:
                self.logger.warning(f"⚠️  No se pudo verificar Superset: {str(e)}")
        
        except Exception as e:
            error_msg = f"Error configurando Superset: {str(e)}"
            self.logger.error(f"❌ {error_msg}")
            issues.append(error_msg)
        
        success = len(issues) == 0
        if success:
            self.logger.info("✅ Superset configurado exitosamente")
        
        return success, issues
    
    def test_users_connectivity(self) -> Dict[str, bool]:
        """Probar conectividad de todos los usuarios creados"""
        results = {}
        
        self.logger.info("🧪 Probando conectividad de usuarios...")
        
        # Test MySQL Debezium user
        try:
            if self.db_connections:
                db_config = self.db_connections[0]  # Usar primera conexión
                debezium_config = self.users_config['mysql']['debezium_user']
                
                test_connection = pymysql.connect(
                    host=db_config['host'],
                    port=int(db_config.get('port', 3306)),
                    user=debezium_config['username'],
                    password=debezium_config['password'],
                    database=db_config['db'],
                    charset='utf8mb4'
                )
                
                with test_connection.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES")
                    table_count = cursor.fetchone()[0]
                    
                test_connection.close()
                results['mysql_debezium'] = True
                self.logger.info(f"✅ MySQL Debezium: {table_count} tablas accesibles")
            else:
                results['mysql_debezium'] = False
                self.logger.warning("⚠️  No hay conexiones MySQL para probar")
        
        except Exception as e:
            results['mysql_debezium'] = False
            self.logger.error(f"❌ Test MySQL Debezium: {str(e)}")
        
        # Test ClickHouse users
        ch_host = os.getenv('CLICKHOUSE_HTTP_HOST', 'clickhouse')
        ch_port = int(os.getenv('CLICKHOUSE_HTTP_PORT', 8123))
        
        for user_key, user_config in self.users_config['clickhouse'].items():
            try:
                username = user_config['username']
                password = user_config['password']
                
                # Test con autenticación
                auth = requests.auth.HTTPBasicAuth(username, password)
                response = requests.post(
                    f"http://{ch_host}:{ch_port}/",
                    data="SELECT 1",
                    auth=auth,
                    timeout=10
                )
                
                if response.status_code == 200:
                    results[f'clickhouse_{user_key}'] = True
                    self.logger.info(f"✅ ClickHouse {username}: conexión exitosa")
                else:
                    results[f'clickhouse_{user_key}'] = False
                    self.logger.error(f"❌ ClickHouse {username}: {response.status_code}")
            
            except Exception as e:
                results[f'clickhouse_{user_key}'] = False
                self.logger.error(f"❌ Test ClickHouse {user_config['username']}: {str(e)}")
        
        # Test Superset admin
        try:
            superset_url = os.getenv('SUPERSET_URL', 'http://localhost:8088')
            admin_config = self.users_config['superset']['admin_user']
            
            login_payload = {
                "username": admin_config['username'],
                "password": admin_config['password'],
                "provider": "db",
                "refresh": True
            }
            
            response = self.session.post(f"{superset_url}/api/v1/security/login", json=login_payload)
            
            if response.status_code == 200:
                results['superset_admin'] = True
                self.logger.info("✅ Superset admin: login exitoso")
            else:
                results['superset_admin'] = False
                self.logger.error(f"❌ Superset admin login: {response.status_code}")
        
        except Exception as e:
            results['superset_admin'] = False
            self.logger.error(f"❌ Test Superset admin: {str(e)}")
        
        # Resumen
        successful_tests = sum(results.values())
        total_tests = len(results)
        self.logger.info(f"🏁 Tests completados: {successful_tests}/{total_tests} exitosos")
        
        return results
    
    def comprehensive_user_setup(self) -> Tuple[bool, Dict[str, Any]]:
        """Ejecutar configuración completa de usuarios"""
        start_time = datetime.now()
        self.logger.info("👥 === INICIANDO CONFIGURACIÓN AUTOMÁTICA DE USUARIOS ===")
        
        results = {
            'timestamp': start_time.isoformat(),
            'mysql': {'success': False, 'issues': []},
            'clickhouse': {'success': False, 'issues': []},
            'superset': {'success': False, 'issues': []},
            'connectivity_tests': {},
            'overall_success': False,
            'duration_seconds': 0
        }
        
        # 1. Configurar usuarios MySQL
        mysql_ok, mysql_issues = self.create_mysql_users()
        results['mysql']['success'] = mysql_ok
        results['mysql']['issues'] = mysql_issues
        
        # 2. Configurar usuarios ClickHouse
        ch_ok, ch_issues = self.create_clickhouse_users()
        results['clickhouse']['success'] = ch_ok
        results['clickhouse']['issues'] = ch_issues
        
        # 3. Configurar Superset
        superset_ok, superset_issues = self.create_superset_admin()
        results['superset']['success'] = superset_ok
        results['superset']['issues'] = superset_issues
        
        # 4. Probar conectividad
        if mysql_ok or ch_ok or superset_ok:
            self.logger.info("🔗 Esperando 10s antes de probar conectividad...")
            time.sleep(10)
            results['connectivity_tests'] = self.test_users_connectivity()
        
        # Determinar éxito general
        critical_success = mysql_ok and ch_ok
        results['overall_success'] = critical_success
        
        # Calcular duración
        end_time = datetime.now()
        results['duration_seconds'] = (end_time - start_time).total_seconds()
        
        # Resumen final
        self.logger.info(f"🏁 Configuración de usuarios completada en {results['duration_seconds']:.1f}s")
        
        if results['overall_success']:
            self.logger.info("🎉 USUARIOS CONFIGURADOS EXITOSAMENTE")
        else:
            self.logger.error("❌ PROBLEMAS EN CONFIGURACIÓN DE USUARIOS")
            all_issues = mysql_issues + ch_issues + superset_issues
            for issue in all_issues[:5]:  # Mostrar primeros 5 errores
                self.logger.error(f"   • {issue}")
        
        # Guardar resultados
        try:
            results_file = '/app/logs/users_setup_results.json'
            os.makedirs(os.path.dirname(results_file), exist_ok=True)
            with open(results_file, 'w') as f:
                json.dump(results, f, indent=2)
            self.logger.info(f"💾 Resultados guardados en {results_file}")
        except Exception as e:
            self.logger.warning(f"⚠️  No se pudieron guardar resultados: {e}")
        
        return results['overall_success'], results


def main():
    """Función principal para ejecutar configuración independiente"""
    manager = AutomaticUserManager()
    success, results = manager.comprehensive_user_setup()
    
    return 0 if success else 1


if __name__ == "__main__":
    exit(main())