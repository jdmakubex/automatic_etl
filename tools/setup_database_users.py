#!/usr/bin/env python3
"""
👥 CONFIGURADOR AUTOMÁTICO DE USUARIOS Y PERMISOS
Verifica y crea automáticamente usuarios necesarios con permisos apropiados:
- Usuarios MySQL con permisos de replicación
- Usuarios ClickHouse con permisos de escritura
- Validación de conectividad
- Configuración de seguridad básica
"""

import pymysql
import clickhouse_connect
import logging
import os
import time
import json
from datetime import datetime
from typing import Dict, List, Tuple, Optional

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/users_setup.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatabaseUsersManager:
    def __init__(self):
        # Configuración MySQL - usar credenciales del primer DB_CONNECTION
        db_connections_str = os.getenv('DB_CONNECTIONS', '[]')
        try:
            import json
            db_connections = json.loads(db_connections_str)
            if db_connections:
                first_db = db_connections[0]
                self.mysql_config = {
                    'host': first_db.get('host', '172.21.61.53'),
                    'port': int(first_db.get('port', 3306)),
                    'user': first_db.get('user', 'juan.marcos'),
                    'password': first_db.get('pass', '123456'),
                    'database': first_db.get('db', 'archivos'),
                    'charset': 'utf8mb4'
                }
            else:
                # Fallback por defecto
                self.mysql_config = {
                    'host': os.getenv('MYSQL_HOST', '172.21.61.53'),
                    'port': int(os.getenv('MYSQL_PORT', 3306)),
                    'user': os.getenv('MYSQL_ADMIN_USER', 'juan.marcos'),
                    'password': os.getenv('MYSQL_ADMIN_PASSWORD', '123456'),
                    'database': os.getenv('MYSQL_DATABASE', 'archivos'),
                    'charset': 'utf8mb4'
                }
        except Exception as e:
            logger.warning(f"⚠️  Error parseando DB_CONNECTIONS: {e}")
            # Usar fallback
            self.mysql_config = {
                'host': os.getenv('MYSQL_HOST', '172.21.61.53'),
                'port': int(os.getenv('MYSQL_PORT', 3306)),
                'user': os.getenv('MYSQL_ADMIN_USER', 'juan.marcos'),
                'password': os.getenv('MYSQL_ADMIN_PASSWORD', '123456'),
                'database': os.getenv('MYSQL_DATABASE', 'archivos'),
                'charset': 'utf8mb4'
            }
        
        # Usuario Debezium que se debe crear/verificar
        self.debezium_user = {
            'username': 'debezium_auto',
            'password': 'Dbz_Auto_2025!',
            'host': '%',  # Permitir conexiones desde cualquier IP
            'privileges': [
                'SELECT',
                'RELOAD', 
                'SHOW DATABASES',
                'REPLICATION SLAVE',
                'REPLICATION CLIENT'
            ]
        }
        
        # Configuración ClickHouse
        self.clickhouse_config = {
            'host': 'clickhouse',
            'port': 8123,
            'database': 'fgeo_analytics'
        }
        
        # Usuario ClickHouse que se debe crear/verificar
        self.clickhouse_user = {
            'username': 'etl_auto',
            'password': 'Etl_Auto_2025!',
            'database': 'fgeo_analytics'
        }
    
    def connect_mysql_admin(self) -> pymysql.Connection:
        """Conectar a MySQL con usuario administrador"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                logger.info(f"🔌 Conectando a MySQL como admin (intento {attempt + 1})")
                connection = pymysql.connect(**self.mysql_config)
                logger.info("✅ Conexión MySQL admin establecida")
                return connection
            except Exception as e:
                logger.error(f"❌ Error conectando como admin: {str(e)}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(2)
    
    def check_mysql_user_exists(self, connection: pymysql.Connection, username: str, host: str) -> bool:
        """Verificar si un usuario MySQL existe"""
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT User, Host FROM mysql.user WHERE User = %s AND Host = %s", (username, host))
                result = cursor.fetchone()
                return result is not None
        except Exception as e:
            logger.error(f"❌ Error verificando usuario {username}: {str(e)}")
            return False
    
    def create_mysql_user(self, connection: pymysql.Connection) -> bool:
        """Crear usuario Debezium con permisos necesarios"""
        try:
            username = self.debezium_user['username']
            password = self.debezium_user['password']
            host = self.debezium_user['host']
            
            with connection.cursor() as cursor:
                # Verificar si el usuario ya existe
                if self.check_mysql_user_exists(connection, username, host):
                    logger.info(f"ℹ️  Usuario {username}@{host} ya existe")
                    
                    # Actualizar password por si cambió
                    cursor.execute(f"ALTER USER '{username}'@'{host}' IDENTIFIED BY '{password}'")
                    logger.info(f"🔄 Password actualizado para {username}@{host}")
                else:
                    # Crear nuevo usuario
                    cursor.execute(f"CREATE USER '{username}'@'{host}' IDENTIFIED BY '{password}'")
                    logger.info(f"👤 Usuario creado: {username}@{host}")
                
                # Otorgar permisos específicos
                database = self.mysql_config['database']
                
                # Permisos a nivel global (necesarios para replicación)
                global_privileges = ['RELOAD', 'REPLICATION SLAVE', 'REPLICATION CLIENT', 'SHOW DATABASES']
                for privilege in global_privileges:
                    cursor.execute(f"GRANT {privilege} ON *.* TO '{username}'@'{host}'")
                    logger.info(f"🔐 Permiso global otorgado: {privilege}")
                
                # Permisos específicos de la base de datos
                cursor.execute(f"GRANT SELECT ON {database}.* TO '{username}'@'{host}'")
                logger.info(f"🔐 Permiso SELECT otorgado en {database}.*")
                
                # Aplicar cambios
                cursor.execute("FLUSH PRIVILEGES")
                connection.commit()
                
                logger.info(f"✅ Usuario {username} configurado correctamente")
                return True
                
        except Exception as e:
            logger.error(f"❌ Error creando usuario MySQL: {str(e)}")
            connection.rollback()
            return False
    
    def verify_mysql_permissions(self, connection: pymysql.Connection) -> bool:
        """Verificar que los permisos estén correctamente configurados"""
        try:
            username = self.debezium_user['username']
            host = self.debezium_user['host']
            
            with connection.cursor() as cursor:
                # Verificar permisos globales
                cursor.execute(f"SHOW GRANTS FOR '{username}'@'{host}'")
                grants = [row[0] for row in cursor.fetchall()]
                
                logger.info(f"📋 Permisos actuales para {username}@{host}:")
                for grant in grants:
                    logger.info(f"   🔐 {grant}")
                
                # Verificar permisos requeridos
                required_found = 0
                required_perms = ['RELOAD', 'REPLICATION SLAVE', 'REPLICATION CLIENT', 'SELECT']
                
                for grant in grants:
                    for perm in required_perms:
                        if perm in grant.upper():
                            required_found += 1
                            break
                
                success = required_found >= len(required_perms)
                if success:
                    logger.info("✅ Todos los permisos requeridos están presentes")
                else:
                    logger.warning(f"⚠️  Solo {required_found}/{len(required_perms)} permisos encontrados")
                
                return success
                
        except Exception as e:
            logger.error(f"❌ Error verificando permisos: {str(e)}")
            return False
    
    def test_mysql_debezium_connection(self) -> bool:
        """Probar conexión con credenciales de Debezium"""
        try:
            logger.info("🧪 Probando conexión con usuario Debezium...")
            
            test_config = self.mysql_config.copy()
            test_config['user'] = self.debezium_user['username']
            test_config['password'] = self.debezium_user['password']
            
            connection = pymysql.connect(**test_config)
            
            with connection.cursor() as cursor:
                # Probar SELECT
                cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = %s", 
                             (self.mysql_config['database'],))
                table_count = cursor.fetchone()[0]
                
                logger.info(f"✅ Conexión Debezium exitosa - {table_count} tablas accesibles")
                
            connection.close()
            return True
            
        except Exception as e:
            logger.error(f"❌ Error en conexión Debezium: {str(e)}")
            return False
    
    def setup_clickhouse_user(self) -> bool:
        """Configurar usuario en ClickHouse"""
        try:
            logger.info("🏠 Configurando usuario ClickHouse...")
            
            # Conectar como usuario default para crear otros usuarios
            client = clickhouse_connect.get_client(
                host=self.clickhouse_config['host'],
                port=self.clickhouse_config['port']
            )
            
            username = self.clickhouse_user['username']
            password = self.clickhouse_user['password']
            database = self.clickhouse_user['database']
            
            # Crear base de datos si no existe
            client.command(f"CREATE DATABASE IF NOT EXISTS {database}")
            logger.info(f"📋 Base de datos {database} verificada")
            
            # Crear usuario (ClickHouse permite CREATE OR REPLACE)
            create_user_sql = f"""
            CREATE USER IF NOT EXISTS {username} 
            IDENTIFIED WITH plaintext_password BY '{password}'
            """
            client.command(create_user_sql)
            logger.info(f"👤 Usuario {username} creado/actualizado")
            
            # Otorgar permisos
            grant_sql = f"GRANT ALL ON {database}.* TO {username}"
            client.command(grant_sql)
            logger.info(f"🔐 Permisos otorgados a {username} en {database}")
            
            client.close()
            return True
            
        except Exception as e:
            logger.error(f"❌ Error configurando ClickHouse: {str(e)}")
            return False
    
    def test_clickhouse_connection(self) -> bool:
        """Probar conexión ClickHouse con nuevo usuario"""
        try:
            logger.info("🧪 Probando conexión ClickHouse...")
            
            client = clickhouse_connect.get_client(
                host=self.clickhouse_config['host'],
                port=self.clickhouse_config['port'],
                username=self.clickhouse_user['username'],
                password=self.clickhouse_user['password'],
                database=self.clickhouse_user['database']
            )
            
            # Probar consulta básica
            result = client.command("SELECT 1 as test")
            logger.info(f"✅ Conexión ClickHouse exitosa - resultado: {result}")
            
            # Probar creación de tabla de prueba
            client.command("""
            CREATE TABLE IF NOT EXISTS connection_test (
                id UInt32,
                timestamp DateTime DEFAULT now()
            ) ENGINE = Memory
            """)
            
            # Insertar y consultar
            client.command("INSERT INTO connection_test (id) VALUES (1)")
            count = client.command("SELECT COUNT(*) FROM connection_test")
            
            # Limpiar tabla de prueba
            client.command("DROP TABLE IF EXISTS connection_test")
            
            logger.info(f"✅ Test de escritura ClickHouse exitoso - registros: {count}")
            
            client.close()
            return True
            
        except Exception as e:
            logger.error(f"❌ Error en test ClickHouse: {str(e)}")
            return False
    
    def save_connection_configs(self):
        """Guardar configuraciones de conexión para otros scripts"""
        try:
            logger.info("💾 Guardando configuraciones de conexión...")
            
            # Configuración para Debezium
            debezium_config = {
                'mysql_host': self.mysql_config['host'],
                'mysql_port': self.mysql_config['port'],
                'mysql_user': self.debezium_user['username'],
                'mysql_password': self.debezium_user['password'],
                'mysql_database': self.mysql_config['database']
            }
            
            # Configuración para ClickHouse
            clickhouse_config = {
                'clickhouse_host': self.clickhouse_config['host'],
                'clickhouse_port': self.clickhouse_config['port'],
                'clickhouse_user': self.clickhouse_user['username'],
                'clickhouse_password': self.clickhouse_user['password'],
                'clickhouse_database': self.clickhouse_user['database']
            }
            
            # Guardar como archivo .env
            env_file = '/app/generated/default/.env_auto'
            with open(env_file, 'w') as f:
                f.write("# Configuración automática de usuarios ETL\n")
                f.write(f"# Generado: {datetime.now().isoformat()}\n\n")
                
                f.write("# MySQL Debezium\n")
                for key, value in debezium_config.items():
                    f.write(f"{key.upper()}={value}\n")
                
                f.write("\n# ClickHouse ETL\n")
                for key, value in clickhouse_config.items():
                    f.write(f"{key.upper()}={value}\n")
            
            logger.info(f"✅ Configuración guardada: {env_file}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error guardando configuración: {str(e)}")
            return False
    
    def run_users_setup(self) -> bool:
        """Ejecutar configuración completa de usuarios"""
        start_time = datetime.now()
        logger.info("🚀 === INICIANDO CONFIGURACIÓN DE USUARIOS Y PERMISOS ===")
        logger.info(f"⏰ Inicio: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        success_steps = 0
        total_steps = 6
        
        try:
            # 1. Configurar usuario MySQL
            logger.info("\n📋 1. CONFIGURACIÓN USUARIO MYSQL")
            connection = self.connect_mysql_admin()
            if self.create_mysql_user(connection):
                success_steps += 1
                logger.info("✅ Usuario MySQL configurado")
            else:
                logger.error("❌ Falló configuración usuario MySQL")
            
            # 2. Verificar permisos MySQL
            logger.info("\n📋 2. VERIFICACIÓN PERMISOS MYSQL")
            if self.verify_mysql_permissions(connection):
                success_steps += 1
                logger.info("✅ Permisos MySQL verificados")
            else:
                logger.error("❌ Falló verificación permisos MySQL")
            
            connection.close()
            
            # 3. Probar conexión Debezium
            logger.info("\n📋 3. TEST CONEXIÓN DEBEZIUM")
            if self.test_mysql_debezium_connection():
                success_steps += 1
                logger.info("✅ Conexión Debezium verificada")
            else:
                logger.error("❌ Falló test conexión Debezium")
            
            # 4. Configurar usuario ClickHouse
            logger.info("\n📋 4. CONFIGURACIÓN USUARIO CLICKHOUSE")
            if self.setup_clickhouse_user():
                success_steps += 1
                logger.info("✅ Usuario ClickHouse configurado")
            else:
                logger.error("❌ Falló configuración ClickHouse")
            
            # 5. Probar conexión ClickHouse
            logger.info("\n📋 5. TEST CONEXIÓN CLICKHOUSE")
            if self.test_clickhouse_connection():
                success_steps += 1
                logger.info("✅ Conexión ClickHouse verificada")
            else:
                logger.error("❌ Falló test conexión ClickHouse")
            
            # 6. Guardar configuraciones
            logger.info("\n📋 6. GUARDAR CONFIGURACIONES")
            if self.save_connection_configs():
                success_steps += 1
                logger.info("✅ Configuraciones guardadas")
            else:
                logger.error("❌ Falló guardado de configuraciones")
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info(f"\n🏁 === CONFIGURACIÓN DE USUARIOS COMPLETADA ===")
            logger.info(f"⏰ Duración: {duration:.1f} segundos")
            logger.info(f"📊 Éxito: {success_steps}/{total_steps} pasos")
            
            success = success_steps == total_steps
            if success:
                logger.info("🎉 CONFIGURACIÓN DE USUARIOS EXITOSA")
                logger.info(f"👤 Usuario MySQL: {self.debezium_user['username']}")
                logger.info(f"🏠 Usuario ClickHouse: {self.clickhouse_user['username']}")
            else:
                logger.warning("⚠️  CONFIGURACIÓN PARCIAL - Revisar errores")
            
            return success
            
        except Exception as e:
            logger.error(f"💥 Error crítico en configuración: {str(e)}")
            return False

def main():
    """Función principal"""
    try:
        manager = DatabaseUsersManager()
        success = manager.run_users_setup()
        
        print(f"\n{'='*60}")
        if success:
            print("🎉 CONFIGURACIÓN DE USUARIOS EXITOSA")
            print("✅ Usuarios y permisos configurados correctamente")
            print("🔐 Credenciales guardadas en .env_auto")
        else:
            print("❌ CONFIGURACIÓN DE USUARIOS FALLÓ")
            print("💡 Revisar logs para detalles de errores")
        print(f"{'='*60}")
        
        return 0 if success else 1
        
    except KeyboardInterrupt:
        logger.info("🛑 Configuración interrumpida por el usuario")
        return 1
    except Exception as e:
        logger.error(f"💥 Error crítico: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())