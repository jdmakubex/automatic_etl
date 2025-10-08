#!/usr/bin/env python3
"""
🔧 REPARACIÓN AUTOMÁTICA DE CONFIGURACIÓN MYSQL 
Soluciona problemas de sintaxis SQL y permisos de usuarios
Se conecta directamente al servidor MySQL externo
"""

import pymysql
import logging
import time
import json
import sys
import os
from datetime import datetime

# Configurar logging
log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'mysql_fix.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file)
    ]
)

logger = logging.getLogger(__name__)

class MySQLConfigurationFixer:
    def __init__(self):
        self.fixed_issues = []
        self.errors = []
        
        # Configuración MySQL - obtener de variables de entorno
        db_connections_str = os.getenv('DB_CONNECTIONS', '[]')
        try:
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
                    'user': os.getenv('MYSQL_USER', 'juan.marcos'),
                    'password': os.getenv('MYSQL_PASSWORD', '123456'),
                    'database': os.getenv('MYSQL_DATABASE', 'archivos'),
                    'charset': 'utf8mb4'
                }
        except Exception:
            # Fallback por defecto
            self.mysql_config = {
                'host': '172.21.61.53',
                'port': 3306,
                'user': 'juan.marcos',
                'password': '123456',
                'database': 'archivos',
                'charset': 'utf8mb4'
            }
            
        # Configuración del usuario Debezium
        self.debezium_user = {
            'username': 'debezium_auto',
            'password': 'Dbz_Auto_2025!',
            'host': '%'
        }
        
    def connect_mysql(self):
        """Crear conexión a MySQL"""
        try:
            logger.info(f"🔌 Conectando a MySQL {self.mysql_config['host']}:{self.mysql_config['port']}...")
            connection = pymysql.connect(
                host=self.mysql_config['host'],
                port=self.mysql_config['port'],
                user=self.mysql_config['user'],
                password=self.mysql_config['password'],
                database=self.mysql_config['database'],
                charset=self.mysql_config['charset'],
                autocommit=False
            )
            logger.info("✅ Conexión MySQL establecida")
            return connection
        except Exception as e:
            logger.error(f"❌ Error conectando a MySQL: {e}")
            return None
    
    def check_user_exists(self, connection, username, host):
        """Verificar si un usuario existe"""
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(*) FROM mysql.user WHERE User = %s AND Host = %s",
                    (username, host)
                )
                count = cursor.fetchone()[0]
                return count > 0
        except Exception as e:
            logger.error(f"❌ Error verificando usuario: {e}")
            return False
    
    def fix_debezium_user(self, connection):
        """Corregir usuario debezium_auto con permisos apropiados"""
        try:
            logger.info("👥 Corrigiendo usuario debezium_auto...")
            
            username = self.debezium_user['username']
            password = self.debezium_user['password'] 
            host = self.debezium_user['host']
            database = self.mysql_config['database']
            
            with connection.cursor() as cursor:
                # Verificar si el usuario existe
                user_exists = self.check_user_exists(connection, username, host)
                
                if user_exists:
                    logger.info(f"ℹ️  Usuario {username}@{host} ya existe - actualizando...")
                    # Actualizar password
                    cursor.execute(f"ALTER USER '{username}'@'{host}' IDENTIFIED BY %s", (password,))
                    logger.info(f"🔄 Password actualizado para {username}@{host}")
                else:
                    # Crear nuevo usuario
                    cursor.execute(f"CREATE USER '{username}'@'{host}' IDENTIFIED BY %s", (password,))
                    logger.info(f"👤 Usuario creado: {username}@{host}")
                
                # Revocar todos los permisos esistentes primero (opcional)
                try:
                    cursor.execute(f"REVOKE ALL PRIVILEGES ON *.* FROM '{username}'@'{host}'")
                    logger.info("🗑️ Permisos anteriores revocados")
                except:
                    pass  # Es normal que falle si no hay permisos previos
                
                # Otorgar permisos específicos necesarios para replicación
                global_privileges = ['RELOAD', 'REPLICATION SLAVE', 'REPLICATION CLIENT', 'SHOW DATABASES']
                
                for privilege in global_privileges:
                    cursor.execute(f"GRANT {privilege} ON *.* TO '{username}'@'{host}'")
                    logger.info(f"🔐 Permiso global otorgado: {privilege}")
                
                # Otorgar permisos SELECT en la base de datos específica
                cursor.execute(f"GRANT SELECT ON `{database}`.* TO '{username}'@'{host}'")
                logger.info(f"🔐 Permiso SELECT otorgado en {database}.*")
                
                # Aplicar cambios
                cursor.execute("FLUSH PRIVILEGES")
                connection.commit()
                
                logger.info(f"✅ Usuario {username} configurado correctamente")
                self.fixed_issues.append("Usuario debezium_auto corregido")
                return True
                
        except Exception as e:
            logger.error(f"❌ Error corrigiendo usuario: {e}")
            self.errors.append(f"Error corrigiendo usuario: {e}")
            connection.rollback()
            return False
    
    def verify_user_permissions(self, connection):
        """Verificar que los permisos estén correctamente configurados"""
        try:
            logger.info("🔍 Verificando permisos de debezium_auto...")
            
            username = self.debezium_user['username']
            host = self.debezium_user['host']
            
            with connection.cursor() as cursor:
                cursor.execute(f"SHOW GRANTS FOR '{username}'@'{host}'")
                grants = [row[0] for row in cursor.fetchall()]
                
                logger.info(f"📋 Permisos actuales para {username}@{host}:")
                for grant in grants:
                    logger.info(f"   🔐 {grant}")
                
                # Verificar permisos específicos
                required_permissions = [
                    'RELOAD', 'REPLICATION SLAVE', 'REPLICATION CLIENT', 
                    'SHOW DATABASES', 'SELECT'
                ]
                
                found_permissions = []
                grants_text = ' '.join(grants)
                
                for perm in required_permissions:
                    if perm in grants_text:
                        found_permissions.append(perm)
                
                logger.info(f"📊 Permisos encontrados: {found_permissions}")
                
                if len(found_permissions) >= 4:  # Al menos los permisos principales
                    logger.info("✅ Permisos suficientes configurados")
                    self.fixed_issues.append("Permisos verificados correctamente")
                    return True
                else:
                    logger.warning(f"⚠️ Solo {len(found_permissions)} de {len(required_permissions)} permisos encontrados")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ Error verificando permisos: {e}")
            self.errors.append(f"Error verificando permisos: {e}")
            return False
    
    def test_connection(self):
        """Probar conexión con el usuario debezium_auto"""
        try:
            logger.info("🧪 Probando conexión con debezium_auto...")
            
            # Crear conexión con el usuario debezium_auto
            test_connection = pymysql.connect(
                host=self.mysql_config['host'],
                port=self.mysql_config['port'],
                user=self.debezium_user['username'],
                password=self.debezium_user['password'],
                database=self.mysql_config['database'],
                charset=self.mysql_config['charset']
            )
            
            with test_connection.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) as tabla_count FROM information_schema.tables WHERE table_schema=%s", 
                             (self.mysql_config['database'],))
                result = cursor.fetchone()
                table_count = result[0]
                
                logger.info(f"✅ Conexión debezium_auto exitosa - {table_count} tablas accesibles")
                self.fixed_issues.append("Conectividad verificada")
                
            test_connection.close()
            return True
                
        except Exception as e:
            logger.error(f"❌ Error probando conexión: {e}")
            self.errors.append(f"Error probando conexión: {e}")
            return False
    
    def run_complete_fix(self):
        """Ejecutar reparación completa de MySQL"""
        logger.info("🚀 === INICIANDO REPARACIÓN AUTOMÁTICA MYSQL ===")
        start_time = datetime.now()
        
        steps_completed = 0
        total_steps = 4
        
        connection = None
        
        try:
            # 1. Conectar a MySQL
            connection = self.connect_mysql()
            if connection:
                steps_completed += 1
                logger.info(f"📊 Progreso: {steps_completed}/{total_steps}")
            else:
                raise Exception("No se pudo conectar a MySQL")
            
            # 2. Corregir usuario debezium_auto
            if self.fix_debezium_user(connection):
                steps_completed += 1
                logger.info(f"📊 Progreso: {steps_completed}/{total_steps}")
            
            # 3. Verificar permisos
            if self.verify_user_permissions(connection):
                steps_completed += 1
                logger.info(f"📊 Progreso: {steps_completed}/{total_steps}")
            
            # 4. Probar conexión
            if self.test_connection():
                steps_completed += 1
                logger.info(f"📊 Progreso: {steps_completed}/{total_steps}")
            
            # Resultado final
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            if steps_completed == total_steps:
                logger.info("🎉 === REPARACIÓN MYSQL COMPLETADA ===")
                logger.info(f"✅ Todos los pasos completados ({steps_completed}/{total_steps})")
                logger.info(f"⏰ Duración: {duration:.1f} segundos")
                logger.info(f"🔧 Problemas corregidos: {len(self.fixed_issues)}")
                for issue in self.fixed_issues:
                    logger.info(f"   ✅ {issue}")
                return True
            else:
                logger.warning("⚠️ === REPARACIÓN PARCIAL ===")
                logger.warning(f"📊 Completado: {steps_completed}/{total_steps}")
                logger.warning(f"❌ Errores: {len(self.errors)}")
                for error in self.errors:
                    logger.error(f"   ❌ {error}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error fatal en reparación: {e}")
            return False
        finally:
            if connection:
                connection.close()
                logger.info("🔌 Conexión MySQL cerrada")

def main():
    """Función principal"""
    print("\n🔧 REPARACIÓN AUTOMÁTICA MYSQL")
    print("=" * 50)
    
    fixer = MySQLConfigurationFixer()
    success = fixer.run_complete_fix()
    
    if success:
        print("\n✅ Reparación MySQL completada exitosamente")
        print("🔄 Usuario debezium_auto listo para usar")
        sys.exit(0)
    else:
        print("\n❌ Reparación MySQL falló")
        print("📋 Revisa los logs para más detalles")
        sys.exit(1)

if __name__ == "__main__":
    main()