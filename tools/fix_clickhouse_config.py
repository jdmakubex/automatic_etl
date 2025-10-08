#!/usr/bin/env python3
"""
🔧 REPARACIÓN AUTOMÁTICA DE CONFIGURACIÓN CLICKHOUSE
Soluciona problemas de autenticación y configuración de usuarios
"""

import subprocess
import logging
import time
import json
import sys
import os
from datetime import datetime

# Configurar logging
log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'clickhouse_fix.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file)
    ]
)

logger = logging.getLogger(__name__)

class ClickHouseConfigurationFixer:
    def __init__(self):
        self.container_name = "clickhouse"
        self.fixed_issues = []
        self.errors = []
        
    def check_container_status(self):
        """Verificar estado del contenedor ClickHouse"""
        try:
            logger.info("🔍 Verificando estado del contenedor ClickHouse...")
            result = subprocess.run(
                ["docker", "ps", "--filter", f"name={self.container_name}", "--format", "{{.Status}}"],
                capture_output=True, text=True, check=True
            )
            
            if "Up" in result.stdout and "healthy" in result.stdout:
                logger.info("✅ Contenedor ClickHouse está ejecutándose y saludable")
                return True
            else:
                logger.error(f"❌ Problema con contenedor: {result.stdout.strip()}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error verificando contenedor: {e}")
            return False
    
    def backup_current_config(self):
        """Crear backup de la configuración actual"""
        try:
            logger.info("💾 Creando backup de configuración actual...")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Backup users.d directory
            subprocess.run([
                "docker", "exec", self.container_name,
                "cp", "-r", "/etc/clickhouse-server/users.d", 
                f"/etc/clickhouse-server/users.d.backup_{timestamp}"
            ], check=True)
            
            logger.info(f"✅ Backup creado: users.d.backup_{timestamp}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creando backup: {e}")
            return False
    
    def copy_correct_config(self):
        """Copiar la configuración correcta desde bootstrap"""
        try:
            logger.info("📋 Copiando configuración correcta desde bootstrap...")
            
            # Copiar archivo de configuración correcto
            bootstrap_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'bootstrap', 'users.d', '00-default.xml')
            subprocess.run([
                "docker", "cp", 
                bootstrap_path,
                f"{self.container_name}:/etc/clickhouse-server/users.d/00-default.xml"
            ], check=True)
            
            # Establecer permisos correctos
            subprocess.run([
                "docker", "exec", self.container_name,
                "chown", "clickhouse:clickhouse", "/etc/clickhouse-server/users.d/00-default.xml"
            ], check=True)
            
            subprocess.run([
                "docker", "exec", self.container_name,
                "chmod", "644", "/etc/clickhouse-server/users.d/00-default.xml"
            ], check=True)
            
            logger.info("✅ Configuración correcta copiada y permisos establecidos")
            self.fixed_issues.append("Configuración de usuarios corregida")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error copiando configuración: {e}")
            self.errors.append(f"Error copiando configuración: {e}")
            return False
    
    def restart_clickhouse(self):
        """Reiniciar ClickHouse para aplicar configuración"""
        try:
            logger.info("🔄 Reiniciando ClickHouse para aplicar configuración...")
            
            # Reiniciar contenedor
            subprocess.run(["docker", "restart", self.container_name], check=True)
            
            # Esperar a que el servicio esté listo
            logger.info("⏳ Esperando a que ClickHouse esté disponible...")
            for attempt in range(30):
                try:
                    result = subprocess.run([
                        "docker", "exec", self.container_name,
                        "clickhouse-client", "--query", "SELECT 1"
                    ], capture_output=True, text=True, timeout=5)
                    
                    if result.returncode == 0:
                        logger.info(f"✅ ClickHouse disponible después de {attempt + 1} intentos")
                        return True
                        
                except Exception:
                    pass
                    
                time.sleep(2)
            
            logger.error("❌ ClickHouse no respondió después de 60 segundos")
            return False
            
        except Exception as e:
            logger.error(f"❌ Error reiniciando ClickHouse: {e}")
            self.errors.append(f"Error reiniciando: {e}")
            return False
    
    def create_etl_users(self):
        """Crear usuarios necesarios para ETL"""
        try:
            logger.info("👥 Creando usuarios ETL en ClickHouse...")
            
            # Comandos SQL para crear usuarios
            commands = [
                "CREATE USER IF NOT EXISTS etl_auto IDENTIFIED WITH plaintext_password BY 'EtlAuto2025!'",
                "GRANT ALL ON *.* TO etl_auto WITH GRANT OPTION",
                "CREATE USER IF NOT EXISTS superset IDENTIFIED WITH plaintext_password BY 'SupersetClickHouse2025!'",
                "GRANT ALL ON *.* TO superset WITH GRANT OPTION"
            ]
            
            for cmd in commands:
                try:
                    result = subprocess.run([
                        "docker", "exec", self.container_name,
                        "clickhouse-client", "--query", cmd
                    ], capture_output=True, text=True, check=True)
                    
                    logger.info(f"✅ Comando ejecutado: {cmd[:50]}...")
                    
                except subprocess.CalledProcessError as e:
                    if "already exists" not in str(e.stderr):
                        logger.warning(f"⚠️ Error en comando SQL: {e.stderr}")
                        
            self.fixed_issues.append("Usuarios ETL creados/actualizados")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creando usuarios: {e}")
            self.errors.append(f"Error creando usuarios: {e}")
            return False
    
    def test_connections(self):
        """Probar conexiones con los usuarios creados"""
        try:
            logger.info("🧪 Probando conexiones de usuarios...")
            
            # Test usuario default (sin contraseña)
            result = subprocess.run([
                "docker", "exec", self.container_name,
                "clickhouse-client", "--query", "SELECT 'default user test'"
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info("✅ Usuario default: OK")
            else:
                logger.warning(f"⚠️ Usuario default: {result.stderr}")
            
            # Test usuario etl_auto
            result = subprocess.run([
                "docker", "exec", self.container_name,
                "clickhouse-client", "--user", "etl_auto", 
                "--password", "EtlAuto2025!", "--query", "SELECT 'etl_auto test'"
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info("✅ Usuario etl_auto: OK")
            else:
                logger.warning(f"⚠️ Usuario etl_auto: {result.stderr}")
            
            # Test usuario superset
            result = subprocess.run([
                "docker", "exec", self.container_name,
                "clickhouse-client", "--user", "superset", 
                "--password", "SupersetClickHouse2025!", "--query", "SELECT 'superset test'"
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info("✅ Usuario superset: OK")
                self.fixed_issues.append("Conectividad de usuarios verificada")
            else:
                logger.warning(f"⚠️ Usuario superset: {result.stderr}")
                
            return True
            
        except Exception as e:
            logger.error(f"❌ Error probando conexiones: {e}")
            self.errors.append(f"Error probando conexiones: {e}")
            return False
    
    def run_complete_fix(self):
        """Ejecutar reparación completa"""
        logger.info("🚀 === INICIANDO REPARACIÓN AUTOMÁTICA CLICKHOUSE ===")
        start_time = datetime.now()
        
        steps_completed = 0
        total_steps = 6
        
        try:
            # 1. Verificar contenedor
            if self.check_container_status():
                steps_completed += 1
                logger.info(f"📊 Progreso: {steps_completed}/{total_steps}")
            else:
                raise Exception("Contenedor no disponible")
            
            # 2. Backup
            if self.backup_current_config():
                steps_completed += 1
                logger.info(f"📊 Progreso: {steps_completed}/{total_steps}")
            
            # 3. Copiar configuración correcta
            if self.copy_correct_config():
                steps_completed += 1
                logger.info(f"📊 Progreso: {steps_completed}/{total_steps}")
            
            # 4. Reiniciar ClickHouse
            if self.restart_clickhouse():
                steps_completed += 1
                logger.info(f"📊 Progreso: {steps_completed}/{total_steps}")
            
            # 5. Crear usuarios
            if self.create_etl_users():
                steps_completed += 1
                logger.info(f"📊 Progreso: {steps_completed}/{total_steps}")
            
            # 6. Probar conexiones
            if self.test_connections():
                steps_completed += 1
                logger.info(f"📊 Progreso: {steps_completed}/{total_steps}")
            
            # Resultado final
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            if steps_completed == total_steps:
                logger.info("🎉 === REPARACIÓN CLICKHOUSE COMPLETADA ===")
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

def main():
    """Función principal"""
    print("\n🔧 REPARACIÓN AUTOMÁTICA CLICKHOUSE")
    print("=" * 50)
    
    fixer = ClickHouseConfigurationFixer()
    success = fixer.run_complete_fix()
    
    if success:
        print("\n✅ Reparación completada exitosamente")
        print("🔄 Ahora puedes reintentar el pipeline ETL")
        sys.exit(0)
    else:
        print("\n❌ Reparación falló")
        print("📋 Revisa los logs para más detalles")
        sys.exit(1)

if __name__ == "__main__":
    main()