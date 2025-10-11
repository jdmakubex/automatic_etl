#!/usr/bin/env python3
"""
🛡️ PRE-VALIDADOR PARA AUTONOMÍA COMPLETA DEL PIPELINE ETL
==========================================================

Script que valida todas las condiciones previas necesarias para que
el pipeline ETL funcione completamente sin asistencia humana.

Ejecutar ANTES del pipeline principal para detectar problemas temprano.
"""

import os
import sys
import json
import logging
import subprocess
import stat
from pathlib import Path
from typing import Dict, List, Tuple
from datetime import datetime

def load_env_file(env_path='.env'):
    """Cargar variables de entorno desde archivo .env"""
    if not os.path.exists(env_path):
        return
    
    with open(env_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                # Remover comillas si existen
                if value.startswith('"') and value.endswith('"'):
                    value = value[1:-1]
                elif value.startswith("'") and value.endswith("'"):
                    value = value[1:-1]
                os.environ[key] = value

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/pre_validation.log', mode='w')
    ]
)
logger = logging.getLogger(__name__)

class PreValidationError(Exception):
    """Error que impide la ejecución autónoma del pipeline"""
    pass

class PreValidator:
    """Validador completo de condiciones previas para autonomía"""
    
    def __init__(self):
        """Inicializar el pre-validador"""
        # Cargar variables de entorno desde .env
        load_env_file()
        
        self.validation_results = {
            'timestamp': datetime.now().isoformat(),
            'validations': {},
            'summary': {
                'total': 0,
                'passed': 0,
                'failed': 0,
                'errors': 0,
                'warnings': 0
            },
            'critical_issues': [],
            'recommendations': []
        }
        
    def validate_directory_permissions(self) -> bool:
        """Validar permisos de escritura en directorios críticos"""
        logger.info("🔒 Validando permisos de directorios...")
        
        critical_dirs = [
            "logs",
            "generated", 
            "/tmp/logs",
            "/app/logs" if os.path.exists("/app") else None
        ]
        
        success = True
        for dir_path in filter(None, critical_dirs):
            try:
                # Crear directorio si no existe
                os.makedirs(dir_path, exist_ok=True)
                
                # Probar escritura
                test_file = os.path.join(dir_path, '.write_test')
                with open(test_file, 'w') as f:
                    f.write("test")
                os.remove(test_file)
                
                logger.info(f"✅ {dir_path}: Permisos OK")
                
            except PermissionError as e:
                logger.error(f"❌ {dir_path}: Sin permisos de escritura - {e}")
                self.validation_results["critical_issues"].append(f"Sin permisos de escritura en {dir_path}")
                success = False
            except Exception as e:
                logger.warning(f"⚠️  {dir_path}: Problema de acceso - {e}")
                self.validation_results["recommendations"].append(f"Problema de acceso en {dir_path}: {e}")
        
        self.validation_results["validations"]["directory_permissions"] = success
        return success
    
    def validate_environment_variables(self) -> bool:
        """Validar todas las variables críticas del .env"""
        logger.info("🌍 Validando variables de entorno...")
        
        # Variables críticas con sus valores por defecto aceptables
        critical_vars = {
            "DB_CONNECTIONS": None,  # Obligatoria, sin default
            "CLICKHOUSE_HTTP_HOST": "clickhouse",
            "CLICKHOUSE_HTTP_PORT": "8123", 
            "CLICKHOUSE_NATIVE_PORT": "9000",
            "CLICKHOUSE_USER": "superset",
            "CLICKHOUSE_PASSWORD": "Sup3rS3cret!",
            "CLICKHOUSE_DATABASE": "fgeo_analytics",
            "CH_USER": "etl",
            "CH_PASSWORD": "Et1Ingest!",
            "SUPERSET_URL": "http://superset:8088",
            "SUPERSET_ADMIN": "admin",
            "SUPERSET_PASSWORD": "Admin123!",
            "CONNECT_URL": "http://connect:8083"
        }
        
        success = True
        missing_vars = []
        
        for var_name, default_val in critical_vars.items():
            value = os.getenv(var_name, default_val)
            
            if value is None:
                logger.error(f"❌ Variable crítica faltante: {var_name}")
                missing_vars.append(var_name)
                success = False
            else:
                logger.info(f"✅ {var_name}: {'[DEFINIDA]' if value != default_val else '[DEFAULT]'}")
        
        # Validar formato de DB_CONNECTIONS
        if os.getenv("DB_CONNECTIONS"):
            try:
                connections = json.loads(os.getenv("DB_CONNECTIONS"))
                if isinstance(connections, dict):
                    connections = [connections]
                
                logger.info(f"✅ DB_CONNECTIONS: {len(connections)} conexión(es) parseadas")
                
                # Validar estructura de cada conexión
                for i, conn in enumerate(connections):
                    required_fields = ["host", "user", "pass", "db"]
                    missing_fields = [f for f in required_fields if f not in conn]
                    if missing_fields:
                        logger.error(f"❌ Conexión #{i}: Faltan campos {missing_fields}")
                        success = False
                    else:
                        logger.info(f"✅ Conexión #{i} ({conn.get('name', 'unnamed')}): Estructura OK")
                        
            except json.JSONDecodeError as e:
                logger.error(f"❌ DB_CONNECTIONS: JSON inválido - {e}")
                success = False
        
        if missing_vars:
            self.validation_results["critical_issues"].extend([f"Variable faltante: {var}" for var in missing_vars])
            self.validation_results["recommendations"].append(
                "Revisar archivo .env y asegurar que todas las variables críticas estén definidas"
            )
        
        self.validation_results["validations"]["environment_variables"] = success
        return success
    
    def validate_docker_services(self) -> bool:
        """Validar que Docker y docker-compose estén disponibles"""
        logger.info("🐳 Validando servicios Docker...")
        
        success = True
        
        # Verificar Docker
        try:
            result = subprocess.run(['docker', '--version'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                logger.info(f"✅ Docker disponible: {result.stdout.strip()}")
            else:
                logger.error("❌ Docker no responde correctamente")
                success = False
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            logger.error(f"❌ Docker no encontrado: {e}")
            success = False
        
        # Verificar docker-compose
        try:
            result = subprocess.run(['docker-compose', '--version'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                logger.info(f"✅ Docker Compose disponible: {result.stdout.strip()}")
            else:
                logger.error("❌ Docker Compose no responde correctamente")
                success = False
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            logger.error(f"❌ Docker Compose no encontrado: {e}")
            success = False
        
        # Verificar archivo docker-compose.yml
        if os.path.exists("docker-compose.yml"):
            logger.info("✅ docker-compose.yml encontrado")
        else:
            logger.error("❌ docker-compose.yml no encontrado")
            success = False
        
        if not success:
            self.validation_results["critical_issues"].append("Docker o Docker Compose no disponibles")
            self.validation_results["recommendations"].append(
                "Instalar Docker y Docker Compose antes de ejecutar el pipeline"
            )
        
        self.validation_results["validations"]["docker_services"] = success
        return success
    
    def validate_python_dependencies(self) -> bool:
        """Validar dependencias Python críticas"""
        logger.info("🐍 Validando dependencias Python...")
        
        critical_packages = [
            "json",  # Estándar
            "os",    # Estándar
            "sys",   # Estándar
            "subprocess",  # Estándar
            "logging",     # Estándar
            "datetime",    # Estándar
        ]
        
        # Paquetes opcionales que mejoran la funcionalidad
        optional_packages = [
            "clickhouse_driver",
            "requests", 
            "pymysql",
            "pandas",
            "sqlalchemy"
        ]
        
        success = True
        missing_critical = []
        missing_optional = []
        
        # Validar paquetes críticos
        for package in critical_packages:
            try:
                __import__(package)
                logger.info(f"✅ {package}: Disponible")
            except ImportError:
                logger.error(f"❌ {package}: FALTANTE (CRÍTICO)")
                missing_critical.append(package)
                success = False
        
        # Validar paquetes opcionales
        for package in optional_packages:
            try:
                __import__(package.replace("-", "_"))
                logger.info(f"✅ {package}: Disponible")
            except ImportError:
                logger.warning(f"⚠️  {package}: No disponible (opcional)")
                missing_optional.append(package)
        
        if missing_critical:
            self.validation_results["critical_issues"].extend([f"Paquete crítico faltante: {pkg}" for pkg in missing_critical])
        
        if missing_optional:
            self.validation_results["warnings"].extend([f"Paquete opcional faltante: {pkg}" for pkg in missing_optional])
            self.validation_results["recommendations"].append(
                f"Instalar paquetes opcionales: pip install {' '.join(missing_optional)}"
            )
        
        self.validation_results["validations"]["python_dependencies"] = success
        return success
    
    def validate_file_structure(self) -> bool:
        """Validar estructura de archivos críticos"""
        logger.info("📁 Validando estructura de archivos...")
        
        critical_files = [
            "docker-compose.yml",
            ".env",
            "tools/master_etl_agent.py",
            "tools/ingest_runner.py",
            "tools/multi_database_auditor.py",
        ]
        
        success = True
        missing_files = []
        
        for file_path in critical_files:
            if os.path.exists(file_path):
                # Verificar que sea legible
                try:
                    with open(file_path, 'r') as f:
                        # Solo leer primera línea para verificar acceso
                        f.readline()
                    logger.info(f"✅ {file_path}: Existe y es legible")
                except Exception as e:
                    logger.error(f"❌ {file_path}: No legible - {e}")
                    success = False
            else:
                logger.error(f"❌ {file_path}: No encontrado")
                missing_files.append(file_path)
                success = False
        
        if missing_files:
            self.validation_results["critical_issues"].extend([f"Archivo faltante: {f}" for f in missing_files])
            self.validation_results["recommendations"].append(
                "Verificar que todos los archivos del pipeline estén presentes"
            )
        
        self.validation_results["validations"]["file_structure"] = success
        return success
    
    def validate_network_connectivity(self) -> bool:
        """Validar conectividad básica (opcional)"""
        logger.info("🌐 Validando conectividad de red...")
        
        # Esta validación es menos crítica, ya que puede fallar por firewall
        # pero el pipeline interno puede funcionar
        success = True
        
        # Verificar si hay conexiones MySQL configuradas
        db_connections = os.getenv("DB_CONNECTIONS")
        if db_connections:
            try:
                connections = json.loads(db_connections)
                if isinstance(connections, dict):
                    connections = [connections]
                
                for conn in connections:
                    host = conn.get("host", "unknown")
                    port = conn.get("port", 3306)
                    
                    # Intentar conectividad básica (sin autenticación)
                    try:
                        import socket
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.settimeout(5)
                        result = sock.connect_ex((host, port))
                        sock.close()
                        
                        if result == 0:
                            logger.info(f"✅ MySQL {host}:{port}: Alcanzable")
                        else:
                            logger.warning(f"⚠️  MySQL {host}:{port}: No alcanzable (puede funcionar desde contenedor)")
                    except Exception as e:
                        logger.warning(f"⚠️  MySQL {host}:{port}: Error de conectividad - {e}")
                        
            except json.JSONDecodeError:
                logger.warning("⚠️  No se pudo parsear DB_CONNECTIONS para validar conectividad")
        
        # La conectividad no es crítica para el éxito general
        self.validation_results["validations"]["network_connectivity"] = True
        return True
    
    def run_complete_validation(self) -> bool:
        """Ejecutar validación completa"""
        logger.info("🚀 === INICIANDO PRE-VALIDACIÓN PARA AUTONOMÍA COMPLETA ===")
        
        validations = [
            ("Permisos de Directorios", self.validate_directory_permissions),
            ("Variables de Entorno", self.validate_environment_variables), 
            ("Servicios Docker", self.validate_docker_services),
            ("Dependencias Python", self.validate_python_dependencies),
            ("Estructura de Archivos", self.validate_file_structure),
            ("Conectividad de Red", self.validate_network_connectivity),
        ]
        
        all_success = True
        
        for name, validation_func in validations:
            logger.info(f"\n🔍 === {name.upper()} ===")
            try:
                success = validation_func()
                if not success:
                    all_success = False
                    logger.error(f"❌ {name}: FALLÓ")
                else:
                    logger.info(f"✅ {name}: EXITOSO")
            except Exception as e:
                logger.error(f"💥 {name}: ERROR CRÍTICO - {e}")
                self.validation_results["critical_issues"].append(f"Error en {name}: {e}")
                all_success = False
        
        self.validation_results["overall_success"] = all_success
        
        # Guardar reporte detallado
        self._save_validation_report()
        
        # Mostrar resumen final
        self._display_final_summary()
        
        return all_success
    
    def _save_validation_report(self):
        """Guardar reporte detallado de validación"""
        try:
            os.makedirs("logs", exist_ok=True)
            report_file = "logs/pre_validation_report.json"
            
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(self.validation_results, f, indent=2, ensure_ascii=False)
            
            logger.info(f"📋 Reporte guardado: {report_file}")
            
        except Exception as e:
            logger.warning(f"⚠️  No se pudo guardar reporte: {e}")
    
    def _display_final_summary(self):
        """Mostrar resumen final de validación"""
        logger.info("\n" + "="*80)
        logger.info("📊 RESUMEN FINAL DE PRE-VALIDACIÓN")
        logger.info("="*80)
        
        total_validations = len(self.validation_results["validations"])
        successful_validations = sum(1 for v in self.validation_results["validations"].values() if v)
        
        logger.info(f"Total de validaciones: {total_validations}")
        logger.info(f"Exitosas: {successful_validations}")
        logger.info(f"Fallidas: {total_validations - successful_validations}")
        logger.info(f"Errores: {len(self.validation_results['critical_issues'])}")
        logger.info(f"Advertencias: {len(self.validation_results['recommendations'])}")
        
        if self.validation_results["overall_success"]:
            logger.info("\n🎉 ✅ SISTEMA LISTO PARA EJECUCIÓN AUTÓNOMA")
            logger.info("Todos los componentes críticos están validados.")
        else:
            logger.error("\n🚨 ❌ SISTEMA NO LISTO PARA EJECUCIÓN AUTÓNOMA")
            logger.error("Se encontraron problemas críticos que deben corregirse:")
            
            for error in self.validation_results["critical_issues"]:
                logger.error(f"   • {error}")
        
        if self.validation_results["recommendations"]:
            logger.info("\n💡 RECOMENDACIONES:")
            for rec in self.validation_results["recommendations"]:
                logger.info(f"   • {rec}")
        
        logger.info("="*80)

def main():
    """Función principal"""
    try:
        validator = PreValidator()
        success = validator.run_complete_validation()
        
        return 0 if success else 1
        
    except Exception as e:
        logger.error(f"💥 Error crítico en pre-validación: {e}")
        return 2

if __name__ == "__main__":
    sys.exit(main())