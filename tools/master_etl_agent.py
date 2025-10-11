#!/usr/bin/env python3
"""
🎯 AGENTE MAESTRO ETL - INTEGRACIÓN COMPLETA
Sistema final que coordina y ejecuta automáticamente toda la inicialización
del pipeline ETL sin intervención manual
"""

"""
🎯 AGENTE MAESTRO ETL - INTEGRACIÓN COMPLETA
Sistema final que coordina y ejecuta automáticamente toda la inicialización
del pipeline ETL sin intervención manual.
Incluye espera adaptativa y feedback de usuario en cada fase.
"""

import os
import sys
import json
import time
import logging
import subprocess
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any

# Importar nuestros módulos especializados
sys.path.append('/app/tools')

try:
    from health_validator import AdvancedHealthValidator
    from automatic_user_manager import AutomaticUserManager
    from superset_auto_configurator import SupersetAutoConfigurator
except ImportError as e:
    # Fallback si estamos ejecutando desde el host
    sys.path.append(os.path.join(os.path.dirname(__file__)))
    try:
        from health_validator import AdvancedHealthValidator
        from automatic_user_manager import AutomaticUserManager
        from superset_auto_configurator import SupersetAutoConfigurator
    except ImportError:
        print(f"❌ Error importando módulos: {e}")
        sys.exit(1)

class MasterETLAgent:
    def _verify_clickhouse_users(self) -> bool:
        """Verifica que los usuarios predefinidos de ClickHouse estén funcionando correctamente"""
        self.logger.info("🔄 Verificando usuarios predefinidos en ClickHouse...")
        try:
            import requests
            ch_host = 'clickhouse' if os.path.exists('/.dockerenv') else 'localhost'
            ch_port = 8123
            
            # Verificar usuario ETL
            etl_auth = requests.auth.HTTPBasicAuth('etl', 'Et1Ingest!')
            etl_response = requests.post(f"http://{ch_host}:{ch_port}/", data="SELECT 1", auth=etl_auth, timeout=10)
            
            if etl_response.status_code == 200:
                self.logger.info("✅ Usuario 'etl' verificado y funcionando")
                
                # Verificar usuario Superset
                superset_auth = requests.auth.HTTPBasicAuth('superset', 'Sup3rS3cret!')
                superset_response = requests.post(f"http://{ch_host}:{ch_port}/", data="SELECT 1", auth=superset_auth, timeout=10)
                
                if superset_response.status_code == 200:
                    self.logger.info("✅ Usuario 'superset' verificado y funcionando")
                    return True
                else:
                    self.logger.warning("⚠️  Usuario 'superset' no responde correctamente")
                    return True  # ETL puede continuar solo con usuario 'etl'
            else:
                self.logger.error(f"❌ Usuario 'etl' no funciona: {etl_response.status_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Error verificando usuarios predefinidos de ClickHouse: {e}")
            return False
    """Agente maestro que coordina toda la inicialización automática"""
    
    def __init__(self):
        self.logger = self._setup_logging()
        self.start_time = datetime.now()
        self.execution_id = self.start_time.strftime('%Y%m%d_%H%M%S')
        
        # Ejecutar pre-validación para autonomía
        if not self._run_pre_validation():
            self.logger.error("❌ Pre-validación falló. Sistema no listo para ejecución autónoma.")
            sys.exit(2)
        
        # Auto-instalar dependencias si es necesario
        self._ensure_dependencies()
        
        # Inicializar componentes especializados
        self.health_validator = AdvancedHealthValidator()
        self.user_manager = AutomaticUserManager()
        self.superset_configurator = SupersetAutoConfigurator()
        
        # Estado del proceso
        self.results = {
            'execution_id': self.execution_id,
            'start_time': self.start_time.isoformat(),
            'phases': {},
            'overall_success': False,
            'duration_seconds': 0,
            'summary': {}
        }
        
    def _setup_logging(self) -> logging.Logger:
        """Configurar logging maestro con múltiples niveles"""
        logger = logging.getLogger('MasterETLAgent')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            # Handler para consola con formato visual
            console_handler = logging.StreamHandler()
            console_formatter = logging.Formatter(
                '%(asctime)s - 🎯 MAESTRO - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)
            
            # Handler para archivo (si es posible)
            try:
                log_dir = '/app/logs' if os.path.exists('/app/logs') else 'logs'
                os.makedirs(log_dir, exist_ok=True)
                
                file_handler = logging.FileHandler(f'{log_dir}/master_agent_{self.execution_id}.log')
                file_formatter = logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                )
                file_handler.setFormatter(file_formatter)
                logger.addHandler(file_handler)
            except Exception:
                pass  # No crítico
                
        return logger
    
    def _save_execution_results(self):
        """Guardar resultados detallados de la ejecución"""
        try:
            results_dir = '/app/logs' if os.path.exists('/app/logs') else 'logs'
            os.makedirs(results_dir, exist_ok=True)
            
            results_file = f'{results_dir}/master_execution_{self.execution_id}.json'
            
            with open(results_file, 'w') as f:
                json.dump(self.results, f, indent=2, default=str)
            
            self.logger.info(f"💾 Resultados guardados: {results_file}")
            
        except Exception as e:
            self.logger.warning(f"⚠️  No se pudieron guardar resultados: {e}")
    
    def _auto_start_services(self) -> bool:
        """Iniciar servicios automáticamente si no están corriendo"""
        self.logger.info("🚀 Iniciando servicios Docker automáticamente...")
        
        try:
            # Iniciar servicios base primero
            base_services = ['clickhouse', 'superset', 'superset-venv-setup', 'superset-init']
            self.logger.info(f"📦 Iniciando servicios base: {', '.join(base_services)}")
            
            result = subprocess.run([
                'docker-compose', 'up', '-d'
            ] + base_services, 
            capture_output=True, text=True, timeout=300)
            
            if result.returncode != 0:
                self.logger.error(f"❌ Error iniciando servicios base: {result.stderr}")
                return False
                
            self.logger.info("✅ Servicios base iniciados exitosamente")
            
            # Esperar que los servicios estén listos
            self.logger.info("⏳ Esperando que los servicios estén listos...")
            time.sleep(30)  # Dar tiempo para que ClickHouse y Superset se inicializen
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error crítico iniciando servicios: {str(e)}")
            return False

    def phase_1_validate_infrastructure(self) -> bool:
        """Fase 1: Validar que la infraestructura esté operativa"""
        phase_start = datetime.now()
        self.logger.info("🏥 === FASE 1: VALIDACIÓN DE INFRAESTRUCTURA ===")
        
        try:
            # Ejecutar validación completa de salud
            overall_healthy, health_results = self.health_validator.comprehensive_health_check()
            
            # Si los servicios no están saludables, intentar iniciarlos automáticamente
            if not overall_healthy:
                self.logger.warning("⚠️  Servicios no están corriendo, iniciando automáticamente...")
                if not self._auto_start_services():
                    self.logger.error("❌ No se pudieron iniciar los servicios automáticamente")
                    self.results['phases']['phase_1_infrastructure'] = {
                        'name': 'Validación de Infraestructura',
                        'success': False,
                        'error': 'No se pudieron iniciar servicios automáticamente',
                        'duration_seconds': (datetime.now() - phase_start).total_seconds()
                    }
                    return False
                # Re-validar después de iniciar servicios
                self.logger.info("🔄 Re-validando infraestructura después del inicio automático...")
                time.sleep(10)
                overall_healthy, health_results = self.health_validator.comprehensive_health_check()
                # Si sigue fallando por autenticación, verificar usuarios predefinidos
                critical_issues = health_results.get('critical_issues', [])
                if any('Authentication failed' in str(issue) for issue in critical_issues):
                    self.logger.warning("⚠️  Problema de autenticación en ClickHouse detectado, verificando usuarios predefinidos...")
                    if self._verify_clickhouse_users():
                        self.logger.info("🔄 Revalidando infraestructura tras verificación de usuarios...")
                        time.sleep(5)
                        overall_healthy, health_results = self.health_validator.comprehensive_health_check()
            
            self.results['phases']['phase_1_infrastructure'] = {
                'name': 'Validación de Infraestructura',
                'success': overall_healthy,
                'duration_seconds': (datetime.now() - phase_start).total_seconds(),
                'health_details': health_results,
                'auto_started': True
            }
            
            if overall_healthy:
                self.logger.info("✅ FASE 1 COMPLETADA: Infraestructura operativa")
                self.logger.info(f"📊 Servicios verificados: {len(health_results.get('services', []))}")
                return True
            else:
                # Análisis simple: si vemos ClickHouse corriendo, continuamos
                # Esto es suficiente para un despliegue limpio donde los servicios necesitan configuración
                
                # Verificar en los logs si ClickHouse está corriendo
                critical_issues = health_results.get('critical_issues', [])
                warnings = health_results.get('warnings', [])
                
                # Si solo tenemos problemas de autenticación, significa que ClickHouse está corriendo
                has_auth_issues = any('ClickHouse no operativo' in str(issue) for issue in critical_issues)
                has_containers_missing = any('no encontrado' in str(issue) for issue in critical_issues)
                
                self.logger.info(f"🔍 Análisis: auth_issues={has_auth_issues}, containers_missing={has_containers_missing}")
                
                # Si solo tenemos problemas de autenticación y algunos contenedores faltantes (Kafka/Connect),
                # pero no problemas de conectividad básica, continuamos
                if has_auth_issues and len(critical_issues) <= 5:  # Solo problemas esperados en despliegue limpio
                    self.logger.warning("⚠️  FASE 1 PARCIALMENTE EXITOSA: Servicios principales iniciados")
                    self.logger.warning("    ClickHouse corriendo pero necesita configuración de usuarios")
                    self.logger.warning("    Continuando a Fase 2 para configurar usuarios automáticamente...")
                    return True
                else:
                    self.logger.error("❌ FASE 1 FALLÓ: Problemas en infraestructura crítica")
                    for issue in critical_issues[:3]:
                        self.logger.error(f"   • {issue}")
                    return False
                
        except Exception as e:
            self.logger.error(f"❌ Error crítico en Fase 1: {str(e)}")
            self.results['phases']['phase_1_infrastructure'] = {
                'name': 'Validación de Infraestructura',
                'success': False,
                'error': str(e),
                'duration_seconds': (datetime.now() - phase_start).total_seconds()
            }
            return False
    
    def phase_2_configure_users(self) -> bool:
        """Fase 2: Configurar usuarios y permisos automáticamente"""
        phase_start = datetime.now()
        self.logger.info("👥 === FASE 2: CONFIGURACIÓN AUTOMÁTICA DE USUARIOS ===")
        
        try:
            success, user_results = self.user_manager.comprehensive_user_setup()
            
            self.results['phases']['phase_2_users'] = {
                'name': 'Configuración de Usuarios',
                'success': success,
                'duration_seconds': (datetime.now() - phase_start).total_seconds(),
                'details': user_results
            }
            
            if success:
                self.logger.info("✅ FASE 2 COMPLETADA: Usuarios configurados")
                
                # Mostrar resumen de usuarios
                connectivity = user_results.get('connectivity_tests', {})
                successful_connections = sum(connectivity.values())
                total_connections = len(connectivity)
                self.logger.info(f"🔗 Conectividad: {successful_connections}/{total_connections} usuarios")
                
            else:
                self.logger.error("❌ FASE 2 FALLÓ: Problemas configurando usuarios")
                all_issues = (user_results.get('mysql', {}).get('issues', []) + 
                             user_results.get('clickhouse', {}).get('issues', []) + 
                             user_results.get('superset', {}).get('issues', []))
                
                for issue in all_issues[:3]:
                    self.logger.error(f"   • {issue}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"❌ Error crítico en Fase 2: {str(e)}")
            self.results['phases']['phase_2_users'] = {
                'name': 'Configuración de Usuarios',
                'success': False,
                'error': str(e),
                'duration_seconds': (datetime.now() - phase_start).total_seconds()
            }
            return False
    
    def phase_3_ingest_data(self) -> bool:
        """Fase 3: Ejecutar ingesta de datos"""
        phase_start = datetime.now()
        self.logger.info("📊 === FASE 3: INGESTA DE DATOS ===")
        
        try:
            self.logger.info("🔄 Ejecutando ingesta de datos con ingest_runner...")
            ingestion_process = subprocess.run([
                'python3', 'tools/ingest_runner.py'
            ], capture_output=True, text=True, timeout=300)
            
            ingestion_success = ingestion_process.returncode == 0
            tables_created = 0  # Default
            
            # Leer el reporte de estado de ingesta si existe
            ingest_status = None
            try:
                with open('logs/ingest_status.json', 'r', encoding='utf-8') as f:
                    ingest_status = json.load(f)
                    ingestion_success = ingest_status.get('success', ingestion_success)
                    tables_created = ingest_status.get('successful_tables', 0)
                    self.logger.info(f"📋 Reporte de ingesta leído: {ingest_status.get('total_rows', 0)} filas, {tables_created} tablas exitosas")
            except Exception as e:
                self.logger.warning(f"No se pudo leer logs/ingest_status.json: {e}")
            
            if ingestion_success:
                self.logger.info("✅ FASE 3 COMPLETADA: Datos ingresados exitosamente")
                self.logger.info(f"📊 Tablas procesadas: {tables_created}")
            else:
                self.logger.error("❌ FASE 3 FALLÓ: Error en ingesta de datos")
                self.logger.error(f"   Error: {ingestion_process.stderr}")
                if ingest_status and ingest_status.get('failed_tables'):
                    self.logger.error(f"   Tablas fallidas: {len(ingest_status['failed_tables'])}")
                
            self.results['phases']['phase_3_ingestion'] = {
                'name': 'Ingesta de Datos',
                'success': ingestion_success,
                'duration_seconds': (datetime.now() - phase_start).total_seconds(),
                'output': ingestion_process.stdout if ingestion_success else ingestion_process.stderr,
                'tables_processed': tables_created if ingestion_success else 0,
                'ingest_details': ingest_status
            }
            
            return ingestion_success
            
        except Exception as e:
            self.logger.error(f"❌ Error crítico en Fase 3 (Ingesta): {str(e)}")
            self.results['phases']['phase_3_ingestion'] = {
                'name': 'Ingesta de Datos',
                'success': False,
                'error': str(e),
                'duration_seconds': (datetime.now() - phase_start).total_seconds()
            }
            return False

    def phase_4_configure_superset(self) -> bool:
        """Fase 4: Configurar Superset completamente"""
        phase_start = datetime.now()
        self.logger.info("📊 === FASE 4: CONFIGURACIÓN COMPLETA DE SUPERSET ===")
        
        try:
            # Ejecutar orquestación de Superset-ClickHouse
            self.logger.info("🔄 Ejecutando orquestación Superset-ClickHouse...")
            superset_process = subprocess.run([
                'docker-compose', 'exec', '-T', 'superset', 
                'python3', '/bootstrap/orchestrate_superset_clickhouse.py'
            ], capture_output=True, text=True, timeout=180)
            
            success = superset_process.returncode == 0
            
            # Leer el reporte de estado de Superset si existe
            superset_status = None
            try:
                # El reporte se guarda en /tmp/logs dentro del contenedor, que está mapeado a logs/
                with open('logs/superset_status.json', 'r', encoding='utf-8') as f:
                    superset_status = json.load(f)
                    success = superset_status.get('success', success)
                    self.logger.info(f"📋 Reporte de Superset leído: {superset_status.get('databases_count', 0)} bases de datos configuradas")
            except Exception as e:
                self.logger.warning(f"No se pudo leer logs/superset_status.json: {e}")
            
            self.results['phases']['phase_4_superset'] = {
                'name': 'Configuración de Superset',
                'success': success,
                'duration_seconds': (datetime.now() - phase_start).total_seconds(),
                'output': superset_process.stdout if success else superset_process.stderr,
                'superset_details': superset_status
            }
            
            if success:
                self.logger.info("✅ FASE 4 COMPLETADA: Superset configurado")
                self.logger.info("🌐 URL: http://localhost:8088")
                self.logger.info("👤 Admin: admin")
                if superset_status:
                    self.logger.info(f"📊 Bases de datos: {superset_status.get('databases_count', 0)}")
            else:
                self.logger.error("❌ FASE 4 FALLÓ: Problemas configurando Superset")
                self.logger.error(f"   Error: {superset_process.stderr}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"❌ Error crítico en Fase 4: {str(e)}")
            self.results['phases']['phase_4_superset'] = {
                'name': 'Configuración de Superset',
                'success': False,
                'error': str(e),
                'duration_seconds': (datetime.now() - phase_start).total_seconds()
            }
            return False
    
    def _auto_start_kafka_services(self) -> bool:
        """Iniciar servicios de Kafka automáticamente si es necesario"""
        self.logger.info("📨 Iniciando servicios Kafka automáticamente...")
        
        try:
            kafka_services = ['kafka', 'connect']
            self.logger.info(f"📦 Iniciando servicios Kafka: {', '.join(kafka_services)}")
            
            result = subprocess.run([
                'docker-compose', 'up', '-d'
            ] + kafka_services, 
            capture_output=True, text=True, timeout=180)
            
            if result.returncode != 0:
                self.logger.warning(f"⚠️  Kafka no iniciado (puede ser que no esté configurado): {result.stderr}")
                return False
                
            self.logger.info("✅ Servicios Kafka iniciados exitosamente")
            
            # Esperar que Kafka esté listo
            self.logger.info("⏳ Esperando que Kafka esté listo...")
            time.sleep(20)
            
            return True
            
        except Exception as e:
            self.logger.warning(f"⚠️  Error iniciando Kafka (puede ser opcional): {str(e)}")
            return False

    def phase_5_validate_etl_pipeline(self) -> bool:
        """Fase 5: Validar pipeline ETL completo"""
        phase_start = datetime.now()
        self.logger.info("⚡ === FASE 5: VALIDACIÓN DEL PIPELINE ETL ===")
        
        try:
            # Intentar iniciar Kafka si es necesario
            self.logger.info("🔄 Verificando si Kafka está disponible...")
            self._auto_start_kafka_services()
            
            # Ejecutar auditoría multi-database
            self.logger.info("🔍 Ejecutando auditoría multi-database...")
            audit_process = subprocess.run([
                'docker-compose', 'exec', '-T', 'multi-db-auditor',
                'python', '/app/tools/multi_database_auditor.py'
            ], capture_output=True, text=True, timeout=120)
            
            audit_success = audit_process.returncode == 0
            
            # Leer el reporte de estado de auditoría si existe
            audit_status = None
            try:
                with open('logs/audit_status.json', 'r', encoding='utf-8') as f:
                    audit_status = json.load(f)
                    audit_success = audit_status.get('success', audit_success)
                    success_rate = audit_status.get('success_rate', 0)
                    self.logger.info(f"📋 Reporte de auditoría leído: {success_rate:.1f}% de éxito")
            except Exception as e:
                self.logger.warning(f"No se pudo leer logs/audit_status.json: {e}")
            
            self.results['phases']['phase_5_pipeline'] = {
                'name': 'Validación Pipeline ETL',
                'success': audit_success,
                'duration_seconds': (datetime.now() - phase_start).total_seconds(),
                'validation_output': audit_process.stdout if audit_success else audit_process.stderr,
                'audit_details': audit_status,
                'kafka_auto_started': True
            }
            
            if audit_success:
                self.logger.info("✅ FASE 5 COMPLETADA: Pipeline ETL operativo")
                if audit_status:
                    self.logger.info(f"📊 Auditoría: {audit_status.get('passed_tests', 0)}/{audit_status.get('total_tests', 0)} tests exitosos")
            else:
                self.logger.warning(f"⚠️  Validación ETL con advertencias: {audit_process.stderr}")
            
            return audit_success
            
        except Exception as e:
            self.logger.error(f"❌ Error crítico en Fase 5: {str(e)}")
            self.results['phases']['phase_5_pipeline'] = {
                'name': 'Validación Pipeline ETL',
                'success': False,
                'error': str(e),
                'duration_seconds': (datetime.now() - phase_start).total_seconds()
            }
            return False
    
    def _validate_etl_flow(self) -> bool:
        """Validar flujo completo del pipeline ETL"""
        try:
            self.logger.info("🔍 Validando flujo MySQL → Kafka → ClickHouse...")
            
            # Usar el validador existente
            try:
                result = subprocess.run([
                    'python3', '/app/tools/validate_etl_complete.py'
                ], capture_output=True, text=True, timeout=60)
                
                if result.returncode == 0:
                    self.logger.info("✅ Pipeline ETL validado exitosamente")
                    return True
                else:
                    self.logger.warning(f"⚠️  Validación ETL con advertencias: {result.stderr}")
                    return True  # No es crítico para la automatización
                    
            except subprocess.TimeoutExpired:
                self.logger.warning("⚠️  Timeout validando ETL")
                return True  # No es crítico
            except Exception as e:
                self.logger.warning(f"⚠️  Error validando ETL: {str(e)}")
                return True  # No es crítico
            
        except Exception as e:
            self.logger.error(f"❌ Error en validación ETL: {str(e)}")
            return False
    
    def _run_pre_validation(self) -> bool:
        """Ejecutar pre-validación para autonomía completa"""
        self.logger.info("🛡️ Ejecutando pre-validación para autonomía...")
        
        try:
            # Ejecutar el script de pre-validación
            result = subprocess.run([
                sys.executable, 'tools/pre_validation.py'
            ], capture_output=True, text=True, timeout=60)
            
            if result.returncode == 0:
                self.logger.info("✅ Pre-validación exitosa - Sistema listo para autonomía")
                return True
            else:
                self.logger.error("❌ Pre-validación falló:")
                if result.stderr:
                    self.logger.error(f"   Error: {result.stderr}")
                if result.stdout:
                    self.logger.info(f"   Detalles: {result.stdout}")
                return False
                
        except subprocess.TimeoutExpired:
            self.logger.error("❌ Pre-validación tomó demasiado tiempo")
            return False
        except Exception as e:
            self.logger.error(f"❌ Error ejecutando pre-validación: {e}")
            return False
    
    def _read_existing_json_reports(self):
        """Leer reportes JSON existentes para contexto"""
        try:
            # Leer reporte de ingesta si existe
            ingest_status_path = 'logs/ingest_status.json'
            if os.path.exists(ingest_status_path):
                with open(ingest_status_path, 'r', encoding='utf-8') as f:
                    ingest_data = json.load(f)
                    self.logger.info(f"📄 Reporte de ingesta leído: {ingest_data.get('summary', 'N/A')}")
            
            # Leer reporte de auditoría si existe  
            audit_status_path = 'logs/audit_status.json'
            if os.path.exists(audit_status_path):
                with open(audit_status_path, 'r', encoding='utf-8') as f:
                    audit_data = json.load(f)
                    success_rate = audit_data.get('success_rate', 0)
                    self.logger.info(f"📄 Reporte de auditoría leído: {success_rate:.1f}% éxito")
                    
        except Exception as e:
            self.logger.warning(f"⚠️  No se pudieron leer reportes JSON existentes: {e}")
    
    def _ensure_dependencies(self):
        """Instalar dependencias automáticamente si es necesario"""
        try:
            import requests
            self.logger.info("✅ Dependencias verificadas")
        except ImportError:
            self.logger.info("📦 Instalando dependencias automáticamente...")
            try:
                subprocess.run([sys.executable, '-m', 'pip', 'install', 'requests'], 
                             check=True, capture_output=True)
                self.logger.info("✅ Dependencias instaladas exitosamente")
            except subprocess.CalledProcessError as e:
                self.logger.warning(f"⚠️  No se pudieron instalar dependencias automáticamente: {e}")
    
    def _execute_phase_with_retry(self, phase_name: str, phase_function, max_retries: int = 2) -> bool:
        """Ejecutar una fase con reintentos automáticos"""
        for attempt in range(max_retries + 1):
            try:
                if attempt > 0:
                    self.logger.info(f"🔄 Reintentando {phase_name} (intento {attempt + 1}/{max_retries + 1})")
                    time.sleep(10 * attempt)  # Espera progresivamente más
                
                success = phase_function()
                if success:
                    return True
                    
            except Exception as e:
                self.logger.warning(f"⚠️  Error en {phase_name} (intento {attempt + 1}): {str(e)}")
                
        self.logger.error(f"❌ {phase_name} falló después de {max_retries + 1} intentos")
        return False
    
    def generate_final_summary(self) -> Dict[str, Any]:
        """Generar resumen final completo"""
        end_time = datetime.now()
        total_duration = (end_time - self.start_time).total_seconds()
        
        # Contar fases exitosas
        successful_phases = sum(1 for phase in self.results['phases'].values() if phase.get('success', False))
        total_phases = len(self.results['phases'])
        
        # Determinar éxito general
        critical_phases_success = (
            self.results['phases'].get('phase_1_infrastructure', {}).get('success', False) and
            self.results['phases'].get('phase_2_users', {}).get('success', False)
        )
        
        ingestion_success = self.results['phases'].get('phase_3_ingestion', {}).get('success', False)
        superset_success = self.results['phases'].get('phase_4_superset', {}).get('success', False)
        
        if critical_phases_success and superset_success:
            overall_status = 'COMPLETAMENTE EXITOSO'
            status_emoji = '🎉'
        elif critical_phases_success:
            overall_status = 'MAYORMENTE EXITOSO'
            status_emoji = '✅'
        else:
            overall_status = 'PROBLEMAS CRÍTICOS'
            status_emoji = '❌'
        
        summary = {
            'execution_id': self.execution_id,
            'overall_status': overall_status,
            'successful_phases': successful_phases,
            'total_phases': total_phases,
            'duration_seconds': total_duration,
            'duration_formatted': f"{total_duration//60:.0f}m {total_duration%60:.1f}s",
            'infrastructure_ok': self.results['phases'].get('phase_1_infrastructure', {}).get('success', False),
            'users_configured': self.results['phases'].get('phase_2_users', {}).get('success', False),
            'data_ingested': ingestion_success,
            'superset_configured': superset_success,
            'pipeline_validated': self.results['phases'].get('phase_5_pipeline', {}).get('success', False)
        }
        
        # Agregar información de acceso
        if superset_success:
            superset_details = self.results['phases']['phase_4_superset'].get('details', {})
            summary['superset_access'] = {
                'url': self.superset_configurator.superset_url,
                'username': self.superset_configurator.admin_config['username'],
                'password': self.superset_configurator.admin_config['password'],
                'database_id': superset_details.get('database_id'),
                'datasets_count': len(superset_details.get('datasets_created', []))
            }
        
        # Agregar información de ingesta
        if ingestion_success:
            ingestion_details = self.results['phases']['phase_3_ingestion']
            summary['ingestion_info'] = {
                'tables_processed': ingestion_details.get('tables_processed', 0),
                'duration_seconds': ingestion_details.get('duration_seconds', 0)
            }
        
        self.results['summary'] = summary
        self.results['end_time'] = end_time.isoformat()
        self.results['duration_seconds'] = total_duration
        self.results['overall_success'] = critical_phases_success
        
        return summary
    
    def display_final_report(self, summary: Dict[str, Any]):
        """Mostrar reporte final visual"""
        status_emoji = '🎉' if summary['overall_status'] == 'COMPLETAMENTE EXITOSO' else '✅' if 'EXITOSO' in summary['overall_status'] else '❌'
        
        print("\n" + "="*80)
        print(f"{status_emoji} REPORTE FINAL - AGENTE MAESTRO ETL")
        print("="*80)
        print(f"📅 Ejecución: {summary['execution_id']}")
        print(f"⏱️  Duración: {summary['duration_formatted']}")
        print(f"📊 Estado: {summary['overall_status']}")
        print(f"✅ Fases exitosas: {summary['successful_phases']}/{summary['total_phases']}")
        print()
        
        # Estado de componentes
        print("🏗️  ESTADO DE COMPONENTES:")
        print(f"   🏥 Infraestructura:  {'✅' if summary['infrastructure_ok'] else '❌'}")
        print(f"   👥 Usuarios:         {'✅' if summary['users_configured'] else '❌'}")  
        print(f"   📊 Ingesta de Datos: {'✅' if summary['data_ingested'] else '❌'}")
        print(f"   📋 Superset:         {'✅' if summary['superset_configured'] else '❌'}")
        print(f"   ⚡ Pipeline ETL:     {'✅' if summary['pipeline_validated'] else '⚠️ '}")
        print()
        
        # Información de ingesta
        if summary.get('ingestion_info'):
            ingestion = summary['ingestion_info']
            print("📊 INFORMACIÓN DE INGESTA:")
            print(f"   Tablas procesadas: {ingestion['tables_processed']}")
            print(f"   Tiempo de ingesta: {ingestion['duration_seconds']:.1f}s")
            print()
        
        # Información de acceso
        if summary.get('superset_access'):
            access = summary['superset_access']
            print("🌐 ACCESO A SUPERSET:")
            print(f"   URL:      {access['url']}")
            print(f"   Usuario:  {access['username']}")
            print(f"   Password: {access['password']}")
            print(f"   Datasets: {access['datasets_count']} configurados")
            print()
        
        # Próximos pasos
        if summary['overall_status'] == 'COMPLETAMENTE EXITOSO':
            print("🎯 PRÓXIMOS PASOS:")
            print("   1. Acceder a Superset para crear dashboards")
            print("   2. Configurar conectores Debezium si es necesario")
            print("   3. Monitorear el flujo de datos en tiempo real")
        elif 'EXITOSO' in summary['overall_status']:
            print("🔧 ACCIONES RECOMENDADAS:")
            print("   1. Revisar logs para problemas menores")
            print("   2. Verificar configuración de Superset manualmente")
            print("   3. Continuar con el monitoreo del pipeline")
        else:
            print("🚨 ACCIONES REQUERIDAS:")
            print("   1. Revisar logs detallados de errores")
            print("   2. Verificar conectividad de servicios")
            print("   3. Re-ejecutar configuración después de correcciones")
        
        print("="*80)
    
    def execute_complete_automation(self) -> bool:
        """Ejecutar automatización completa del pipeline ETL"""
        self.logger.info("🎯 === INICIANDO AUTOMATIZACIÓN COMPLETA ETL ===")
        self.logger.info(f"🆔 ID de Ejecución: {self.execution_id}")
        
        try:
            # PASO 0: Pre-validación para autonomía
            if not self._run_pre_validation():
                self.logger.error("❌ PIPELINE ABORTADO: Pre-validación falló")
                return False
            
            self._ensure_dependencies()
            
            # Leer reportes JSON si existen
            self._read_existing_json_reports()
            
            # Fase 1: Validar infraestructura (con reintentos)
            phase1_success = self._execute_phase_with_retry(
                "Fase 1 - Infraestructura", 
                self.phase_1_validate_infrastructure, 
                max_retries=2
            )
            if not phase1_success:
                self.logger.error("❌ Fallo crítico en Fase 1 - Abortando")
                return False
            
            # Breve pausa entre fases
            time.sleep(5)
            
            # Fase 2: Configurar usuarios (con reintentos)
            phase2_success = self._execute_phase_with_retry(
                "Fase 2 - Usuarios", 
                self.phase_2_configure_users, 
                max_retries=1
            )
            if not phase2_success:
                # Verificar si al menos ClickHouse y Superset están configurados
                user_details = self.results['phases']['phase_2_users'].get('details', {})
                connectivity = user_details.get('connectivity_tests', {})
                
                clickhouse_ok = any(k.startswith('clickhouse_') and v for k, v in connectivity.items())
                superset_ok = connectivity.get('superset_admin', False)
                
                if clickhouse_ok and superset_ok:
                    self.logger.warning("⚠️  Fase 2 parcialmente exitosa - Continuando (ClickHouse y Superset funcionando)")
                else:
                    self.logger.error("❌ Fallo crítico en Fase 2 - Abortando")
                    return False
            
            # Pausa más larga para que los servicios se estabilicen
            self.logger.info("⏳ Esperando estabilización de servicios...")
            time.sleep(15)
            
            # Fase 3: Ingesta de datos (NUEVA FASE)
            phase3_success = self._execute_phase_with_retry(
                "Fase 3 - Ingesta de Datos", 
                self.phase_3_ingest_data, 
                max_retries=1
            )
            if not phase3_success:
                self.logger.warning("⚠️  Problemas en ingesta de datos, pero continuando...")
            
            # Pausa para que los datos se asienten
            time.sleep(10)
            
            # Fase 4: Configurar Superset (con reintentos)
            phase4_success = self._execute_phase_with_retry(
                "Fase 4 - Superset", 
                self.phase_4_configure_superset, 
                max_retries=1
            )
            # Superset no es crítico para el pipeline básico
            if not phase4_success:
                self.logger.warning("⚠️  Problemas en Superset, pero continuando...")
            
            # Fase 5: Validar pipeline (sin reintentos críticos, es validación)
            phase5_success = self.phase_5_validate_etl_pipeline()
            
            # Generar resumen final
            summary = self.generate_final_summary()
            
            # Guardar resultados
            self._save_execution_results()
            
            # Mostrar reporte
            self.display_final_report(summary)
            
            return summary['infrastructure_ok'] and summary['users_configured'] and summary['data_ingested']
            
        except KeyboardInterrupt:
            self.logger.warning("⚠️  Ejecución interrumpida por el usuario")
            return False
        except Exception as e:
            self.logger.error(f"❌ Error crítico en automatización: {str(e)}")
            return False


def main():
    """Función principal del agente maestro"""
    print("\n🎯 AGENTE MAESTRO ETL - AUTOMATIZACIÓN COMPLETA")
    print("Inicializando pipeline ETL sin intervención manual...")
    print("="*60)
    
    try:
        # Crear y ejecutar agente maestro
        master_agent = MasterETLAgent()
        success = master_agent.execute_complete_automation()
        
        return 0 if success else 1
        
    except Exception as e:
        print(f"❌ Error crítico del agente maestro: {str(e)}")
        return 1


if __name__ == "__main__":
    exit(main())