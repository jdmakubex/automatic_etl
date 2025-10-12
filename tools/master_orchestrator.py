#!/usr/bin/env python3
"""
master_orchestrator.py
ORQUESTADOR MAESTRO CENTRALIZADO - Coordina toda la infraestructura ETL
Ejecución recomendada: Docker (servicio maestro).
Maneja fases secuenciales con comunicación entre contenedores y validaciones robustas.
"""

import os
import sys
import json
import time
import logging
import subprocess
import requests
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, asdict
from enum import Enum

# Estados de coordianación
class PhaseStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"

@dataclass
class PhaseResult:
    name: str
    status: PhaseStatus
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_seconds: float = 0.0
    output: str = ""
    error: str = ""
    validation_passed: bool = False
    retry_count: int = 0
    container: str = ""
    dependencies_met: bool = False

class MasterOrchestrator:
    """Orquestador maestro que coordina todas las fases del pipeline ETL"""
    
    def __init__(self):
        self.start_time = datetime.now()
        self.logger = self._setup_logging()
        self.state_file = "/app/logs/orchestrator_state.json"
        self.phase_results: Dict[str, PhaseResult] = {}
        
        # Configuración de fases con dependencias explícitas
        self.phases_config = {
            # FASE 1: INFRAESTRUCTURA BASE
            "infrastructure_validation": {
                "name": "🏥 Validación de Infraestructura",
                "container": "etl-orchestrator", 
                "script": "tools/health_validator.py",
                "description": "Verifica que todos los servicios estén operativos",
                "dependencies": [],
                "timeout": 180,
                "required": True,
                "max_retries": 3,
                "validation_script": None
            },
            
            # FASE 2: CONFIGURACIÓN DE USUARIOS Y PERMISOS
            "user_setup": {
                "name": "👥 Configuración de Usuarios",
                "container": "etl-orchestrator",
                "script": "tools/setup_database_users.py", 
                "description": "Crea usuarios y permisos en MySQL/ClickHouse",
                "dependencies": ["infrastructure_validation"],
                "timeout": 120,
                "required": True,
                "max_retries": 2,
                "validation_script": "tools/verify_dependencies.py"
            },
            
            # FASE 3: DESCUBRIMIENTO Y CONFIGURACIÓN DE ESQUEMAS
            "schema_discovery": {
                "name": "🔍 Descubrimiento de Esquemas", 
                "container": "etl-tools",
                "script": "tools/discover_mysql_tables.py",
                "description": "Detecta tablas MySQL y genera configuraciones",
                "dependencies": ["user_setup"],
                "timeout": 90,
                "required": True,
                "max_retries": 2,
                "validation_script": None
            },
            
            "clickhouse_models": {
                "name": "🏗️ Creación de Modelos ClickHouse",
                "container": "etl-tools", 
                "script": "tools/create_clickhouse_models.py",
                "description": "Crea tablas y esquemas en ClickHouse",
                "dependencies": ["schema_discovery"],
                "timeout": 120,
                "required": True,
                "max_retries": 2,
                "validation_script": "tools/validate_clickhouse.py"
            },
            
            # FASE 4: CONFIGURACIÓN DE CONECTORES
            "connector_generation": {
                "name": "⚙️ Generación de Pipeline",
                "container": "etl-tools",
                "script": "tools/gen_pipeline.py", 
                "description": "Genera configuraciones de conectores Debezium",
                "dependencies": ["clickhouse_models"],
                "timeout": 90,
                "required": True,
                "max_retries": 2,
                "validation_script": None
            },
            
            "connector_deployment": {
                "name": "🔌 Despliegue de Conectores",
                "container": "etl-tools",
                "script": "tools/apply_connectors_auto.py",
                "description": "Aplica conectores Debezium en Kafka Connect", 
                "dependencies": ["connector_generation"],
                "timeout": 180,
                "required": True,
                "max_retries": 3,
                "validation_script": None
            },
            
            # FASE 5: INGESTA Y VALIDACIÓN FINAL
            "data_ingestion": {
                "name": "📊 Ingesta de Datos",
                "container": "etl-tools",
                "script": "tools/ingest_runner.py",
                "description": "Ejecuta flujo de ingesta completo", 
                "dependencies": ["connector_deployment"],
                "timeout": 300,
                "required": True,
                "max_retries": 2,
                "validation_script": "tools/pipeline_status.py"
            },
            
            # FASE 6: CONFIGURACIÓN DE SUPERSET (OPCIONAL)
            "superset_setup": {
                "name": "📈 Configuración de Superset",
                "container": "superset",
                "script": "tools/superset_auto_configurator.py",
                "description": "Configura dashboards y datasets en Superset",
                "dependencies": ["data_ingestion"], 
                "timeout": 180,
                "required": False,  # Opcional
                "max_retries": 2,
                "validation_script": None
            }
        }
        
        # Inicializar resultados de fases
        for phase_id, config in self.phases_config.items():
            self.phase_results[phase_id] = PhaseResult(
                name=config["name"],
                status=PhaseStatus.PENDING,
                container=config["container"]
            )
    
    def _setup_logging(self) -> logging.Logger:
        """Configurar logging maestro"""
        logger = logging.getLogger('MasterOrchestrator')
        logger.setLevel(logging.INFO)
        
        # Handler para archivo con rotación
        os.makedirs('/app/logs', exist_ok=True)
        file_handler = logging.FileHandler('/app/logs/master_orchestrator.log')
        file_handler.setLevel(logging.DEBUG)
        
        # Handler para consola  
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        
        # Formatters
        formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
    
    def save_state(self):
        """Guardar estado actual del orquestador"""
        state_data = {
            "start_time": self.start_time.isoformat(),
            "current_time": datetime.now().isoformat(),
            "phase_results": {}
        }
        
        for phase_id, result in self.phase_results.items():
            state_data["phase_results"][phase_id] = {
                "name": result.name,
                "status": result.status.value,
                "start_time": result.start_time.isoformat() if result.start_time else None,
                "end_time": result.end_time.isoformat() if result.end_time else None,
                "duration_seconds": result.duration_seconds,
                "validation_passed": result.validation_passed,
                "retry_count": result.retry_count,
                "container": result.container,
                "dependencies_met": result.dependencies_met,
                "error": result.error[:500] if result.error else ""  # Truncar errores largos
            }
        
        try:
            with open(self.state_file, 'w') as f:
                json.dump(state_data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            self.logger.warning(f"No se pudo guardar estado: {e}")
    
    def check_dependencies(self, phase_id: str) -> bool:
        """Verificar que las dependencias de una fase estén completadas"""
        config = self.phases_config[phase_id]
        dependencies = config.get("dependencies", [])
        
        for dep_id in dependencies:
            if dep_id not in self.phase_results:
                self.logger.error(f"Dependencia '{dep_id}' no encontrada para fase '{phase_id}'")
                return False
            
            dep_result = self.phase_results[dep_id]
            if dep_result.status != PhaseStatus.COMPLETED:
                self.logger.warning(f"Dependencia '{dep_id}' no completada (estado: {dep_result.status.value})")
                return False
            
            if not dep_result.validation_passed and self.phases_config[dep_id].get("required", True):
                self.logger.error(f"Dependencia '{dep_id}' no pasó validación")
                return False
        
        return True
    
    def execute_script_in_container(self, container: str, script: str, timeout: int = 120) -> Tuple[bool, str, str]:
        """Ejecutar script en un contenedor específico"""
        try:
            # Construir comando Docker
            cmd = [
                "docker", "compose", "exec", "-T", container,
                "python3", script
            ]
            self.logger.info(f"Ejecutando en '{container}': {' '.join(cmd[4:])}")
            # Usar wrapper para scripts principales si están disponibles
            script_basename = script.split('/')[-1]
            wrapper_scripts = [
                "health_validator.py", "setup_database_users.py", "discover_mysql_tables.py",
                "create_clickhouse_models.py", "gen_pipeline.py", "apply_connectors_auto.py",
                "ingest_runner.py", "superset_auto_configurator.py"
            ]
            if script_basename in wrapper_scripts:
                # Usar wrapper para coordinación automática
                cmd = [
                    "docker", "compose", "exec", "-T", container,
                    "python3", "tools/etl_script_wrapper.py", script_basename
                ]
                self.logger.info(f"Usando wrapper coordinado: {script_basename}")
            # Determinar cwd correcto
            # Si ejecutamos en un contenedor, el directorio de trabajo debe ser /app
            cwd = "/app"
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=cwd
            )
            success = result.returncode == 0
            return success, result.stdout, result.stderr
        except subprocess.TimeoutExpired:
            error_msg = f"Timeout ({timeout}s) ejecutando script en contenedor '{container}'"
            return False, "", error_msg
        except Exception as e:
            error_msg = f"Error ejecutando script: {str(e)}"
            return False, "", error_msg
    
    def validate_phase(self, phase_id: str) -> bool:
        """Ejecutar validación específica de una fase"""
        config = self.phases_config[phase_id]
        validation_script = config.get("validation_script")
        
        if not validation_script:
            self.logger.debug(f"No hay script de validación para '{phase_id}'")
            return True  # Asumir éxito si no hay validación específica
        
        self.logger.info(f"🔍 Validando fase '{phase_id}' con '{validation_script}'")
        
        success, output, error = self.execute_script_in_container(
            container=config["container"],
            script=validation_script,
            timeout=60
        )
        
        if success:
            self.logger.info(f"✅ Validación de '{phase_id}' exitosa")
        else:
            self.logger.error(f"❌ Validación de '{phase_id}' falló: {error[:200]}")
        
        return success
    
    def comprehensive_phase_validation(self, phase_id: str) -> Tuple[bool, Dict[str, Any]]:
        """Validación exhaustiva de una fase incluyendo estado, datos e integridad"""
        config = self.phases_config[phase_id]
        validation_results = {
            "phase": phase_id,
            "validations": {},
            "overall_success": False,
            "critical_issues": [],
            "warnings": []
        }
        
        self.logger.info(f"🔍 Iniciando validación exhaustiva para '{config['name']}'")
        
        try:
            # 1. Validación básica con script específico
            if config.get("validation_script"):
                basic_success = self.validate_phase(phase_id)
                validation_results["validations"]["basic_script"] = basic_success
                if not basic_success:
                    validation_results["critical_issues"].append("Script de validación básica falló")
            else:
                validation_results["validations"]["basic_script"] = True
            
            # 2. Validaciones específicas por tipo de fase
            if phase_id == "infrastructure_validation":
                infra_success = self._validate_infrastructure_health()
                validation_results["validations"]["infrastructure_health"] = infra_success
                if not infra_success:
                    validation_results["critical_issues"].append("Servicios de infraestructura no saludables")
            
            elif phase_id == "user_setup":
                user_success = self._validate_database_connectivity()
                validation_results["validations"]["database_connectivity"] = user_success
                if not user_success:
                    validation_results["critical_issues"].append("Conectividad de base de datos falló")
            
            elif phase_id == "schema_discovery":
                schema_success = self._validate_schema_discovery()
                validation_results["validations"]["schema_discovery"] = schema_success
                if not schema_success:
                    validation_results["critical_issues"].append("Descubrimiento de esquemas falló")
            
            elif phase_id == "clickhouse_models":
                ch_success = self._validate_clickhouse_models()
                validation_results["validations"]["clickhouse_models"] = ch_success
                if not ch_success:
                    validation_results["critical_issues"].append("Modelos de ClickHouse no válidos")
            
            elif phase_id == "connector_deployment":
                connector_success = self._validate_connectors()
                validation_results["validations"]["connectors"] = connector_success
                if not connector_success:
                    validation_results["critical_issues"].append("Conectores Debezium no funcionando")
            
            elif phase_id == "data_ingestion":
                data_success = self._validate_data_flow()
                validation_results["validations"]["data_flow"] = data_success
                if not data_success:
                    validation_results["critical_issues"].append("Flujo de datos no operativo")
            
            # 3. Determinar éxito general
            critical_failures = len(validation_results["critical_issues"])
            total_validations = len(validation_results["validations"])
            successful_validations = sum(validation_results["validations"].values())
            
            # Criterio: al menos 80% de validaciones exitosas y sin issues críticos
            success_rate = (successful_validations / total_validations) if total_validations > 0 else 0
            validation_results["overall_success"] = success_rate >= 0.8 and critical_failures == 0
            
            if validation_results["overall_success"]:
                self.logger.info(f"✅ Validación exhaustiva exitosa para '{phase_id}' ({success_rate:.1%})")
            else:
                self.logger.error(f"❌ Validación exhaustiva falló para '{phase_id}': {critical_failures} issues críticos")
            
            return validation_results["overall_success"], validation_results
            
        except Exception as e:
            validation_results["critical_issues"].append(f"Error en validación: {str(e)}")
            self.logger.error(f"💥 Error en validación exhaustiva de '{phase_id}': {e}")
            return False, validation_results
    
    def _validate_infrastructure_health(self) -> bool:
        """Validar salud de infraestructura"""
        try:
            # Verificar servicios básicos
            services = ["clickhouse:8123/ping", "connect:8083/", "kafka:9092"]
            healthy_count = 0
            
            for service in services:
                try:
                    host, port_path = service.split(':')
                    if '/' in port_path:
                        port, path = port_path.split('/', 1)
                        url = f"http://{host}:{port}/{path}"
                    else:
                        # Para Kafka, solo verificar conectividad TCP
                        continue
                    
                    cmd = ["docker", "compose", "exec", "-T", "etl-orchestrator", "curl", "-s", "-f", url, "-m", "5"]
                    result = subprocess.run(cmd, capture_output=True, timeout=10)
                    if result.returncode == 0:
                        healthy_count += 1
                except:
                    continue
            
            return healthy_count >= 2  # Al menos ClickHouse y Connect
            
        except Exception as e:
            self.logger.warning(f"Error validando infraestructura: {e}")
            return False
    
    def _validate_database_connectivity(self) -> bool:
        """Validar conectividad a bases de datos"""
        try:
            success, output, error = self.execute_script_in_container(
                container="etl-orchestrator",
                script="tools/verify_dependencies.py",
                timeout=30
            )
            return success
        except Exception:
            return False
    
    def _validate_schema_discovery(self) -> bool:
        """Validar que el descubrimiento de esquemas fue exitoso"""
        try:
            # Verificar que se generaron archivos de configuración
            generated_dir = Path("/mnt/c/proyectos/etl_prod/generated/default")
            if not generated_dir.exists():
                return False
            
            # Buscar archivos de configuración generados
            config_files = list(generated_dir.glob("*.json"))
            return len(config_files) > 0
            
        except Exception:
            return False
    
    def _validate_clickhouse_models(self) -> bool:
        """Validar modelos de ClickHouse"""
        try:
            success, output, error = self.execute_script_in_container(
                container="etl-tools",
                script="tools/validate_clickhouse.py",
                timeout=60
            )
            return success
        except Exception:
            return False
    
    def _validate_connectors(self) -> bool:
        """Validar estado de conectores Debezium"""
        try:
            # Verificar conectores via API de Kafka Connect
            cmd = [
                "docker", "compose", "exec", "-T", "etl-orchestrator",
                "curl", "-s", "http://connect:8083/connectors", "-m", "10"
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            
            if result.returncode == 0:
                try:
                    connectors = json.loads(result.stdout)
                    return len(connectors) > 0  # Al menos un conector
                except json.JSONDecodeError:
                    return False
            return False
            
        except Exception:
            return False
    
    def _validate_data_flow(self) -> bool:
        """Validar flujo completo de datos"""
        try:
            success, output, error = self.execute_script_in_container(
                container="etl-tools",
                script="tools/pipeline_status.py",
                timeout=90
            )
            
            # Analizar output para verificar flujo de datos
            if success and output:
                # Buscar indicadores de datos fluyendo
                data_indicators = ["data flowing", "records processed", "tables synchronized"]
                return any(indicator in output.lower() for indicator in data_indicators)
            
            return False
            
        except Exception:
            return False
    
    def execute_phase(self, phase_id: str) -> bool:
        """Ejecutar una fase específica con reintentos"""
        config = self.phases_config[phase_id]
        result = self.phase_results[phase_id]
        
        self.logger.info(f"\n{'='*80}")
        self.logger.info(f"🚀 INICIANDO FASE: {config['name']}")
        self.logger.info(f"📋 Descripción: {config['description']}")
        self.logger.info(f"🐳 Contenedor: {config['container']}")
        self.logger.info(f"🔧 Script: {config['script']}")
        self.logger.info(f"{'='*80}")
        
        # Verificar dependencias
        if not self.check_dependencies(phase_id):
            result.status = PhaseStatus.FAILED
            result.error = "Dependencias no satisfechas"
            result.dependencies_met = False
            self.logger.error(f"❌ FASE FALLÓ: {config['name']} - Dependencias no satisfechas")
            return False
        
        result.dependencies_met = True
        result.status = PhaseStatus.RUNNING
        result.start_time = datetime.now()
        
        # Intentar ejecución con reintentos
        max_retries = config.get("max_retries", 1)
        
        for attempt in range(max_retries + 1):
            if attempt > 0:
                self.logger.warning(f"🔄 Reintento {attempt}/{max_retries} para '{config['name']}'")
                time.sleep(10)  # Esperar entre reintentos
            
            result.retry_count = attempt
            
            # Ejecutar script principal
            success, output, error = self.execute_script_in_container(
                container=config["container"],
                script=config["script"],
                timeout=config.get("timeout", 120)
            )
            
            if success:
                result.output = output[-1000:] if output else ""  # Guardar últimos 1000 chars
                
                # Ejecutar validación si está definida
                validation_passed = self.validate_phase(phase_id)
                result.validation_passed = validation_passed
                
                if validation_passed or not config.get("required", True):
                    # Éxito completo
                    result.status = PhaseStatus.COMPLETED
                    result.end_time = datetime.now()
                    result.duration_seconds = (result.end_time - result.start_time).total_seconds()
                    
                    self.logger.info(f"✅ FASE COMPLETADA: {config['name']} ({result.duration_seconds:.1f}s)")
                    self.save_state()
                    return True
                else:
                    # Script exitoso pero validación falló
                    if attempt < max_retries:
                        self.logger.warning(f"⚠️ Validación falló, reintentando...")
                        continue
                    else:
                        result.status = PhaseStatus.FAILED
                        result.error = "Validación falló después de todos los reintentos"
                        break
            else:
                # Script falló
                result.error = error[-500:] if error else "Script falló sin mensaje de error"
                
                # Auto-reparación para errores de permisos Docker
                if "permission denied while trying to connect to the Docker daemon socket" in (error or ""):
                    self.logger.warning("🔧 Error de permisos en Docker socket detectado. Ejecutando reparación automática...")
                    try:
                        # Ejecutar como root para permisos de sistema
                        repair_result = subprocess.run([
                            "docker", "compose", "run", "--rm", "--user", "root", "etl-orchestrator", 
                            "python3", "tools/fix_docker_socket.py"
                        ], capture_output=True, text=True, timeout=60)
                        
                        if repair_result.returncode == 0:
                            self.logger.info("✅ Reparación automática de Docker socket ejecutada. Reintentando...")
                        else:
                            self.logger.error(f"❌ Reparación automática falló: {repair_result.stderr[:200]}")
                    except Exception as e:
                        self.logger.error(f"❌ Error ejecutando reparación automática: {e}")
                
                if attempt < max_retries:
                    self.logger.warning(f"⚠️ Script falló, reintentando: {result.error[:100]}")
                    continue
                else:
                    result.status = PhaseStatus.FAILED
                    break
        
        # Falló después de todos los reintentos
        result.end_time = datetime.now()
        result.duration_seconds = (result.end_time - result.start_time).total_seconds()
        
        # Decidir si continuar o abortar
        if config.get("required", True):
            self.logger.error(f"❌ FASE CRÍTICA FALLÓ: {config['name']}")
            self.logger.error(f"💥 Error: {result.error}")
            return False
        else:
            result.status = PhaseStatus.SKIPPED
            self.logger.warning(f"⚠️ FASE OPCIONAL FALLÓ: {config['name']} - Continuando")
            return True
    
    def wait_for_infrastructure(self) -> bool:
        """
        Espera que la infraestructura esté lista usando el validador avanzado de salud.
        Si detecta error de autenticación en ClickHouse, ejecuta fix_clickhouse_config.py en etl-tools y reintenta.
        """
        self.logger.info("⏳ Validando conectividad de red entre servicios críticos...")
        net_result = subprocess.run([
            "python3", "tools/network_validator.py"
        ], capture_output=True, text=True, timeout=60)
        net_output = net_result.stdout + net_result.stderr
        try:
            with open("/app/logs/network_check_results.json") as f:
                net_health = json.load(f)
            if net_health.get("overall_status") != "healthy":
                self.logger.error(f"❌ Fallo de red detectado: {net_health.get('errors')}")
                return False
            else:
                self.logger.info("✅ Conectividad de red OK. Continuando con validación de infraestructura...")
        except Exception as e:
            self.logger.error(f"❌ No se pudo leer network_check_results.json: {e}")
            return False
        max_wait = 300
        start_time = time.time()
        repair_attempted = False
        while (time.time() - start_time) < max_wait:
            try:
                # Ejecutar health_validator.py directamente
                result = subprocess.run([
                    "python3", "tools/health_validator.py"
                ], capture_output=True, text=True, timeout=60)
                output = result.stdout + result.stderr
                # Detectar error de permisos Docker
                if "permission denied while trying to connect to the Docker daemon socket" in output:
                    self.logger.warning("🔧 Error de permisos en Docker socket detectado. Ejecutando reparación automática...")
                    # Ejecutar como root para permisos de sistema
                    result = subprocess.run([
                        "docker", "compose", "run", "--rm", "--user", "root", "etl-orchestrator", 
                        "python3", "tools/fix_docker_socket.py"
                    ], capture_output=True, text=True, timeout=60)
                    success = result.returncode == 0
                    out = result.stdout
                    err = result.stderr
                    if success:
                        self.logger.info("✅ Reparación automática de Docker socket ejecutada. Reintentando validación...")
                    else:
                        self.logger.error(f"❌ Reparación automática Docker socket falló: {err[:200]}")
                    time.sleep(5)
                    continue
                if result.returncode == 0:
                    # Parsear el resultado JSON
                    try:
                        with open("/app/logs/health_check_results.json") as f:
                            health = json.load(f)
                        if health.get("overall_status") in ["fully_healthy", "mostly_healthy"]:
                            self.logger.info(f"✅ Infraestructura lista ({time.time()-start_time:.1f}s)")
                            return True
                        else:
                            self.logger.info(f"⏳ Infraestructura aún no healthy: {health.get('overall_status')}")
                            # Detectar error de autenticación ClickHouse
                            if (not repair_attempted and health.get("critical_issues") and any("AUTHENTICATION_FAILED" in err or "Authentication failed" in err for err in health.get("critical_issues", []))):
                                self.logger.warning("🔧 Error de autenticación ClickHouse detectado. Ejecutando reparación automática...")
                                success, out, err = self.execute_script_in_container(
                                    container="etl-tools",
                                    script="tools/fix_clickhouse_config.py",
                                    timeout=90
                                )
                                if success:
                                    self.logger.info("✅ Reparación automática de ClickHouse ejecutada. Reintentando validación...")
                                else:
                                    self.logger.error(f"❌ Reparación automática falló: {err[:200]}")
                                repair_attempted = True
                    except Exception as e:
                        self.logger.warning(f"No se pudo leer health_check_results.json: {e}")
                else:
                    self.logger.info(f"⏳ health_validator.py aún no retorna healthy")
                    # Detectar error de autenticación en output si no hay JSON
                    if (not repair_attempted and ("AUTHENTICATION_FAILED" in output or "Authentication failed" in output)):
                        self.logger.warning("🔧 Error de autenticación ClickHouse detectado. Ejecutando reparación automática...")
                        success, out, err = self.execute_script_in_container(
                            container="etl-tools",
                            script="tools/fix_clickhouse_config.py",
                            timeout=90
                        )
                        if success:
                            self.logger.info("✅ Reparación automática de ClickHouse ejecutada. Reintentando validación...")
                        else:
                            self.logger.error(f"❌ Reparación automática falló: {err[:200]}")
                        repair_attempted = True
            except Exception as e:
                self.logger.warning(f"Error ejecutando health_validator: {e}")
            time.sleep(10)
        self.logger.error(f"❌ Infraestructura no disponible después de {max_wait}s (health_validator)")
        return False
    
    def run_orchestration(self) -> bool:
        """Ejecutar orquestación completa"""
        self.logger.info("🎯 === INICIANDO ORQUESTACIÓN MAESTRA DEL PIPELINE ETL ===")
        self.logger.info(f"⏰ Inicio: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"📋 Fases programadas: {len(self.phases_config)}")
        
        try:
            # 1. Esperar infraestructura básica
            if not self.wait_for_infrastructure():
                self.logger.error("❌ Infraestructura no disponible, abortando")
                return False
            
            # 2. Ejecutar fases en orden topológico
            phases_order = [
                "infrastructure_validation",
                "user_setup", 
                "schema_discovery",
                "clickhouse_models",
                "connector_generation",
                "connector_deployment", 
                "data_ingestion",
                "superset_setup"
            ]
            
            successful_phases = 0
            
            for phase_id in phases_order:
                if self.execute_phase(phase_id):
                    successful_phases += 1
                else:
                    config = self.phases_config[phase_id]
                    if config.get("required", True):
                        self.logger.error(f"❌ Fase crítica '{phase_id}' falló, abortando orquestación")
                        return False
                    else:
                        self.logger.warning(f"⚠️ Fase opcional '{phase_id}' falló, continuando")
                
                # Guardar estado después de cada fase
                self.save_state()
            
            # 3. Verificar éxito general
            total_phases = len(phases_order)
            required_phases = sum(1 for p in phases_order if self.phases_config[p].get("required", True))
            completed_required = sum(
                1 for p in phases_order 
                if self.phases_config[p].get("required", True) 
                and self.phase_results[p].status == PhaseStatus.COMPLETED
            )
            
            if completed_required == required_phases:
                self.logger.info(f"🎉 ORQUESTACIÓN EXITOSA: {successful_phases}/{total_phases} fases completadas")
                self.logger.info(f"✅ Todas las fases críticas completadas ({completed_required}/{required_phases})")
                return True
            else:
                self.logger.error(f"❌ ORQUESTACIÓN FALLÓ: Solo {completed_required}/{required_phases} fases críticas completadas")
                return False
            
        except Exception as e:
            self.logger.error(f"💥 Error crítico en orquestación: {str(e)}")
            return False
        
        finally:
            self.save_state()
            self.print_final_summary()
    
    def print_final_summary(self):
        """Imprimir resumen final detallado"""
        end_time = datetime.now()
        total_duration = (end_time - self.start_time).total_seconds()
        
        print(f"\n{'='*100}")
        print(f"🏁 RESUMEN FINAL DE ORQUESTACIÓN MAESTRA")
        print(f"{'='*100}")
        print(f"⏰ Inicio: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏰ Fin: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏱️  Duración total: {total_duration:.1f} segundos")
        
        # Estadísticas por estado
        status_counts = {}
        for result in self.phase_results.values():
            status = result.status.value
            status_counts[status] = status_counts.get(status, 0) + 1
        
        print(f"\n📊 ESTADÍSTICAS DE FASES:")
        print(f"   ✅ Completadas: {status_counts.get('completed', 0)}")
        print(f"   ❌ Fallidas: {status_counts.get('failed', 0)}")
        print(f"   ⚠️  Saltadas: {status_counts.get('skipped', 0)}")
        print(f"   ⏳ Pendientes: {status_counts.get('pending', 0)}")
        
        # Detalle de cada fase
        print(f"\n📋 DETALLE DE FASES:")
        for phase_id, result in self.phase_results.items():
            status_icon = {
                PhaseStatus.COMPLETED: "✅",
                PhaseStatus.FAILED: "❌", 
                PhaseStatus.SKIPPED: "⚠️",
                PhaseStatus.PENDING: "⏳",
                PhaseStatus.RUNNING: "🔄"
            }.get(result.status, "❓")
            
            print(f"   {status_icon} {result.name}")
            if result.duration_seconds > 0:
                print(f"      ⏱️  Duración: {result.duration_seconds:.1f}s")
            if result.retry_count > 0:
                print(f"      🔄 Reintentos: {result.retry_count}")
            if result.error:
                print(f"      💥 Error: {result.error[:80]}...")
        
        # Próximos pasos
        completed_count = status_counts.get('completed', 0)
        failed_count = status_counts.get('failed', 0)
        
        print(f"\n💡 PRÓXIMOS PASOS:")
        if completed_count > failed_count and failed_count == 0:
            print(f"   1. ✅ Pipeline completamente operativo")
            print(f"   2. 🔍 Verificar datos: python3 tools/pipeline_status.py")
            print(f"   3. 📊 Acceder a Superset: http://localhost:8088")
            print(f"   4. 📈 Configurar monitoreo de producción")
        elif completed_count > failed_count:
            print(f"   1. ⚠️  Pipeline parcialmente operativo")
            print(f"   2. 🔧 Revisar fases fallidas en logs")
            print(f"   3. 🔄 Ejecutar reintentos manuales si es necesario")
        else:
            print(f"   1. ❌ Pipeline no operativo")
            print(f"   2. 🔍 Revisar logs detallados en /app/logs/")
            print(f"   3. 🛠️  Corregir errores de infraestructura")
            print(f"   4. 🔄 Re-ejecutar orquestación")
        
        print(f"{'='*100}")

def main():
    """Función principal del orquestador maestro"""
    try:
        # Banner de inicio
        print(f"\n{'='*100}")
        print(f"🎯 ORQUESTADOR MAESTRO CENTRALIZADO DEL PIPELINE ETL")
        print(f"🚀 Coordinación completa con validaciones entre fases")
        print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*100}")
        
        orchestrator = MasterOrchestrator()
        success = orchestrator.run_orchestration()
        
        return 0 if success else 1
        
    except KeyboardInterrupt:
        print("\n🛑 Orquestación interrumpida por el usuario")
        return 1
    except Exception as e:
        print(f"💥 Error crítico en main: {str(e)}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)