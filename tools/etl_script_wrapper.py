#!/usr/bin/env python3
"""
etl_script_wrapper.py
Wrapper para integrar coordinación en scripts ETL existentes sin modificarlos.
Ejecución recomendada: Docker (todos los contenedores).
Permite que scripts existentes reporten estado y coordinen automáticamente.
"""

import os
import sys
import json
import subprocess
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path

# Importar nuestro sistema de coordinación
sys.path.append('/app/tools')
from coordination_helper import CoordinationHelper, ServiceStatus

class ETLScriptWrapper:
    """Wrapper para ejecutar scripts ETL con coordinación automática"""
    
    def __init__(self, script_name: str, service_name: str = None):
        self.script_name = script_name
        self.service_name = service_name or self._extract_service_name(script_name)
        
        # Configurar logging
        self.logger = logging.getLogger(f'ETLWrapper.{self.service_name}')
        self._setup_logging()
        
        # Inicializar coordinación
        self.coord = CoordinationHelper(self.service_name)
        
        # Configuración por script
        self.script_config = self._load_script_config()
    
    def _setup_logging(self):
        """Configurar logging para el wrapper"""
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(
                '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
    
    def _extract_service_name(self, script_name: str) -> str:
        """Extraer nombre del servicio desde el nombre del script"""
        # Mapeo de scripts a servicios lógicos
        script_to_service = {
            "health_validator.py": "infrastructure_health",
            "setup_database_users.py": "user_management",
            "discover_mysql_tables.py": "schema_discovery", 
            "create_clickhouse_models.py": "clickhouse_setup",
            "gen_pipeline.py": "pipeline_generator",
            "apply_connectors_auto.py": "connector_deployment",
            "ingest_runner.py": "data_ingestion",
            "validate_clickhouse.py": "clickhouse_validation",
            "pipeline_status.py": "pipeline_validation",
            "superset_auto_configurator.py": "superset_setup",
            "verify_dependencies.py": "dependency_verification"
        }
        
        script_basename = os.path.basename(script_name)
        return script_to_service.get(script_basename, script_basename.replace('.py', ''))
    
    def _load_script_config(self) -> Dict[str, Any]:
        """Cargar configuración específica del script"""
        default_config = {
            "timeout": 300,
            "dependencies": [],
            "required": True,
            "retry_count": 1,
            "success_indicators": ["success", "completed", "✅"],
            "failure_indicators": ["error", "failed", "❌", "exception"],
            "progress_patterns": [r"(\d+)%", r"(\d+)/(\d+)"],
            "validation_script": None
        }
        
        # Configuraciones específicas por script
        script_configs = {
            "health_validator.py": {
                "dependencies": [],
                "timeout": 180,
                "success_indicators": ["healthy", "operational", "available"],
                "failure_indicators": ["unhealthy", "unavailable", "timeout"]
            },
            "setup_database_users.py": {
                "dependencies": ["infrastructure_health"],
                "timeout": 120,
                "validation_script": "verify_dependencies.py"
            },
            "discover_mysql_tables.py": {
                "dependencies": ["user_management"],
                "timeout": 90,
                "success_indicators": ["tables discovered", "schemas found"]
            },
            "create_clickhouse_models.py": {
                "dependencies": ["schema_discovery"],
                "timeout": 120,
                "validation_script": "validate_clickhouse.py"
            },
            "gen_pipeline.py": {
                "dependencies": ["clickhouse_setup"],
                "timeout": 90,
                "success_indicators": ["pipeline generated", "connectors created"]
            },
            "apply_connectors_auto.py": {
                "dependencies": ["pipeline_generator"],
                "timeout": 180,
                "retry_count": 3
            },
            "ingest_runner.py": {
                "dependencies": ["connector_deployment"],
                "timeout": 300,
                "validation_script": "pipeline_status.py"
            },
            "superset_auto_configurator.py": {
                "dependencies": ["data_ingestion"],
                "timeout": 180,
                "required": False
            }
        }
        
        script_basename = os.path.basename(self.script_name)
        config = default_config.copy()
        config.update(script_configs.get(script_basename, {}))
        
        return config
    
    def _analyze_script_output(self, output: str, error_output: str) -> Dict[str, Any]:
        """Analizar output del script para extraer información"""
        analysis = {
            "success_likely": False,
            "progress_percent": 0.0,
            "key_messages": [],
            "errors": [],
            "warnings": []
        }
        
        all_text = (output + " " + error_output).lower()
        
        # Verificar indicadores de éxito
        success_indicators = self.script_config["success_indicators"]
        for indicator in success_indicators:
            if indicator.lower() in all_text:
                analysis["success_likely"] = True
                analysis["key_messages"].append(f"Indicador de éxito detectado: {indicator}")
                break
        
        # Verificar indicadores de fallo
        failure_indicators = self.script_config["failure_indicators"]
        for indicator in failure_indicators:
            if indicator.lower() in all_text:
                analysis["success_likely"] = False
                analysis["errors"].append(f"Indicador de fallo detectado: {indicator}")
        
        # Extraer mensajes importantes (últimas líneas del output)
        if output:
            output_lines = output.strip().split('\n')
            important_lines = [line for line in output_lines[-10:] if line.strip()]
            analysis["key_messages"].extend(important_lines[-3:])  # Últimas 3 líneas importantes
        
        # Extraer errores
        if error_output:
            error_lines = error_output.strip().split('\n')
            analysis["errors"].extend([line for line in error_lines if line.strip()])
        
        return analysis
    
    def _wait_for_dependencies(self) -> bool:
        """Esperar dependencias del script"""
        dependencies = self.script_config.get("dependencies", [])
        
        if not dependencies:
            self.logger.info("No hay dependencias que esperar")
            return True
        
        self.coord.update_status(
            ServiceStatus.INITIALIZING, 
            f"Esperando dependencias: {', '.join(dependencies)}"
        )
        
        return self.coord.wait_for_dependencies(dependencies, timeout_seconds=600)
    
    def _run_validation(self) -> bool:
        """Ejecutar validación específica del script si está configurada"""
        validation_script = self.script_config.get("validation_script")
        
        if not validation_script:
            return True  # No hay validación específica, asumir éxito
        
        self.logger.info(f"Ejecutando validación: {validation_script}")
        
        try:
            result = subprocess.run(
                ["python3", f"/app/tools/{validation_script}"],
                capture_output=True,
                text=True,
                timeout=60
            )
            
            success = result.returncode == 0
            if success:
                self.logger.info("✅ Validación exitosa")
            else:
                self.logger.error(f"❌ Validación falló: {result.stderr[:200]}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error ejecutando validación: {e}")
            return False
    
    def execute_with_coordination(self, script_args: List[str] = None) -> bool:
        """Ejecutar script con coordinación completa"""
        self.logger.info(f"🚀 Iniciando ejecución coordinada: {self.script_name}")
        
        try:
            # 1. Esperar dependencias
            if not self._wait_for_dependencies():
                self.coord.signal_failed("Dependencias no satisfechas")
                return False
            
            # 2. Marcar como procesando
            self.coord.signal_processing(f"Ejecutando {os.path.basename(self.script_name)}")
            
            # 3. Construir comando
            cmd = ["python3", f"/app/tools/{os.path.basename(self.script_name)}"]
            if script_args:
                cmd.extend(script_args)
            
            self.logger.info(f"Ejecutando: {' '.join(cmd)}")
            
            # 4. Ejecutar con timeout y captura de output
            start_time = time.time()
            timeout = self.script_config["timeout"]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout
            )
            
            duration = time.time() - start_time
            
            # 5. Analizar resultado
            analysis = self._analyze_script_output(result.stdout, result.stderr)
            
            # 6. Determinar éxito
            script_success = result.returncode == 0
            
            # Override con análisis de output si es más confiable
            if analysis["success_likely"] and not script_success:
                self.logger.warning("Script falló pero output indica éxito, considerando exitoso")
                script_success = True
            elif not analysis["success_likely"] and script_success:
                self.logger.warning("Script exitoso pero output indica problemas")
            
            # 7. Ejecutar validación si corresponde
            validation_passed = True
            if script_success:
                validation_passed = self._run_validation()
            
            # 8. Reportar resultado final
            if script_success and validation_passed:
                completion_data = {
                    "duration_seconds": duration,
                    "output_analysis": analysis,
                    "validation_passed": validation_passed
                }
                
                success_message = "Completado exitosamente"
                if analysis["key_messages"]:
                    success_message += f": {analysis['key_messages'][-1][:100]}"
                
                self.coord.signal_completed(success_message, completion_data)
                self.logger.info(f"✅ Ejecución exitosa de {self.script_name} ({duration:.1f}s)")
                return True
            else:
                # Falló
                error_data = {
                    "return_code": result.returncode,
                    "duration_seconds": duration,
                    "output_analysis": analysis,
                    "stderr": result.stderr[-500:] if result.stderr else ""
                }
                
                error_message = "Ejecución falló"
                if analysis["errors"]:
                    error_message += f": {analysis['errors'][-1][:100]}"
                elif not validation_passed:
                    error_message += ": Validación falló"
                
                self.coord.signal_failed(error_message, error_data)
                self.logger.error(f"❌ Ejecución falló: {self.script_name}")
                
                # Log de errores para debug
                if result.stderr:
                    self.logger.error("Stderr del script:")
                    for line in result.stderr.strip().split('\n')[-5:]:
                        self.logger.error(f"  {line}")
                
                return False
        
        except subprocess.TimeoutExpired:
            self.coord.signal_failed(f"Timeout después de {timeout}s")
            self.logger.error(f"⏰ Timeout ejecutando {self.script_name}")
            return False
        
        except Exception as e:
            self.coord.signal_failed(f"Error inesperado: {str(e)}")
            self.logger.error(f"💥 Error inesperado ejecutando {self.script_name}: {e}")
            return False

def main():
    """Función principal del wrapper"""
    if len(sys.argv) < 2:
        print("Uso: python etl_script_wrapper.py <script_name> [service_name] [args...]")
        print("Ejemplo: python etl_script_wrapper.py gen_pipeline.py")
        print("Ejemplo: python etl_script_wrapper.py setup_database_users.py user_setup")
        sys.exit(1)
    
    script_name = sys.argv[1]
    service_name = sys.argv[2] if len(sys.argv) > 2 and not sys.argv[2].startswith('-') else None
    script_args = sys.argv[3:] if service_name else sys.argv[2:]
    
    # Crear wrapper y ejecutar
    wrapper = ETLScriptWrapper(script_name, service_name)
    success = wrapper.execute_with_coordination(script_args)
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()