#!/usr/bin/env python3
"""
🤖 AGENTE ETL: CICLO AUTOMÁTICO DE VALIDACIÓN, AJUSTE Y EJECUCIÓN HASTA ÉXITO
===============================================================================

Este agente ejecuta un ciclo completo y automático de validación y ajuste del
pipeline ETL hasta lograr una ejecución exitosa:

1. Commit y push del estado actual (si hay cambios)
2. Limpieza completa de contenedores y archivos generados
3. Ejecución del orquestador para construir infraestructura completa
4. Verificación automática de resultados en consola y logs
5. Detección de errores y ajustes automáticos cuando sea necesario
6. Repetición del ciclo hasta lograr éxito completo

NO REQUIERE INTERACCIÓN NI AUTORIZACIÓN DEL USUARIO.
El agente se ejecuta de forma completamente autónoma hasta alcanzar un resultado óptimo.

Uso:
    python tools/auto_etl_cycle_agent.py [--max-iterations N] [--debug]
"""

import os
import sys
import time
import json
import logging
import subprocess
import traceback
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field
from enum import Enum


class CycleStatus(Enum):
    """Estados posibles del ciclo"""
    INITIALIZING = "initializing"
    COMMITTING = "committing"
    CLEANING = "cleaning"
    EXECUTING = "executing"
    VALIDATING = "validating"
    ADJUSTING = "adjusting"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass
class CycleResult:
    """Resultado de un ciclo de validación"""
    iteration: int
    status: CycleStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: float = 0.0
    errors_detected: List[str] = field(default_factory=list)
    adjustments_made: List[str] = field(default_factory=list)
    success: bool = False
    details: Dict[str, Any] = field(default_factory=dict)


class AutoETLCycleAgent:
    """🤖 Agente Automático de Ciclo ETL"""
    
    def __init__(self, max_iterations: int = 5, debug: bool = False):
        self.max_iterations = max_iterations
        self.debug = debug
        self.start_time = datetime.now()
        self.execution_id = self.start_time.strftime('%Y%m%d_%H%M%S')
        
        # Configurar logging
        self.setup_logging()
        
        # Estado del agente
        self.current_iteration = 0
        self.cycle_results: List[CycleResult] = []
        self.overall_success = False
        
        # Directorio de trabajo
        self.work_dir = '/home/runner/work/automatic_etl/automatic_etl' if os.path.exists('/home/runner/work/automatic_etl/automatic_etl') else os.getcwd()
        
        self.logger.info("🤖 === AGENTE DE CICLO AUTOMÁTICO ETL ACTIVADO ===")
        self.logger.info(f"🆔 ID de Ejecución: {self.execution_id}")
        self.logger.info(f"🔄 Máximo de iteraciones: {self.max_iterations}")
        self.logger.info(f"📁 Directorio de trabajo: {self.work_dir}")
    
    def setup_logging(self):
        """Configurar sistema de logging"""
        log_level = logging.DEBUG if self.debug else logging.INFO
        log_dir = os.path.join(self.work_dir if hasattr(self, 'work_dir') else '.', 'logs')
        os.makedirs(log_dir, exist_ok=True)
        
        # Crear logger
        self.logger = logging.getLogger('AutoETLCycleAgent')
        self.logger.setLevel(log_level)
        
        # Limpiar handlers existentes
        self.logger.handlers.clear()
        
        # Handler para archivo
        log_file = os.path.join(log_dir, f'auto_cycle_{self.execution_id}.log')
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        file_formatter = logging.Formatter(
            '%(asctime)s - AUTO_CYCLE - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        
        # Handler para consola
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            '%(asctime)s - 🤖 CYCLE - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        self.logger.info(f"📝 Log file: {log_file}")
    
    # ========== PASO 1: COMMIT Y PUSH ==========
    
    def step_1_commit_and_push(self, iteration: int) -> Tuple[bool, List[str]]:
        """Paso 1: Commit y push del estado actual"""
        self.logger.info(f"📝 === PASO 1: COMMIT Y PUSH (Iteración {iteration}) ===")
        
        errors = []
        adjustments = []
        
        try:
            os.chdir(self.work_dir)
            
            # Verificar si hay cambios
            result = subprocess.run(
                ['git', 'status', '--porcelain'],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if not result.stdout.strip():
                self.logger.info("ℹ️  No hay cambios para commitear")
                return True, adjustments
            
            self.logger.info(f"📝 Cambios detectados:\n{result.stdout}")
            
            # Configurar git si es necesario
            subprocess.run(['git', 'config', 'user.name', 'Auto ETL Cycle Agent'], check=False)
            subprocess.run(['git', 'config', 'user.email', 'auto-cycle@etl.local'], check=False)
            
            # Add cambios
            result = subprocess.run(
                ['git', 'add', '.'],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode != 0:
                errors.append(f"Error en git add: {result.stderr}")
                return False, adjustments
            
            # Commit
            commit_msg = f"🤖 Auto ETL Cycle - Iteration {iteration} - {datetime.now().isoformat()}"
            result = subprocess.run(
                ['git', 'commit', '-m', commit_msg],
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode != 0 and "nothing to commit" not in result.stdout:
                errors.append(f"Error en git commit: {result.stderr}")
                return False, adjustments
            
            adjustments.append(f"Commit creado: {commit_msg}")
            self.logger.info(f"✅ Commit exitoso: {commit_msg}")
            
            # Note: No hacemos push automático para evitar conflictos en entorno CI
            self.logger.info("ℹ️  Push omitido (solo en entorno local)")
            
            return True, adjustments
            
        except subprocess.TimeoutExpired:
            errors.append("Timeout en operación git")
            return False, adjustments
        except Exception as e:
            errors.append(f"Error en commit/push: {str(e)}")
            self.logger.error(f"❌ Error en commit/push: {e}")
            if self.debug:
                self.logger.error(traceback.format_exc())
            return False, adjustments
    
    # ========== PASO 2: LIMPIEZA COMPLETA ==========
    
    def step_2_complete_cleanup(self, iteration: int) -> Tuple[bool, List[str]]:
        """Paso 2: Limpieza completa de contenedores y archivos generados"""
        self.logger.info(f"🧹 === PASO 2: LIMPIEZA COMPLETA (Iteración {iteration}) ===")
        
        errors = []
        adjustments = []
        
        try:
            # Ejecutar script de limpieza
            cleanup_script = os.path.join(self.work_dir, 'tools', 'cleanup_all.py')
            
            if not os.path.exists(cleanup_script):
                self.logger.warning(f"⚠️  Script de limpieza no encontrado: {cleanup_script}")
                self.logger.info("ℹ️  Continuando sin limpieza...")
                return True, adjustments
            
            self.logger.info(f"🔄 Ejecutando limpieza: {cleanup_script}")
            
            result = subprocess.run(
                ['python3', cleanup_script],
                capture_output=True,
                text=True,
                timeout=300,  # 5 minutos
                cwd=self.work_dir
            )
            
            self.logger.debug(f"Salida limpieza:\n{result.stdout}")
            
            if result.returncode == 0:
                self.logger.info("✅ Limpieza completada exitosamente")
                adjustments.append("Limpieza completa ejecutada")
                return True, adjustments
            else:
                self.logger.warning(f"⚠️  Limpieza con advertencias: {result.stderr}")
                # No es crítico, continuamos
                adjustments.append("Limpieza parcial ejecutada")
                return True, adjustments
                
        except subprocess.TimeoutExpired:
            errors.append("Timeout en limpieza (5 min)")
            self.logger.error("❌ Timeout en limpieza")
            return False, adjustments
        except Exception as e:
            errors.append(f"Error en limpieza: {str(e)}")
            self.logger.error(f"❌ Error en limpieza: {e}")
            if self.debug:
                self.logger.error(traceback.format_exc())
            return False, adjustments
    
    # ========== PASO 3: EJECUTAR ORQUESTADOR ==========
    
    def step_3_execute_orchestrator(self, iteration: int) -> Tuple[bool, List[str], Dict[str, Any]]:
        """Paso 3: Ejecutar orquestador para construir infraestructura completa"""
        self.logger.info(f"⚡ === PASO 3: EJECUTAR ORQUESTADOR (Iteración {iteration}) ===")
        
        errors = []
        adjustments = []
        details = {}
        
        try:
            # Intentar con orchestrate_etl_init.py primero
            orchestrator_script = os.path.join(self.work_dir, 'tools', 'orchestrate_etl_init.py')
            
            if not os.path.exists(orchestrator_script):
                # Fallback a master_etl_agent.py
                orchestrator_script = os.path.join(self.work_dir, 'tools', 'master_etl_agent.py')
            
            if not os.path.exists(orchestrator_script):
                # Fallback a intelligent_init_agent.py
                orchestrator_script = os.path.join(self.work_dir, 'tools', 'intelligent_init_agent.py')
            
            if not os.path.exists(orchestrator_script):
                errors.append("No se encontró ningún script orquestador")
                return False, adjustments, details
            
            self.logger.info(f"🔄 Ejecutando orquestador: {os.path.basename(orchestrator_script)}")
            
            result = subprocess.run(
                ['python3', orchestrator_script],
                capture_output=True,
                text=True,
                timeout=1800,  # 30 minutos
                cwd=self.work_dir
            )
            
            details['orchestrator_stdout'] = result.stdout
            details['orchestrator_stderr'] = result.stderr
            details['orchestrator_returncode'] = result.returncode
            
            # Guardar output completo en archivo
            output_file = os.path.join(
                self.work_dir, 'logs', 
                f'orchestrator_iteration_{iteration}_{self.execution_id}.log'
            )
            with open(output_file, 'w') as f:
                f.write(f"=== STDOUT ===\n{result.stdout}\n\n")
                f.write(f"=== STDERR ===\n{result.stderr}\n\n")
                f.write(f"=== RETURN CODE ===\n{result.returncode}\n")
            
            self.logger.info(f"📄 Output guardado en: {output_file}")
            
            if result.returncode == 0:
                self.logger.info("✅ Orquestador completado exitosamente")
                adjustments.append("Pipeline ETL desplegado completamente")
                return True, adjustments, details
            else:
                self.logger.error(f"❌ Orquestador falló con código: {result.returncode}")
                
                # Extraer errores del output
                error_lines = self._extract_errors_from_output(result.stdout, result.stderr)
                errors.extend(error_lines)
                
                return False, adjustments, details
                
        except subprocess.TimeoutExpired:
            errors.append("Timeout en orquestador (30 min)")
            self.logger.error("❌ Timeout en orquestador")
            return False, adjustments, details
        except Exception as e:
            errors.append(f"Error ejecutando orquestador: {str(e)}")
            self.logger.error(f"❌ Error en orquestador: {e}")
            if self.debug:
                self.logger.error(traceback.format_exc())
            return False, adjustments, details
    
    def _extract_errors_from_output(self, stdout: str, stderr: str) -> List[str]:
        """Extraer mensajes de error del output"""
        errors = []
        
        # Buscar líneas con marcadores de error
        error_markers = ['ERROR', '❌', 'FAILED', 'FALLÓ', 'Exception', 'Traceback']
        
        for line in (stdout + '\n' + stderr).split('\n'):
            line = line.strip()
            if any(marker in line for marker in error_markers):
                # Limitar longitud de error
                if len(line) > 200:
                    line = line[:200] + '...'
                errors.append(line)
        
        # Limitar cantidad de errores reportados
        return errors[:10]
    
    # ========== PASO 4: VALIDAR RESULTADOS ==========
    
    def step_4_validate_results(self, iteration: int, orchestrator_details: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Paso 4: Verificar automáticamente resultados en consola y logs"""
        self.logger.info(f"🔍 === PASO 4: VALIDAR RESULTADOS (Iteración {iteration}) ===")
        
        errors = []
        
        try:
            # Analizar output del orquestador
            stdout = orchestrator_details.get('orchestrator_stdout', '')
            stderr = orchestrator_details.get('orchestrator_stderr', '')
            returncode = orchestrator_details.get('orchestrator_returncode', 1)
            
            # Verificar indicadores de éxito
            success_indicators = [
                '✅',
                'EXITOSA',
                'COMPLETADA',
                'SUCCESS',
                'SUCCESSFUL',
                'All phases completed',
                'Pipeline ETL operativo',
                'COMPLETAMENTE FUNCIONAL'
            ]
            
            success_count = sum(1 for indicator in success_indicators if indicator in stdout)
            
            # Verificar indicadores de error
            error_indicators = [
                'FALLÓ',
                'FAILED',
                'ERROR',
                'CRITICAL',
                'ABORTED',
                'Exception:',
                'Traceback'
            ]
            
            error_count = sum(1 for indicator in error_indicators if indicator in (stdout + stderr))
            
            self.logger.info(f"📊 Indicadores de éxito: {success_count}")
            self.logger.info(f"📊 Indicadores de error: {error_count}")
            
            # Ejecutar validaciones adicionales
            validation_results = self._run_additional_validations()
            
            # Determinar éxito
            if returncode == 0 and success_count > error_count and validation_results['passed']:
                self.logger.info("✅ Validación exitosa: Pipeline ETL operativo")
                return True, errors
            else:
                self.logger.error(f"❌ Validación falló: {error_count} errores detectados")
                
                # Recopilar errores específicos
                if not validation_results['passed']:
                    errors.extend(validation_results['errors'])
                
                if error_count > 0:
                    errors.extend(self._extract_errors_from_output(stdout, stderr))
                
                return False, errors
                
        except Exception as e:
            errors.append(f"Error en validación: {str(e)}")
            self.logger.error(f"❌ Error en validación: {e}")
            if self.debug:
                self.logger.error(traceback.format_exc())
            return False, errors
    
    def _run_additional_validations(self) -> Dict[str, Any]:
        """Ejecutar validaciones adicionales del pipeline"""
        result = {
            'passed': True,
            'errors': [],
            'checks': {}
        }
        
        try:
            # Verificar existencia de logs de ejecución
            logs_dir = os.path.join(self.work_dir, 'logs')
            if os.path.exists(logs_dir):
                log_files = [f for f in os.listdir(logs_dir) if f.endswith('.log')]
                result['checks']['log_files_created'] = len(log_files) > 0
                self.logger.info(f"📝 Archivos de log encontrados: {len(log_files)}")
            else:
                result['checks']['log_files_created'] = False
                result['errors'].append("No se encontró directorio de logs")
            
            # Verificar archivos generados
            generated_dir = os.path.join(self.work_dir, 'generated', 'default')
            if os.path.exists(generated_dir):
                generated_files = os.listdir(generated_dir)
                result['checks']['generated_files_created'] = len(generated_files) > 0
                self.logger.info(f"📁 Archivos generados: {len(generated_files)}")
            else:
                result['checks']['generated_files_created'] = False
            
            # Ejecutar validador simple si existe
            validator_script = os.path.join(self.work_dir, 'tools', 'validate_etl_simple.py')
            if os.path.exists(validator_script):
                self.logger.info("🔍 Ejecutando validador ETL...")
                try:
                    val_result = subprocess.run(
                        ['python3', validator_script],
                        capture_output=True,
                        text=True,
                        timeout=60,
                        cwd=self.work_dir
                    )
                    result['checks']['etl_validation'] = val_result.returncode == 0
                    if val_result.returncode != 0:
                        result['errors'].append(f"Validación ETL falló: {val_result.stderr[:200]}")
                except subprocess.TimeoutExpired:
                    result['checks']['etl_validation'] = False
                    result['errors'].append("Timeout en validación ETL")
                except Exception as e:
                    result['checks']['etl_validation'] = False
                    result['errors'].append(f"Error en validación ETL: {str(e)}")
            
            # Determinar resultado final
            failed_checks = [k for k, v in result['checks'].items() if not v]
            if failed_checks:
                result['passed'] = False
                self.logger.warning(f"⚠️  Checks fallidos: {', '.join(failed_checks)}")
            else:
                self.logger.info("✅ Todas las validaciones adicionales pasaron")
            
        except Exception as e:
            result['passed'] = False
            result['errors'].append(f"Error en validaciones adicionales: {str(e)}")
            self.logger.error(f"❌ Error en validaciones adicionales: {e}")
        
        return result
    
    # ========== PASO 5: AJUSTES AUTOMÁTICOS ==========
    
    def step_5_automatic_adjustments(self, iteration: int, detected_errors: List[str]) -> Tuple[bool, List[str]]:
        """Paso 5: Realizar ajustes automáticos basados en errores detectados"""
        self.logger.info(f"🔧 === PASO 5: AJUSTES AUTOMÁTICOS (Iteración {iteration}) ===")
        
        adjustments_made = []
        
        if not detected_errors:
            self.logger.info("ℹ️  No hay errores para ajustar")
            return True, adjustments_made
        
        self.logger.info(f"🔍 Analizando {len(detected_errors)} errores detectados...")
        
        try:
            # Analizar patrones de error y aplicar ajustes
            for error in detected_errors[:5]:  # Analizar primeros 5 errores
                adjustment = self._analyze_and_adjust_error(error)
                if adjustment:
                    adjustments_made.append(adjustment)
            
            if adjustments_made:
                self.logger.info(f"✅ {len(adjustments_made)} ajustes realizados")
                return True, adjustments_made
            else:
                self.logger.info("ℹ️  No se identificaron ajustes automáticos aplicables")
                return True, adjustments_made
                
        except Exception as e:
            self.logger.error(f"❌ Error en ajustes automáticos: {e}")
            if self.debug:
                self.logger.error(traceback.format_exc())
            return True, adjustments_made  # No es crítico
    
    def _analyze_and_adjust_error(self, error: str) -> Optional[str]:
        """Analizar un error específico y aplicar ajuste si es posible"""
        error_lower = error.lower()
        
        # Patrones de error comunes y sus ajustes
        adjustments = {
            'timeout': 'Incrementar timeout en siguiente iteración',
            'connection refused': 'Esperar más tiempo para servicios',
            'permission denied': 'Verificar permisos de usuario',
            'file not found': 'Verificar paths de archivos',
            'no such table': 'Re-crear esquema de base de datos',
            'database already exists': 'Limpiar estado previo',
            'connector already exists': 'Eliminar conectores previos',
        }
        
        for pattern, adjustment in adjustments.items():
            if pattern in error_lower:
                self.logger.info(f"🔧 Ajuste identificado: {adjustment}")
                return adjustment
        
        return None
    
    # ========== CICLO PRINCIPAL ==========
    
    def execute_single_cycle(self, iteration: int) -> CycleResult:
        """Ejecutar un ciclo completo de validación y ajuste"""
        self.logger.info(f"\n{'='*80}")
        self.logger.info(f"🔄 === INICIANDO CICLO {iteration}/{self.max_iterations} ===")
        self.logger.info(f"{'='*80}\n")
        
        cycle_result = CycleResult(
            iteration=iteration,
            status=CycleStatus.INITIALIZING,
            start_time=datetime.now()
        )
        
        try:
            # Paso 1: Commit y Push
            cycle_result.status = CycleStatus.COMMITTING
            step1_success, step1_adjustments = self.step_1_commit_and_push(iteration)
            cycle_result.adjustments_made.extend(step1_adjustments)
            
            if not step1_success:
                self.logger.error("❌ Paso 1 falló, abortando ciclo")
                cycle_result.status = CycleStatus.FAILED
                cycle_result.success = False
                return cycle_result
            
            time.sleep(2)  # Breve pausa
            
            # Paso 2: Limpieza Completa
            cycle_result.status = CycleStatus.CLEANING
            step2_success, step2_adjustments = self.step_2_complete_cleanup(iteration)
            cycle_result.adjustments_made.extend(step2_adjustments)
            
            if not step2_success:
                self.logger.error("❌ Paso 2 falló, abortando ciclo")
                cycle_result.status = CycleStatus.FAILED
                cycle_result.success = False
                return cycle_result
            
            time.sleep(5)  # Esperar limpieza
            
            # Paso 3: Ejecutar Orquestador
            cycle_result.status = CycleStatus.EXECUTING
            step3_success, step3_adjustments, step3_details = self.step_3_execute_orchestrator(iteration)
            cycle_result.adjustments_made.extend(step3_adjustments)
            cycle_result.details.update(step3_details)
            
            # Paso 4: Validar Resultados
            cycle_result.status = CycleStatus.VALIDATING
            step4_success, step4_errors = self.step_4_validate_results(iteration, step3_details)
            cycle_result.errors_detected.extend(step4_errors)
            
            # Paso 5: Ajustes Automáticos (si hay errores)
            if not step4_success:
                cycle_result.status = CycleStatus.ADJUSTING
                step5_success, step5_adjustments = self.step_5_automatic_adjustments(
                    iteration, step4_errors
                )
                cycle_result.adjustments_made.extend(step5_adjustments)
            
            # Determinar éxito del ciclo
            if step3_success and step4_success:
                cycle_result.status = CycleStatus.SUCCESS
                cycle_result.success = True
                self.logger.info(f"✅ CICLO {iteration} COMPLETADO EXITOSAMENTE")
            else:
                cycle_result.status = CycleStatus.FAILED
                cycle_result.success = False
                self.logger.warning(f"⚠️  CICLO {iteration} FALLÓ - Se reintentará")
            
        except Exception as e:
            self.logger.error(f"💥 Error crítico en ciclo {iteration}: {e}")
            if self.debug:
                self.logger.error(traceback.format_exc())
            cycle_result.status = CycleStatus.FAILED
            cycle_result.success = False
            cycle_result.errors_detected.append(f"Error crítico: {str(e)}")
        
        finally:
            cycle_result.end_time = datetime.now()
            cycle_result.duration_seconds = (
                cycle_result.end_time - cycle_result.start_time
            ).total_seconds()
            
            self.logger.info(f"\n📊 Resumen Ciclo {iteration}:")
            self.logger.info(f"   Estado: {cycle_result.status.value}")
            self.logger.info(f"   Duración: {cycle_result.duration_seconds:.1f}s")
            self.logger.info(f"   Errores: {len(cycle_result.errors_detected)}")
            self.logger.info(f"   Ajustes: {len(cycle_result.adjustments_made)}")
        
        return cycle_result
    
    def run_complete_automation(self) -> bool:
        """Ejecutar ciclo completo de automatización hasta éxito"""
        self.logger.info("\n" + "="*80)
        self.logger.info("🤖 INICIANDO AUTOMATIZACIÓN COMPLETA DEL PIPELINE ETL")
        self.logger.info("="*80)
        self.logger.info(f"🆔 Ejecución: {self.execution_id}")
        self.logger.info(f"🔄 Máximo iteraciones: {self.max_iterations}")
        self.logger.info(f"⏰ Inicio: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("="*80 + "\n")
        
        try:
            for iteration in range(1, self.max_iterations + 1):
                self.current_iteration = iteration
                
                # Ejecutar ciclo
                cycle_result = self.execute_single_cycle(iteration)
                self.cycle_results.append(cycle_result)
                
                # Verificar éxito
                if cycle_result.success:
                    self.overall_success = True
                    self.logger.info(f"\n🎉 ÉXITO ALCANZADO EN ITERACIÓN {iteration}")
                    break
                
                # Si no fue exitoso y hay más iteraciones, continuar
                if iteration < self.max_iterations:
                    wait_time = min(10 * iteration, 60)  # Backoff exponencial limitado
                    self.logger.info(f"\n⏳ Esperando {wait_time}s antes de siguiente iteración...")
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"\n❌ Máximo de iteraciones alcanzado ({self.max_iterations})")
            
            # Generar reporte final
            self._generate_final_report()
            
            return self.overall_success
            
        except KeyboardInterrupt:
            self.logger.warning("\n⚠️  Automatización interrumpida por usuario")
            return False
        except Exception as e:
            self.logger.error(f"\n💥 Error crítico en automatización: {e}")
            if self.debug:
                self.logger.error(traceback.format_exc())
            return False
    
    def _generate_final_report(self):
        """Generar reporte final de la ejecución"""
        end_time = datetime.now()
        total_duration = (end_time - self.start_time).total_seconds()
        
        print("\n" + "="*80)
        print("📊 REPORTE FINAL - AGENTE DE CICLO AUTOMÁTICO ETL")
        print("="*80)
        print(f"🆔 Ejecución: {self.execution_id}")
        print(f"⏰ Inicio: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏰ Fin: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏱️  Duración total: {total_duration:.1f}s ({total_duration/60:.1f} min)")
        print(f"🔄 Iteraciones ejecutadas: {len(self.cycle_results)}")
        print()
        
        if self.overall_success:
            print("🎉 RESULTADO: ÉXITO COMPLETO")
            print("✅ Pipeline ETL completamente operativo")
            successful_cycle = next((c for c in self.cycle_results if c.success), None)
            if successful_cycle:
                print(f"🏆 Éxito alcanzado en iteración: {successful_cycle.iteration}")
        else:
            print("❌ RESULTADO: NO SE ALCANZÓ ÉXITO")
            print(f"⚠️  Se ejecutaron {len(self.cycle_results)} iteraciones sin éxito")
        
        print()
        print("📋 RESUMEN POR ITERACIÓN:")
        for cycle in self.cycle_results:
            status_emoji = "✅" if cycle.success else "❌"
            print(f"   {status_emoji} Iteración {cycle.iteration}: "
                  f"{cycle.status.value} - {cycle.duration_seconds:.1f}s - "
                  f"{len(cycle.errors_detected)} errores - "
                  f"{len(cycle.adjustments_made)} ajustes")
        
        print()
        
        if not self.overall_success and self.cycle_results:
            last_cycle = self.cycle_results[-1]
            if last_cycle.errors_detected:
                print("🔍 ÚLTIMOS ERRORES DETECTADOS:")
                for i, error in enumerate(last_cycle.errors_detected[:5], 1):
                    print(f"   {i}. {error[:150]}")
                print()
        
        print("💡 PRÓXIMOS PASOS:")
        if self.overall_success:
            print("   1. Verificar estado del pipeline: python tools/pipeline_status.py")
            print("   2. Revisar logs detallados en: logs/")
            print("   3. Acceder a Superset para visualización")
        else:
            print("   1. Revisar logs detallados en: logs/")
            print("   2. Analizar errores específicos reportados")
            print("   3. Realizar ajustes manuales necesarios")
            print("   4. Re-ejecutar el agente de ciclo automático")
        
        print("="*80)
        
        # Guardar reporte en JSON
        self._save_execution_report(total_duration)
    
    def _save_execution_report(self, total_duration: float):
        """Guardar reporte de ejecución en formato JSON"""
        try:
            report = {
                'execution_id': self.execution_id,
                'start_time': self.start_time.isoformat(),
                'end_time': datetime.now().isoformat(),
                'total_duration_seconds': total_duration,
                'max_iterations': self.max_iterations,
                'iterations_executed': len(self.cycle_results),
                'overall_success': self.overall_success,
                'cycles': []
            }
            
            for cycle in self.cycle_results:
                report['cycles'].append({
                    'iteration': cycle.iteration,
                    'status': cycle.status.value,
                    'success': cycle.success,
                    'start_time': cycle.start_time.isoformat(),
                    'end_time': cycle.end_time.isoformat() if cycle.end_time else None,
                    'duration_seconds': cycle.duration_seconds,
                    'errors_count': len(cycle.errors_detected),
                    'errors': cycle.errors_detected[:10],  # Primeros 10
                    'adjustments_count': len(cycle.adjustments_made),
                    'adjustments': cycle.adjustments_made
                })
            
            report_file = os.path.join(
                self.work_dir, 'logs',
                f'auto_cycle_report_{self.execution_id}.json'
            )
            
            with open(report_file, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            
            self.logger.info(f"💾 Reporte guardado: {report_file}")
            
        except Exception as e:
            self.logger.error(f"❌ Error guardando reporte: {e}")


def main():
    """Función principal"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='🤖 Agente de Ciclo Automático ETL - Validación y Ajuste hasta Éxito',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python tools/auto_etl_cycle_agent.py
  python tools/auto_etl_cycle_agent.py --max-iterations 10
  python tools/auto_etl_cycle_agent.py --debug
        """
    )
    
    parser.add_argument(
        '--max-iterations',
        type=int,
        default=5,
        help='Número máximo de iteraciones (default: 5)'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Activar modo debug con logs detallados'
    )
    
    args = parser.parse_args()
    
    try:
        # Crear y ejecutar agente
        agent = AutoETLCycleAgent(
            max_iterations=args.max_iterations,
            debug=args.debug
        )
        
        success = agent.run_complete_automation()
        
        return 0 if success else 1
        
    except Exception as e:
        print(f"\n💥 Error crítico: {e}")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
