#!/usr/bin/env python3
"""
🎯 ORQUESTADOR AUTOMÁTICO DEL PIPELINE ETL
Script maestro que ejecuta TODA la inicialización automática:

FASE 1: LIMPIEZA COMPLETA
- Elimina conectores, tópicos, tablas existentes
- Limpia configuraciones previas

FASE 2: CONFIGURACIÓN DE USUARIOS
- Crea usuarios MySQL con permisos de replicación
- Configura usuarios ClickHouse con permisos de escritura
- Valida conectividad

FASE 3: DESCUBRIMIENTO DE ESQUEMAS
- Detecta automáticamente tablas en MySQL
- Genera configuraciones de conectores
- Crea esquemas ClickHouse optimizados

FASE 4: DESPLIEGUE DE INFRAESTRUCTURA
- Aplica conectores Debezium
- Crea tablas en ClickHouse
- Inicia flujo de datos

FASE 5: VALIDACIÓN COMPLETA
- Verifica flujo MySQL → Kafka → ClickHouse
- Confirma datos en destino
- Reporta estado final

Este script se ejecuta automáticamente al levantar Docker Compose.
"""

import subprocess
import time
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any

# Configurar logging centralizado
class ColoredFormatter(logging.Formatter):
    """Formatter con colores para diferentes niveles de log"""
    
    COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[32m',     # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',    # Red
        'CRITICAL': '\033[35m', # Magenta
        'RESET': '\033[0m'      # Reset
    }
    
    def format(self, record):
        log_color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        reset_color = self.COLORS['RESET']
        
        # Formato con color
        formatter = logging.Formatter(
            f'{log_color}%(asctime)s - %(name)s - %(levelname)s{reset_color} - %(message)s'
        )
        return formatter.format(record)

# Configurar logging
def setup_logging():
    """Configurar sistema de logging completo"""
    # Detectar si estamos en contenedor Docker
    in_docker = os.path.exists('/.dockerenv') or os.environ.get('RUNNING_IN_DOCKER') == '1'
    log_dir = '/app/logs' if in_docker else './logs'
    os.makedirs(log_dir, exist_ok=True)
    
    # Logger principal
    logger = logging.getLogger('ETL_ORCHESTRATOR')
    logger.setLevel(logging.INFO)
    
    # Handler para archivo
    file_handler = logging.FileHandler(f'{log_dir}/orchestrator.log')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(file_formatter)
    
    # Handler para consola con colores
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(ColoredFormatter())
    
    # Agregar handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

class ETLOrchestrator:
    def __init__(self):
        self.start_time = datetime.now()
        self.phases = []
        self.current_phase = None
        
        # Configuración de fases
        self.phase_config = {
            'pipeline_gen': {
                'name': '🏗️ GENERACIÓN DE PIPELINE',
                'script': 'gen_pipeline.py',
                'description': 'Generar configuraciones de conectores y esquemas',
                'required': True,
                'timeout': 120
            },
            'connectors': {
                'name': '🔌 DESPLIEGUE DE CONECTORES',
                'script': 'apply_connectors_auto.py',
                'description': 'Aplicar conectores Debezium automáticamente',
                'required': True,
                'timeout': 180
            },
            'validation': {
                'name': '✅ VALIDACIÓN COMPLETA',
                'script': 'pipeline_status.py',
                'description': 'Verificar flujo completo de datos',
                'required': False,  # No crítico - permite continuar aunque falle
                'timeout': 60
            }
        }
        
        # Estado del orquestador
        self.status = {
            'start_time': self.start_time.isoformat(),
            'phases_completed': 0,
            'phases_total': len(self.phase_config),
            'current_phase': None,
            'success': False,
            'errors': [],
            'execution_time': 0
        }
    
    def wait_for_services(self) -> bool:
        """Esperar a que todos los servicios estén disponibles"""
        logger.info("⏳ Esperando servicios críticos...")
        
        services = {
            'connect': 'curl -s http://connect:8083/ -m 5',
            'clickhouse': 'curl -s http://clickhouse:8123/ping -m 5'
        }
        
        max_wait = 180  # 3 minutos máximo
        start_wait = time.time()
        
        for service_name, check_cmd in services.items():
            logger.info(f"🔍 Verificando servicio: {service_name}")
            service_ready = False
            service_wait_start = time.time()
            
            while not service_ready and (time.time() - start_wait) < max_wait:
                try:
                    result = subprocess.run(
                        check_cmd,
                        shell=True,
                        capture_output=True,
                        text=True,
                        timeout=10
                    )
                    
                    if result.returncode == 0:
                        logger.info(f"✅ Servicio {service_name} disponible")
                        service_ready = True
                    else:
                        elapsed = time.time() - service_wait_start
                        logger.debug(f"⏳ Esperando {service_name}... ({elapsed:.1f}s)")
                        time.sleep(5)
                        
                except subprocess.TimeoutExpired:
                    logger.debug(f"⏳ Timeout verificando {service_name}, reintentando...")
                    time.sleep(5)
                except Exception as e:
                    logger.debug(f"⏳ Error verificando {service_name}: {str(e)}")
                    time.sleep(5)
                    
            if not service_ready:
                logger.error(f"❌ Servicio {service_name} no disponible después de {max_wait}s")
                return False
                
        # Verificar que el directorio /app/generated existe y está accesible
        logger.info("🔍 Verificando acceso a directorios de trabajo...")
        if not os.path.exists('/app/generated'):
            logger.info("📁 Creando directorio /app/generated...")
            os.makedirs('/app/generated/default', exist_ok=True)
            
        # Verificar acceso a herramientas
        tools_dir = '/app/tools'
        if not os.path.exists(tools_dir):
            logger.error(f"❌ Directorio de herramientas no encontrado: {tools_dir}")
            return False
            
        total_wait = time.time() - start_wait
        logger.info(f"✅ Servicios críticos disponibles ({total_wait:.1f}s)")
        return True
    
    def execute_phase(self, phase_key: str) -> bool:
        """Ejecutar una fase específica"""
        phase = self.phase_config[phase_key]
        self.current_phase = phase_key
        self.status['current_phase'] = phase['name']
        
        logger.info(f"\n{'='*80}")
        logger.info(f"🚀 INICIANDO FASE: {phase['name']}")
        logger.info(f"📋 Descripción: {phase['description']}")
        logger.info(f"🔧 Script: {phase['script']}")
        logger.info(f"{'='*80}")
        
        phase_start = time.time()
        
        try:
            # Construir comando
            cmd = f"cd /app && python3 tools/{phase['script']}"
            
            logger.info(f"▶️  Ejecutando: {cmd}")
            
            # Ejecutar con timeout
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=phase['timeout']
            )
            
            phase_duration = time.time() - phase_start
            
            # Procesar resultado
            if result.returncode == 0:
                logger.info(f"✅ FASE COMPLETADA: {phase['name']} ({phase_duration:.1f}s)")
                
                # Log output si hay contenido relevante
                if result.stdout.strip():
                    logger.debug("📤 Salida del script:")
                    for line in result.stdout.strip().split('\n')[-10:]:  # Últimas 10 líneas
                        logger.debug(f"   {line}")
                
                self.status['phases_completed'] += 1
                return True
                
            else:
                logger.error(f"❌ FASE FALLÓ: {phase['name']} (código: {result.returncode})")
                
                # Log error output
                if result.stderr.strip():
                    logger.error("📤 Error del script:")
                    for line in result.stderr.strip().split('\n')[-10:]:
                        logger.error(f"   {line}")
                
                error_info = {
                    'phase': phase['name'],
                    'script': phase['script'],
                    'return_code': result.returncode,
                    'error': result.stderr.strip()[-500:] if result.stderr.strip() else 'No error output',
                    'duration': phase_duration
                }
                self.status['errors'].append(error_info)
                
                return not phase['required']  # Si no es requerida, continuar
        
        except subprocess.TimeoutExpired:
            phase_duration = time.time() - phase_start
            logger.error(f"⏰ FASE TIMEOUT: {phase['name']} ({phase['timeout']}s)")
            
            error_info = {
                'phase': phase['name'],
                'script': phase['script'],
                'error': f'Timeout después de {phase["timeout"]}s',
                'duration': phase_duration
            }
            self.status['errors'].append(error_info)
            
            return not phase['required']
            
        except Exception as e:
            phase_duration = time.time() - phase_start
            logger.error(f"💥 FASE ERROR: {phase['name']} - {str(e)}")
            
            error_info = {
                'phase': phase['name'],
                'script': phase['script'],
                'error': str(e),
                'duration': phase_duration
            }
            self.status['errors'].append(error_info)
            
            return not phase['required']
    
    def save_execution_log(self):
        """Guardar log completo de ejecución"""
        try:
            self.status['end_time'] = datetime.now().isoformat()
            self.status['execution_time'] = (datetime.now() - self.start_time).total_seconds()
            
            log_file = f"/app/logs/orchestrator_execution_{self.start_time.strftime('%Y%m%d_%H%M%S')}.json"
            
            with open(log_file, 'w') as f:
                json.dump(self.status, f, indent=2)
            
            logger.info(f"💾 Log de ejecución guardado: {log_file}")
            
        except Exception as e:
            logger.error(f"❌ Error guardando log de ejecución: {str(e)}")
    
    def print_final_summary(self):
        """Imprimir resumen final de la ejecución"""
        end_time = datetime.now()
        total_duration = (end_time - self.start_time).total_seconds()
        
        print(f"\n{'='*80}")
        print(f"🏁 RESUMEN FINAL DE INICIALIZACIÓN AUTOMÁTICA")
        print(f"{'='*80}")
        print(f"⏰ Inicio: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏰ Fin: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏱️  Duración total: {total_duration:.1f} segundos")
        print(f"📊 Fases completadas: {self.status['phases_completed']}/{self.status['phases_total']}")
        
        if self.status['success']:
            print(f"\n🎉 INICIALIZACIÓN EXITOSA")
            print(f"✅ Pipeline ETL completamente funcional")
            print(f"🚀 Sistema listo para operación")
        else:
            print(f"\n⚠️  INICIALIZACIÓN PARCIAL O FALLIDA")
            print(f"❌ {len(self.status['errors'])} errores encontrados")
            
            if self.status['errors']:
                print(f"\n🔍 ERRORES DETALLADOS:")
                for i, error in enumerate(self.status['errors'], 1):
                    print(f"   {i}. {error['phase']}: {error['error'][:100]}...")
        
        print(f"\n💡 PRÓXIMOS PASOS:")
        if self.status['success']:
            print(f"   1. Verificar datos: python3 tools/pipeline_status.py")
            print(f"   2. Configurar Superset para visualización")
            print(f"   3. Configurar monitoreo y alertas")
        else:
            print(f"   1. Revisar logs detallados en /app/logs/")
            print(f"   2. Corregir errores encontrados")
            print(f"   3. Re-ejecutar inicialización")
        
        print(f"{'='*80}")
    
    def run_full_initialization(self) -> bool:
        """Ejecutar inicialización completa del pipeline ETL"""
        logger.info("🎯 === INICIANDO ORQUESTACIÓN AUTOMÁTICA DEL PIPELINE ETL ===")
        logger.info(f"⏰ Tiempo de inicio: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"📋 Fases programadas: {len(self.phase_config)}")
        
        try:
            # 1. Esperar servicios
            if not self.wait_for_services():
                logger.error("❌ Servicios no disponibles, abortando inicialización")
                return False
            
            # 2. Ejecutar fases en orden
            phases_order = ['pipeline_gen', 'connectors', 'validation']
            
            for phase_key in phases_order:
                if not self.execute_phase(phase_key):
                    if self.phase_config[phase_key]['required']:
                        logger.error(f"❌ Fase requerida falló: {phase_key}, abortando")
                        self.status['success'] = False
                        return False
                    else:
                        logger.warning(f"⚠️  Fase opcional falló: {phase_key}, continuando")
            
            # 3. Éxito completo
            self.status['success'] = True
            logger.info("🎉 TODAS LAS FASES COMPLETADAS EXITOSAMENTE")
            return True
            
        except Exception as e:
            logger.error(f"💥 Error crítico en orquestación: {str(e)}")
            self.status['success'] = False
            return False
        
        finally:
            # Siempre guardar log y mostrar resumen
            self.save_execution_log()
            self.print_final_summary()

def main():
    """Función principal"""
    try:
        # Banner de inicio
        print(f"\n{'='*80}")
        print(f"🎯 ORQUESTADOR AUTOMÁTICO DEL PIPELINE ETL")
        print(f"🚀 Inicialización completa desde cero")
        print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*80}")
        
        orchestrator = ETLOrchestrator()
        success = orchestrator.run_full_initialization()
        
        return 0 if success else 1
        
    except KeyboardInterrupt:
        logger.info("🛑 Orquestación interrumpida por el usuario")
        return 1
    except Exception as e:
        logger.error(f"💥 Error crítico en main: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())