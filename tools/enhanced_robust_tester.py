#!/usr/bin/env python3
"""
🛠️ SERVICE AUTO-RECOVERY SYSTEM
================================

Sistema de auto-recuperación que puede diagnosticar y arreglar
problemas comunes de servicios automáticamente.
"""

import time
import logging
import subprocess
import json
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class RecoveryAction:
    """Acción de recuperación"""
    name: str
    description: str
    command: List[str]
    timeout: int = 30
    requires_docker: bool = True

class ServiceAutoRecovery:
    """Sistema de auto-recuperación de servicios"""
    
    def __init__(self):
        self.recovery_actions = self._define_recovery_actions()
    
    def _define_recovery_actions(self) -> Dict[str, List[RecoveryAction]]:
        """Define las acciones de recuperación por servicio"""
        return {
            "kafka": [
                RecoveryAction(
                    name="restart_kafka",
                    description="Reiniciar contenedor Kafka",
                    command=["docker", "compose", "restart", "kafka"],
                    timeout=60
                ),
                RecoveryAction(
                    name="recreate_kafka",
                    description="Recrear contenedor Kafka",
                    command=["docker", "compose", "up", "-d", "--force-recreate", "kafka"],
                    timeout=120
                )
            ],
            "clickhouse": [
                RecoveryAction(
                    name="restart_clickhouse", 
                    description="Reiniciar contenedor ClickHouse",
                    command=["docker", "compose", "restart", "clickhouse"],
                    timeout=60
                ),
                RecoveryAction(
                    name="reset_clickhouse_data",
                    description="Limpiar datos temporales de ClickHouse",
                    command=["docker", "exec", "clickhouse", "rm", "-rf", "/var/lib/clickhouse/tmp/*"],
                    timeout=30
                ),
                RecoveryAction(
                    name="recreate_clickhouse",
                    description="Recrear contenedor ClickHouse",
                    command=["docker", "compose", "up", "-d", "--force-recreate", "clickhouse"],
                    timeout=120
                )
            ],
            "superset": [
                RecoveryAction(
                    name="restart_superset",
                    description="Reiniciar Superset",
                    command=["docker", "compose", "restart", "superset"],
                    timeout=60
                ),
                RecoveryAction(
                    name="rebuild_superset_cache",
                    description="Limpiar cache de Superset", 
                    command=["docker", "exec", "superset", "superset", "cache", "clear"],
                    timeout=30
                )
            ],
            "metabase": [
                RecoveryAction(
                    name="restart_metabase",
                    description="Reiniciar Metabase",
                    command=["docker", "compose", "restart", "metabase"],
                    timeout=60
                ),
                RecoveryAction(
                    name="reset_metabase_cache",
                    description="Limpiar cache interno de Metabase",
                    command=["docker", "exec", "metabase", "rm", "-rf", "/tmp/*"],
                    timeout=30
                )
            ],
            "connect": [
                RecoveryAction(
                    name="restart_connect",
                    description="Reiniciar Kafka Connect", 
                    command=["docker", "compose", "restart", "connect"],
                    timeout=60
                ),
                RecoveryAction(
                    name="recreate_connect",
                    description="Recrear Kafka Connect",
                    command=["docker", "compose", "up", "-d", "--force-recreate", "connect"],
                    timeout=120
                )
            ]
        }
    
    def execute_recovery_action(self, action: RecoveryAction) -> bool:
        """Ejecuta una acción de recuperación"""
        logger.info(f"🔧 Ejecutando: {action.description}")
        
        try:
            result = subprocess.run(
                action.command,
                capture_output=True,
                text=True,
                timeout=action.timeout
            )
            
            if result.returncode == 0:
                logger.info(f"✅ Acción exitosa: {action.name}")
                return True
            else:
                logger.error(f"❌ Acción falló: {action.name} - {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error(f"⏰ Acción timeout: {action.name}")
            return False
        except Exception as e:
            logger.error(f"💥 Error ejecutando {action.name}: {e}")
            return False
    
    def recover_service(self, service: str, max_actions: int = 2) -> bool:
        """Intenta recuperar un servicio usando acciones progresivas"""
        
        if service not in self.recovery_actions:
            logger.warning(f"⚠️ No hay acciones de recuperación definidas para {service}")
            return False
        
        actions = self.recovery_actions[service]
        
        logger.info(f"🚨 Iniciando recuperación automática para {service}")
        
        for i, action in enumerate(actions[:max_actions]):
            logger.info(f"🔄 Acción {i+1}/{min(max_actions, len(actions))}: {action.description}")
            
            if self.execute_recovery_action(action):
                # Esperar un poco para que el servicio se estabilice
                logger.info(f"⏳ Esperando estabilización del servicio...")
                time.sleep(10)
                
                # Aquí podrías agregar un test rápido para verificar si se recuperó
                logger.info(f"✅ Recuperación de {service} completada")
                return True
            else:
                logger.warning(f"⚠️ Acción {action.name} no tuvo éxito, probando siguiente...")
                
        logger.error(f"❌ No se pudo recuperar {service} después de {max_actions} intentos")
        return False
    
    def recover_all_failed_services(self, test_results: Dict[str, Any]) -> Dict[str, bool]:
        """Recupera todos los servicios que fallaron"""
        
        recovery_results = {}
        
        if 'test_results' not in test_results:
            return recovery_results
        
        for service, result in test_results['test_results'].items():
            if hasattr(result, 'success') and not result.success:
                logger.info(f"🚨 Servicio {service} requiere recuperación")
                recovery_results[service] = self.recover_service(service)
            elif isinstance(result, dict) and not result.get('success', True):
                logger.info(f"🚨 Servicio {service} requiere recuperación")  
                recovery_results[service] = self.recover_service(service)
        
        return recovery_results

def create_enhanced_robust_tester():
    """Crea un tester robusto con auto-recuperación"""
    
    # Importar el tester robusto
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    
    from robust_service_tester import RobustServiceTester
    
    class EnhancedRobustTester(RobustServiceTester):
        """Tester robusto con capacidades de auto-recuperación"""
        
        def __init__(self):
            super().__init__()
            self.auto_recovery = ServiceAutoRecovery()
            self.recovery_enabled = True
        
        def run_tests_with_recovery(self, max_recovery_cycles: int = 2) -> Dict[str, Any]:
            """Ejecuta tests con recuperación automática"""
            
            for cycle in range(max_recovery_cycles):
                logger.info(f"\n🔍 CICLO DE TESTING {cycle + 1}/{max_recovery_cycles}")
                logger.info("=" * 50)
                
                # Ejecutar tests
                results = self.run_all_tests()
                
                # Verificar si todos pasaron
                if results['services_successful'] == results['services_tested']:
                    logger.info(f"🎉 ¡Todos los servicios están funcionando perfectamente!")
                    results['recovery_cycle'] = cycle + 1
                    results['recovery_applied'] = cycle > 0
                    return results
                
                # Si no es el último ciclo, intentar recuperación
                if cycle < max_recovery_cycles - 1 and self.recovery_enabled:
                    logger.info(f"\n🛠️ INICIANDO AUTO-RECUPERACIÓN (Ciclo {cycle + 1})")
                    logger.info("=" * 50)
                    
                    recovery_results = self.auto_recovery.recover_all_failed_services(results)
                    
                    if recovery_results:
                        logger.info(f"🔄 Recuperación completada, reintentando tests...")
                        time.sleep(5)  # Esperar estabilización
                    else:
                        logger.warning(f"⚠️ No se aplicaron recuperaciones")
            
            # Agregar información de recuperación a los resultados finales
            results['recovery_cycles'] = max_recovery_cycles
            results['recovery_applied'] = True
            return results
    
    return EnhancedRobustTester()

def main():
    """Función principal con auto-recuperación"""
    tester = create_enhanced_robust_tester()
    
    # Configurar para ser más agresivo
    tester.retry_config.max_attempts = 3
    tester.retry_config.initial_delay = 1.5
    tester.retry_config.timeout_per_attempt = 12.0
    
    # Ejecutar con recuperación automática
    results = tester.run_tests_with_recovery(max_recovery_cycles=2)
    
    # Guardar resultados
    import os
    logs_dir = "/app/logs" if os.path.exists("/app/logs") else "./logs"
    os.makedirs(logs_dir, exist_ok=True)
    
    with open(f"{logs_dir}/enhanced_test_results.json", 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    # Mostrar resumen final
    logger.info("\n" + "🏁 RESUMEN FINAL CON AUTO-RECUPERACIÓN")
    logger.info("=" * 55)
    logger.info(f"📊 Score final: {results['average_score']:.1f}%")
    logger.info(f"🎯 Estado: {results['overall_status']}")
    logger.info(f"🛠️ Recuperación aplicada: {'Sí' if results.get('recovery_applied', False) else 'No'}")
    logger.info(f"🔄 Ciclos usados: {results.get('recovery_cycle', results.get('recovery_cycles', 1))}")
    
    # Salir con código apropiado
    if results['average_score'] >= 90:
        logger.info("🎉 ¡SISTEMA COMPLETAMENTE OPERATIVO!")
        exit(0)
    elif results['average_score'] >= 80:
        logger.info("✅ Sistema funcional con alta confiabilidad")
        exit(0) 
    else:
        logger.warning("⚠️ Sistema con problemas persistentes")
        exit(1)

if __name__ == "__main__":
    main()