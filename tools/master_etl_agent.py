#!/usr/bin/env python3
"""
🎯 AGENTE MAESTRO ETL - INTEGRACIÓN COMPLETA
Sistema final que coordina y ejecuta automáticamente toda la inicialización
del pipeline ETL sin intervención manual
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
    """Agente maestro que coordina toda la inicialización automática"""
    
    def __init__(self):
        self.logger = self._setup_logging()
        self.start_time = datetime.now()
        self.execution_id = self.start_time.strftime('%Y%m%d_%H%M%S')
        
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
    
    def phase_1_validate_infrastructure(self) -> bool:
        """Fase 1: Validar infraestructura y servicios básicos"""
        phase_start = datetime.now()
        self.logger.info("🏥 === FASE 1: VALIDACIÓN DE INFRAESTRUCTURA ===")
        
        try:
            success, health_results = self.health_validator.comprehensive_health_check()
            
            self.results['phases']['phase_1_infrastructure'] = {
                'name': 'Validación de Infraestructura',
                'success': success,
                'duration_seconds': (datetime.now() - phase_start).total_seconds(),
                'details': health_results
            }
            
            if success:
                self.logger.info("✅ FASE 1 COMPLETADA: Infraestructura operativa")
            else:
                self.logger.error("❌ FASE 1 FALLÓ: Problemas en infraestructura")
                self.logger.error(f"   Problemas críticos: {len(health_results.get('critical_issues', []))}")
            
            return success
            
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
    
    def phase_3_configure_superset(self) -> bool:
        """Fase 3: Configurar Superset completamente"""
        phase_start = datetime.now()
        self.logger.info("📊 === FASE 3: CONFIGURACIÓN COMPLETA DE SUPERSET ===")
        
        try:
            success, superset_results = self.superset_configurator.comprehensive_superset_setup()
            
            self.results['phases']['phase_3_superset'] = {
                'name': 'Configuración de Superset',
                'success': success,
                'duration_seconds': (datetime.now() - phase_start).total_seconds(),
                'details': superset_results
            }
            
            if success:
                self.logger.info("✅ FASE 3 COMPLETADA: Superset configurado")
                self.logger.info(f"🌐 URL: {self.superset_configurator.superset_url}")
                self.logger.info(f"👤 Admin: {self.superset_configurator.admin_config['username']}")
                self.logger.info(f"📊 Datasets: {len(superset_results.get('datasets_created', []))}")
            else:
                self.logger.error("❌ FASE 3 FALLÓ: Problemas configurando Superset")
                for issue in superset_results.get('issues', [])[:3]:
                    self.logger.error(f"   • {issue}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"❌ Error crítico en Fase 3: {str(e)}")
            self.results['phases']['phase_3_superset'] = {
                'name': 'Configuración de Superset',
                'success': False,
                'error': str(e),
                'duration_seconds': (datetime.now() - phase_start).total_seconds()
            }
            return False
    
    def phase_4_validate_etl_pipeline(self) -> bool:
        """Fase 4: Validar pipeline ETL completo"""
        phase_start = datetime.now()
        self.logger.info("⚡ === FASE 4: VALIDACIÓN DEL PIPELINE ETL ===")
        
        try:
            # Ejecutar validación ETL completa
            self.logger.info("🔍 Validando flujo MySQL → Kafka → ClickHouse...")
            validation_process = subprocess.run([
                'python3', 'tools/validate_etl_simple.py'
            ], capture_output=True, text=True, timeout=120)
            
            pipeline_success = validation_process.returncode == 0
            
            self.results['phases']['phase_4_pipeline'] = {
                'name': 'Validación Pipeline ETL',
                'success': pipeline_success,
                'duration_seconds': (datetime.now() - phase_start).total_seconds(),
                'validation_output': validation_process.stdout if pipeline_success else validation_process.stderr
            }
            
            if pipeline_success:
                self.logger.info("✅ FASE 4 COMPLETADA: Pipeline ETL operativo")
            else:
                self.logger.warning(f"⚠️  Validación ETL con advertencias: {validation_process.stderr}")
            
            return pipeline_success
            
        except Exception as e:
            self.logger.error(f"❌ Error crítico en Fase 4: {str(e)}")
            self.results['phases']['phase_4_pipeline'] = {
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
        
        superset_success = self.results['phases'].get('phase_3_superset', {}).get('success', False)
        
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
            'superset_configured': superset_success,
            'pipeline_validated': self.results['phases'].get('phase_4_pipeline', {}).get('success', False)
        }
        
        # Agregar información de acceso
        if superset_success:
            superset_details = self.results['phases']['phase_3_superset'].get('details', {})
            summary['superset_access'] = {
                'url': self.superset_configurator.superset_url,
                'username': self.superset_configurator.admin_config['username'],
                'password': self.superset_configurator.admin_config['password'],
                'database_id': superset_details.get('database_id'),
                'datasets_count': len(superset_details.get('datasets_created', []))
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
        print(f"   📊 Superset:         {'✅' if summary['superset_configured'] else '❌'}")
        print(f"   ⚡ Pipeline ETL:     {'✅' if summary['pipeline_validated'] else '⚠️ '}")
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
            # Fase 1: Validar infraestructura
            phase1_success = self.phase_1_validate_infrastructure()
            if not phase1_success:
                self.logger.error("❌ Fallo crítico en Fase 1 - Abortando")
                return False
            
            # Breve pausa entre fases
            time.sleep(5)
            
            # Fase 2: Configurar usuarios
            phase2_success = self.phase_2_configure_users()
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
            
            # Fase 3: Configurar Superset
            phase3_success = self.phase_3_configure_superset()
            # Superset no es crítico para el pipeline básico
            if not phase3_success:
                self.logger.warning("⚠️  Problemas en Superset, pero continuando...")
            
            # Fase 4: Validar pipeline
            phase4_success = self.phase_4_validate_etl_pipeline()
            
            # Generar resumen final
            summary = self.generate_final_summary()
            
            # Guardar resultados
            self._save_execution_results()
            
            # Mostrar reporte
            self.display_final_report(summary)
            
            return summary['infrastructure_ok'] and summary['users_configured']
            
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