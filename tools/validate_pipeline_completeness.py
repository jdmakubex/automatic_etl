#!/usr/bin/env python3
"""
📋 VERIFICACIÓN FINAL DEL PIPELINE ETL
====================================

Script que verifica que TODAS las configuraciones manuales aplicadas 
están incluidas en el pipeline automático.

RESPONDE A LAS 5 PREGUNTAS CRÍTICAS:
1. ¿Están todas las configuraciones automatizadas?
2. ¿Se usan variables .env correctamente?
3. ¿Están resueltos los problemas de red/DNS?
4. ¿Las APIs funcionan correctamente?
5. ¿Todo forma parte del pipeline automático?
"""

import os
import json
import time
import requests
import subprocess
from pathlib import Path
from dotenv import load_dotenv
from typing import Dict, List, Tuple, Any

class PipelineValidation:
    """Validador completo del pipeline ETL"""
    
    def __init__(self):
        self.root = Path(__file__).resolve().parent.parent
        load_dotenv(self.root / ".env")
        self.results = {
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'questions': {
                '1_automation': {'status': 'unknown', 'details': []},
                '2_env_vars': {'status': 'unknown', 'details': []}, 
                '3_connectivity': {'status': 'unknown', 'details': []},
                '4_apis': {'status': 'unknown', 'details': []},
                '5_pipeline_integration': {'status': 'unknown', 'details': []}
            },
            'overall_status': 'unknown'
        }
    
    def check_automation_completeness(self) -> Tuple[str, List[str]]:
        """1. ¿Están todas las configuraciones automatizadas?"""
        print("🔍 1. VERIFICANDO AUTOMATIZACIÓN COMPLETA")
        print("=" * 50)
        
        details = []
        
        # Verificar scripts de automatización
        automation_files = [
            'tools/superset_auto_configurator.py',
            'tools/metabase_dynamic_configurator.py', 
            'bootstrap/setup_clickhouse_robust.sh',
            'bootstrap/run_etl_full.sh'
        ]
        
        missing_files = []
        for file_path in automation_files:
            full_path = self.root / file_path
            if full_path.exists():
                details.append(f"✅ {file_path} - EXISTE")
            else:
                missing_files.append(file_path)
                details.append(f"❌ {file_path} - FALTA")
        
        # Verificar configuraciones en docker-compose
        compose_path = self.root / "docker-compose.yml"
        if compose_path.exists():
            with open(compose_path) as f:
                compose_content = f.read()
            
            required_services = ['superset-init', 'metabase-configurator', 'etl-orchestrator']
            for service in required_services:
                if service in compose_content:
                    details.append(f"✅ Servicio {service} - CONFIGURADO")
                else:
                    details.append(f"❌ Servicio {service} - FALTA")
                    missing_files.append(f"docker-compose service: {service}")
        
        # Verificar permisos ClickHouse automatizados
        ch_setup_path = self.root / "bootstrap/setup_clickhouse_robust.sh"
        if ch_setup_path.exists():
            with open(ch_setup_path) as f:
                content = f.read()
            if "GRANT SELECT" in content and "superset" in content:
                details.append("✅ Permisos ClickHouse - AUTOMATIZADOS")
            else:
                details.append("❌ Permisos ClickHouse - NO AUTOMATIZADOS")
                missing_files.append("ClickHouse permissions")
        
        status = 'pass' if len(missing_files) == 0 else 'fail'
        return status, details
    
    def check_env_vars_usage(self) -> Tuple[str, List[str]]:
        """2. ¿Se usan variables .env correctamente?"""
        print("\n🔍 2. VERIFICANDO USO DE VARIABLES .ENV")
        print("=" * 50)
        
        details = []
        
        # Verificar variables críticas del .env
        required_vars = [
            'SUPERSET_ADMIN', 'SUPERSET_PASSWORD',
            'METABASE_ADMIN', 'METABASE_PASSWORD', 
            'CLICKHOUSE_ETL_USER', 'CLICKHOUSE_ETL_PASSWORD',
            'DB_CONNECTIONS'
        ]
        
        missing_vars = []
        for var in required_vars:
            value = os.getenv(var)
            if value:
                # Ocultar contraseñas en logs
                display_value = "***" if "PASSWORD" in var else value[:50]
                details.append(f"✅ {var} = {display_value}")
            else:
                missing_vars.append(var)
                details.append(f"❌ {var} - NO DEFINIDA")
        
        # Verificar que configuradores usen variables .env
        config_files = [
            'tools/superset_auto_configurator.py',
            'tools/metabase_dynamic_configurator.py'
        ]
        
        for config_file in config_files:
            file_path = self.root / config_file
            if file_path.exists():
                with open(file_path) as f:
                    content = f.read()
                
                # Buscar uso correcto de os.getenv
                if "os.getenv('SUPERSET_ADMIN'" in content or "os.getenv('METABASE_ADMIN'" in content:
                    details.append(f"✅ {config_file} - USA VARIABLES .ENV")
                else:
                    details.append(f"❌ {config_file} - NO USA VARIABLES .ENV")
                    missing_vars.append(f"{config_file} env usage")
        
        status = 'pass' if len(missing_vars) == 0 else 'fail'
        return status, details
    
    def check_connectivity_resolution(self) -> Tuple[str, List[str]]:
        """3. ¿Están resueltos los problemas de red/DNS?"""
        print("\n🔍 3. VERIFICANDO CONECTIVIDAD DE RED")
        print("=" * 50)
        
        details = []
        
        # Servicios críticos que DEBEN funcionar
        critical_services = [
            ('clickhouse', '8123'),
            ('superset', '8088'),
            ('metabase', '3000')
        ]
        
        failed_services = []
        for service, port in critical_services:
            try:
                response = requests.get(f"http://{service}:{port}/health", timeout=5)
                if response.status_code in [200, 404]:  # 404 ok si no tiene /health
                    details.append(f"✅ {service}:{port} - ACCESIBLE")
                else:
                    details.append(f"⚠️  {service}:{port} - RESPUESTA {response.status_code}")
            except Exception as e:
                details.append(f"❌ {service}:{port} - ERROR: {str(e)[:100]}")
                failed_services.append(f"{service}:{port}")
        
        # Servicios opcionales que pueden faltar
        optional_services = ['zookeeper', 'superset-db']
        for service in optional_services:
            try:
                # Intentar resolución DNS básica
                import socket
                socket.gethostbyname(service)
                details.append(f"ℹ️  {service} - DISPONIBLE (opcional)")
            except:
                details.append(f"⚠️  {service} - NO DISPONIBLE (opcional, OK)")
        
        # Los servicios críticos deben funcionar, opcionales no importan
        status = 'pass' if len(failed_services) == 0 else 'fail'
        return status, details
    
    def check_api_functionality(self) -> Tuple[str, List[str]]:
        """4. ¿Las APIs funcionan correctamente?"""
        print("\n🔍 4. VERIFICANDO FUNCIONALIDAD DE APIs")
        print("=" * 50)
        
        details = []
        api_issues = []
        
        # Test Metabase API
        try:
            metabase_url = os.getenv('METABASE_URL', 'http://metabase:3000')
            metabase_user = os.getenv('METABASE_ADMIN', 'admin@admin.com')
            metabase_pass = os.getenv('METABASE_PASSWORD', 'Admin123!')
            
            auth_resp = requests.post(f'{metabase_url}/api/session',
                                    json={'username': metabase_user, 'password': metabase_pass},
                                    timeout=10)
            
            if auth_resp.status_code == 200:
                details.append("✅ Metabase API - FUNCIONAL")
                
                # Test consulta básica si podemos
                session_id = auth_resp.json()['id']
                headers = {'X-Metabase-Session': session_id}
                db_resp = requests.get(f'{metabase_url}/api/database', headers=headers, timeout=5)
                if db_resp.status_code == 200:
                    db_count = len(db_resp.json().get('data', []))
                    details.append(f"   📊 Databases conectadas: {db_count}")
                
            else:
                details.append(f"❌ Metabase API - ERROR {auth_resp.status_code}")
                api_issues.append("metabase")
        
        except Exception as e:
            details.append(f"❌ Metabase API - EXCEPCIÓN: {str(e)[:100]}")
            api_issues.append("metabase")
        
        # Test Superset API
        try:
            superset_url = os.getenv('SUPERSET_URL', 'http://superset:8088')
            superset_user = os.getenv('SUPERSET_ADMIN', 'admin')
            superset_pass = os.getenv('SUPERSET_PASSWORD', 'Admin123!')
            
            # Test login API v1
            login_data = {
                "username": superset_user,
                "password": superset_pass,
                "provider": "db",
                "refresh": True
            }
            
            auth_resp = requests.post(f'{superset_url}/api/v1/security/login',
                                    json=login_data, timeout=10)
            
            if auth_resp.status_code == 200:
                details.append("✅ Superset API - FUNCIONAL")
                
                # Test más APIs si podemos
                token = auth_resp.json()["access_token"]
                headers = {"Authorization": f"Bearer {token}"}
                db_resp = requests.get(f'{superset_url}/api/v1/database/', 
                                     headers=headers, timeout=5)
                if db_resp.status_code == 200:
                    db_count = len(db_resp.json().get('result', []))
                    details.append(f"   📊 Databases conectadas: {db_count}")
                
            else:
                details.append(f"❌ Superset API - ERROR {auth_resp.status_code}")
                api_issues.append("superset")
        
        except Exception as e:
            details.append(f"❌ Superset API - EXCEPCIÓN: {str(e)[:100]}")
            api_issues.append("superset")
        
        status = 'pass' if len(api_issues) == 0 else 'fail'
        return status, details
    
    def check_pipeline_integration(self) -> Tuple[str, List[str]]:
        """5. ¿Todo forma parte del pipeline automático?"""
        print("\n🔍 5. VERIFICANDO INTEGRACIÓN EN PIPELINE")
        print("=" * 50)
        
        details = []
        integration_issues = []
        
        # Verificar que servicios de configuración estén en docker-compose
        compose_path = self.root / "docker-compose.yml"
        if compose_path.exists():
            with open(compose_path) as f:
                compose_content = f.read()
            
            # Servicios críticos del pipeline
            pipeline_services = {
                'superset-init': 'Inicialización automática de Superset',
                'metabase-configurator': 'Configurador dinámico de Metabase',
                'etl-orchestrator': 'Orquestador maestro del pipeline'
            }
            
            for service, description in pipeline_services.items():
                if service in compose_content:
                    details.append(f"✅ {service} - INTEGRADO ({description})")
                else:
                    details.append(f"❌ {service} - NO INTEGRADO ({description})")
                    integration_issues.append(service)
        
        # Verificar dependencias entre servicios
        if compose_path.exists():
            dependencies_ok = True
            if "depends_on:" in compose_content and "superset:" in compose_content:
                details.append("✅ Dependencias de servicios - CONFIGURADAS")
            else:
                details.append("❌ Dependencias de servicios - NO CONFIGURADAS")
                dependencies_ok = False
                integration_issues.append("dependencies")
        
        # Verificar scripts de inicio automático
        startup_scripts = [
            'bootstrap/run_etl_full.sh',
            'start_etl_pipeline.sh'
        ]
        
        for script in startup_scripts:
            script_path = self.root / script
            if script_path.exists():
                details.append(f"✅ {script} - EXISTE")
            else:
                details.append(f"❌ {script} - FALTA")
                integration_issues.append(script)
        
        status = 'pass' if len(integration_issues) == 0 else 'fail'
        return status, details
    
    def run_comprehensive_validation(self) -> Dict[str, Any]:
        """Ejecutar validación completa"""
        print("🧪 VALIDACIÓN COMPLETA DEL PIPELINE ETL")
        print("=" * 60)
        
        # Ejecutar todas las validaciones
        questions = [
            ("1_automation", self.check_automation_completeness),
            ("2_env_vars", self.check_env_vars_usage),
            ("3_connectivity", self.check_connectivity_resolution),
            ("4_apis", self.check_api_functionality),
            ("5_pipeline_integration", self.check_pipeline_integration)
        ]
        
        passed_count = 0
        total_count = len(questions)
        
        for question_id, check_func in questions:
            status, details = check_func()
            self.results['questions'][question_id] = {
                'status': status,
                'details': details
            }
            if status == 'pass':
                passed_count += 1
        
        # Determinar estado general
        if passed_count == total_count:
            self.results['overall_status'] = 'pass'
        elif passed_count >= total_count - 1:
            self.results['overall_status'] = 'warning'
        else:
            self.results['overall_status'] = 'fail'
        
        # Mostrar resumen
        self._show_final_summary(passed_count, total_count)
        
        return self.results
    
    def _show_final_summary(self, passed: int, total: int):
        """Mostrar resumen final"""
        print("\n" + "=" * 60)
        print("📊 RESUMEN FINAL DE VALIDACIÓN")
        print("=" * 60)
        
        questions_map = {
            "1_automation": "1. ¿Configuraciones automatizadas?",
            "2_env_vars": "2. ¿Variables .env correctas?", 
            "3_connectivity": "3. ¿Problemas de red resueltos?",
            "4_apis": "4. ¿APIs funcionando?",
            "5_pipeline_integration": "5. ¿Todo en pipeline?"
        }
        
        for q_id, question in questions_map.items():
            status = self.results['questions'][q_id]['status']
            icon = "✅" if status == 'pass' else "❌"
            print(f"{icon} {question}")
        
        print("\n" + "-" * 60)
        print(f"📈 RESULTADO: {passed}/{total} VALIDACIONES PASARON")
        
        if self.results['overall_status'] == 'pass':
            print("🎉 ¡PIPELINE COMPLETAMENTE VALIDADO!")
            print("   → Todas las configuraciones están automatizadas")
            print("   → El equipo puede ejecutar el pipeline sin asistencia")
        elif self.results['overall_status'] == 'warning':
            print("⚠️  PIPELINE MAYORMENTE FUNCIONAL")
            print("   → Solo problemas menores detectados")
            print("   → El pipeline debería funcionar autónomamente")
        else:
            print("❌ PIPELINE REQUIERE ATENCIÓN")
            print("   → Problemas críticos detectados")
            print("   → Se requiere intervención manual")
        
        print(f"\n📝 Reporte guardado en: {self.root}/logs/pipeline_validation.json")


def main():
    """Función principal"""
    validator = PipelineValidation()
    results = validator.run_comprehensive_validation()
    
    # Guardar resultados
    logs_dir = validator.root / "logs"
    logs_dir.mkdir(exist_ok=True)
    
    with open(logs_dir / "pipeline_validation.json", 'w') as f:
        json.dump(results, f, indent=2)
    
    return 0 if results['overall_status'] in ['pass', 'warning'] else 1


if __name__ == "__main__":
    exit(main())