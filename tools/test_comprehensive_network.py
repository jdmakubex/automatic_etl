#!/usr/bin/env python3
"""
🌐 TEST INTEGRAL DE COMUNICACIÓN ENTRE CONTENEDORES
==================================================

Verifica comunicación completa:
1. Interna (contenedor → contenedor)
2. Externa (host → contenedor) 
3. Por DNS, IP, HTTP, puertos
4. Bidireccional

RESPONDE A LOS 3 REQUERIMIENTOS:
1. Comunicación externa e interna por puertos, IP, HTTP, DNS
2. Test de red integrado que todo funcione
3. Scripts ejecutables desde host y contenedores
"""

import os
import json
import time
import socket
import requests
import subprocess
import concurrent.futures
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional
from dataclasses import dataclass
import logging

@dataclass
class ServiceConfig:
    """Configuración de un servicio para testing"""
    name: str
    internal_host: str  # Nombre DNS interno
    external_host: str  # Host desde fuera (localhost)
    ports: List[int]
    health_endpoint: Optional[str] = None
    expected_response: str = "200"

class ComprehensiveNetworkTester:
    """Tester completo de comunicación de red"""
    
    def __init__(self):
        self.logger = self._setup_logging()
        self.results = {
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'execution_context': self._detect_execution_context(),
            'network_info': {},
            'tests': {
                'dns_resolution': {'internal': {}, 'external': {}},
                'port_connectivity': {'internal': {}, 'external': {}},
                'http_communication': {'internal': {}, 'external': {}},
                'bidirectional_tests': {},
                'service_health': {}
            },
            'summary': {
                'total_tests': 0,
                'passed_tests': 0,
                'failed_tests': 0,
                'warnings': 0
            }
        }
        
        # Configuración de servicios críticos
        # Para contenedores: external_host debe ser la IP del host Docker
        external_host = self._get_external_host()
        self.services = [
            ServiceConfig("clickhouse", "clickhouse", external_host, [8123, 9000], "/ping", "200"),
            ServiceConfig("superset", "superset", external_host, [8088], "/health", "200"),
            ServiceConfig("metabase", "metabase", external_host, [3000], "/api/health", "200"),
            ServiceConfig("kafka", "kafka", external_host, [9092], None, None),
            ServiceConfig("connect", "connect", external_host, [8083], "/", "200"),
            ServiceConfig("redis", "superset-redis", external_host, [6379], None, None),
        ]
    
    def _setup_logging(self):
        """Configurar logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def _detect_execution_context(self) -> str:
        """Detectar si se ejecuta desde contenedor o host"""
        if os.path.exists('/.dockerenv'):
            return 'container'
        elif os.path.exists('/app') and os.path.exists('/app/tools'):
            return 'container'
        else:
            return 'host'
    
    def _get_external_host(self) -> str:
        """Obtener host externo apropiado según contexto"""
        context = self._detect_execution_context()
        
        if context == 'container':
            # Desde contenedor, usar la IP del gateway Docker
            try:
                # Intentar obtener gateway de la red etl_net
                result = subprocess.run([
                    'docker', 'network', 'inspect', 'etl_prod_etl_net'
                ], capture_output=True, text=True, timeout=5)
                
                if result.returncode == 0:
                    import json
                    network_info = json.loads(result.stdout)[0]
                    gateway = network_info['IPAM']['Config'][0]['Gateway']
                    return gateway
            except:
                pass
            
            # Fallback: usar host.docker.internal o gateway típico
            try:
                socket.gethostbyname('host.docker.internal')
                return 'host.docker.internal'
            except:
                return '172.19.0.1'  # Gateway típico de Docker
        else:
            # Desde host, usar localhost
            return 'localhost'
    
    def test_docker_network_info(self):
        """Obtener información de la red Docker"""
        self.logger.info("🔍 Obteniendo información de red Docker...")
        
        try:
            # Información de la red etl_net
            result = subprocess.run(
                ['docker', 'network', 'inspect', 'etl_prod_etl_net'],
                capture_output=True, text=True, timeout=10
            )
            
            if result.returncode == 0:
                import json
                network_info = json.loads(result.stdout)[0]
                
                self.results['network_info'] = {
                    'name': network_info.get('Name'),
                    'subnet': network_info['IPAM']['Config'][0]['Subnet'],
                    'gateway': network_info['IPAM']['Config'][0]['Gateway'],
                    'containers': {}
                }
                
                # Extraer IPs de contenedores
                for container_id, container_info in network_info.get('Containers', {}).items():
                    container_name = container_info['Name']
                    container_ip = container_info['IPv4Address'].split('/')[0]
                    self.results['network_info']['containers'][container_name] = container_ip
                
                self.logger.info(f"✅ Red Docker: {len(self.results['network_info']['containers'])} contenedores")
                return True
            
        except Exception as e:
            self.logger.error(f"❌ Error obteniendo info de red: {e}")
            
        return False
    
    def test_dns_resolution(self):
        """Test resolución DNS interna y externa"""
        self.logger.info("🔍 Testing resolución DNS...")
        
        context = self.results['execution_context']
        
        for service in self.services:
            # Test resolución DNS interna (nombres de contenedor)
            internal_success = False
            external_success = False
            
            # DNS interno (desde contenedor o con docker exec)
            try:
                if context == 'container':
                    # Dentro del contenedor, usar resolución directa
                    socket.gethostbyname(service.internal_host)
                    internal_success = True
                else:
                    # Desde host, usar docker exec para probar DNS interno
                    result = subprocess.run([
                        'docker', 'exec', 'etl_prod-etl-tools-1', 
                        'nslookup', service.internal_host
                    ], capture_output=True, text=True, timeout=5)
                    internal_success = result.returncode == 0
                
            except Exception as e:
                self.logger.debug(f"DNS interno {service.name}: {e}")
            
            # DNS externo (localhost)
            try:
                socket.gethostbyname(service.external_host)
                external_success = True
            except Exception as e:
                self.logger.debug(f"DNS externo {service.name}: {e}")
            
            self.results['tests']['dns_resolution']['internal'][service.name] = internal_success
            self.results['tests']['dns_resolution']['external'][service.name] = external_success
            
            # Logging
            internal_icon = "✅" if internal_success else "❌"
            external_icon = "✅" if external_success else "❌"
            
            self.logger.info(f"{internal_icon} DNS interno: {service.internal_host}")
            self.logger.info(f"{external_icon} DNS externo: {service.external_host}")
    
    def test_port_connectivity(self):
        """Test conectividad de puertos"""
        self.logger.info("🔍 Testing conectividad de puertos...")
        
        context = self.results['execution_context']
        
        def test_port(host: str, port: int, timeout: int = 3) -> bool:
            """Test individual de puerto"""
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(timeout)
                result = sock.connect_ex((host, port))
                sock.close()
                return result == 0
            except:
                return False
        
        for service in self.services:
            for port in service.ports:
                # Test puerto interno
                if context == 'container':
                    internal_success = test_port(service.internal_host, port)
                else:
                    # Desde host, usar docker exec
                    try:
                        result = subprocess.run([
                            'docker', 'exec', 'etl_prod-etl-tools-1',
                            'timeout', '3', 'bash', '-c', 
                            f'echo > /dev/tcp/{service.internal_host}/{port}'
                        ], capture_output=True, timeout=5)
                        internal_success = result.returncode == 0
                    except:
                        internal_success = False
                
                # Test puerto externo
                external_success = test_port(service.external_host, port)
                
                port_key = f"{service.name}:{port}"
                self.results['tests']['port_connectivity']['internal'][port_key] = internal_success
                self.results['tests']['port_connectivity']['external'][port_key] = external_success
                
                # Logging
                internal_icon = "✅" if internal_success else "❌"
                external_icon = "✅" if external_success else "❌"
                
                self.logger.info(f"{internal_icon} Puerto interno: {service.internal_host}:{port}")
                self.logger.info(f"{external_icon} Puerto externo: {service.external_host}:{port}")
    
    def test_http_communication(self):
        """Test comunicación HTTP completa"""
        self.logger.info("🔍 Testing comunicación HTTP...")
        
        context = self.results['execution_context']
        
        for service in self.services:
            if not service.health_endpoint:
                continue
                
            for port in service.ports:
                # Test HTTP interno
                internal_success = False
                external_success = False
                
                internal_url = f"http://{service.internal_host}:{port}{service.health_endpoint}"
                external_url = f"http://{service.external_host}:{port}{service.health_endpoint}"
                
                # HTTP interno
                try:
                    if context == 'container':
                        response = requests.get(internal_url, timeout=5)
                        internal_success = str(response.status_code).startswith(service.expected_response[:1])
                    else:
                        # Desde host, usar curl dentro del contenedor
                        result = subprocess.run([
                            'docker', 'exec', 'etl_prod-etl-tools-1',
                            'curl', '-s', '-o', '/dev/null', '-w', '%{http_code}', internal_url
                        ], capture_output=True, text=True, timeout=10)
                        
                        if result.returncode == 0:
                            status_code = result.stdout.strip()
                            internal_success = status_code.startswith(service.expected_response[:1])
                
                except Exception as e:
                    self.logger.debug(f"HTTP interno {service.name}: {e}")
                
                # HTTP externo
                try:
                    response = requests.get(external_url, timeout=5)
                    external_success = str(response.status_code).startswith(service.expected_response[:1])
                except Exception as e:
                    self.logger.debug(f"HTTP externo {service.name}: {e}")
                
                http_key = f"{service.name}:{port}"
                self.results['tests']['http_communication']['internal'][http_key] = internal_success
                self.results['tests']['http_communication']['external'][http_key] = external_success
                
                # Logging
                internal_icon = "✅" if internal_success else "❌"
                external_icon = "✅" if external_success else "❌"
                
                self.logger.info(f"{internal_icon} HTTP interno: {internal_url}")
                self.logger.info(f"{external_icon} HTTP externo: {external_url}")
    
    def test_bidirectional_communication(self):
        """Test comunicación bidireccional (contenedor <-> host)"""
        self.logger.info("🔍 Testing comunicación bidireccional...")
        
        # Test que contenedores puedan alcanzar servicios del host si existen
        
        # Test básico: contenedor puede resolver IPs del gateway
        try:
            gateway_ip = self.results['network_info'].get('gateway', '172.19.0.1')
            
            if self.results['execution_context'] == 'container':
                # Ping al gateway desde contenedor
                import subprocess
                result = subprocess.run(['ping', '-c', '1', gateway_ip], 
                                      capture_output=True, timeout=5)
                gateway_reachable = result.returncode == 0
            else:
                # Ping al gateway desde host usando docker exec
                result = subprocess.run([
                    'docker', 'exec', 'etl_prod-etl-tools-1',
                    'ping', '-c', '1', gateway_ip
                ], capture_output=True, timeout=5)
                gateway_reachable = result.returncode == 0
            
            self.results['tests']['bidirectional_tests']['gateway_reachable'] = gateway_reachable
            
            icon = "✅" if gateway_reachable else "❌"
            self.logger.info(f"{icon} Gateway alcanzable: {gateway_ip}")
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error test bidireccional: {e}")
            self.results['tests']['bidirectional_tests']['gateway_reachable'] = False
    
    def test_service_health_comprehensive(self):
        """Test de salud integral de servicios"""
        self.logger.info("🔍 Testing salud integral de servicios...")
        
        # Tests específicos por servicio
        service_tests = {
            'clickhouse': self._test_clickhouse_health,
            'superset': self._test_superset_health,
            'metabase': self._test_metabase_health
        }
        
        for service_name, test_func in service_tests.items():
            try:
                success, details = test_func()
                self.results['tests']['service_health'][service_name] = {
                    'success': success,
                    'details': details
                }
                
                icon = "✅" if success else "❌"
                self.logger.info(f"{icon} {service_name}: {details}")
                
            except Exception as e:
                self.logger.error(f"❌ Error testing {service_name}: {e}")
                self.results['tests']['service_health'][service_name] = {
                    'success': False,
                    'details': f"Error: {str(e)}"
                }
    
    def _test_clickhouse_health(self) -> Tuple[bool, str]:
        """Test específico de ClickHouse"""
        try:
            # Usar el contexto apropiado
            if self.results['execution_context'] == 'container':
                response = requests.get('http://clickhouse:8123/ping', timeout=5)
                if response.status_code == 200:
                    # Test query básica
                    query_resp = requests.post('http://clickhouse:8123/', 
                                             data='SELECT version()', timeout=5)
                    if query_resp.status_code == 200:
                        version = query_resp.text.strip()
                        return True, f"Funcional, version: {version}"
            else:
                # Desde host usando docker exec
                result = subprocess.run([
                    'docker', 'exec', 'clickhouse',
                    'clickhouse-client', '--query', 'SELECT version()'
                ], capture_output=True, text=True, timeout=10)
                
                if result.returncode == 0:
                    version = result.stdout.strip()
                    return True, f"Funcional, version: {version}"
            
            return False, "No responde"
            
        except Exception as e:
            return False, f"Error: {str(e)}"
    
    def _test_superset_health(self) -> Tuple[bool, str]:
        """Test específico de Superset"""
        try:
            if self.results['execution_context'] == 'container':
                response = requests.get('http://superset:8088/health', timeout=5)
            else:
                response = requests.get('http://localhost:8088/health', timeout=5)
            
            if response.status_code == 200:
                return True, f"Funcional, status: {response.text.strip()}"
            else:
                return False, f"HTTP {response.status_code}"
                
        except Exception as e:
            return False, f"Error: {str(e)}"
    
    def _test_metabase_health(self) -> Tuple[bool, str]:
        """Test específico de Metabase"""
        try:
            if self.results['execution_context'] == 'container':
                response = requests.get('http://metabase:3000/api/health', timeout=5)
            else:
                response = requests.get('http://localhost:3000/api/health', timeout=5)
            
            if response.status_code == 200:
                health_data = response.json()
                status = health_data.get('status', 'unknown')
                return True, f"Funcional, status: {status}"
            else:
                return False, f"HTTP {response.status_code}"
                
        except Exception as e:
            return False, f"Error: {str(e)}"
    
    def calculate_summary(self):
        """Calcular resumen de tests"""
        total = 0
        passed = 0
        warnings = 0
        
        # Contar todos los tests
        for test_category in self.results['tests'].values():
            if isinstance(test_category, dict):
                for subcategory, tests in test_category.items():
                    if isinstance(tests, dict):
                        for test_name, result in tests.items():
                            total += 1
                            if isinstance(result, bool):
                                if result:
                                    passed += 1
                            elif isinstance(result, dict):
                                if result.get('success', False):
                                    passed += 1
        
        failed = total - passed
        
        self.results['summary'] = {
            'total_tests': total,
            'passed_tests': passed,
            'failed_tests': failed,
            'warnings': warnings,
            'success_rate': round((passed / total * 100) if total > 0 else 0, 1)
        }
    
    def run_comprehensive_test(self) -> Dict[str, Any]:
        """Ejecutar test completo de comunicación"""
        self.logger.info("🌐 INICIANDO TEST INTEGRAL DE COMUNICACIÓN")
        self.logger.info("=" * 60)
        
        # Detectar contexto
        context = self.results['execution_context']
        self.logger.info(f"📍 Contexto de ejecución: {context}")
        
        # Ejecutar todos los tests
        test_methods = [
            ("Información de red Docker", self.test_docker_network_info),
            ("Resolución DNS", self.test_dns_resolution),
            ("Conectividad de puertos", self.test_port_connectivity),
            ("Comunicación HTTP", self.test_http_communication),
            ("Comunicación bidireccional", self.test_bidirectional_communication),
            ("Salud integral de servicios", self.test_service_health_comprehensive)
        ]
        
        for test_name, test_method in test_methods:
            self.logger.info(f"\n🔍 {test_name}...")
            try:
                test_method()
            except Exception as e:
                self.logger.error(f"❌ Error en {test_name}: {e}")
        
        # Calcular resumen
        self.calculate_summary()
        
        # Mostrar resumen final
        self._show_final_summary()
        
        return self.results
    
    def _show_final_summary(self):
        """Mostrar resumen final"""
        summary = self.results['summary']
        
        self.logger.info("\n" + "=" * 60)
        self.logger.info("📊 RESUMEN DE COMUNICACIÓN INTEGRAL")
        self.logger.info("=" * 60)
        
        self.logger.info(f"📍 Contexto: {self.results['execution_context']}")
        self.logger.info(f"📊 Tests totales: {summary['total_tests']}")
        self.logger.info(f"✅ Tests exitosos: {summary['passed_tests']}")
        self.logger.info(f"❌ Tests fallidos: {summary['failed_tests']}")
        self.logger.info(f"📈 Tasa de éxito: {summary['success_rate']}%")
        
        # Estado general
        if summary['success_rate'] >= 90:
            self.logger.info("🎉 COMUNICACIÓN EXCELENTE")
        elif summary['success_rate'] >= 70:
            self.logger.info("✅ COMUNICACIÓN BUENA")
        elif summary['success_rate'] >= 50:
            self.logger.info("⚠️ COMUNICACIÓN CON PROBLEMAS")
        else:
            self.logger.error("❌ COMUNICACIÓN CRÍTICA")
        
        # Guardar reporte
        logs_dir = Path("/app/logs") if Path("/app/logs").exists() else Path("./logs")
        logs_dir.mkdir(exist_ok=True)
        
        report_file = logs_dir / "comprehensive_network_test.json"
        with open(report_file, 'w') as f:
            json.dump(self.results, f, indent=2)
        
        self.logger.info(f"📝 Reporte completo: {report_file}")


def main():
    """Función principal"""
    tester = ComprehensiveNetworkTester()
    results = tester.run_comprehensive_test()
    
    # Exit code basado en resultado
    success_rate = results['summary']['success_rate']
    if success_rate >= 70:
        return 0  # Éxito
    else:
        return 1  # Fallo


if __name__ == "__main__":
    exit(main())