#!/usr/bin/env python3
"""
🎯 UNIFIED CONFIGURATOR - ENHANCED VERSION
==========================================

Configurador unificado robusto que funciona tanto desde host como desde contenedores,
con tests comprehensivos, reintentos automáticos y verificaciones profundas.
"""

import os
import sys
import time
import json
import logging
import subprocess
import requests
from typing import Dict, List, Any, Optional, Tuple
import socket
from urllib.parse import urlparse
from pathlib import Path
from dataclasses import dataclass

# Importar el sistema robusto
try:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from robust_service_tester import RobustServiceTester
    ROBUST_AVAILABLE = True
except ImportError:
    ROBUST_AVAILABLE = False

@dataclass
class ServiceEndpoint:
    """Configuración de endpoint de servicio"""
    name: str
    internal_host: str
    external_host: str
    port: int
    protocol: str = "http"
    health_path: str = "/health"

class UnifiedConfigurator:
    """Configurador que funciona en cualquier contexto"""
    
    def __init__(self):
        self.logger = self._setup_logging()
        self.context = self._detect_context()
        self.docker_gateway = self._get_docker_gateway()
        self.endpoints = self._configure_endpoints()
        
        self.logger.info(f"🎯 Contexto detectado: {self.context}")
        self.logger.info(f"🌐 Gateway Docker: {self.docker_gateway}")
    
    def _setup_logging(self):
        """Configurar logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def _detect_context(self) -> str:
        """Detectar contexto de ejecución"""
        if os.path.exists('/.dockerenv'):
            return 'container'
        elif os.path.exists('/app') and os.path.exists('/app/tools'):
            return 'container'
        elif 'DOCKER_HOST' in os.environ:
            return 'docker_client'
        else:
            return 'host'
    
    def _get_docker_gateway(self) -> str:
        """Obtener IP del gateway Docker"""
        if self.context == 'container':
            # Desde contenedor, usar host.docker.internal o gateway
            try:
                socket.gethostbyname('host.docker.internal')
                return 'host.docker.internal'
            except:
                return '172.19.0.1'  # Gateway típico
        else:
            return 'localhost'
    
    def _configure_endpoints(self) -> Dict[str, ServiceEndpoint]:
        """Configurar endpoints según contexto"""
        if self.context == 'container':
            # Desde contenedor: usar nombres DNS internos y host.docker.internal para externo
            return {
                'clickhouse': ServiceEndpoint("clickhouse", "clickhouse", self.docker_gateway, 8123, "http", "/ping"),
                'superset': ServiceEndpoint("superset", "superset", self.docker_gateway, 8088, "http", "/health"),
                'metabase': ServiceEndpoint("metabase", "metabase", self.docker_gateway, 3000, "http", "/api/health"),
                'kafka_internal': ServiceEndpoint("kafka", "kafka", "kafka", 9092, "tcp", None),
                'kafka': ServiceEndpoint("kafka", "kafka", self.docker_gateway, 19092, "tcp", None),
                'connect': ServiceEndpoint("connect", "connect", self.docker_gateway, 8083, "http", "/"),
            }
        else:
            # Desde host: usar localhost para todo
            return {
                'clickhouse': ServiceEndpoint("clickhouse", "localhost", "localhost", 8123, "http", "/ping"),
                'superset': ServiceEndpoint("superset", "localhost", "localhost", 8088, "http", "/health"),
                'metabase': ServiceEndpoint("metabase", "localhost", "localhost", 3000, "http", "/api/health"),
                'kafka': ServiceEndpoint("kafka", "localhost", "localhost", 19092, "tcp", None),
                'connect': ServiceEndpoint("connect", "localhost", "localhost", 8083, "http", "/"),
            }
    
    def get_service_url(self, service_name: str, prefer_internal: bool = True) -> str:
        """Obtener URL de servicio según contexto"""
        endpoint = self.endpoints.get(service_name)
        if not endpoint:
            raise ValueError(f"Servicio '{service_name}' no configurado")
        
        # Elegir host apropiado
        if prefer_internal and self.context == 'container':
            host = endpoint.internal_host
        else:
            host = endpoint.external_host
        
        if endpoint.protocol == "http":
            return f"http://{host}:{endpoint.port}"
        else:
            return f"{host}:{endpoint.port}"
    
    def execute_command(self, command: List[str], container: Optional[str] = None, timeout: int = 30) -> Tuple[bool, str]:
        """Ejecutar comando según contexto"""
        try:
            if self.context == 'container' and not container:
                # Dentro de contenedor, ejecutar directamente
                result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
                return result.returncode == 0, result.stdout + result.stderr
            
            elif container:
                # Ejecutar en contenedor específico
                docker_cmd = ['docker', 'exec', container] + command
                result = subprocess.run(docker_cmd, capture_output=True, text=True, timeout=timeout)
                return result.returncode == 0, result.stdout + result.stderr
            
            else:
                # Desde host, ejecutar directamente
                result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
                return result.returncode == 0, result.stdout + result.stderr
        
        except subprocess.TimeoutExpired:
            return False, f"Comando timeout después de {timeout}s"
        except Exception as e:
            return False, f"Error ejecutando comando: {str(e)}"
    
    def make_http_request(self, service_name: str, path: str = "/", method: str = "GET", 
                         data: Optional[Dict] = None, prefer_internal: bool = True,
                         timeout: int = 10) -> Tuple[bool, Any]:
        """Hacer petición HTTP según contexto"""
        try:
            base_url = self.get_service_url(service_name, prefer_internal)
            url = f"{base_url}{path}"
            
            self.logger.debug(f"🌐 HTTP {method} {url}")
            
            if method.upper() == "GET":
                response = requests.get(url, timeout=timeout)
            elif method.upper() == "POST":
                response = requests.post(url, json=data, timeout=timeout)
            else:
                raise ValueError(f"Método HTTP {method} no soportado")
            
            # Intentar parsear JSON, sino devolver texto
            try:
                return response.status_code == 200, response.json()
            except:
                return response.status_code == 200, response.text
        
        except Exception as e:
            self.logger.debug(f"❌ Error HTTP {service_name}: {e}")
            return False, str(e)
    
    def test_service_connectivity(self, service_name: str) -> Dict[str, Any]:
        """Test completo de conectividad de un servicio"""
        endpoint = self.endpoints.get(service_name)
        if not endpoint:
            return {"error": f"Servicio '{service_name}' no configurado"}
        
        result = {
            "service": service_name,
            "context": self.context,
            "tests": {
                "dns_internal": False,
                "dns_external": False,
                "port_internal": False,
                "port_external": False,
                "http_internal": False,
                "http_external": False,
                "kafka_specific": False  # Test específico para Kafka
            },
            "details": {}
        }
        
        # Test DNS
        try:
            socket.gethostbyname(endpoint.internal_host)
            result["tests"]["dns_internal"] = True
        except:
            pass
        
        try:
            socket.gethostbyname(endpoint.external_host)
            result["tests"]["dns_external"] = True
        except:
            pass
        
        # Test puertos
        def test_port(host: str, port: int) -> bool:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(3)
                result = sock.connect_ex((host, port))
                sock.close()
                return result == 0
            except:
                return False
        
        result["tests"]["port_internal"] = test_port(endpoint.internal_host, endpoint.port)
        result["tests"]["port_external"] = test_port(endpoint.external_host, endpoint.port)
        
        # Test HTTP si aplica
        if endpoint.protocol == "http" and endpoint.health_path:
            success, response = self.make_http_request(service_name, endpoint.health_path, prefer_internal=True)
            result["tests"]["http_internal"] = success
            if success:
                result["details"]["internal_response"] = str(response)[:100]
            
            success, response = self.make_http_request(service_name, endpoint.health_path, prefer_internal=False)
            result["tests"]["http_external"] = success
            if success:
                result["details"]["external_response"] = str(response)[:100]
        
        # Test específico para Kafka
        if "kafka" in service_name.lower():
            kafka_success = self._test_kafka_broker(endpoint)
            result["tests"]["kafka_specific"] = kafka_success
            if kafka_success:
                result["details"]["kafka_test"] = "Broker accessible and topics listable"
        
        # Calcular score
        total_tests = sum(1 for v in result["tests"].values() if isinstance(v, bool))
        passed_tests = sum(1 for v in result["tests"].values() if v is True)
        result["score"] = round((passed_tests / total_tests * 100) if total_tests > 0 else 0, 1)
        
        return result
    
    def _test_kafka_broker(self, endpoint: ServiceEndpoint) -> bool:
        """Test específico de broker Kafka"""
        try:
            if self.context == 'container':
                # Desde contenedor, usar puerto interno
                if "internal" in endpoint.name:
                    bootstrap_server = f"kafka:9092"
                else:
                    bootstrap_server = f"{self.docker_gateway}:19092"
            else:
                # Desde host
                bootstrap_server = f"localhost:19092"
            
            # Test usando kafka-topics command
            result = subprocess.run([
                'docker', 'exec', 'kafka', 'kafka-topics',
                '--bootstrap-server', bootstrap_server,
                '--list'
            ], capture_output=True, text=True, timeout=10)
            
            return result.returncode == 0
            
        except Exception as e:
            self.logger.debug(f"Kafka broker test error: {e}")
            return False
    
    def run_comprehensive_tests(self) -> Dict[str, Any]:
        """Ejecuta todos los tests de conectividad (usa sistema robusto si está disponible)"""
        
        # Si el sistema robusto está disponible, úsalo
        if ROBUST_AVAILABLE:
            self.logger.info("🚀 Usando sistema de tests ROBUSTO")
            try:
                tester = RobustServiceTester()
                # Configurar para ser más tolerante
                tester.retry_config.max_attempts = 3
                tester.retry_config.initial_delay = 1.0
                tester.retry_config.timeout_per_attempt = 10.0
                
                results = tester.run_all_tests()
                
                # Convertir formato para compatibilidad
                services_dict = {}
                for service, result in results['test_results'].items():
                    if hasattr(result, 'score'):
                        services_dict[service] = {
                            "score": result.score,
                            "status": f"{result.score:.1f}% - {result.message}"
                        }
                
                return {
                    "context": results['context'],
                    "gateway": results['gateway'],
                    "services": services_dict,
                    "functional_services": results['services_successful'],
                    "total_services": results['services_tested'],
                    "average_score": results['average_score'],
                    "overall_status": f"COMUNICACIÓN {results['overall_status']}",
                    "timestamp": results['timestamp'],
                    "enhanced": True
                }
                
            except Exception as e:
                self.logger.warning(f"⚠️ Sistema robusto falló: {e}, usando sistema básico")
        
        # Sistema básico de fallback
        self.logger.info("🧪 INICIANDO TEST UNIFICADO DE SERVICIOS")
        self.logger.info("=" * 50)
        
        services = ['clickhouse', 'superset', 'metabase', 'kafka', 'connect']
        results = {}
        total_score = 0
        functional_services = 0
        
        for service in services:
            self.logger.info(f"🔍 Testing {service}...")
            score, status = self.test_service_connectivity(service)
            results[service] = {"score": score, "status": status}
            total_score += score
            
            if score >= 70:  # Considerar funcional si score >= 70%
                functional_services += 1
                self.logger.info(f"   ✅ {service}: {score:.1f}% funcional")
            else:
                self.logger.info(f"   ⚠️ {service}: {score:.1f}% funcional")
        
        # Calcular score promedio
        avg_score = total_score / len(services) if services else 0
        
        # Determinar estado general
        if avg_score >= 85:
            overall_status = "COMUNICACIÓN EXCELENTE"
        elif avg_score >= 70:
            overall_status = "COMUNICACIÓN BUENA"
        elif avg_score >= 50:
            overall_status = "COMUNICACIÓN FUNCIONAL"
        else:
            overall_status = "COMUNICACIÓN CON PROBLEMAS"
        
        summary = {
            "context": self.context,
            "gateway": self.gateway,
            "services": results,
            "functional_services": functional_services,
            "total_services": len(services),
            "average_score": avg_score,
            "overall_status": overall_status,
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
            "enhanced": False
        }
        
        self.logger.info(f"\n{' ' * 10}=" * 50)
        self.logger.info("📊 RESUMEN UNIFICADO")
        self.logger.info(f"📍 Contexto: {self.context}")
        self.logger.info(f"📊 Servicios funcionales: {functional_services}/{len(services)}")
        self.logger.info(f"📈 Score promedio: {avg_score:.1f}%")
        self.logger.info(f"🎉 {overall_status}")
        
        return summary


def main():
    """Función principal"""
    configurator = UnifiedConfigurator()
    results = configurator.run_comprehensive_tests()
    
    # Guardar resultados
    logs_dir = Path("/app/logs") if Path("/app/logs").exists() else Path("./logs")
    logs_dir.mkdir(exist_ok=True)
    
    with open(logs_dir / "unified_connectivity_test.json", 'w') as f:
        json.dump(results, f, indent=2)
    
    # Exit code basado en resultado
    average_score = results.get("average_score", 0)
    return 0 if average_score >= 60 else 1


if __name__ == "__main__":
    exit(main())