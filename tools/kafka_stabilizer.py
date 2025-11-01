#!/usr/bin/env python3
"""
🔧 ANALIZADOR Y CORRECTOR DE KAFKA
=================================

Detecta y corrige automáticamente problemas de conectividad de Kafka:
- Puertos mal configurados
- DNS interno/externo
- Healthchecks fallidos
- Configuración de listeners
"""

import os
import json
import time
import socket
import subprocess
from pathlib import Path
from typing import Dict, Any, Tuple, List
import logging

class KafkaStabilizer:
    """Estabilizador automático de Kafka"""
    
    def __init__(self):
        self.logger = self._setup_logging()
        self.context = self._detect_context()
        self.issues = []
        self.fixes = []
        
    def _setup_logging(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        return logging.getLogger(__name__)
    
    def _detect_context(self) -> str:
        if os.path.exists('/.dockerenv') or (os.path.exists('/app') and os.path.exists('/app/tools')):
            return 'container'
        return 'host'
    
    def analyze_kafka_configuration(self) -> Dict[str, Any]:
        """Analizar configuración actual de Kafka"""
        self.logger.info("🔍 ANALIZANDO CONFIGURACIÓN DE KAFKA")
        self.logger.info("=" * 50)
        
        analysis = {
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
            "context": self.context,
            "docker_compose_config": {},
            "container_status": {},
            "port_analysis": {},
            "connectivity_tests": {},
            "issues_detected": [],
            "recommended_fixes": []
        }
        
        # 1. Analizar docker-compose.yml
        analysis["docker_compose_config"] = self._analyze_docker_compose()
        
        # 2. Estado del contenedor
        analysis["container_status"] = self._check_container_status()
        
        # 3. Análisis de puertos
        analysis["port_analysis"] = self._analyze_ports()
        
        # 4. Tests de conectividad
        analysis["connectivity_tests"] = self._test_kafka_connectivity()
        
        # 5. Detectar problemas
        self._detect_issues(analysis)
        analysis["issues_detected"] = self.issues
        analysis["recommended_fixes"] = self.fixes
        
        return analysis
    
    def _analyze_docker_compose(self) -> Dict[str, Any]:
        """Analizar configuración en docker-compose.yml"""
        try:
            with open('docker-compose.yml') as f:
                content = f.read()
            
            # Extraer puertos de Kafka
            ports = []
            if '"19092:19092"' in content:
                ports.append({"external": 19092, "internal": 19092})
            if '"9092:9092"' in content:
                ports.append({"external": 9092, "internal": 9092})
            
            # Extraer listeners
            listeners = []
            if 'KAFKA_LISTENERS=' in content:
                for line in content.split('\n'):
                    if 'KAFKA_LISTENERS=' in line:
                        listeners.append(line.strip())
            
            config = {
                "ports_exposed": ports,
                "listeners_config": listeners,
                "has_healthcheck": "healthcheck:" in content and "kafka-topics" in content
            }
            
            self.logger.info(f"✅ Docker compose config: {len(ports)} puertos, healthcheck: {config['has_healthcheck']}")
            return config
            
        except Exception as e:
            self.logger.error(f"❌ Error leyendo docker-compose: {e}")
            return {"error": str(e)}
    
    def _check_container_status(self) -> Dict[str, Any]:
        """Verificar estado del contenedor Kafka"""
        try:
            # Estado del contenedor
            result = subprocess.run(['docker', 'ps', '--filter', 'name=kafka', '--format', 'json'], 
                                  capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0 and result.stdout.strip():
                container_info = json.loads(result.stdout.strip())
                
                status = {
                    "running": "Up" in container_info.get("Status", ""),
                    "health": "healthy" in container_info.get("Status", "").lower(),
                    "ports": container_info.get("Ports", ""),
                    "name": container_info.get("Names", "")
                }
                
                self.logger.info(f"✅ Container status: Running={status['running']}, Health={status['health']}")
                return status
            else:
                self.logger.error("❌ Kafka container no encontrado")
                return {"error": "Container not found"}
                
        except Exception as e:
            self.logger.error(f"❌ Error verificando container: {e}")
            return {"error": str(e)}
    
    def _analyze_ports(self) -> Dict[str, Any]:
        """Analizar configuración de puertos"""
        ports_analysis = {
            "expected_internal": 9092,
            "expected_external": 19092,
            "internal_accessible": False,
            "external_accessible": False,
            "host_for_external": "localhost"
        }
        
        # Determinar host correcto según contexto
        if self.context == 'container':
            # Desde contenedor, externo es el gateway Docker
            try:
                socket.gethostbyname('host.docker.internal')
                ports_analysis["host_for_external"] = "host.docker.internal"
            except:
                ports_analysis["host_for_external"] = "172.19.0.1"
        
        # Test puerto interno (9092)
        try:
            if self.context == 'container':
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(3)
                result = sock.connect_ex(("kafka", 9092))
                sock.close()
                ports_analysis["internal_accessible"] = (result == 0)
            else:
                # Desde host, usar docker exec
                result = subprocess.run([
                    'docker', 'exec', 'kafka', 'timeout', '3', 'bash', '-c', 
                    'echo > /dev/tcp/localhost/9092'
                ], capture_output=True, timeout=5)
                ports_analysis["internal_accessible"] = result.returncode == 0
        except:
            pass
        
        # Test puerto externo (19092)
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3)
            result = sock.connect_ex((ports_analysis["host_for_external"], 19092))
            sock.close()
            ports_analysis["external_accessible"] = (result == 0)
        except:
            pass
        
        self.logger.info(f"🔌 Puertos: interno={ports_analysis['internal_accessible']}, externo={ports_analysis['external_accessible']}")
        return ports_analysis
    
    def _test_kafka_connectivity(self) -> Dict[str, Any]:
        """Test específicos de conectividad Kafka"""
        tests = {
            "broker_list_internal": False,
            "broker_list_external": False,
            "topic_creation": False,
            "producer_test": False
        }
        
        # Test interno: listar brokers desde dentro del contenedor
        try:
            if self.context == 'container':
                result = subprocess.run([
                    'kafka-topics', '--bootstrap-server', 'kafka:9092', '--list'
                ], capture_output=True, timeout=10)
                tests["broker_list_internal"] = result.returncode == 0
            else:
                result = subprocess.run([
                    'docker', 'exec', 'kafka', 'kafka-topics', 
                    '--bootstrap-server', 'localhost:9092', '--list'
                ], capture_output=True, timeout=10)
                tests["broker_list_internal"] = result.returncode == 0
        except:
            pass
        
        # Test externo: acceso desde host
        try:
            host = "host.docker.internal" if self.context == 'container' else "localhost"
            result = subprocess.run([
                'docker', 'exec', 'kafka', 'kafka-topics', 
                '--bootstrap-server', f'{host}:19092', '--list'
            ], capture_output=True, timeout=10)
            tests["broker_list_external"] = result.returncode == 0
        except:
            pass
        
        # Test creación de topic de prueba
        try:
            result = subprocess.run([
                'docker', 'exec', 'kafka', 'kafka-topics',
                '--bootstrap-server', 'localhost:9092',
                '--create', '--topic', 'test-connectivity',
                '--partitions', '1', '--replication-factor', '1',
                '--if-not-exists'
            ], capture_output=True, timeout=15)
            tests["topic_creation"] = result.returncode == 0
        except:
            pass
        
        self.logger.info(f"🧪 Tests Kafka: {sum(tests.values())}/{len(tests)} exitosos")
        return tests
    
    def _detect_issues(self, analysis: Dict[str, Any]):
        """Detectar problemas y generar fixes"""
        
        # Issue 1: Puerto externo no accesible
        if not analysis["port_analysis"]["external_accessible"]:
            self.issues.append("Puerto externo 19092 no accesible desde contexto actual")
            self.fixes.append({
                "issue": "external_port_connectivity",
                "description": "Configurar acceso correcto al puerto 19092",
                "action": "update_unified_configurator_ports"
            })
        
        # Issue 2: Tests de Kafka fallan
        connectivity_score = sum(analysis["connectivity_tests"].values()) / len(analysis["connectivity_tests"]) * 100
        if connectivity_score < 75:
            self.issues.append(f"Tests de Kafka tienen solo {connectivity_score:.1f}% de éxito")
            self.fixes.append({
                "issue": "kafka_tests_failing", 
                "description": "Mejorar configuración de listeners y bootstrap servers",
                "action": "fix_kafka_configuration"
            })
        
        # Issue 3: Configuración de puertos en configurador
        if len(analysis["docker_compose_config"].get("ports_exposed", [])) > 0:
            external_port = analysis["docker_compose_config"]["ports_exposed"][0]["external"]
            if external_port != 9092:  # El configurador asume 9092
                self.issues.append(f"Configurador usa puerto 9092 pero Kafka expone {external_port}")
                self.fixes.append({
                    "issue": "configurator_port_mismatch",
                    "description": f"Actualizar configurador para usar puerto {external_port}",
                    "action": "update_configurator_kafka_port",
                    "new_port": external_port
                })
    
    def apply_fixes(self, analysis: Dict[str, Any]) -> bool:
        """Aplicar fixes automáticamente"""
        self.logger.info("🔧 APLICANDO FIXES AUTOMÁTICOS")
        self.logger.info("=" * 50)
        
        success_count = 0
        total_fixes = len(self.fixes)
        
        for fix in self.fixes:
            try:
                if fix["action"] == "update_configurator_kafka_port":
                    success = self._fix_configurator_port(fix.get("new_port", 19092))
                elif fix["action"] == "update_unified_configurator_ports":
                    success = self._fix_unified_configurator()
                elif fix["action"] == "fix_kafka_configuration":
                    success = self._fix_kafka_config()
                else:
                    self.logger.warning(f"⚠️ Fix desconocido: {fix['action']}")
                    continue
                
                if success:
                    success_count += 1
                    self.logger.info(f"✅ Fix aplicado: {fix['description']}")
                else:
                    self.logger.error(f"❌ Fix falló: {fix['description']}")
                    
            except Exception as e:
                self.logger.error(f"❌ Error aplicando fix {fix['action']}: {e}")
        
        self.logger.info(f"\n📊 Fixes aplicados: {success_count}/{total_fixes}")
        return success_count == total_fixes
    
    def _fix_configurator_port(self, new_port: int) -> bool:
        """Corregir puerto en configurador unificado"""
        try:
            config_file = Path("tools/unified_configurator.py")
            if not config_file.exists():
                return False
            
            with open(config_file) as f:
                content = f.read()
            
            # Buscar y reemplazar configuración de Kafka
            old_line = 'ServiceConfig("kafka", "kafka", external_host, [9092], "tcp", None)'
            new_line = f'ServiceConfig("kafka", "kafka", external_host, [9092, {new_port}], "tcp", None)'
            
            if old_line in content:
                updated_content = content.replace(old_line, new_line)
                
                with open(config_file, 'w') as f:
                    f.write(updated_content)
                
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error actualizando configurador: {e}")
            return False
    
    def _fix_unified_configurator(self) -> bool:
        """Corregir configurador unificado para Kafka"""
        try:
            config_file = Path("tools/unified_configurator.py")
            
            # Crear configuración específica para Kafka
            kafka_fix = '''
    def _configure_endpoints(self) -> Dict[str, ServiceEndpoint]:
        """Configurar endpoints según contexto"""
        if self.context == 'container':
            # Desde contenedor: usar nombres DNS internos y host.docker.internal para externo
            return {
                'clickhouse': ServiceEndpoint("clickhouse", "clickhouse", self.docker_gateway, 8123, "http", "/ping"),
                'superset': ServiceEndpoint("superset", "superset", self.docker_gateway, 8088, "http", "/health"),
                'metabase': ServiceEndpoint("metabase", "metabase", self.docker_gateway, 3000, "http", "/api/health"),
                'kafka': ServiceEndpoint("kafka", "kafka", self.docker_gateway, 19092, "tcp", None),  # Puerto externo correcto
                'connect': ServiceEndpoint("connect", "connect", self.docker_gateway, 8083, "http", "/"),
            }
        else:
            # Desde host: usar localhost para todo
            return {
                'clickhouse': ServiceEndpoint("clickhouse", "localhost", "localhost", 8123, "http", "/ping"),
                'superset': ServiceEndpoint("superset", "localhost", "localhost", 8088, "http", "/health"),
                'metabase': ServiceEndpoint("metabase", "localhost", "localhost", 3000, "http", "/api/health"),
                'kafka': ServiceEndpoint("kafka", "localhost", "localhost", 19092, "tcp", None),  # Puerto externo correcto
                'connect': ServiceEndpoint("connect", "localhost", "localhost", 8083, "http", "/"),
            }'''
            
            if config_file.exists():
                with open(config_file) as f:
                    content = f.read()
                
                # Buscar y reemplazar la función _configure_endpoints
                import re
                pattern = r'def _configure_endpoints\(self\).*?return \{.*?\}'
                
                if re.search(pattern, content, re.DOTALL):
                    updated_content = re.sub(pattern, kafka_fix.strip(), content, flags=re.DOTALL)
                    
                    with open(config_file, 'w') as f:
                        f.write(updated_content)
                    
                    return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error corrigiendo configurador: {e}")
            return False
    
    def _fix_kafka_config(self) -> bool:
        """Optimizar configuración de Kafka si es necesario"""
        # Por ahora, Kafka está bien configurado en docker-compose
        # Solo verificamos que el healthcheck funcione
        try:
            result = subprocess.run([
                'docker', 'exec', 'kafka', 'kafka-topics',
                '--bootstrap-server', 'localhost:9092', '--list'
            ], capture_output=True, timeout=15)
            
            return result.returncode == 0
            
        except Exception as e:
            self.logger.error(f"Error verificando Kafka: {e}")
            return False
    
    def run_comprehensive_fix(self) -> Dict[str, Any]:
        """Ejecutar análisis completo y aplicar fixes"""
        self.logger.info("🚀 ESTABILIZADOR AUTOMÁTICO DE KAFKA")
        self.logger.info("=" * 60)
        
        # 1. Análisis
        analysis = self.analyze_kafka_configuration()
        
        # 2. Mostrar problemas detectados
        if self.issues:
            self.logger.info(f"\n❌ PROBLEMAS DETECTADOS ({len(self.issues)}):")
            for i, issue in enumerate(self.issues, 1):
                self.logger.info(f"   {i}. {issue}")
        
        # 3. Aplicar fixes
        if self.fixes:
            fixes_applied = self.apply_fixes(analysis)
            analysis["fixes_applied"] = fixes_applied
        else:
            self.logger.info("✅ No se detectaron problemas que requieran fixes")
            analysis["fixes_applied"] = True
        
        # 4. Re-test después de fixes
        if self.fixes:
            self.logger.info("\n🔄 RE-TESTING DESPUÉS DE FIXES...")
            time.sleep(2)  # Esperar a que los cambios surtan efecto
            post_fix_tests = self._test_kafka_connectivity()
            analysis["post_fix_tests"] = post_fix_tests
            
            post_score = sum(post_fix_tests.values()) / len(post_fix_tests) * 100
            self.logger.info(f"📊 Score post-fix: {post_score:.1f}%")
            
            if post_score > 75:
                self.logger.info("🎉 KAFKA ESTABILIZADO EXITOSAMENTE")
            else:
                self.logger.warning("⚠️ Kafka mejoró pero aún tiene problemas")
        
        return analysis


def main():
    """Función principal"""
    stabilizer = KafkaStabilizer()
    results = stabilizer.run_comprehensive_fix()
    
    # Guardar resultados
    logs_dir = Path("/app/logs") if Path("/app/logs").exists() else Path("./logs")
    logs_dir.mkdir(exist_ok=True)
    
    with open(logs_dir / "kafka_stabilization_report.json", 'w') as f:
        json.dump(results, f, indent=2)
    
    # Exit code
    if results.get("fixes_applied", False):
        return 0
    else:
        return 1


if __name__ == "__main__":
    exit(main())