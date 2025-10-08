#!/usr/bin/env python3
"""
🏥 VALIDADOR DE SALUD AVANZADO
Sistema robusto para verificar el estado completo de todos los servicios ETL
"""

import requests
import json
import time
import logging
import os
import subprocess
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any

class AdvancedHealthValidator:
    """Validador avanzado de salud de servicios con recuperación automática"""
    
    def __init__(self):
        self.logger = self._setup_logging()
        # Detectar si estamos ejecutando desde host o contenedor
        self.execution_context = self._detect_execution_context()
        
        if self.execution_context == 'host':
            # Ejecución desde host - usar localhost con puertos mapeados
            self.clickhouse_host = os.getenv('CLICKHOUSE_HTTP_HOST', 'localhost')
            self.clickhouse_database = os.getenv('CLICKHOUSE_DATABASE', 'fgeo_analytics')
            self.kafka_host = os.getenv('KAFKA_HOST', 'localhost')
            self.connect_url = os.getenv('DEBEZIUM_CONNECT_URL', 'http://localhost:8083')
            self.superset_url = os.getenv('SUPERSET_URL', 'http://localhost:8088')
        else:
            # Ejecución desde contenedor - usar nombres de servicio
            self.clickhouse_host = os.getenv('CLICKHOUSE_HTTP_HOST', 'clickhouse')
            self.clickhouse_database = os.getenv('CLICKHOUSE_DATABASE', 'fgeo_analytics')
            self.kafka_host = os.getenv('KAFKA_HOST', 'kafka')
            self.connect_url = os.getenv('DEBEZIUM_CONNECT_URL', 'http://connect:8083')
            self.superset_url = os.getenv('SUPERSET_URL', 'http://superset:8088')
            
        # Cargar configuración después de establecer las URLs
        self.services_config = self._load_services_config()
        
    def _setup_logging(self) -> logging.Logger:
        """Configurar logging avanzado"""
        logger = logging.getLogger('HealthValidator')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
        
    def _detect_execution_context(self) -> str:
        """Detectar si estamos ejecutando desde host o contenedor"""
        try:
            # Verificar si estamos en un contenedor
            if os.path.exists('/.dockerenv'):
                return 'container'
            # Verificar si podemos resolver nombres de contenedor
            import socket
            try:
                socket.gethostbyname('clickhouse')
                return 'container'
            except socket.gaierror:
                return 'host'
        except Exception:
            return 'host'
    
    def _load_services_config(self) -> Dict[str, Any]:
        """Cargar configuración de servicios desde variables de entorno"""
        # Ajustar URLs según el contexto de ejecución
        if self.execution_context == 'host':
            clickhouse_endpoint = 'http://localhost:8123/ping'
            connect_endpoint = 'http://localhost:8083/connectors'
            superset_endpoint = 'http://localhost:8088/health'
        else:
            clickhouse_endpoint = 'http://clickhouse:8123/ping'
            connect_endpoint = 'http://connect:8083/connectors'
            superset_endpoint = 'http://superset:8088/health'
            
        return {
            'clickhouse': {
                'name': 'ClickHouse',
                'container': 'clickhouse',
                'health_endpoint': clickhouse_endpoint,
                'required': True,
                'timeout': 30
            },
            'kafka': {
                'name': 'Kafka',
                'container': 'kafka',
                'health_port': 19092,
                'required': True,
                'timeout': 45
            },
            'connect': {
                'name': 'Debezium Connect',
                'container': 'connect',
                'health_endpoint': connect_endpoint,
                'required': True,
                'timeout': 60
            },
            'superset': {
                'name': 'Superset',
                'container': 'superset',
                'health_endpoint': superset_endpoint,
                'required': False,
                'timeout': 30
            }
        }
    
    def validate_docker_services(self) -> Tuple[bool, List[str]]:
        """Validar que todos los contenedores Docker estén corriendo y saludables"""
        try:
            self.logger.info("🐳 Validando contenedores Docker...")
            
            # Obtener estado de contenedores
            result = subprocess.run(
                ['docker', 'ps', '--format', 'json'], 
                capture_output=True, 
                text=True, 
                timeout=30
            )
            
            if result.returncode != 0:
                self.logger.error(f"❌ Error consultando Docker: {result.stderr}")
                return False, ["Docker no accesible"]
            
            containers = []
            for line in result.stdout.strip().split('\n'):
                if line.strip():
                    containers.append(json.loads(line))
            
            issues = []
            required_containers = [svc['container'] for svc in self.services_config.values() if svc['required']]
            
            for container_name in required_containers:
                container = next((c for c in containers if c['Names'] == container_name), None)
                
                if not container:
                    issues.append(f"Contenedor {container_name} no encontrado")
                    continue
                
                status = container['Status']
                if 'healthy' not in status.lower() and 'up' not in status.lower():
                    issues.append(f"Contenedor {container_name} no saludable: {status}")
                    continue
                
                self.logger.info(f"✅ Contenedor {container_name}: {status}")
            
            success = len(issues) == 0
            if success:
                self.logger.info("✅ Todos los contenedores Docker están operativos")
            else:
                self.logger.error(f"❌ Problemas en contenedores: {', '.join(issues)}")
            
            return success, issues
            
        except Exception as e:
            self.logger.error(f"❌ Error validando Docker: {str(e)}")
            return False, [f"Error Docker: {str(e)}"]
    
    def validate_clickhouse_health(self) -> bool:
        """Validar que ClickHouse esté operativo y accesible"""
        try:
            self.logger.info("🏠 Validando ClickHouse...")
            
            # 1. Verificar conectividad HTTP básica
            response = requests.get(f"http://{self.clickhouse_host}:8123/ping", timeout=10)
            if response.status_code != 200:
                self.logger.error(f"❌ ClickHouse HTTP no responde: {response.status_code}")
                return False
            
            # 2. Verificar consulta básica
            query_response = requests.post(
                f"http://{self.clickhouse_host}:8123/",
                data="SELECT 1",
                timeout=10
            )
            if query_response.status_code != 200:
                self.logger.error(f"❌ ClickHouse no puede ejecutar consultas: {query_response.text}")
                return False
            
            # 3. Verificar que la base de datos existe, si no la crea
            db_response = requests.post(
                f"http://{self.clickhouse_host}:8123/",
                data=f"SHOW DATABASES",
                timeout=10
            )
            if self.clickhouse_database not in db_response.text:
                self.logger.info(f"📝 Creando base de datos {self.clickhouse_database}")
                create_response = requests.post(
                    f"http://{self.clickhouse_host}:8123/",
                    data=f"CREATE DATABASE IF NOT EXISTS {self.clickhouse_database}",
                    timeout=10
                )
                if create_response.status_code != 200:
                    self.logger.error(f"❌ No se pudo crear la base de datos: {create_response.text}")
                    return False
            
            # 4. Verificar usuarios y permisos básicos
            try:
                users_response = requests.post(
                    f"http://{self.clickhouse_host}:8123/",
                    data="SELECT name FROM system.users WHERE name != 'default'",
                    timeout=10
                )
                if users_response.status_code == 200:
                    users = users_response.text.strip().split('\n')
                    self.logger.info(f"👤 Usuarios ClickHouse detectados: {len([u for u in users if u])}")
                else:
                    self.logger.warning("⚠️  No se pudieron obtener usuarios de ClickHouse")
            except Exception as e:
                self.logger.debug(f"Info usuarios ClickHouse no disponible: {e}")
            
            self.logger.info("✅ ClickHouse operativo")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error validando ClickHouse: {str(e)}")
            return False
    
    def validate_kafka_health(self) -> bool:
        """Validar que Kafka esté operativo y accesible"""
        try:
            self.logger.info("📨 Validando Kafka...")
            
            # Usar kafkacat o scripts internos de Kafka para verificar
            result = subprocess.run([
                'docker', 'exec', 'kafka', 
                'kafka-topics', '--bootstrap-server', 'localhost:9092', '--list'
            ], capture_output=True, text=True, timeout=30)
            
            if result.returncode != 0:
                self.logger.warning(f"⚠️  Kafka topics no accesibles: {result.stderr}")
                # Intentar método alternativo
                return self._validate_kafka_alternative()
            
            topics = result.stdout.strip().split('\n')
            topic_count = len([t for t in topics if t.strip()])
            self.logger.info(f"📂 Kafka operativo - {topic_count} topics encontrados")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error validando Kafka: {str(e)}")
            return False
    
    def _validate_kafka_alternative(self) -> bool:
        """Método alternativo para validar Kafka"""
        try:
            # Verificar que el proceso Java de Kafka esté corriendo
            result = subprocess.run([
                'docker', 'exec', 'kafka', 'ps', 'aux'
            ], capture_output=True, text=True, timeout=15)
            
            if 'kafka.Kafka' in result.stdout:
                self.logger.info("✅ Proceso Kafka detectado")
                return True
            else:
                self.logger.error("❌ Proceso Kafka no encontrado")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Validación alternativa Kafka falló: {str(e)}")
            return False
    
    def validate_connect_health(self) -> bool:
        """Validar que Debezium Connect esté operativo"""
        try:
            self.logger.info("🔗 Validando Debezium Connect...")
            
            # 1. Verificar endpoint de salud
            response = requests.get(f"{self.connect_url}/connectors", timeout=15)
            if response.status_code != 200:
                self.logger.error(f"❌ Connect no responde: {response.status_code}")
                return False
            
            # 2. Verificar plugins disponibles
            plugins_response = requests.get(f"{self.connect_url}/connector-plugins", timeout=15)
            if plugins_response.status_code == 200:
                plugins = plugins_response.json()
                debezium_plugins = [p for p in plugins if 'debezium' in p.get('class', '').lower()]
                self.logger.info(f"🔌 Connect operativo - {len(debezium_plugins)} plugins Debezium")
            else:
                self.logger.warning("⚠️  No se pudieron obtener plugins de Connect")
            
            # 3. Verificar conectores activos
            connectors = response.json()
            self.logger.info(f"⚡ Conectores activos: {len(connectors)}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error validando Connect: {str(e)}")
            return False
    
    def validate_superset_health(self) -> bool:
        """Validar que Superset esté operativo (opcional)"""
        try:
            self.logger.info("📊 Validando Superset...")
            
            response = requests.get(f"{self.superset_url}/health", timeout=15)
            if response.status_code != 200:
                self.logger.warning(f"⚠️  Superset no responde: {response.status_code}")
                return False
            
            if response.text.strip() == "OK":
                self.logger.info("✅ Superset operativo")
                return True
            else:
                self.logger.warning(f"⚠️  Respuesta Superset inesperada: {response.text}")
                return False
                
        except Exception as e:
            self.logger.warning(f"⚠️  Superset no disponible: {str(e)}")
            return False
    
    def comprehensive_health_check(self) -> Tuple[bool, Dict[str, Any]]:
        """Ejecutar validación completa de salud de todos los servicios"""
        start_time = datetime.now()
        self.logger.info("🏥 === INICIANDO VALIDACIÓN COMPLETA DE SALUD ===")
        
        results = {
            'timestamp': start_time.isoformat(),
            'services': {},
            'overall_status': 'unknown',
            'critical_issues': [],
            'warnings': [],
            'duration_seconds': 0
        }
        
        # 1. Validar contenedores Docker
        docker_ok, docker_issues = self.validate_docker_services()
        results['services']['docker'] = {
            'status': 'healthy' if docker_ok else 'unhealthy',
            'issues': docker_issues
        }
        if not docker_ok:
            results['critical_issues'].extend(docker_issues)
        
        # 2. Validar ClickHouse
        ch_ok = self.validate_clickhouse_health()
        results['services']['clickhouse'] = {
            'status': 'healthy' if ch_ok else 'unhealthy',
            'required': True
        }
        if not ch_ok:
            results['critical_issues'].append("ClickHouse no operativo")
        
        # 3. Validar Kafka
        kafka_ok = self.validate_kafka_health()
        results['services']['kafka'] = {
            'status': 'healthy' if kafka_ok else 'unhealthy',
            'required': True
        }
        if not kafka_ok:
            results['critical_issues'].append("Kafka no operativo")
        
        # 4. Validar Debezium Connect
        connect_ok = self.validate_connect_health()
        results['services']['connect'] = {
            'status': 'healthy' if connect_ok else 'unhealthy',
            'required': True
        }
        if not connect_ok:
            results['critical_issues'].append("Debezium Connect no operativo")
        
        # 5. Validar Superset (opcional)
        superset_ok = self.validate_superset_health()
        results['services']['superset'] = {
            'status': 'healthy' if superset_ok else 'unhealthy',
            'required': False
        }
        if not superset_ok:
            results['warnings'].append("Superset no disponible")
        
        # Determinar estado general
        critical_services_ok = docker_ok and ch_ok and kafka_ok and connect_ok
        
        if critical_services_ok:
            if superset_ok:
                results['overall_status'] = 'fully_healthy'
                self.logger.info("🎉 TODOS LOS SERVICIOS OPERATIVOS")
            else:
                results['overall_status'] = 'mostly_healthy'
                self.logger.info("✅ SERVICIOS CRÍTICOS OPERATIVOS (Superset con problemas)")
        else:
            results['overall_status'] = 'unhealthy'
            self.logger.error("❌ SERVICIOS CRÍTICOS CON PROBLEMAS")
        
        # Calcular duración
        end_time = datetime.now()
        results['duration_seconds'] = (end_time - start_time).total_seconds()
        
        # Resumen final
        self.logger.info(f"🏁 Validación completada en {results['duration_seconds']:.1f}s")
        self.logger.info(f"📊 Estado general: {results['overall_status']}")
        if results['critical_issues']:
            self.logger.error(f"🚨 Problemas críticos: {len(results['critical_issues'])}")
        if results['warnings']:
            self.logger.warning(f"⚠️  Advertencias: {len(results['warnings'])}")
        
        return critical_services_ok, results


def main():
    """Función principal para ejecutar validación independiente"""
    validator = AdvancedHealthValidator()
    success, results = validator.comprehensive_health_check()
    
    # Guardar resultados
    results_file = '/app/logs/health_check_results.json'
    try:
        os.makedirs(os.path.dirname(results_file), exist_ok=True)
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2)
        validator.logger.info(f"📝 Resultados guardados en {results_file}")
    except Exception as e:
        validator.logger.warning(f"⚠️  No se pudieron guardar resultados: {e}")
    
    return 0 if success else 1


if __name__ == "__main__":
    exit(main())