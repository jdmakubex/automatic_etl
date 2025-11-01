#!/usr/bin/env python3
"""
🚀 ROBUST SERVICE TESTER
========================

Sistema de verificación robusto con reintentos automáticos, 
tolerancia a fallos y health checks profundos.
"""

import time
import json
import logging
import subprocess
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from urllib.parse import urlparse
import requests
import socket
from contextlib import contextmanager
import threading

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class RetryConfig:
    """Configuración de reintentos"""
    max_attempts: int = 5
    initial_delay: float = 1.0
    backoff_factor: float = 1.5
    max_delay: float = 30.0
    timeout_per_attempt: float = 10.0

@dataclass  
class TestResult:
    """Resultado de un test individual"""
    service: str
    test_name: str
    success: bool
    score: float
    message: str
    duration: float
    attempts_made: int = 1
    details: Dict[str, Any] = None

    def __post_init__(self):
        if self.details is None:
            self.details = {}

class RobustServiceTester:
    """Tester robusto de servicios con reintentos automáticos"""
    
    def __init__(self):
        self.retry_config = RetryConfig()
        self.context = self._detect_context()
        self.gateway = self._get_docker_gateway()
        self.test_results = []
        
    def _detect_context(self) -> str:
        """Detecta si estamos en host o contenedor"""
        try:
            with open('/proc/1/cgroup', 'r') as f:
                return 'container' if 'docker' in f.read() else 'host'
        except:
            return 'host'
    
    def _get_docker_gateway(self) -> str:
        """Obtiene el gateway de Docker según el contexto"""
        if self.context == 'container':
            try:
                result = subprocess.run(['ip', 'route', 'show', 'default'], 
                                      capture_output=True, text=True, timeout=5)
                if result.returncode == 0:
                    gateway = result.stdout.split()[2]
                    logger.info(f"🐳 Gateway container: {gateway}")
                    return gateway
            except:
                pass
            return 'host.docker.internal'
        else:
            return 'localhost'
    
    def retry_with_backoff(self, func, *args, **kwargs) -> Tuple[bool, Any, int]:
        """
        Ejecuta una función con reintentos y backoff exponencial
        
        Returns:
            (success, result, attempts_made)
        """
        delay = self.retry_config.initial_delay
        
        for attempt in range(1, self.retry_config.max_attempts + 1):
            try:
                logger.info(f"🔄 Intento {attempt}/{self.retry_config.max_attempts}")
                result = func(*args, **kwargs)
                logger.info(f"✅ Éxito en intento {attempt}")
                return True, result, attempt
                
            except Exception as e:
                logger.warning(f"❌ Intento {attempt} falló: {e}")
                
                if attempt < self.retry_config.max_attempts:
                    logger.info(f"⏳ Esperando {delay:.1f}s antes del siguiente intento...")
                    time.sleep(delay)
                    delay = min(delay * self.retry_config.backoff_factor, self.retry_config.max_delay)
                else:
                    logger.error(f"💥 Todos los intentos fallaron después de {attempt} intentos")
        
        return False, None, self.retry_config.max_attempts
    
    @contextmanager
    def timeout_context(self, seconds: float):
        """Context manager para timeout"""
        def timeout_handler():
            raise TimeoutError(f"Operación excedió {seconds}s")
        
        timer = threading.Timer(seconds, timeout_handler)
        timer.start()
        try:
            yield
        finally:
            timer.cancel()
    
    def test_kafka_comprehensive(self) -> TestResult:
        """Test comprehensivo de Kafka con reintentos"""
        start_time = time.time()
        service = "kafka"
        
        def _test_kafka():
            results = {}
            
            # Test 1: Conexión TCP al broker
            logger.info("🔍 Test 1: Conexión TCP al broker")
            with self.timeout_context(self.retry_config.timeout_per_attempt):
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                result = sock.connect_ex((self.gateway, 19092))
                sock.close()
                results['tcp_connection'] = result == 0
            
            # Test 2: Listar topics usando kafka-topics
            logger.info("🔍 Test 2: Listar topics")
            with self.timeout_context(self.retry_config.timeout_per_attempt):
                cmd = [
                    'docker', 'exec', 'kafka', 
                    'kafka-topics', '--bootstrap-server', 'localhost:9092', '--list'
                ]
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=8)
                results['list_topics'] = result.returncode == 0
                if results['list_topics']:
                    results['topics_count'] = len(result.stdout.strip().split('\n')) if result.stdout.strip() else 0
            
            # Test 3: Crear topic de prueba
            logger.info("🔍 Test 3: Crear topic de prueba")
            with self.timeout_context(self.retry_config.timeout_per_attempt):
                test_topic = f"health-check-{int(time.time())}"
                cmd = [
                    'docker', 'exec', 'kafka',
                    'kafka-topics', '--bootstrap-server', 'localhost:9092',
                    '--create', '--topic', test_topic, '--partitions', '1', '--replication-factor', '1'
                ]
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
                results['create_topic'] = result.returncode == 0
                
                # Test 4: Verificar que el topic existe
                if results['create_topic']:
                    logger.info("🔍 Test 4: Verificar topic creado")
                    cmd_verify = [
                        'docker', 'exec', 'kafka',
                        'kafka-topics', '--bootstrap-server', 'localhost:9092', '--describe', '--topic', test_topic
                    ]
                    verify_result = subprocess.run(cmd_verify, capture_output=True, text=True, timeout=5)
                    results['verify_topic'] = verify_result.returncode == 0
                    
                    # Limpiar: eliminar topic de prueba
                    cmd_delete = [
                        'docker', 'exec', 'kafka',
                        'kafka-topics', '--bootstrap-server', 'localhost:9092', '--delete', '--topic', test_topic
                    ]
                    subprocess.run(cmd_delete, capture_output=True, timeout=5)
                else:
                    results['verify_topic'] = False
            
            # Test 5: Verificar broker metadata
            logger.info("🔍 Test 5: Broker metadata")
            with self.timeout_context(self.retry_config.timeout_per_attempt):
                cmd = [
                    'docker', 'exec', 'kafka',
                    'kafka-broker-api-versions', '--bootstrap-server', 'localhost:9092'
                ]
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=8)
                results['broker_metadata'] = result.returncode == 0
            
            return results
        
        # Ejecutar con reintentos
        success, kafka_results, attempts = self.retry_with_backoff(_test_kafka)
        
        if success and kafka_results:
            passed_tests = sum(1 for v in kafka_results.values() if isinstance(v, bool) and v)
            total_tests = sum(1 for v in kafka_results.values() if isinstance(v, bool))
            score = (passed_tests / total_tests) * 100 if total_tests > 0 else 0
            
            if score >= 90:
                status = "EXCELENTE"
            elif score >= 80:
                status = "BUENO" 
            elif score >= 60:
                status = "FUNCIONAL"
            else:
                status = "PROBLEMAS"
                
            message = f"{score:.1f}% - {status} ({passed_tests}/{total_tests} tests)"
        else:
            score = 0
            message = "FALLO TOTAL - Sin conectividad"
            kafka_results = {}
        
        duration = time.time() - start_time
        
        return TestResult(
            service=service,
            test_name="comprehensive_kafka",
            success=success and score >= 80,
            score=score,
            message=message,
            duration=duration,
            attempts_made=attempts,
            details=kafka_results or {}
        )
    
    def test_clickhouse_comprehensive(self) -> TestResult:
        """Test comprehensivo de ClickHouse"""
        start_time = time.time()
        service = "clickhouse"
        
        def _test_clickhouse():
            results = {}
            base_url = f"http://{self.gateway}:8123"
            
            # Credenciales de ClickHouse
            ch_user = "default"
            ch_password = os.getenv("CLICKHOUSE_DEFAULT_PASSWORD", "ClickHouse123!")
            auth = (ch_user, ch_password)
            
            # Test 1: Ping básico
            logger.info("🔍 Test 1: ClickHouse ping")
            with self.timeout_context(self.retry_config.timeout_per_attempt):
                response = requests.get(f"{base_url}/ping", auth=auth, timeout=5)
                results['ping'] = response.status_code == 200 and "Ok." in response.text
            
            # Test 2: Query de sistema  
            logger.info("🔍 Test 2: System query")
            with self.timeout_context(self.retry_config.timeout_per_attempt):
                response = requests.get(f"{base_url}/?query=SELECT version()", auth=auth, timeout=8)
                results['system_query'] = response.status_code == 200 and len(response.text.strip()) > 0
                if results['system_query']:
                    results['version'] = response.text.strip()
            
            # Test 3: Crear tabla de prueba
            logger.info("🔍 Test 3: Crear tabla de prueba")
            with self.timeout_context(self.retry_config.timeout_per_attempt):
                table_name = f"health_check_{int(time.time())}"
                create_query = f"CREATE TABLE {table_name} (id UInt32, timestamp DateTime) ENGINE = Memory"
                
                # Usar POST con autenticación
                headers = {'Content-Type': 'text/plain'}
                response = requests.post(f"{base_url}/", data=create_query, headers=headers, auth=auth, timeout=10)
                results['create_table'] = response.status_code == 200
                
                if results['create_table']:
                    # Test 4: Insertar datos
                    logger.info("🔍 Test 4: Insertar datos")
                    insert_query = f"INSERT INTO {table_name} VALUES (1, now())"
                    insert_response = requests.post(f"{base_url}/", data=insert_query, headers=headers, auth=auth, timeout=5)
                    results['insert_data'] = insert_response.status_code == 200
                    
                    if results['insert_data']:
                        # Test 5: Consultar datos
                        logger.info("🔍 Test 5: Consultar datos")
                        select_query = f"SELECT count(*) FROM {table_name}"
                        select_response = requests.post(f"{base_url}/", data=select_query, headers=headers, auth=auth, timeout=5)
                        results['select_data'] = (select_response.status_code == 200 and 
                                                select_response.text.strip() == "1")
                    else:
                        results['select_data'] = False
                    
                    # Limpiar
                    try:
                        drop_query = f"DROP TABLE {table_name}"
                        requests.post(f"{base_url}/", data=drop_query, headers=headers, auth=auth, timeout=5)
                    except:
                        pass  # Ignorar errores de limpieza
                else:
                    results['insert_data'] = False
                    results['select_data'] = False
            
            return results
        
        success, ch_results, attempts = self.retry_with_backoff(_test_clickhouse)
        
        if success and ch_results:
            passed_tests = sum(1 for v in ch_results.values() if isinstance(v, bool) and v)
            total_tests = sum(1 for v in ch_results.values() if isinstance(v, bool))
            score = (passed_tests / total_tests) * 100 if total_tests > 0 else 0
            message = f"{score:.1f}% - {passed_tests}/{total_tests} tests"
        else:
            score = 0
            message = "FALLO TOTAL"
            ch_results = {}
        
        duration = time.time() - start_time
        
        return TestResult(
            service=service,
            test_name="comprehensive_clickhouse", 
            success=success and score >= 80,
            score=score,
            message=message,
            duration=duration,
            attempts_made=attempts,
            details=ch_results or {}
        )
    
    def test_superset_comprehensive(self) -> TestResult:
        """Test comprehensivo de Superset"""
        start_time = time.time()
        service = "superset"
        
        def _test_superset():
            results = {}
            base_url = f"http://{self.gateway}:8088"
            
            # Test 1: Health endpoint
            logger.info("🔍 Test 1: Superset health")
            with self.timeout_context(self.retry_config.timeout_per_attempt):
                response = requests.get(f"{base_url}/health", timeout=5)
                results['health'] = response.status_code == 200
            
            # Test 2: API heartbeat
            logger.info("🔍 Test 2: API heartbeat")
            with self.timeout_context(self.retry_config.timeout_per_attempt):
                response = requests.get(f"{base_url}/api/v1/security/login", timeout=8)
                results['api_available'] = response.status_code in [200, 401, 405]  # Login puede requerir auth
            
            # Test 3: Assets loading
            logger.info("🔍 Test 3: Assets loading")
            with self.timeout_context(self.retry_config.timeout_per_attempt):
                response = requests.get(f"{base_url}/static/assets/images/superset-logo-horiz.png", timeout=5)
                results['assets_loading'] = response.status_code == 200
            
            return results
        
        success, superset_results, attempts = self.retry_with_backoff(_test_superset)
        
        if success and superset_results:
            passed_tests = sum(1 for v in superset_results.values() if isinstance(v, bool) and v)
            total_tests = sum(1 for v in superset_results.values() if isinstance(v, bool))
            score = (passed_tests / total_tests) * 100 if total_tests > 0 else 0
            message = f"{score:.1f}% - {passed_tests}/{total_tests} tests"
        else:
            score = 0
            message = "FALLO TOTAL"
            superset_results = {}
        
        duration = time.time() - start_time
        
        return TestResult(
            service=service,
            test_name="comprehensive_superset",
            success=success and score >= 80,
            score=score,
            message=message,
            duration=duration,
            attempts_made=attempts,
            details=superset_results or {}
        )
    
    def test_metabase_comprehensive(self) -> TestResult:
        """Test comprehensivo de Metabase"""
        start_time = time.time()
        service = "metabase"
        
        def _test_metabase():
            results = {}
            base_url = f"http://{self.gateway}:3000"
            
            # Test 1: Health check
            logger.info("🔍 Test 1: Metabase health")
            with self.timeout_context(self.retry_config.timeout_per_attempt):
                response = requests.get(f"{base_url}/api/health", timeout=5)
                results['health'] = response.status_code == 200
            
            # Test 2: Session info
            logger.info("🔍 Test 2: Session info")  
            with self.timeout_context(self.retry_config.timeout_per_attempt):
                response = requests.get(f"{base_url}/api/session/properties", timeout=8)
                results['session'] = response.status_code == 200
            
            # Test 3: Database list
            logger.info("🔍 Test 3: Database endpoint")
            with self.timeout_context(self.retry_config.timeout_per_attempt):
                response = requests.get(f"{base_url}/api/database", timeout=5)
                results['database_api'] = response.status_code in [200, 401, 403]  # Puede requerir auth
            
            return results
        
        success, metabase_results, attempts = self.retry_with_backoff(_test_metabase)
        
        if success and metabase_results:
            passed_tests = sum(1 for v in metabase_results.values() if isinstance(v, bool) and v) 
            total_tests = sum(1 for v in metabase_results.values() if isinstance(v, bool))
            score = (passed_tests / total_tests) * 100 if total_tests > 0 else 0
            message = f"{score:.1f}% - {passed_tests}/{total_tests} tests"
        else:
            score = 0
            message = "FALLO TOTAL"
            metabase_results = {}
        
        duration = time.time() - start_time
        
        return TestResult(
            service=service,
            test_name="comprehensive_metabase",
            success=success and score >= 80,
            score=score,
            message=message,
            duration=duration,
            attempts_made=attempts,
            details=metabase_results or {}
        )
    
    def test_connect_comprehensive(self) -> TestResult:
        """Test comprehensivo de Kafka Connect"""
        start_time = time.time()
        service = "connect"
        
        def _test_connect():
            results = {}
            base_url = f"http://{self.gateway}:8083"
            
            # Test 1: Root endpoint
            logger.info("🔍 Test 1: Connect root")
            with self.timeout_context(self.retry_config.timeout_per_attempt):
                response = requests.get(f"{base_url}/", timeout=5)
                results['root'] = response.status_code == 200
            
            # Test 2: Connectors list
            logger.info("🔍 Test 2: Connectors")
            with self.timeout_context(self.retry_config.timeout_per_attempt):
                response = requests.get(f"{base_url}/connectors", timeout=8)
                results['connectors'] = response.status_code == 200
            
            # Test 3: Connector plugins
            logger.info("🔍 Test 3: Connector plugins")
            with self.timeout_context(self.retry_config.timeout_per_attempt):
                response = requests.get(f"{base_url}/connector-plugins", timeout=5)
                results['plugins'] = response.status_code == 200
                if results['plugins']:
                    plugins_data = response.json()
                    results['plugins_count'] = len(plugins_data)
            
            return results
        
        success, connect_results, attempts = self.retry_with_backoff(_test_connect)
        
        if success and connect_results:
            passed_tests = sum(1 for v in connect_results.values() if isinstance(v, bool) and v)
            total_tests = sum(1 for v in connect_results.values() if isinstance(v, bool))
            score = (passed_tests / total_tests) * 100 if total_tests > 0 else 0
            message = f"{score:.1f}% - {passed_tests}/{total_tests} tests"
        else:
            score = 0
            message = "FALLO TOTAL"
            connect_results = {}
        
        duration = time.time() - start_time
        
        return TestResult(
            service=service,
            test_name="comprehensive_connect",
            success=success and score >= 80,
            score=score,
            message=message,
            duration=duration,
            attempts_made=attempts,
            details=connect_results or {}
        )
    
    def run_all_tests(self) -> Dict[str, Any]:
        """Ejecuta todos los tests comprehensivos"""
        logger.info("🚀 INICIANDO TESTS ROBUSTOS DE SERVICIOS")
        logger.info("=" * 60)
        logger.info(f"🎯 Contexto: {self.context}")
        logger.info(f"🌐 Gateway: {self.gateway}")
        logger.info(f"🔄 Max reintentos: {self.retry_config.max_attempts}")
        logger.info("=" * 60)
        
        tests = [
            ("kafka", self.test_kafka_comprehensive),
            ("clickhouse", self.test_clickhouse_comprehensive), 
            ("superset", self.test_superset_comprehensive),
            ("metabase", self.test_metabase_comprehensive),
            ("connect", self.test_connect_comprehensive)
        ]
        
        results = {}
        total_score = 0
        successful_services = 0
        
        for service_name, test_func in tests:
            logger.info(f"\n🔍 Testing {service_name.upper()}...")
            try:
                result = test_func()
                results[service_name] = result
                total_score += result.score
                
                if result.success:
                    successful_services += 1
                    status_icon = "✅"
                else:
                    status_icon = "❌"
                
                logger.info(f"   {status_icon} {service_name}: {result.message} "
                           f"({result.duration:.1f}s, {result.attempts_made} intentos)")
                
            except Exception as e:
                logger.error(f"   💥 {service_name}: Error crítico - {e}")
                results[service_name] = TestResult(
                    service=service_name,
                    test_name="error",
                    success=False,
                    score=0,
                    message=f"Error: {e}",
                    duration=0,
                    attempts_made=0
                )
        
        # Calcular métricas finales
        avg_score = total_score / len(tests) if tests else 0
        
        if avg_score >= 95:
            overall_status = "PERFECTO"
        elif avg_score >= 85:
            overall_status = "EXCELENTE"
        elif avg_score >= 75:
            overall_status = "BUENO"
        elif avg_score >= 60:
            overall_status = "FUNCIONAL"
        else:
            overall_status = "PROBLEMAS"
        
        summary = {
            "context": self.context,
            "gateway": self.gateway,
            "services_tested": len(tests),
            "services_successful": successful_services,
            "average_score": avg_score,
            "overall_status": overall_status,
            "test_results": results,
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        logger.info("\n" + "=" * 60)
        logger.info("📊 RESUMEN ROBUSTO DE TESTS")
        logger.info(f"📍 Contexto: {self.context}")
        logger.info(f"📊 Servicios exitosos: {successful_services}/{len(tests)}")
        logger.info(f"📈 Score promedio: {avg_score:.1f}%")
        logger.info(f"🎉 Estado general: {overall_status}")
        logger.info("=" * 60)
        
        return summary

def main():
    """Función principal"""
    tester = RobustServiceTester()
    
    # Configurar reintentos más agresivos
    tester.retry_config.max_attempts = 3
    tester.retry_config.initial_delay = 2.0
    tester.retry_config.timeout_per_attempt = 15.0
    
    results = tester.run_all_tests()
    
    # Guardar resultados
    import os
    logs_dir = "/app/logs" if os.path.exists("/app/logs") else "./logs"
    os.makedirs(logs_dir, exist_ok=True)
    
    with open(f"{logs_dir}/robust_test_results.json", 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    # Salir con código apropiado
    if results['average_score'] >= 80:
        exit(0)
    else:
        exit(1)

if __name__ == "__main__":
    main()