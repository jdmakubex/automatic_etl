#!/usr/bin/env python3
"""
Test Exhaustivo de Conectividad y DNS entre Contenedores
======================================================

Prueba minuciosa de conectividad de red, DNS resolution, y comunicación
entre todos los contenedores del stack ETL.
"""
import os
import subprocess
import socket
import requests
import time
import json
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ContainerConnectivityTester:
    """Tester exhaustivo de conectividad entre contenedores"""
    
    def __init__(self):
        """Inicializar tester con configuración de servicios"""
        self.services = {
            'clickhouse': {'host': 'clickhouse', 'ports': [8123, 9000]},
            'metabase': {'host': 'metabase', 'ports': [3000]},
            'superset': {'host': 'superset', 'ports': [8088]},
            'connect': {'host': 'connect', 'ports': [8083]},
            'kafka': {'host': 'kafka', 'ports': [9092]},
            'zookeeper': {'host': 'zookeeper', 'ports': [2181]},
            'superset-db': {'host': 'superset-db', 'ports': [5432]},
            'metabase-db': {'host': 'metabase-db', 'ports': [5432]}
        }
        
        # URLs de health check
        self.health_urls = {
            'clickhouse': 'http://clickhouse:8123/ping',
            'metabase': 'http://metabase:3000/api/health',
            'superset': 'http://superset:8088/health',
            'connect': 'http://connect:8083/connectors'
        }
        
        self.results = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'tests': {},
            'summary': {'passed': 0, 'failed': 0, 'warnings': 0}
        }
    
    def test_dns_resolution(self) -> dict:
        """Test DNS resolution para todos los servicios"""
        logger.info("🔍 TEST 1: DNS Resolution")
        test_result = {'name': 'dns_resolution', 'status': 'passed', 'details': []}
        
        for service, config in self.services.items():
            hostname = config['host']
            try:
                # Intentar resolver DNS
                ip = socket.gethostbyname(hostname)
                test_result['details'].append(f"✅ {hostname} → {ip}")
                logger.info(f"   ✅ {hostname} → {ip}")
                
            except socket.gaierror as e:
                test_result['status'] = 'failed'
                test_result['details'].append(f"❌ {hostname} → DNS ERROR: {e}")
                logger.error(f"   ❌ {hostname} → DNS ERROR: {e}")
        
        return test_result
    
    def test_port_connectivity(self) -> dict:
        """Test conectividad TCP a todos los puertos"""
        logger.info("🔌 TEST 2: Port Connectivity")
        test_result = {'name': 'port_connectivity', 'status': 'passed', 'details': []}
        
        for service, config in self.services.items():
            hostname = config['host']
            for port in config['ports']:
                try:
                    # Test TCP connection
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(5)
                    result = sock.connect_ex((hostname, port))
                    sock.close()
                    
                    if result == 0:
                        test_result['details'].append(f"✅ {hostname}:{port} → OPEN")
                        logger.info(f"   ✅ {hostname}:{port} → OPEN")
                    else:
                        test_result['status'] = 'failed' if test_result['status'] == 'passed' else test_result['status']
                        test_result['details'].append(f"❌ {hostname}:{port} → CLOSED")
                        logger.error(f"   ❌ {hostname}:{port} → CLOSED")
                        
                except Exception as e:
                    test_result['status'] = 'failed'
                    test_result['details'].append(f"❌ {hostname}:{port} → ERROR: {e}")
                    logger.error(f"   ❌ {hostname}:{port} → ERROR: {e}")
        
        return test_result
    
    def test_http_health_checks(self) -> dict:
        """Test HTTP health checks de servicios web"""
        logger.info("🌐 TEST 3: HTTP Health Checks")
        test_result = {'name': 'http_health', 'status': 'passed', 'details': []}
        
        for service, url in self.health_urls.items():
            try:
                response = requests.get(url, timeout=10)
                status_code = response.status_code
                
                if status_code == 200:
                    test_result['details'].append(f"✅ {service} ({url}) → {status_code} OK")
                    logger.info(f"   ✅ {service} → {status_code} OK")
                else:
                    test_result['status'] = 'warning' if test_result['status'] == 'passed' else test_result['status']
                    test_result['details'].append(f"⚠️  {service} ({url}) → {status_code}")
                    logger.warning(f"   ⚠️  {service} → {status_code}")
                    
            except requests.exceptions.ConnectTimeout:
                test_result['status'] = 'failed'
                test_result['details'].append(f"❌ {service} ({url}) → TIMEOUT")
                logger.error(f"   ❌ {service} → TIMEOUT")
                
            except requests.exceptions.ConnectionError as e:
                test_result['status'] = 'failed'
                test_result['details'].append(f"❌ {service} ({url}) → CONNECTION ERROR")
                logger.error(f"   ❌ {service} → CONNECTION ERROR: {e}")
                
            except Exception as e:
                test_result['status'] = 'failed'
                test_result['details'].append(f"❌ {service} ({url}) → ERROR: {str(e)[:100]}")
                logger.error(f"   ❌ {service} → ERROR: {e}")
        
        return test_result
    
    def test_clickhouse_specific(self) -> dict:
        """Test específico de ClickHouse con queries reales"""
        logger.info("🗄️  TEST 4: ClickHouse Specific Tests")
        test_result = {'name': 'clickhouse_specific', 'status': 'passed', 'details': []}
        
        try:
            import clickhouse_connect
            from dotenv import load_dotenv
            
            load_dotenv('/app/.env')
            
            # Test conexión directa
            client = clickhouse_connect.get_client(
                host='clickhouse',
                port=8123,
                username=os.getenv('CH_USER', 'etl'),
                password=os.getenv('CH_PASSWORD', 'Et1Ingest!'),
                database=os.getenv('CLICKHOUSE_DATABASE', 'fgeo_analytics')
            )
            
            # Test 1: Conectividad básica
            version = client.query("SELECT version()").result_rows[0][0]
            test_result['details'].append(f"✅ ClickHouse Version: {version}")
            logger.info(f"   ✅ ClickHouse Version: {version}")
            
            # Test 2: Acceso a la database
            databases = client.query("SHOW DATABASES").result_rows
            db_names = [row[0] for row in databases]
            test_result['details'].append(f"✅ Databases disponibles: {', '.join(db_names)}")
            logger.info(f"   ✅ Databases: {len(db_names)} encontradas")
            
            # Test 3: Tablas en fgeo_analytics
            tables = client.query("SELECT name, total_rows FROM system.tables WHERE database = 'fgeo_analytics' AND total_rows > 0").result_rows
            if tables:
                test_result['details'].append(f"✅ Tablas con datos: {len(tables)}")
                for table_name, rows in tables[:5]:  # Mostrar primeras 5
                    test_result['details'].append(f"   📊 {table_name}: {rows:,} filas")
                    logger.info(f"      📊 {table_name}: {rows:,} filas")
            else:
                test_result['status'] = 'warning'
                test_result['details'].append(f"⚠️  No hay tablas con datos en fgeo_analytics")
                logger.warning("   ⚠️  No hay tablas con datos")
            
            # Test 4: Permisos de usuario
            current_user = client.query("SELECT currentUser()").result_rows[0][0]
            test_result['details'].append(f"✅ Usuario actual: {current_user}")
            logger.info(f"   ✅ Usuario: {current_user}")
            
        except Exception as e:
            test_result['status'] = 'failed'
            test_result['details'].append(f"❌ ClickHouse connection failed: {e}")
            logger.error(f"   ❌ ClickHouse ERROR: {e}")
        
        return test_result
    
    def test_metabase_specific(self) -> dict:
        """Test específico de Metabase"""
        logger.info("📊 TEST 5: Metabase Specific Tests")
        test_result = {'name': 'metabase_specific', 'status': 'passed', 'details': []}
        
        try:
            from dotenv import load_dotenv
            load_dotenv('/app/.env')
            
            METABASE_URL = 'http://metabase:3000'
            ADMIN = os.getenv('METABASE_ADMIN', 'admin@admin.com')
            PASSWORD = os.getenv('METABASE_PASSWORD', 'Admin123!')
            
            # Test 1: Health check
            health_resp = requests.get(f'{METABASE_URL}/api/health', timeout=10)
            if health_resp.status_code == 200:
                test_result['details'].append(f"✅ Metabase health check OK")
                logger.info(f"   ✅ Health check OK")
            else:
                test_result['status'] = 'warning'
                test_result['details'].append(f"⚠️  Health check: {health_resp.status_code}")
            
            # Test 2: Autenticación
            auth_resp = requests.post(f'{METABASE_URL}/api/session', 
                                    json={'username': ADMIN, 'password': PASSWORD})
            if auth_resp.status_code == 200:
                session_id = auth_resp.json().get('id')
                test_result['details'].append(f"✅ Autenticación exitosa")
                logger.info(f"   ✅ Autenticación OK")
                
                # Test 3: Databases
                headers = {'X-Metabase-Session': session_id}
                db_resp = requests.get(f'{METABASE_URL}/api/database', headers=headers)
                if db_resp.status_code == 200:
                    databases = db_resp.json().get('data', [])
                    clickhouse_dbs = [db for db in databases if db.get('engine') == 'clickhouse']
                    test_result['details'].append(f"✅ Databases: {len(databases)}, ClickHouse: {len(clickhouse_dbs)}")
                    logger.info(f"   ✅ Databases encontradas: {len(clickhouse_dbs)} ClickHouse")
                    
                    # Test 4: Test query a ClickHouse desde Metabase
                    if clickhouse_dbs:
                        ch_db_id = clickhouse_dbs[0]['id']
                        query_payload = {
                            "type": "native",
                            "native": {"query": "SELECT 1 as test"},
                            "database": ch_db_id
                        }
                        query_resp = requests.post(f'{METABASE_URL}/api/dataset', 
                                                 json=query_payload, headers=headers)
                        if query_resp.status_code in [200, 202]:
                            test_result['details'].append(f"✅ Query test a ClickHouse exitosa")
                            logger.info(f"   ✅ Query test OK")
                        else:
                            test_result['status'] = 'failed'
                            test_result['details'].append(f"❌ Query test falló: {query_resp.status_code}")
                            logger.error(f"   ❌ Query test failed: {query_resp.status_code}")
                else:
                    test_result['status'] = 'failed'
                    test_result['details'].append(f"❌ No se pudieron obtener databases")
                    
            else:
                test_result['status'] = 'failed'
                test_result['details'].append(f"❌ Autenticación falló: {auth_resp.status_code}")
                logger.error(f"   ❌ Auth failed: {auth_resp.status_code}")
                
        except Exception as e:
            test_result['status'] = 'failed'
            test_result['details'].append(f"❌ Metabase test error: {e}")
            logger.error(f"   ❌ Metabase ERROR: {e}")
        
        return test_result
    
    def test_network_configuration(self) -> dict:
        """Test configuración de red Docker"""
        logger.info("🌐 TEST 6: Docker Network Configuration")
        test_result = {'name': 'network_config', 'status': 'passed', 'details': []}
        
        try:
            # Obtener información de red
            result = subprocess.run(['docker', 'network', 'ls'], 
                                  capture_output=True, text=True)
            if result.returncode == 0:
                networks = result.stdout
                if 'etl_net' in networks:
                    test_result['details'].append(f"✅ Red etl_net encontrada")
                    logger.info(f"   ✅ Red etl_net OK")
                else:
                    test_result['status'] = 'failed'
                    test_result['details'].append(f"❌ Red etl_net no encontrada")
                    logger.error(f"   ❌ Red etl_net missing")
            
            # Inspeccionar red etl_net
            result = subprocess.run(['docker', 'network', 'inspect', 'etl_prod_etl_net'], 
                                  capture_output=True, text=True)
            if result.returncode == 0:
                network_info = json.loads(result.stdout)[0]
                containers = network_info.get('Containers', {})
                test_result['details'].append(f"✅ Contenedores en red: {len(containers)}")
                logger.info(f"   ✅ Contenedores en red: {len(containers)}")
                
                for container_id, info in list(containers.items())[:5]:
                    name = info.get('Name', 'unknown')
                    ip = info.get('IPv4Address', '').split('/')[0]
                    test_result['details'].append(f"   🔗 {name}: {ip}")
                    logger.info(f"      🔗 {name}: {ip}")
            else:
                test_result['status'] = 'warning'
                test_result['details'].append(f"⚠️  No se pudo inspeccionar la red")
                
        except Exception as e:
            test_result['status'] = 'failed'
            test_result['details'].append(f"❌ Network test error: {e}")
            logger.error(f"   ❌ Network ERROR: {e}")
        
        return test_result
    
    def run_all_tests(self) -> dict:
        """Ejecutar todos los tests de conectividad"""
        logger.info("🧪 INICIANDO TESTS EXHAUSTIVOS DE CONECTIVIDAD")
        logger.info("=" * 70)
        
        # Ejecutar todos los tests
        tests = [
            self.test_dns_resolution,
            self.test_port_connectivity,
            self.test_http_health_checks,
            self.test_clickhouse_specific,
            self.test_metabase_specific,
            self.test_network_configuration
        ]
        
        for test_func in tests:
            try:
                result = test_func()
                self.results['tests'][result['name']] = result
                
                if result['status'] == 'passed':
                    self.results['summary']['passed'] += 1
                elif result['status'] == 'warning':
                    self.results['summary']['warnings'] += 1
                else:
                    self.results['summary']['failed'] += 1
                    
            except Exception as e:
                logger.error(f"Error en test {test_func.__name__}: {e}")
                self.results['summary']['failed'] += 1
        
        # Generar resumen
        logger.info("\n" + "=" * 70)
        logger.info("📊 RESUMEN DE CONECTIVIDAD:")
        logger.info(f"   ✅ Tests pasados: {self.results['summary']['passed']}")
        logger.info(f"   ⚠️  Tests con warnings: {self.results['summary']['warnings']}")
        logger.info(f"   ❌ Tests fallidos: {self.results['summary']['failed']}")
        logger.info(f"   📊 Total tests: {len(tests)}")
        
        # Calcular score
        total_tests = len(tests)
        score = (self.results['summary']['passed'] + 0.5 * self.results['summary']['warnings']) / total_tests * 100
        logger.info(f"   🎯 Score de conectividad: {score:.1f}%")
        
        self.results['connectivity_score'] = score
        
        if score >= 80:
            logger.info("🎉 ¡CONECTIVIDAD EXCELENTE!")
        elif score >= 60:
            logger.warning("⚠️  CONECTIVIDAD CON PROBLEMAS MENORES")
        else:
            logger.error("💥 CONECTIVIDAD CON PROBLEMAS GRAVES")
        
        # Recomendaciones
        if self.results['summary']['failed'] > 0:
            logger.info("\n🔧 RECOMENDACIONES:")
            logger.info("   1. Verificar que todos los contenedores estén running")
            logger.info("   2. Revisar configuración de red Docker")
            logger.info("   3. Validar variables de entorno")
            logger.info("   4. Comprobar logs de contenedores con errores")
        
        return self.results


def main():
    """Función principal"""
    tester = ContainerConnectivityTester()
    results = tester.run_all_tests()
    
    # Guardar resultados
    report_path = '/app/logs/connectivity_test_report.json'
    with open(report_path, 'w') as f:
        json.dump(results, f, indent=2)
    
    logger.info(f"\n📝 Reporte completo guardado en: {report_path}")
    
    # Return exit code based on connectivity score
    score = results.get('connectivity_score', 0)
    return 0 if score >= 60 else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())