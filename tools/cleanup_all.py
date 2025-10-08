#!/usr/bin/env python3
"""
🧹 SCRIPT DE LIMPIEZA COMPLETA DEL PIPELINE ETL
Limpia TODOS los datos existentes para comenzar desde cero:
- Conectores de Kafka Connect
- Tópicos de Kafka  
- Datos y tablas de ClickHouse
- Configuraciones de Superset
- Logs existentes
"""

import subprocess
import time
import json
import logging
from datetime import datetime

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
            logging.FileHandler('./logs/cleanup.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ETLCleaner:
    def __init__(self):
        self.services = {
            'connect': 'http://connect:8083',
            'kafka': 'kafka:9092', 
            'clickhouse': 'clickhouse:8123',
            'superset': 'http://superset:8088'
        }
        
    def run_command(self, cmd, description, ignore_errors=True):
        """Ejecutar comando con logging"""
        logger.info(f"🔄 {description}")
        logger.debug(f"Comando: {cmd}")
        
        try:
            result = subprocess.run(
                cmd, 
                shell=True, 
                capture_output=True, 
                text=True, 
                timeout=60
            )
            
            if result.returncode == 0:
                logger.info(f"✅ {description} - ÉXITO")
                if result.stdout.strip():
                    logger.debug(f"Output: {result.stdout.strip()}")
                return True, result.stdout
            else:
                if ignore_errors:
                    logger.warning(f"⚠️  {description} - Error ignorado: {result.stderr.strip()}")
                    return True, result.stderr
                else:
                    logger.error(f"❌ {description} - ERROR: {result.stderr.strip()}")
                    return False, result.stderr
                    
        except subprocess.TimeoutExpired:
            logger.error(f"⏰ {description} - TIMEOUT")
            return False, "Timeout"
        except Exception as e:
            logger.error(f"💥 {description} - EXCEPCIÓN: {str(e)}")
            return False, str(e)
    
    def wait_for_service(self, service_name, url, max_retries=30):
        """Esperar a que un servicio esté disponible"""
        logger.info(f"⏳ Esperando servicio {service_name}...")
        
        for i in range(max_retries):
            if service_name == 'kafka':
                cmd = f"docker compose exec kafka kafka-topics --bootstrap-server {url} --list > /dev/null 2>&1"
            elif service_name == 'clickhouse':
                cmd = f"docker compose exec clickhouse clickhouse-client --query 'SELECT 1' > /dev/null 2>&1"
            else:
                cmd = f"curl -s {url}/health > /dev/null 2>&1 || curl -s {url} > /dev/null 2>&1"
            
            success, _ = self.run_command(cmd, f"Verificando {service_name} (intento {i+1})")
            if success:
                logger.info(f"✅ Servicio {service_name} disponible")
                return True
            
            time.sleep(2)
        
        logger.error(f"❌ Servicio {service_name} no disponible después de {max_retries} intentos")
        return False
    
    def clean_kafka_connect(self):
        """Limpiar todos los conectores de Kafka Connect"""
        logger.info("🧹 === LIMPIANDO KAFKA CONNECT ===")
        
        if not self.wait_for_service('connect', self.services['connect']):
            return False
        
        # Obtener lista de conectores existentes
        success, output = self.run_command(
            "curl -s http://connect:8083/connectors",
            "Obteniendo lista de conectores"
        )
        
        if success:
            try:
                connectors = json.loads(output) if output.strip() else []
                
                if not connectors:
                    logger.info("ℹ️  No hay conectores para eliminar")
                    return True
                
                # Eliminar cada conector
                for connector in connectors:
                    self.run_command(
                        f"curl -X DELETE http://connect:8083/connectors/{connector}",
                        f"Eliminando conector: {connector}"
                    )
                    time.sleep(1)
                
                logger.info(f"✅ {len(connectors)} conectores eliminados")
                return True
                
            except json.JSONDecodeError:
                logger.warning("⚠️  No se pudo parsear respuesta de conectores")
                return True
        
        return False
    
    def clean_kafka_topics(self):
        """Limpiar tópicos de Kafka relacionados con el ETL"""
        logger.info("🧹 === LIMPIANDO TÓPICOS DE KAFKA ===")
        
        if not self.wait_for_service('kafka', self.services['kafka']):
            return False
        
        # Obtener tópicos del ETL
        success, output = self.run_command(
            "docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list | grep -E '(dbserver_default|connect-|__connect)'",
            "Obteniendo tópicos del ETL"
        )
        
        if success and output.strip():
            topics = [t.strip() for t in output.strip().split('\n') if t.strip()]
            
            for topic in topics:
                self.run_command(
                    f"docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --delete --topic {topic}",
                    f"Eliminando tópico: {topic}"
                )
                time.sleep(0.5)
            
            logger.info(f"✅ {len(topics)} tópicos eliminados")
        else:
            logger.info("ℹ️  No hay tópicos ETL para eliminar")
        
        return True
    
    def clean_clickhouse(self):
        """Limpiar datos y tablas de ClickHouse"""
        logger.info("🧹 === LIMPIANDO CLICKHOUSE ===")
        
        if not self.wait_for_service('clickhouse', self.services['clickhouse']):
            return False
        
        # Obtener lista de tablas (excluyendo system)
        success, output = self.run_command(
            "docker compose exec clickhouse clickhouse-client --database fgeo_analytics --query \"SHOW TABLES\" 2>/dev/null",
            "Obteniendo tablas de ClickHouse"
        )
        
        if success and output.strip():
            tables = [t.strip() for t in output.strip().split('\n') if t.strip() and t.strip() != 'test_table']
            
            for table in tables:
                self.run_command(
                    f"docker compose exec clickhouse clickhouse-client --database fgeo_analytics --query \"DROP TABLE IF EXISTS {table}\"",
                    f"Eliminando tabla: {table}"
                )
            
            logger.info(f"✅ {len(tables)} tablas eliminadas de ClickHouse")
        else:
            logger.info("ℹ️  No hay tablas para eliminar en ClickHouse")
        
        return True
    
    def clean_generated_files(self):
        """Limpiar archivos generados automáticamente"""
        logger.info("🧹 === LIMPIANDO ARCHIVOS GENERADOS ===")
        
        # Limpiar archivos de configuración generados
        files_to_clean = [
            "/app/generated/default/*.json",
            "/app/generated/default/schemas/*.json", 
            "/app/logs/*.log"
        ]
        
        for file_pattern in files_to_clean:
            self.run_command(
                f"rm -f {file_pattern}",
                f"Eliminando archivos: {file_pattern}"
            )
        
        # Recrear estructura de directorios
        self.run_command(
            "mkdir -p /app/generated/default/schemas /app/logs",
            "Recreando estructura de directorios"
        )
        
        logger.info("✅ Archivos generados limpiados")
        return True
    
    def clean_superset(self):
        """Limpiar configuraciones de Superset"""
        logger.info("🧹 === LIMPIANDO SUPERSET ===")
        
        # Por ahora solo loggeamos, Superset se limpia con el reinicio
        logger.info("ℹ️  Superset se limpiará en el próximo reinicio")
        return True
    
    def run_full_cleanup(self):
        """Ejecutar limpieza completa"""
        start_time = datetime.now()
        logger.info("🚀 === INICIANDO LIMPIEZA COMPLETA DEL PIPELINE ETL ===")
        logger.info(f"⏰ Inicio: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        cleanup_steps = [
            ("Kafka Connect", self.clean_kafka_connect),
            ("Tópicos de Kafka", self.clean_kafka_topics), 
            ("ClickHouse", self.clean_clickhouse),
            ("Archivos generados", self.clean_generated_files),
            ("Superset", self.clean_superset)
        ]
        
        success_count = 0
        total_steps = len(cleanup_steps)
        
        for step_name, step_function in cleanup_steps:
            logger.info(f"\n📋 Paso: {step_name}")
            try:
                if step_function():
                    success_count += 1
                    logger.info(f"✅ {step_name} completado")
                else:
                    logger.error(f"❌ {step_name} falló")
            except Exception as e:
                logger.error(f"💥 {step_name} - Excepción: {str(e)}")
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info(f"\n🏁 === LIMPIEZA COMPLETADA ===")
        logger.info(f"⏰ Duración: {duration:.1f} segundos")
        logger.info(f"📊 Éxito: {success_count}/{total_steps} pasos")
        
        if success_count == total_steps:
            logger.info("🎉 LIMPIEZA COMPLETA EXITOSA")
            return True
        else:
            logger.warning("⚠️  LIMPIEZA PARCIAL - Revisar errores")
            return False

def main():
    """Función principal""" 
    try:
        cleaner = ETLCleaner()
        success = cleaner.run_full_cleanup()
        exit_code = 0 if success else 1
        
        print(f"\n{'='*60}")
        if success:
            print("🎉 LIMPIEZA COMPLETA EXITOSA")
            print("✅ El sistema está listo para inicialización desde cero")
        else:
            print("⚠️  LIMPIEZA PARCIAL")
            print("💡 Revisar logs para detalles de errores")
        print(f"{'='*60}")
        
        return exit_code
        
    except KeyboardInterrupt:
        logger.info("🛑 Limpieza interrumpida por el usuario")
        return 1
    except Exception as e:
        logger.error(f"💥 Error crítico en limpieza: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())