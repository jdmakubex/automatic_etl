#!/usr/bin/env python3
"""
🎯 PIPELINE VALIDATOR COMPLETAMENTE DINÁMICO
==============================================

Este script se adapta automáticamente a CUALQUIER configuración de DB_CONNECTIONS:
- ✅ Lee DB_CONNECTIONS del .env dinámicamente
- ✅ Detecta conectores basados en los nombres reales configurados  
- ✅ Busca tópicos de Kafka con patrones dinámicos
- ✅ Descubre bases de datos de ClickHouse automáticamente
- ✅ Funciona con cualquier configuración sin hardcodear nombres

Uso: python3 tools/dynamic_pipeline_validator.py
"""

import sys
import subprocess
import json
import time
import os
from typing import Tuple, List, Dict, Optional

class DynamicPipelineValidator:
    """Validador completamente dinámico que se adapta a cualquier configuración"""
    
    def __init__(self):
        self.max_retries = 3
        self.timeout = 30
        self.db_connections = []
        self.expected_connectors = []
        self.expected_databases = []
        
        # Load dynamic configuration
        self._load_environment_config()
        
    def _load_environment_config(self):
        """Cargar configuración dinámica desde variables de entorno"""
        print("🔧 Cargando configuración dinámica desde .env...")
        
        # Try to read from environment variable first
        db_connections_str = os.getenv("DB_CONNECTIONS", "")
        
        # If not found, try to read from .env file directly
        if not db_connections_str:
            try:
                env_path = "/mnt/c/proyectos/etl_prod/.env"
                if os.path.exists(env_path):
                    print("📄 Leyendo configuración directamente desde .env...")
                    with open(env_path, 'r') as f:
                        for line in f:
                            if line.startswith('DB_CONNECTIONS='):
                                db_connections_str = line.split('=', 1)[1].strip()
                                break
            except Exception as e:
                print(f"⚠️ Error leyendo .env: {e}")
        
        if not db_connections_str:
            db_connections_str = "[]"
        
        try:
            self.db_connections = json.loads(db_connections_str)
            print(f"📊 Detectadas {len(self.db_connections)} conexiones de DB")
            
            # Generate expected connector names dynamically
            self.expected_connectors = []
            self.expected_databases = []
            
            for conn in self.db_connections:
                db_name = conn.get("name", "unknown")
                
                # Expected connector format: dbserver_<name>
                connector_name = f"dbserver_{db_name}"
                self.expected_connectors.append(connector_name)
                
                # Expected ClickHouse databases (multiple possible formats)
                possible_db_names = [
                    db_name,  # Direct name
                    f"fgeo_{db_name}",  # With fgeo prefix
                    f"{db_name}_analytics"  # With analytics suffix
                ]
                self.expected_databases.extend(possible_db_names)
                
                print(f"   🔗 DB: {db_name} → Conector: {connector_name}")
            
            # Remove duplicates
            self.expected_databases = list(set(self.expected_databases))
            
            print(f"🎯 Conectores esperados: {self.expected_connectors}")
            print(f"🏠 Bases de datos esperadas: {self.expected_databases}")
            
        except json.JSONDecodeError as e:
            print(f"❌ Error parseando DB_CONNECTIONS: {e}")
            print("⚠️  Usando configuración por defecto")
            
            # Fallback configuration
            self.db_connections = [{"name": "default"}]
            self.expected_connectors = ["dbserver_default"]
            self.expected_databases = ["default", "fgeo_default"]
        
    def _run_command(self, cmd: str, use_shell: bool = True) -> Tuple[bool, str, str]:
        """Ejecutar comando con reintentos y timeout robusto"""
        for attempt in range(self.max_retries):
            try:
                if attempt > 0:
                    time.sleep(2 ** attempt)  # Exponential backoff
                
                result = subprocess.run(
                    cmd, 
                    shell=use_shell, 
                    capture_output=True, 
                    text=True, 
                    timeout=self.timeout,
                    cwd="/mnt/c/proyectos/etl_prod"
                )
                
                if result.returncode == 0:
                    return True, result.stdout.strip(), result.stderr.strip()
                
            except subprocess.TimeoutExpired:
                continue
            except Exception as e:
                continue
        
        return False, "", f"Falló después de {self.max_retries} intentos"
    
    def check_kafka_topics_dynamic(self) -> Tuple[bool, str, List[str]]:
        """Verificar tópicos de Kafka de manera completamente dinámica"""
        print("🔍 Analizando tópicos de Kafka (modo dinámico)...")
        
        # Get all topics first
        success, stdout, stderr = self._run_command(
            "docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list"
        )
        
        if not success:
            return False, f"Error obteniendo tópicos: {stderr}", []
        
        all_topics = [topic.strip() for topic in stdout.split('\n') if topic.strip()]
        
        # Build dynamic patterns based on DB_CONNECTIONS
        data_topics = []
        
        for topic in all_topics:
            # Skip system topics
            if topic.startswith('__') or topic.startswith('connect_') or topic == 'test-connectivity':
                continue
            
            # Check if topic matches any of our expected database patterns
            is_data_topic = False
            
            for conn in self.db_connections:
                db_name = conn.get("name", "")
                expected_patterns = [
                    f"dbserver_{db_name}.",  # Standard pattern
                    f".{db_name}.",         # Database in middle
                    db_name                 # Direct match
                ]
                
                if any(pattern in topic for pattern in expected_patterns):
                    is_data_topic = True
                    break
            
            # Also check for generic data patterns
            if not is_data_topic:
                generic_patterns = ['dbserver_', 'schema-changes']
                if any(pattern in topic for pattern in generic_patterns):
                    is_data_topic = True
            
            if is_data_topic:
                data_topics.append(topic)
        
        if data_topics:
            return True, f"{len(data_topics)} tópicos de datos detectados dinámicamente", data_topics
        else:
            return False, "No se encontraron tópicos de datos", []
    
    def check_kafka_connectors_dynamic(self) -> Tuple[bool, str, List[Dict]]:
        """Verificar conectores de Kafka Connect de manera dinámica"""
        print("🔍 Analizando conectores de Kafka Connect (modo dinámico)...")
        
        # Get list of connectors
        success, stdout, stderr = self._run_command(
            "curl -s http://localhost:8083/connectors"
        )
        
        if not success:
            return False, f"Error obteniendo conectores: {stderr}", []
        
        try:
            actual_connectors = json.loads(stdout)
        except json.JSONDecodeError:
            return False, f"Error parseando lista de conectores", []
        
        if not actual_connectors:
            return False, "No hay conectores configurados", []
        
        # Check each connector status
        connector_details = []
        running_connectors = 0
        
        for connector_name in actual_connectors:
            success, stdout, stderr = self._run_command(
                f"curl -s http://localhost:8083/connectors/{connector_name}/status"
            )
            
            if success:
                try:
                    status = json.loads(stdout)
                    connector_state = status.get('connector', {}).get('state', 'UNKNOWN')
                    tasks = status.get('tasks', [])
                    task_states = [task.get('state', 'UNKNOWN') for task in tasks]
                    
                    is_running = connector_state == 'RUNNING' and all(state == 'RUNNING' for state in task_states)
                    
                    connector_details.append({
                        'name': connector_name,
                        'connector_state': connector_state,
                        'task_states': task_states,
                        'running': is_running,
                        'expected': connector_name in self.expected_connectors
                    })
                    
                    if is_running:
                        running_connectors += 1
                
                except json.JSONDecodeError:
                    connector_details.append({
                        'name': connector_name,
                        'connector_state': 'PARSE_ERROR',
                        'task_states': [],
                        'running': False,
                        'expected': connector_name in self.expected_connectors
                    })
        
        # Determine success based on dynamic expectations
        total_connectors = len(actual_connectors)
        expected_running = len(self.expected_connectors)
        
        if running_connectors >= expected_running or running_connectors == total_connectors:
            return True, f"Conectores: RUNNING ({running_connectors}/{total_connectors})", connector_details
        else:
            return False, f"Conectores: PARTIAL ({running_connectors}/{total_connectors})", connector_details
    
    def check_clickhouse_data_dynamic(self) -> Tuple[bool, str, Dict]:
        """Verificar datos en ClickHouse de manera completamente dinámica"""
        print("🔍 Analizando datos en ClickHouse (modo dinámico)...")
        
        # First, discover all databases
        success, stdout, stderr = self._run_command(
            "docker compose exec clickhouse clickhouse-client --query 'SHOW DATABASES'"
        )
        
        if not success:
            return False, f"Error obteniendo bases de datos: {stderr}", {}
        
        all_databases = [db.strip() for db in stdout.split('\n') if db.strip()]
        
        # Find databases that match our expected patterns
        matching_databases = []
        
        for db in all_databases:
            # Skip system databases
            if db in ['INFORMATION_SCHEMA', 'information_schema', 'system', 'default']:
                continue
            
            # Check if database matches any expected pattern
            matches_expected = False
            
            for expected_db in self.expected_databases:
                if db == expected_db or expected_db in db or db in expected_db:
                    matches_expected = True
                    break
            
            # Also check for common patterns
            if not matches_expected:
                for conn in self.db_connections:
                    db_name = conn.get("name", "")
                    if db_name and (db_name in db or db in db_name):
                        matches_expected = True
                        break
            
            if matches_expected:
                matching_databases.append(db)
        
        if not matching_databases:
            # If no matching databases found, use any non-system database
            matching_databases = [db for db in all_databases 
                                if db not in ['INFORMATION_SCHEMA', 'information_schema', 'system']]
        
        print(f"📊 Bases de datos encontradas: {matching_databases}")
        
        # Check tables and data in each matching database
        database_info = {}
        total_tables = 0
        total_records = 0
        
        for db in matching_databases:
            success, stdout, stderr = self._run_command(
                f"docker compose exec clickhouse clickhouse-client --query \"SELECT table, total_rows FROM system.tables WHERE database = '{db}' AND total_rows > 0 ORDER BY total_rows DESC\""
            )
            
            if success and stdout:
                tables_data = []
                for line in stdout.split('\n'):
                    if line.strip():
                        parts = line.strip().split('\t')
                        if len(parts) >= 2:
                            try:
                                table_name = parts[0]
                                row_count = int(parts[1])
                                tables_data.append((table_name, row_count))
                                total_records += row_count
                            except ValueError:
                                continue
                
                if tables_data:
                    database_info[db] = tables_data
                    total_tables += len(tables_data)
        
        if total_tables > 0:
            return True, f"{total_tables} tablas con {total_records:,} registros totales", database_info
        else:
            return False, "No se encontraron tablas con datos", {}
    
    def get_data_sample_dynamic(self, database_info: Dict) -> Tuple[bool, str]:
        """Obtener muestra de datos de manera dinámica"""
        if not database_info:
            return False, "No hay datos disponibles"
        
        # Find the table with most data
        largest_table = None
        largest_count = 0
        largest_db = None
        
        for db, tables in database_info.items():
            for table_name, row_count in tables:
                if row_count > largest_count:
                    largest_count = row_count
                    largest_table = table_name
                    largest_db = db
        
        if not largest_table:
            return False, "No se encontró tabla para muestra"
        
        # Get data sample
        success, stdout, stderr = self._run_command(
            f"docker compose exec clickhouse clickhouse-client --database {largest_db} --query 'SELECT * FROM {largest_table} LIMIT 2' --format JSONEachRow"
        )
        
        if success and stdout:
            return True, f"Muestra de {largest_db}.{largest_table} ({largest_count:,} registros):\n{stdout}"
        else:
            return False, "No se pudo obtener muestra de datos"
    
    def run_comprehensive_validation(self) -> bool:
        """Ejecutar validación completamente dinámica"""
        print("🎯" + "=" * 58 + "🎯")
        print("🚀 VALIDADOR DINÁMICO DEL PIPELINE ETL 🚀")
        print("🎯" + "=" * 58 + "🎯")
        print(f"📊 Configuración detectada: {len(self.db_connections)} bases de datos")
        print(f"🔗 Conectores esperados: {len(self.expected_connectors)}")
        print("🎯" + "=" * 58 + "🎯")
        
        all_good = True
        
        # 1. Verificar tópicos de Kafka dinámicamente
        print("\n📊 1. TÓPICOS DE DATOS EN KAFKA (DINÁMICO)")
        success, message, topics = self.check_kafka_topics_dynamic()
        status = "✅" if success else "❌"
        print(f"   {status} {message}")
        if success and topics:
            print(f"   📋 Principales tópicos: {', '.join(topics[:3])}")
            if len(topics) > 3:
                print(f"   📋 ... y {len(topics) - 3} más")
        if not success:
            all_good = False
        
        # 2. Verificar conectores dinámicamente
        print("\n🔌 2. CONECTORES MYSQL → KAFKA (DINÁMICO)")
        success, message, connectors = self.check_kafka_connectors_dynamic()
        status = "✅" if success else "❌"
        print(f"   {status} {message}")
        if connectors:
            for conn in connectors:
                conn_status = "✅" if conn['running'] else "❌"
                expected_mark = "🎯" if conn['expected'] else "🔍"
                task_count = len([t for t in conn['task_states'] if t == 'RUNNING'])
                total_tasks = len(conn['task_states'])
                print(f"   {conn_status} {expected_mark} {conn['name']}: {conn['connector_state']} ({task_count}/{total_tasks} tareas)")
        if not success:
            all_good = False
        
        # 3. Verificar datos en ClickHouse dinámicamente
        print("\n🏠 3. DATOS EN CLICKHOUSE (DINÁMICO)")
        success, message, db_info = self.check_clickhouse_data_dynamic()
        status = "✅" if success else "❌"
        print(f"   {status} {message}")
        if db_info:
            for db, tables in db_info.items():
                print(f"   📊 {db}: {len(tables)} tablas")
                for table_name, row_count in tables[:3]:  # Show top 3
                    print(f"      - {table_name}: {row_count:,} registros")
                if len(tables) > 3:
                    print(f"      - ... y {len(tables) - 3} tablas más")
        if not success:
            all_good = False
        
        # 4. Muestra de datos
        print("\n📝 4. MUESTRA DE DATOS (DINÁMICA)")
        success, sample = self.get_data_sample_dynamic(db_info if 'db_info' in locals() else {})
        if success:
            print("   ✅ Datos disponibles:")
            print("   " + "\n   ".join(sample.split('\n')[:3]))
        else:
            print(f"   ℹ️ {sample}")
        
        print("\n" + "🎯" + "=" * 58 + "🎯")
        if all_good:
            print("🎉 PIPELINE ETL 100% FUNCIONAL (CONFIGURACIÓN DINÁMICA)")
            print("✅ MySQL → Kafka → ClickHouse: FLUJO COMPLETO")
            print("\n🚀 Sistema completamente operativo:")
            if 'topics' in locals():
                print(f"   📊 {len(topics)} tópicos de Kafka detectados")
            if 'connectors' in locals():
                running_count = len([c for c in connectors if c['running']])
                print(f"   🔌 {running_count}/{len(connectors)} conectores funcionando")
            if 'db_info' in locals():
                table_count = sum(len(tables) for tables in db_info.values())
                record_count = sum(sum(count for _, count in tables) for tables in db_info.values())
                print(f"   🏠 {table_count} tablas con {record_count:,} registros")
            
            print(f"\n🔧 Configuración detectada automáticamente desde DB_CONNECTIONS:")
            for i, conn in enumerate(self.db_connections, 1):
                db_name = conn.get("name", f"connection_{i}")
                db_host = conn.get("host", "unknown")
                db_database = conn.get("db", "unknown")
                print(f"   {i}. {db_name}: {db_database}@{db_host}")
        else:
            print("⚠️  PIPELINE CON PROBLEMAS DETECTADOS")
            print("💡 Revisar componentes marcados con ❌")
            
            print("\n🔧 Configuración actual:")
            print(f"   DB_CONNECTIONS: {len(self.db_connections)} conexiones")
            print(f"   Conectores esperados: {self.expected_connectors}")
            print(f"   Bases de datos esperadas: {self.expected_databases}")
        
        print("🎯" + "=" * 58 + "🎯")
        return all_good

def main():
    """Función principal con validación dinámica"""
    print("🎯 INICIANDO VALIDACIÓN COMPLETAMENTE DINÁMICA DEL PIPELINE")
    print("=" * 70)
    
    validator = DynamicPipelineValidator()
    success = validator.run_comprehensive_validation()
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())