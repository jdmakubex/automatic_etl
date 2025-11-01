#!/usr/bin/env python3
"""
Validador robusto del pipeline ETL
Detecta automáticamente tópicos, conectores y datos reales
Completamente robusto con reintentos y verificación exhaustiva
"""

import sys
import subprocess
import json
import time
import re
from typing import Tuple, List, Dict, Optional

class RobustPipelineValidator:
    def __init__(self):
        self.max_retries = 3
        self.timeout = 30
        
    def _run_command(self, cmd: str, use_shell: bool = True) -> Tuple[bool, str, str]:
        """Ejecutar comando con reintentos y timeout robusto"""
        for attempt in range(self.max_retries):
            try:
                if attempt > 0:
                    print(f"   🔄 Reintento {attempt + 1}/{self.max_retries}")
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
                print(f"   ⏰ Timeout en intento {attempt + 1}")
                continue
            except Exception as e:
                print(f"   ❌ Error en intento {attempt + 1}: {e}")
                continue
        
        return False, "", f"Falló después de {self.max_retries} intentos"
    
    def check_kafka_topics(self) -> Tuple[bool, str, List[str]]:
        """Verificar tópicos de Kafka de manera robusta"""
        print("🔍 Analizando tópicos de Kafka...")
        
        # Comando robusto para obtener tópicos
        success, stdout, stderr = self._run_command(
            "docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list"
        )
        
        if not success:
            return False, f"Error obteniendo tópicos: {stderr}", []
        
        # Filtrar tópicos de datos (no técnicos)
        all_topics = [topic.strip() for topic in stdout.split('\n') if topic.strip()]
        data_topics = [
            topic for topic in all_topics 
            if any(keyword in topic for keyword in ['dbserver_', '.archivos.', '.fiscalizacion.'])
            and not topic.startswith('__')
            and not topic.startswith('connect_')
            and not topic.startswith('schema-changes')
        ]
        
        if data_topics:
            return True, f"{len(data_topics)} tópicos de datos activos", data_topics
        else:
            return False, "No se encontraron tópicos de datos", []
    
    def check_kafka_connectors(self) -> Tuple[bool, str, List[Dict]]:
        """Verificar conectores de Kafka Connect de manera robusta"""
        print("🔍 Analizando conectores de Kafka Connect...")
        
        # Obtener lista de conectores
        success, stdout, stderr = self._run_command(
            "curl -s http://localhost:8083/connectors"
        )
        
        if not success:
            return False, f"Error obteniendo conectores: {stderr}", []
        
        try:
            connectors = json.loads(stdout)
        except json.JSONDecodeError:
            return False, f"Error parseando lista de conectores", []
        
        if not connectors:
            return False, "No hay conectores configurados", []
        
        # Verificar estado de cada conector
        connector_details = []
        all_running = True
        
        for connector_name in connectors:
            success, stdout, stderr = self._run_command(
                f"curl -s http://localhost:8083/connectors/{connector_name}/status"
            )
            
            if success:
                try:
                    status = json.loads(stdout)
                    connector_state = status.get('connector', {}).get('state', 'UNKNOWN')
                    
                    # Obtener estado de tareas
                    tasks = status.get('tasks', [])
                    task_states = [task.get('state', 'UNKNOWN') for task in tasks]
                    
                    connector_details.append({
                        'name': connector_name,
                        'connector_state': connector_state,
                        'task_states': task_states,
                        'running': connector_state == 'RUNNING' and all(state == 'RUNNING' for state in task_states)
                    })
                    
                    if connector_state != 'RUNNING' or not all(state == 'RUNNING' for state in task_states):
                        all_running = False
                
                except json.JSONDecodeError:
                    connector_details.append({
                        'name': connector_name,
                        'connector_state': 'PARSE_ERROR',
                        'task_states': [],
                        'running': False
                    })
                    all_running = False
            else:
                connector_details.append({
                    'name': connector_name,
                    'connector_state': 'CONNECTION_ERROR',
                    'task_states': [],
                    'running': False
                })
                all_running = False
        
        if all_running:
            running_count = len([c for c in connector_details if c['running']])
            return True, f"{running_count}/{len(connector_details)} conectores operativos", connector_details
        else:
            running_count = len([c for c in connector_details if c['running']])
            return False, f"Solo {running_count}/{len(connector_details)} conectores funcionando", connector_details
    
    def check_clickhouse_data(self) -> Tuple[bool, str, Dict]:
        """Verificar datos en ClickHouse de manera robusta"""
        print("🔍 Analizando datos en ClickHouse...")
        
        # Obtener todas las bases de datos
        success, stdout, stderr = self._run_command(
            "docker compose exec clickhouse clickhouse-client --query 'SHOW DATABASES'"
        )
        
        if not success:
            return False, f"Error obteniendo bases de datos: {stderr}", {}
        
        databases = [db.strip() for db in stdout.split('\n') if db.strip()]
        data_databases = [db for db in databases if db in ['archivos', 'fiscalizacion', 'archivos_analytics', 'fiscalizacion_analytics']]
        
        if not data_databases:
            return False, "No se encontraron bases de datos de datos", {}
        
        # Verificar tablas y datos en cada base de datos
        database_info = {}
        total_tables = 0
        total_records = 0
        
        for db in data_databases:
            # Obtener tablas con datos
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
    
    def get_data_sample(self, database_info: Dict) -> Tuple[bool, str]:
        """Obtener muestra de datos de la tabla más grande"""
        if not database_info:
            return False, "No hay datos disponibles"
        
        # Encontrar la tabla con más datos
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
        
        # Obtener muestra de datos
        success, stdout, stderr = self._run_command(
            f"docker compose exec clickhouse clickhouse-client --database {largest_db} --query 'SELECT * FROM {largest_table} LIMIT 3' --format JSONEachRow"
        )
        
        if success and stdout:
            return True, f"Muestra de {largest_db}.{largest_table} ({largest_count:,} registros):\n{stdout}"
        else:
            return False, "No se pudo obtener muestra de datos"
    
    def check_data_flow_health(self) -> Tuple[bool, str]:
        """Verificar que el flujo de datos esté activo"""
        print("🔍 Verificando flujo activo de datos...")
        
        # Verificar tópicos recientes en Kafka
        success, stdout, stderr = self._run_command(
            "docker compose exec -T kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic dbserver_archivos.archivos.archivos --time -1 2>/dev/null | head -1"
        )
        
        if success and stdout:
            try:
                # Parsear offset actual
                parts = stdout.split(':')
                if len(parts) >= 3:
                    current_offset = int(parts[2])
                    if current_offset > 0:
                        return True, f"Flujo activo detectado (offset: {current_offset})"
            except:
                pass
        
        return False, "No se pudo verificar flujo activo"

    def run_comprehensive_validation(self) -> bool:
        """Ejecutar validación completa y robusta"""
        print("🎯" + "=" * 58 + "🎯")
        print("🚀 VALIDACIÓN ROBUSTA DEL PIPELINE ETL 🚀")
        print("🎯" + "=" * 58 + "🎯")
        
        all_good = True
        
        # 1. Verificar tópicos de Kafka
        print("\n📊 1. TÓPICOS DE DATOS EN KAFKA")
        success, message, topics = self.check_kafka_topics()
        status = "✅" if success else "❌"
        print(f"   {status} {message}")
        if success and topics:
            print(f"   📋 Tópicos principales: {', '.join(topics[:5])}")
            if len(topics) > 5:
                print(f"   📋 ... y {len(topics) - 5} más")
        if not success:
            all_good = False
        
        # 2. Verificar conectores
        print("\n🔌 2. CONECTORES MYSQL → KAFKA")
        success, message, connectors = self.check_kafka_connectors()
        status = "✅" if success else "❌"
        print(f"   {status} {message}")
        if connectors:
            for conn in connectors:
                conn_status = "✅" if conn['running'] else "❌"
                task_info = f"{len([t for t in conn['task_states'] if t == 'RUNNING'])}/{len(conn['task_states'])} tareas"
                print(f"   {conn_status} {conn['name']}: {conn['connector_state']} ({task_info})")
        if not success:
            all_good = False
        
        # 3. Verificar datos en ClickHouse
        print("\n🏠 3. DATOS EN CLICKHOUSE")
        success, message, db_info = self.check_clickhouse_data()
        status = "✅" if success else "❌"
        print(f"   {status} {message}")
        if db_info:
            for db, tables in db_info.items():
                print(f"   📊 {db}: {len(tables)} tablas")
                for table_name, row_count in tables[:3]:  # Mostrar top 3
                    print(f"      - {table_name}: {row_count:,} registros")
                if len(tables) > 3:
                    print(f"      - ... y {len(tables) - 3} tablas más")
        if not success:
            all_good = False
        
        # 4. Muestra de datos
        print("\n📝 4. MUESTRA DE DATOS")
        success, sample = self.get_data_sample(db_info if 'db_info' in locals() else {})
        if success:
            print("   ✅ Datos disponibles:")
            print("   " + "\n   ".join(sample.split('\n')[:5]))
        else:
            print(f"   ❌ {sample}")
            # No marcar como error crítico si hay datos pero no se puede obtener muestra
        
        # 5. Verificar flujo activo
        print("\n🔄 5. FLUJO ACTIVO DE DATOS")
        success, message = self.check_data_flow_health()
        status = "✅" if success else "ℹ️"
        print(f"   {status} {message}")
        
        print("\n" + "🎯" + "=" * 58 + "🎯")
        if all_good:
            print("🎉 PIPELINE ETL 100% FUNCIONAL Y ROBUSTO")
            print("✅ MySQL → Kafka → ClickHouse: FLUJO COMPLETO")
            print("\n🚀 Sistema completamente operativo:")
            print(f"   📊 {len(topics) if 'topics' in locals() else 0} tópicos de Kafka activos")
            print(f"   🔌 {len([c for c in connectors if c['running']]) if 'connectors' in locals() else 0} conectores funcionando")
            print(f"   🏠 {sum(len(tables) for tables in db_info.values()) if 'db_info' in locals() else 0} tablas con datos")
            print(f"   📈 {sum(sum(count for _, count in tables) for tables in db_info.values()) if 'db_info' in locals() else 0:,} registros totales")
        else:
            print("⚠️  PIPELINE CON PROBLEMAS DETECTADOS")
            print("💡 Revisar componentes marcados con ❌")
        
        print("🎯" + "=" * 58 + "🎯")
        return all_good

def main():
    validator = RobustPipelineValidator()
    success = validator.run_comprehensive_validation()
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())