#!/usr/bin/env python3
"""
Diagnostica por qué no se están ingiriendo datos reales desde MySQL a ClickHouse
y por qué se crearon tantas tablas vacías.
"""
import os
import requests
from dotenv import load_dotenv

load_dotenv('/app/.env')

def check_mysql_connections():
    """Verifica las conexiones MySQL configuradas en .env"""
    print("🔍 DIAGNÓSTICO DE CONEXIONES MYSQL")
    print("=" * 40)
    
    db_connections = os.getenv("DB_CONNECTIONS", "[]")
    print(f"DB_CONNECTIONS configuradas: {db_connections}")
    
    if not db_connections or db_connections == "[]":
        print("❌ No hay conexiones MySQL configuradas en .env")
        return False
    
    try:
        import json
        connections = json.loads(db_connections)
        print(f"✅ Se encontraron {len(connections)} conexiones:")
        for conn in connections:
            print(f"  - {conn.get('name')}: {conn.get('host')}:{conn.get('port')}/{conn.get('db')}")
        return connections
    except Exception as e:
        print(f"❌ Error parseando DB_CONNECTIONS: {e}")
        return False

def check_debezium_status():
    """Verifica el estado de Debezium Connect"""
    print("\n🔍 DIAGNÓSTICO DE DEBEZIUM CONNECT")
    print("=" * 40)
    
    try:
        # Verificar si Debezium está corriendo
        resp = requests.get("http://localhost:8083/", timeout=5)
        if resp.status_code == 200:
            print("✅ Debezium Connect está corriendo")
        else:
            print(f"❌ Debezium Connect responde con código: {resp.status_code}")
            return False
            
        # Verificar conectores
        connectors_resp = requests.get("http://localhost:8083/connectors", timeout=5)
        if connectors_resp.status_code == 200:
            connectors = connectors_resp.json()
            print(f"📡 Conectores activos: {len(connectors)}")
            for connector in connectors:
                print(f"  - {connector}")
                
                # Estado del conector
                status_resp = requests.get(f"http://localhost:8083/connectors/{connector}/status", timeout=5)
                if status_resp.status_code == 200:
                    status = status_resp.json()
                    print(f"    Estado: {status.get('connector', {}).get('state', 'UNKNOWN')}")
                    
                    # Tareas
                    tasks = status.get('tasks', [])
                    for i, task in enumerate(tasks):
                        print(f"    Tarea {i}: {task.get('state', 'UNKNOWN')}")
                        if task.get('state') == 'FAILED':
                            print(f"      Error: {task.get('trace', 'Sin detalles')}")
            
            return connectors
        else:
            print(f"❌ Error obteniendo conectores: {connectors_resp.status_code}")
            return []
            
    except requests.exceptions.RequestException as e:
        print(f"❌ No se pudo conectar a Debezium: {e}")
        return False

def check_kafka_topics():
    """Verifica los topics de Kafka"""
    print("\n🔍 DIAGNÓSTICO DE KAFKA TOPICS")
    print("=" * 40)
    
    try:
        import subprocess
        # Listar topics de Kafka
        result = subprocess.run([
            "docker", "compose", "exec", "kafka", 
            "kafka-topics", "--bootstrap-server", "localhost:9092", "--list"
        ], capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            topics = result.stdout.strip().split('\n')
            print(f"📂 Topics de Kafka encontrados: {len(topics)}")
            for topic in topics:
                if topic.strip():
                    print(f"  - {topic.strip()}")
            return topics
        else:
            print(f"❌ Error listando topics: {result.stderr}")
            return []
            
    except Exception as e:
        print(f"❌ Error verificando Kafka: {e}")
        return []

def analyze_empty_tables():
    """Analiza por qué hay tantas tablas vacías en ClickHouse"""
    print("\n🔍 ANÁLISIS DE TABLAS VACÍAS")
    print("=" * 40)
    
    try:
        import subprocess
        # Obtener estadísticas de todas las tablas
        result = subprocess.run([
            "docker", "compose", "exec", "clickhouse",
            "clickhouse-client", "--user=etl", "--password=Et1Ingest!", 
            "--database=fgeo_analytics", "--query",
            "SELECT name, engine, total_rows, create_table_query FROM system.tables WHERE database = 'fgeo_analytics' ORDER BY total_rows DESC"
        ], capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            print(f"📊 Análisis de {len(lines)} tablas:")
            
            tables_with_data = 0
            empty_tables = 0
            
            for line in lines:
                if line.strip():
                    parts = line.split('\t')
                    if len(parts) >= 3:
                        name, engine, rows = parts[0], parts[1], parts[2]
                        rows_num = int(rows) if rows.isdigit() else 0
                        
                        if rows_num > 0:
                            print(f"  ✅ {name}: {rows_num} filas ({engine})")
                            tables_with_data += 1
                        else:
                            print(f"  ❌ {name}: 0 filas ({engine})")
                            empty_tables += 1
            
            print(f"\n📈 Resumen:")
            print(f"  - Tablas con datos: {tables_with_data}")
            print(f"  - Tablas vacías: {empty_tables}")
            
            if empty_tables > tables_with_data:
                print("⚠️  PROBLEMA: Demasiadas tablas vacías - posible falla en ingesta")
                
        else:
            print(f"❌ Error consultando ClickHouse: {result.stderr}")
            
    except Exception as e:
        print(f"❌ Error analizando tablas: {e}")

if __name__ == "__main__":
    print("🚀 DIAGNÓSTICO COMPLETO DE INGESTA DE DATOS")
    print("=" * 50)
    
    # Verificar configuración MySQL
    mysql_ok = check_mysql_connections()
    
    # Verificar Debezium
    debezium_ok = check_debezium_status()
    
    # Verificar Kafka
    kafka_ok = check_kafka_topics()
    
    # Analizar tablas vacías
    analyze_empty_tables()
    
    print("\n🎯 CONCLUSIONES:")
    if not mysql_ok:
        print("❌ PROBLEMA: Conexiones MySQL no configuradas correctamente")
    if not debezium_ok:
        print("❌ PROBLEMA: Debezium no está funcionando correctamente")
    if not kafka_ok:
        print("❌ PROBLEMA: Kafka no tiene topics de datos")
        
    print("\n💡 RECOMENDACIONES:")
    print("1. Verificar que las bases MySQL estén accesibles")
    print("2. Configurar conectores Debezium para cada base MySQL")
    print("3. Limpiar tablas vacías innecesarias")
    print("4. Ejecutar pipeline de ingesta real")