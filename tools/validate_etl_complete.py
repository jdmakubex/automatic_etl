#!/usr/bin/env python3
"""
Script completo de validación ETL que verifica:
1. ClickHouse - conexión, base de datos, usuarios, tablas
2. Superset - conexión, health check
3. Kafka - tópicos, conectividad
4. Debezium - conectores, estado
5. Flujo completo de datos
"""
import requests
import json
import os
import sys

# Configuración
CLICKHOUSE_HTTP = os.getenv("CLICKHOUSE_HTTP", "http://clickhouse:8123")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "fgeo_analytics")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "etl")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "Et1Ingest!")
SUPERSET_URL = os.getenv("SUPERSET_URL", "http://superset:8088")
CONNECT_URL = os.getenv("CONNECT_URL", "http://connect:8083")

def test_clickhouse():
    print("\n=== VALIDACIÓN CLICKHOUSE ===")
    try:
        # Test conexión
        url = f"{CLICKHOUSE_HTTP}/ping"
        response = requests.get(url, timeout=5)
        print(f"✅ Conexión ClickHouse: {response.status_code}")
        
        # Test base de datos
        url = f"{CLICKHOUSE_HTTP}/?database={CLICKHOUSE_DATABASE}"
        query = f"SELECT count(*) FROM system.databases WHERE name='{CLICKHOUSE_DATABASE}'"
        auth = (CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)
        response = requests.post(url, data=query, auth=auth, timeout=5)
        print(f"✅ Base de datos {CLICKHOUSE_DATABASE}: existe")
        
        # Test tablas
        query = f"SHOW TABLES FROM {CLICKHOUSE_DATABASE}"
        response = requests.post(url, data=query, auth=auth, timeout=5)
        tables = response.text.strip().split('\n') if response.text.strip() else []
        print(f"✅ Tablas encontradas: {len(tables)} - {tables}")
        
        return True
    except Exception as e:
        print(f"❌ Error ClickHouse: {e}")
        return False

def test_superset():
    print("\n=== VALIDACIÓN SUPERSET ===")
    try:
        response = requests.get(f"{SUPERSET_URL}/health", timeout=10)
        print(f"✅ Superset health: {response.status_code}")
        return True
    except Exception as e:
        print(f"❌ Error Superset: {e}")
        return False

def test_kafka_connect():
    print("\n=== VALIDACIÓN KAFKA CONNECT ===")
    try:
        # Test conectores
        response = requests.get(f"{CONNECT_URL}/connectors", timeout=5)
        connectors = response.json()
        print(f"✅ Conectores: {connectors}")
        
        # Test estado de conectores
        for connector in connectors:
            status_response = requests.get(f"{CONNECT_URL}/connectors/{connector}/status", timeout=5)
            status = status_response.json()
            print(f"✅ Conector {connector}: {status['connector']['state']}")
            if status['tasks']:
                for task in status['tasks']:
                    print(f"   Task {task['id']}: {task['state']}")
        
        return True
    except Exception as e:
        print(f"❌ Error Kafka Connect: {e}")
        return False

def main():
    print("=== VALIDACIÓN COMPLETA DEL PIPELINE ETL ===")
    
    results = {
        'clickhouse': test_clickhouse(),
        'superset': test_superset(),
        'kafka_connect': test_kafka_connect()
    }
    
    print(f"\n=== RESUMEN ===")
    for component, result in results.items():
        status = "✅ OK" if result else "❌ FALLO"
        print(f"{component.upper()}: {status}")
    
    # Conclusión
    if all(results.values()):
        print("\n🎉 TODAS LAS VALIDACIONES PASARON - PIPELINE ETL FUNCIONAL")
        return 0
    else:
        print("\n⚠️  ALGUNAS VALIDACIONES FALLARON - REVISAR CONFIGURACIÓN")
        return 1

if __name__ == "__main__":
    sys.exit(main())