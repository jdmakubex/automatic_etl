#!/usr/bin/env python3
"""
apply_connectors_auto.py
Aplica conectores Debezium automáticamente en Kafka Connect.
Ejecución recomendada: Docker, como parte del pipeline.
Script para aplicar conectores de Debezium automáticamente.
Espera a que Connect esté listo y aplica todos los conectores configurados.
Usa configuración centralizada del archivo .env
"""
import requests
import json
import time
import os
import sys
import glob
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables de entorno del archivo .env
ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

# Configuración desde variables de entorno
CONNECT_URL = os.getenv("CONNECT_URL", "http://connect:8083")
DEBEZIUM_CONNECT_URL = os.getenv("DEBEZIUM_CONNECT_URL", CONNECT_URL)
CONNECTORS_PATH = os.getenv("GENERATED_DIR", "generated") + "/default"
MAX_RETRIES = int(os.getenv("HEALTH_CHECK_RETRIES", "30"))
RETRY_DELAY = int(os.getenv("HEALTH_CHECK_INTERVAL", "10s").rstrip('s'))

def wait_for_connect():
    """Espera a que Connect esté disponible"""
    print("Esperando a que Kafka Connect esté disponible...")
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(f"{CONNECT_URL}", timeout=5)
            if response.status_code == 200:
                print("✅ Kafka Connect está disponible")
                return True
        except requests.exceptions.RequestException:
            pass
        
        print(f"Intento {attempt + 1}/{MAX_RETRIES} - Connect no disponible, esperando {RETRY_DELAY}s...")
        time.sleep(RETRY_DELAY)
    
    print("❌ Timeout esperando a Kafka Connect")
    return False

def get_existing_connectors():
    """Obtiene lista de conectores existentes"""
    try:
        response = requests.get(f"{CONNECT_URL}/connectors", timeout=5)
        return response.json() if response.status_code == 200 else []
    except:
        return []

def apply_connector(connector_file):
    """Aplica un conector desde archivo JSON"""
    try:
        with open(connector_file, 'r') as f:
            connector_config = json.load(f)
        
        connector_name = connector_config.get('name')
        print(f"Aplicando conector: {connector_name}")
        
        # Verificar si ya existe
        existing = get_existing_connectors()
        if connector_name in existing:
            print(f"⚠️  Conector {connector_name} ya existe, saltando...")
            return True
        
        # Aplicar conector usando la URL correcta
        connect_service_url = DEBEZIUM_CONNECT_URL or CONNECT_URL
        response = requests.post(
            f"{connect_service_url}/connectors",
            headers={"Content-Type": "application/json"},
            json=connector_config,
            timeout=10
        )
        
        if response.status_code in [200, 201]:
            print(f"✅ Conector {connector_name} aplicado exitosamente")
            return True
        else:
            print(f"❌ Error aplicando {connector_name}: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error procesando {connector_file}: {e}")
        return False

def main():
    print("=== APLICACIÓN AUTOMÁTICA DE CONECTORES DEBEZIUM ===")
    
    # Esperar a que Connect esté disponible
    if not wait_for_connect():
        sys.exit(1)
    
    # Buscar archivos de conectores
    connector_files = glob.glob(f"{CONNECTORS_PATH}/*.json")
    if not connector_files:
        print(f"⚠️  No se encontraron archivos de conectores en {CONNECTORS_PATH}")
        sys.exit(0)
    
    print(f"Encontrados {len(connector_files)} archivos de conectores")
    
    # Aplicar cada conector
    success_count = 0
    for connector_file in connector_files:
        if apply_connector(connector_file):
            success_count += 1
        time.sleep(2)  # Pausa entre aplicaciones
    
    print(f"\n=== RESUMEN ===")
    print(f"Conectores aplicados exitosamente: {success_count}/{len(connector_files)}")
    
    # Crear archivo de log para indicar que el proceso terminó
    try:
        with open("logs/connectors_applied.log", "w") as f:
            f.write(f"Conectores aplicados: {success_count}/{len(connector_files)}\n")
            f.write(f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    except:
        pass
    
    if success_count == len(connector_files):
        print("🎉 TODOS LOS CONECTORES APLICADOS CORRECTAMENTE")
        return 0
    else:
        print("⚠️  ALGUNOS CONECTORES FALLARON")
        return 1

if __name__ == "__main__":
    sys.exit(main())