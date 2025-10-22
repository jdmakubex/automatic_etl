#!/usr/bin/env python3
"""
Script para aplicar conectores de Debezium automáticamente
Espera a que Connect esté listo y aplica todos los conectores configurados
"""
import requests
import json
import time
import os
import sys
import glob
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)

CONNECT_URL = os.getenv("CONNECT_URL", "http://connect:8083")
# Directorio base donde se generan los conectores. Por defecto 'generated'
# Antes estaba fijo a 'generated/default', lo que omitía otras conexiones.
CONNECTORS_PATH = os.getenv("CONNECTORS_PATH", "generated")
MAX_RETRIES = 30
RETRY_DELAY = 10

class FatalError(Exception):
    """Error fatal que termina la ejecución"""
    pass

class RecoverableError(Exception):
    """Error recuperable que permite continuar"""
    pass

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
    except Exception:
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
        
        # Aplicar conector
        response = requests.post(
            f"{CONNECT_URL}/connectors",
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

def discover_connector_files(base_path: str) -> list[str]:
    """Busca archivos de conectores Debezium generados.

    Criterios:
    - Recursivo bajo base_path
    - Nombres típicos: connector.json o cualquier *.json con campo 'config'
    - Excluye archivos de metadatos (discovery_summary.json, tables_metadata.json)
    """
    files = []
    # 1) Preferir archivos explícitos 'connector.json'
    files.extend(glob.glob(os.path.join(base_path, "**", "connector.json"), recursive=True))
    # 2) Secundario: cualquier *.json excepto los metadatos comunes
    for f in glob.glob(os.path.join(base_path, "**", "*.json"), recursive=True):
        name = os.path.basename(f)
        if name in ("discovery_summary.json", "tables_metadata.json"):
            continue
        if f not in files:
            try:
                with open(f, "r", encoding="utf-8") as fh:
                    js = json.load(fh)
                if isinstance(js, dict) and ("config" in js or "name" in js):
                    files.append(f)
            except Exception:
                # Ignorar JSON inválidos
                pass
    # Orden estable para logs predecibles
    return sorted(files)


def main():
    """Función principal para aplicar conectores automáticamente"""
    print("=== APLICACIÓN AUTOMÁTICA DE CONECTORES DEBEZIUM ===")
    
    # Esperar a que Connect esté disponible
    if not wait_for_connect():
        log.error("Kafka Connect no está disponible")
        sys.exit(1)
    
    # Buscar archivos de conectores (recursivo)
    base_path = CONNECTORS_PATH
    connector_files = discover_connector_files(base_path)
    
    if not connector_files:
        print(f"⚠️  No se encontraron archivos de conectores en {base_path}")
        return  # No es fatal: permite continuar con otras fases
    
    print(f"Encontrados {len(connector_files)} archivos de conectores")
    
    # Aplicar cada conector
    success_count = 0
    for connector_file in connector_files:
        if apply_connector(connector_file):
            success_count += 1
    
    # Resumen
    total = len(connector_files)
    print(f"\n=== RESUMEN ===")
    print(f"Conectores aplicados exitosamente: {success_count}/{total}")
    
    if success_count == total:
        print("🎉 TODOS LOS CONECTORES APLICADOS CORRECTAMENTE")
        sys.exit(0)
    else:
        print("⚠️  ALGUNOS CONECTORES FALLARON")
        sys.exit(1)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(f"Error inesperado: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
