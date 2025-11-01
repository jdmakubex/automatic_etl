#!/usr/bin/env python3
"""
Script para forzar la sincronización de esquemas en Metabase y crear una pregunta de prueba
para ver los datos en la interfaz web.
"""
import os
import requests
import time
from dotenv import load_dotenv

load_dotenv('/app/.env')

METABASE_URL = os.getenv("METABASE_URL")
METABASE_ADMIN = os.getenv("METABASE_ADMIN")
METABASE_PASSWORD = os.getenv("METABASE_PASSWORD")

LOGIN_ENDPOINT = f"{METABASE_URL}/api/session"
DB_ENDPOINT = f"{METABASE_URL}/api/database"
CARD_ENDPOINT = f"{METABASE_URL}/api/card"

def login():
    resp = requests.post(LOGIN_ENDPOINT, json={"username": METABASE_ADMIN, "password": METABASE_PASSWORD})
    if resp.status_code == 200:
        return resp.json()["id"]
    return None

def force_sync_database(session_id, database_id):
    """Fuerza la sincronización completa del esquema de la base de datos"""
    headers = {"X-Metabase-Session": session_id}
    
    print(f"🔄 Forzando sincronización completa de la base {database_id}...")
    
    # Sincronización de esquemas
    sync_resp = requests.post(f"{DB_ENDPOINT}/{database_id}/sync_schema", headers=headers)
    if sync_resp.status_code == 200:
        print("✅ Sincronización de esquemas iniciada")
    else:
        print(f"❌ Error en sync_schema: {sync_resp.status_code}")
    
    # Sincronización de metadatos de campos
    rescan_resp = requests.post(f"{DB_ENDPOINT}/{database_id}/rescan_values", headers=headers)
    if rescan_resp.status_code == 200:
        print("✅ Re-escaneo de valores iniciado")
    else:
        print(f"❌ Error en rescan_values: {rescan_resp.status_code}")
    
    print("⏳ Esperando que termine la sincronización...")
    time.sleep(10)
    
    return True

def create_sample_question(session_id, database_id):
    """Crea una pregunta de muestra para ver datos en Metabase"""
    headers = {"X-Metabase-Session": session_id}
    
    # Pregunta SQL simple para ver datos de archivos
    question_payload = {
        "name": "📄 Archivos - Vista de Datos",
        "description": "Vista simple de los datos de archivos ingresados",
        "dataset_query": {
            "type": "native",
            "native": {
                "query": "SELECT ingested_at, value FROM fgeo_analytics.archivos_archivos_raw LIMIT 10"
            },
            "database": database_id
        },
        "display": "table",
        "visualization_settings": {}
    }
    
    resp = requests.post(CARD_ENDPOINT, json=question_payload, headers=headers)
    if resp.status_code == 200:
        card_id = resp.json()["id"]
        print(f"✅ Pregunta creada: 'Archivos - Vista de Datos' (ID: {card_id})")
        print(f"🔗 Accede en: {METABASE_URL.replace('metabase', 'localhost')}/question/{card_id}")
        return card_id
    else:
        print(f"❌ Error creando pregunta: {resp.status_code} - {resp.text}")
        return None

def create_fiscalizacion_question(session_id, database_id):
    """Crea una pregunta para ver datos de fiscalización"""
    headers = {"X-Metabase-Session": session_id}
    
    question_payload = {
        "name": "⚖️ Fiscalización - Casos Alto Impacto",
        "description": "Vista de casos de fiscalización de alto impacto",
        "dataset_query": {
            "type": "native",
            "native": {
                "query": "SELECT ingested_at, value FROM fgeo_analytics.fiscalizacion_altoimpacto_raw LIMIT 10"
            },
            "database": database_id
        },
        "display": "table",
        "visualization_settings": {}
    }
    
    resp = requests.post(CARD_ENDPOINT, json=question_payload, headers=headers)
    if resp.status_code == 200:
        card_id = resp.json()["id"]
        print(f"✅ Pregunta creada: 'Fiscalización - Casos Alto Impacto' (ID: {card_id})")
        print(f"🔗 Accede en: {METABASE_URL.replace('metabase', 'localhost')}/question/{card_id}")
        return card_id
    else:
        print(f"❌ Error creando pregunta: {resp.status_code} - {resp.text}")
        return None

if __name__ == "__main__":
    print("🚀 CONFIGURANDO VISUALIZACIÓN DE DATOS EN METABASE")
    print("=" * 55)
    
    session_id = login()
    if not session_id:
        print("❌ No se pudo autenticar")
        exit(1)
    
    print("✅ Autenticado en Metabase")
    
    # ID de ClickHouse database (según diagnósticos anteriores)
    clickhouse_db_id = 2
    
    # Forzar sincronización
    force_sync_database(session_id, clickhouse_db_id)
    
    # Crear preguntas de ejemplo
    create_sample_question(session_id, clickhouse_db_id)
    create_fiscalizacion_question(session_id, clickhouse_db_id)
    
    print("\n🎯 ¡LISTO! Accede a Metabase en:")
    print(f"   URL: http://localhost:3000")
    print(f"   Usuario: {METABASE_ADMIN}")
    print(f"   Contraseña: {METABASE_PASSWORD}")
    print("\n📊 Ve a 'Our Analytics' > 'Browse Data' para explorar las tablas")
    print("📈 O revisa las preguntas creadas automáticamente arriba")