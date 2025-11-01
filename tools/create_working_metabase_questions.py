#!/usr/bin/env python3
"""
Script para crear preguntas SQL directas en Metabase que funcionen independientemente
de las estadísticas de metadata. Esto solucionará el problema de visualización.
"""
import os
import requests
from dotenv import load_dotenv
import time

load_dotenv('/app/.env')

METABASE_URL = os.getenv("METABASE_URL", "http://metabase:3000")
METABASE_ADMIN = os.getenv("METABASE_ADMIN", "admin@admin.com")
METABASE_PASSWORD = os.getenv("METABASE_PASSWORD", "Admin123!")

def login():
    """Autenticar en Metabase"""
    resp = requests.post(f"{METABASE_URL}/api/session", 
                        json={"username": METABASE_ADMIN, "password": METABASE_PASSWORD})
    if resp.status_code == 200:
        return resp.json()["id"]
    return None

def create_working_question(session_id, database_id, name, query, display_type="table"):
    """Crear una pregunta SQL que definitivamente funcione"""
    headers = {"X-Metabase-Session": session_id}
    
    payload = {
        "name": name,
        "description": f"Pregunta SQL directa que bypassa problemas de metadata - {name}",
        "dataset_query": {
            "type": "native",
            "native": {
                "query": query
            },
            "database": database_id
        },
        "display": display_type,
        "visualization_settings": {}
    }
    
    resp = requests.post(f"{METABASE_URL}/api/card", json=payload, headers=headers)
    if resp.status_code == 200:
        card_id = resp.json()["id"]
        print(f"   ✅ Creada: {name} (ID: {card_id})")
        
        # Probar inmediatamente que la pregunta funciona
        test_resp = requests.post(f"{METABASE_URL}/api/dataset", json=payload["dataset_query"], headers=headers)
        if test_resp.status_code in [200, 202]:
            data = test_resp.json()
            if "data" in data and "rows" in data["data"]:
                row_count = len(data["data"]["rows"])
                print(f"      📊 Datos confirmados: {row_count} filas")
                return card_id, True
        
        print(f"      ⚠️  Pregunta creada pero sin datos")
        return card_id, False
    else:
        print(f"   ❌ Error creando {name}: {resp.status_code}")
        return None, False

def main():
    print("🔧 CREANDO PREGUNTAS SQL DIRECTAS EN METABASE")
    print("=" * 60)
    print("Estas preguntas funcionarán independientemente de la metadata")
    print("")
    
    session_id = login()
    if not session_id:
        print("❌ No se pudo autenticar")
        return
    
    database_id = 2  # ClickHouse database ID
    
    # Preguntas que sabemos que tienen datos
    working_questions = [
        ("🔢 Total de Registros Test", "SELECT COUNT(*) as total_records FROM fgeo_analytics.test_table", "scalar"),
        ("📋 Datos de Test Table", "SELECT id, name FROM fgeo_analytics.test_table LIMIT 10", "table"),
        ("🔢 Total Archivos", "SELECT COUNT(*) as total_archivos FROM fgeo_analytics.archivos_archivos_raw", "scalar"),
        ("📄 Últimos Archivos", """
            SELECT 
                ingested_at as fecha_ingesta,
                JSONExtractString(value, 'nombre') as nombre_archivo,
                JSONExtractString(value, 'tipo') as tipo_archivo,
                JSONExtractString(value, 'id') as archivo_id
            FROM fgeo_analytics.archivos_archivos_raw 
            ORDER BY ingested_at DESC 
            LIMIT 10
        """, "table"),
        ("🔢 Total Casos Fiscalización", "SELECT COUNT(*) as total_casos FROM fgeo_analytics.fiscalizacion_altoimpacto_raw", "scalar"),
        ("⚖️ Casos de Fiscalización", """
            SELECT 
                ingested_at as fecha_ingesta,
                JSONExtractString(value, 'caso') as numero_caso,
                JSONExtractString(value, 'estado') as estado,
                JSONExtractString(value, 'id') as caso_id
            FROM fgeo_analytics.fiscalizacion_altoimpacto_raw 
            ORDER BY ingested_at DESC
        """, "table"),
        ("📊 Resumen General de Datos", """
            SELECT 
                'Archivos' as categoria,
                COUNT(*) as total_registros
            FROM fgeo_analytics.archivos_archivos_raw
            
            UNION ALL
            
            SELECT 
                'Fiscalización' as categoria,
                COUNT(*) as total_registros
            FROM fgeo_analytics.fiscalizacion_altoimpacto_raw
            
            UNION ALL
            
            SELECT 
                'Test Data' as categoria,
                COUNT(*) as total_registros
            FROM fgeo_analytics.test_table
        """, "table"),
    ]
    
    created_cards = []
    working_cards = 0
    
    print("📝 Creando preguntas SQL directas:")
    
    for name, query, display in working_questions:
        card_id, has_data = create_working_question(session_id, database_id, name, query, display)
        if card_id:
            created_cards.append(card_id)
            if has_data:
                working_cards += 1
    
    # Crear un dashboard simple con las preguntas que funcionan
    if created_cards:
        print(f"\n📊 Creando dashboard con {len(created_cards)} preguntas...")
        
        dashboard_payload = {
            "name": "🎯 Dashboard SQL Directo - Datos Confirmados",
            "description": "Dashboard con preguntas SQL directas que bypassed problemas de metadata de Metabase"
        }
        
        headers = {"X-Metabase-Session": session_id}
        dash_resp = requests.post(f"{METABASE_URL}/api/dashboard", json=dashboard_payload, headers=headers)
        
        if dash_resp.status_code == 200:
            dashboard_id = dash_resp.json()["id"]
            print(f"✅ Dashboard creado (ID: {dashboard_id})")
            print(f"🔗 URL: {METABASE_URL}/dashboard/{dashboard_id}")
            
            # Agregar las primeras 6 tarjetas al dashboard
            dashboard_cards = []
            for i, card_id in enumerate(created_cards[:6]):
                dashboard_cards.append({
                    "id": card_id,
                    "card_id": card_id,
                    "row": (i // 2) * 4,
                    "col": (i % 2) * 6,
                    "sizeX": 6,
                    "sizeY": 4
                })
            
            if dashboard_cards:
                update_payload = {"ordered_cards": dashboard_cards}
                update_resp = requests.put(
                    f"{METABASE_URL}/api/dashboard/{dashboard_id}/cards",
                    json=update_payload,
                    headers=headers
                )
                
                if update_resp.status_code == 200:
                    print(f"✅ {len(dashboard_cards)} tarjetas agregadas al dashboard")
    
    # Resumen final
    print("\n" + "=" * 60)
    print("📊 RESUMEN:")
    print(f"   ✅ Preguntas creadas: {len(created_cards)}")
    print(f"   📊 Preguntas con datos: {working_cards}")
    print(f"   🎯 URL de acceso: {METABASE_URL}")
    print(f"   👤 Usuario: {METABASE_ADMIN}")
    
    if working_cards > 0:
        print("\n🎉 ¡ÉXITO! Ya tienes preguntas funcionando que muestran los datos.")
        print("   Las preguntas SQL directas funcionan independientemente de la metadata.")
        print("   Ve a 'Our Analytics' > 'Browse Data' o al dashboard creado.")
    else:
        print("\n⚠️  Preguntas creadas pero necesitas verificar manualmente que muestren datos.")

if __name__ == "__main__":
    main()