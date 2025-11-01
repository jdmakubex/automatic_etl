#!/usr/bin/env python3
"""
Script para diagnosticar por qué Metabase no muestra datos aunque existan en ClickHouse.
Prueba diferentes tipos de consultas para identificar el problema.
"""
import os
import requests
from dotenv import load_dotenv
import json

load_dotenv('/app/.env')

METABASE_URL = os.getenv("METABASE_URL", "http://metabase:3000")
METABASE_ADMIN = os.getenv("METABASE_ADMIN", "admin@admin.com")
METABASE_PASSWORD = os.getenv("METABASE_PASSWORD", "Admin123!")

def login():
    """Autenticar en Metabase"""
    print("🔐 Autenticando en Metabase...")
    resp = requests.post(f"{METABASE_URL}/api/session", 
                        json={"username": METABASE_ADMIN, "password": METABASE_PASSWORD})
    if resp.status_code == 200:
        session_id = resp.json()["id"]
        print("✅ Autenticación exitosa")
        return session_id
    else:
        print(f"❌ Error autenticación: {resp.status_code} - {resp.text}")
        return None

def test_native_query(session_id, database_id, query, query_name):
    """Probar una consulta nativa en Metabase"""
    print(f"\n🔍 Probando: {query_name}")
    print(f"   Query: {query}")
    
    headers = {"X-Metabase-Session": session_id}
    payload = {
        "type": "native",
        "native": {
            "query": query
        },
        "database": database_id
    }
    
    resp = requests.post(f"{METABASE_URL}/api/dataset", json=payload, headers=headers)
    
    if resp.status_code == 202:
        print("   ✅ Consulta aceptada (202 - procesando)")
        
        # Obtener datos de la respuesta
        data = resp.json()
        if "data" in data and "rows" in data["data"]:
            rows = data["data"]["rows"]
            cols = data["data"]["cols"]
            
            print(f"   📊 Columnas: {len(cols)}")
            for col in cols[:3]:  # Mostrar primeras 3 columnas
                print(f"      - {col['name']} ({col['base_type']})")
            
            print(f"   📋 Filas: {len(rows)}")
            for i, row in enumerate(rows[:3]):  # Mostrar primeras 3 filas
                print(f"      {i+1}. {row}")
                
            if len(rows) == 0:
                print("   ⚠️  SIN DATOS: La consulta no devolvió filas")
            
            return len(rows) > 0
        else:
            print("   ⚠️  Sin datos en la respuesta")
            return False
            
    elif resp.status_code == 200:
        print("   ✅ Consulta exitosa (200)")
        # Similar manejo para 200
        return True
    else:
        print(f"   ❌ Error: {resp.status_code}")
        print(f"      {resp.text[:300]}")
        return False

def get_database_metadata(session_id, database_id):
    """Obtener metadata de la base de datos"""
    print(f"\n🗄️  Obteniendo metadata de base ID: {database_id}")
    headers = {"X-Metabase-Session": session_id}
    
    resp = requests.get(f"{METABASE_URL}/api/database/{database_id}/metadata", headers=headers)
    if resp.status_code == 200:
        metadata = resp.json()
        tables = metadata.get("tables", [])
        print(f"   📊 Tablas detectadas: {len(tables)}")
        
        tables_with_data = 0
        for table in tables[:10]:  # Mostrar primeras 10
            table_name = table.get("name", "unknown")
            row_count = table.get("rows")
            schema = table.get("schema", "default")
            
            if row_count and row_count > 0:
                tables_with_data += 1
                print(f"   ✅ {schema}.{table_name}: {row_count:,} filas")
            else:
                print(f"   ⚠️  {schema}.{table_name}: Sin datos detectados")
        
        print(f"   📈 Tablas con datos según Metabase: {tables_with_data}/{len(tables)}")
        return metadata
    else:
        print(f"   ❌ Error obteniendo metadata: {resp.status_code}")
        return None

def force_sync_database(session_id, database_id):
    """Forzar sincronización de la base de datos"""
    print(f"\n🔄 Forzando sincronización de base ID: {database_id}")
    headers = {"X-Metabase-Session": session_id}
    
    # Sincronización de esquemas
    resp = requests.post(f"{METABASE_URL}/api/database/{database_id}/sync_schema", headers=headers)
    if resp.status_code == 200:
        print("   ✅ Sincronización de esquemas iniciada")
    else:
        print(f"   ⚠️  Sincronización esquemas: {resp.status_code}")
    
    # Re-escaneo de valores
    resp2 = requests.post(f"{METABASE_URL}/api/database/{database_id}/rescan_values", headers=headers)
    if resp2.status_code == 200:
        print("   ✅ Re-escaneo de valores iniciado")
    else:
        print(f"   ⚠️  Re-escaneo valores: {resp2.status_code}")

def main():
    print("🔍 DIAGNÓSTICO DE VISUALIZACIÓN DE DATOS EN METABASE")
    print("=" * 60)
    
    # Autenticar
    session_id = login()
    if not session_id:
        return
    
    # ID de la base ClickHouse (según configurador dinámico)
    database_id = 2
    
    # 1. Obtener metadata actual
    metadata = get_database_metadata(session_id, database_id)
    
    # 2. Forzar sincronización
    force_sync_database(session_id, database_id)
    
    print("\n⏳ Esperando 10 segundos para que termine la sincronización...")
    import time
    time.sleep(10)
    
    # 3. Probar consultas específicas que sabemos tienen datos
    test_queries = [
        ("SELECT COUNT(*) as total FROM fgeo_analytics.test_table", "Conteo test_table"),
        ("SELECT * FROM fgeo_analytics.test_table LIMIT 3", "Muestra test_table"),
        ("SELECT COUNT(*) as total FROM fgeo_analytics.archivos_archivos_raw", "Conteo archivos"),
        ("SELECT * FROM fgeo_analytics.archivos_archivos_raw LIMIT 2", "Muestra archivos"),
        ("SELECT COUNT(*) as total FROM fgeo_analytics.fiscalizacion_altoimpacto_raw", "Conteo fiscalización"),
        ("SELECT * FROM fgeo_analytics.fiscalizacion_altoimpacto_raw LIMIT 2", "Muestra fiscalización"),
    ]
    
    successful_queries = 0
    queries_with_data = 0
    
    for query, name in test_queries:
        has_data = test_native_query(session_id, database_id, query, name)
        successful_queries += 1
        if has_data:
            queries_with_data += 1
    
    # Resumen
    print("\n" + "=" * 60)
    print("📊 RESUMEN DEL DIAGNÓSTICO:")
    print(f"   ✅ Consultas ejecutadas: {successful_queries}")
    print(f"   📋 Consultas con datos: {queries_with_data}")
    print(f"   🎯 Tasa de éxito: {queries_with_data/successful_queries*100:.1f}%")
    
    if queries_with_data == 0:
        print("\n🚨 PROBLEMA IDENTIFICADO:")
        print("   Las consultas se ejecutan pero no devuelven datos")
        print("   Posibles causas:")
        print("   - Problema de permisos de usuario ClickHouse")
        print("   - Schema cache desactualizado en Metabase")
        print("   - Configuración incorrecta de la conexión")
        
    elif queries_with_data < successful_queries:
        print("\n⚠️  PROBLEMA PARCIAL:")
        print("   Algunas consultas devuelven datos, otras no")
        print("   Revisar sincronización de schema específicas")
        
    else:
        print("\n🎉 ¡TODO FUNCIONA CORRECTAMENTE!")
        print("   El problema puede estar en la UI de Metabase")
        print("   Intenta refrescar el navegador o limpiar cache")

if __name__ == "__main__":
    main()