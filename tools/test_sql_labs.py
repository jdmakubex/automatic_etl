#!/usr/bin/env python3
"""
Test directo de SQL Lab en Metabase y Superset
==============================================

Prueba ejecutar consultas SQL reales en ambas plataformas para verificar
si el problema es de conectividad o de configuración de UI.
"""
import os
import requests
from dotenv import load_dotenv
import json

load_dotenv('/app/.env')

def test_metabase_sql():
    """Test SQL directo en Metabase"""
    print("🔍 TESTING METABASE SQL LAB")
    print("=" * 40)
    
    METABASE_URL = 'http://metabase:3000'
    ADMIN = os.getenv('METABASE_ADMIN', 'admin@admin.com')
    PASSWORD = os.getenv('METABASE_PASSWORD', 'Admin123!')
    
    # Autenticar
    auth_resp = requests.post(f'{METABASE_URL}/api/session', 
                            json={'username': ADMIN, 'password': PASSWORD})
    if auth_resp.status_code != 200:
        print(f"❌ Metabase auth failed: {auth_resp.status_code}")
        return False
    
    session_id = auth_resp.json()['id']
    headers = {'X-Metabase-Session': session_id}
    print("✅ Metabase autenticado")
    
    # Obtener database ID
    db_resp = requests.get(f'{METABASE_URL}/api/database', headers=headers)
    clickhouse_dbs = [db for db in db_resp.json()['data'] if db.get('engine') == 'clickhouse']
    
    if not clickhouse_dbs:
        print("❌ No ClickHouse database found")
        return False
    
    ch_db_id = clickhouse_dbs[0]['id']
    print(f"✅ ClickHouse DB ID: {ch_db_id}")
    
    # Test queries
    test_queries = [
        "SELECT 1 as test_number",
        "SELECT COUNT(*) as total FROM fgeo_analytics.test_table",
        "SELECT * FROM fgeo_analytics.test_table LIMIT 3",
        "SELECT name, total_rows FROM system.tables WHERE database = 'fgeo_analytics' AND total_rows > 0"
    ]
    
    successful_queries = 0
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n📊 Query {i}: {query[:50]}...")
        
        payload = {
            "type": "native",
            "native": {"query": query},
            "database": ch_db_id
        }
        
        resp = requests.post(f'{METABASE_URL}/api/dataset', json=payload, headers=headers)
        
        if resp.status_code in [200, 202]:
            data = resp.json()
            if "data" in data and "rows" in data["data"]:
                rows = data["data"]["rows"]
                print(f"   ✅ Exitosa: {len(rows)} filas")
                if rows:
                    print(f"      📋 Sample: {rows[0]}")
                successful_queries += 1
            else:
                print(f"   ⚠️  Sin datos en respuesta")
        else:
            print(f"   ❌ Error: {resp.status_code}")
            print(f"      {resp.text[:200]}")
    
    print(f"\n📊 Metabase Result: {successful_queries}/{len(test_queries)} queries successful")
    return successful_queries == len(test_queries)

def test_superset_sql():
    """Test SQL directo en Superset"""
    print("\n🔍 TESTING SUPERSET SQL LAB")
    print("=" * 40)
    
    SUPERSET_URL = 'http://superset:8088'
    ADMIN = os.getenv('SUPERSET_ADMIN', 'admin')
    PASSWORD = os.getenv('SUPERSET_PASSWORD', 'Admin123!')
    
    session = requests.Session()
    
    # Get CSRF token
    try:
        csrf_resp = session.get(f'{SUPERSET_URL}/login/')
        if csrf_resp.status_code != 200:
            print(f"❌ Superset CSRF failed: {csrf_resp.status_code}")
            return False
    except Exception as e:
        print(f"❌ Superset connection error: {e}")
        return False
    
    # Login
    login_data = {
        'username': ADMIN,
        'password': PASSWORD,
        'csrf_token': session.cookies.get('csrf_token', '')
    }
    
    login_resp = session.post(f'{SUPERSET_URL}/login/', data=login_data)
    if 'logout' not in login_resp.text.lower():
        print(f"❌ Superset login failed")
        return False
    
    print("✅ Superset autenticado")
    
    # Get databases
    db_resp = session.get(f'{SUPERSET_URL}/api/v1/database/')
    if db_resp.status_code != 200:
        print(f"❌ No se pudieron obtener databases: {db_resp.status_code}")
        return False
    
    databases = db_resp.json().get('result', [])
    clickhouse_dbs = [db for db in databases if 'clickhouse' in db.get('backend', '').lower()]
    
    if not clickhouse_dbs:
        print("❌ No ClickHouse database found in Superset")
        return False
    
    ch_db_id = clickhouse_dbs[0]['id']
    print(f"✅ ClickHouse DB ID: {ch_db_id}")
    
    # Test SQL Lab
    test_queries = [
        "SELECT 1 as test_number",
        "SELECT COUNT(*) as total FROM fgeo_analytics.test_table",  
        "SELECT * FROM fgeo_analytics.test_table LIMIT 3"
    ]
    
    successful_queries = 0
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n📊 Query {i}: {query[:50]}...")
        
        # SQL Lab query
        sql_payload = {
            'database_id': ch_db_id,
            'sql': query,
            'schema': 'fgeo_analytics',
            'limit': 1000
        }
        
        try:
            resp = session.post(f'{SUPERSET_URL}/superset/sql_json/', json=sql_payload)
            
            if resp.status_code == 200:
                data = resp.json()
                if data.get('data'):
                    rows = len(data['data'])
                    print(f"   ✅ Exitosa: {rows} filas")
                    if data['data']:
                        print(f"      📋 Sample: {data['data'][0]}")
                    successful_queries += 1
                else:
                    print(f"   ⚠️  Sin datos en respuesta")
            else:
                print(f"   ❌ Error: {resp.status_code}")
                print(f"      {resp.text[:200]}")
        except Exception as e:
            print(f"   ❌ Exception: {e}")
    
    print(f"\n📊 Superset Result: {successful_queries}/{len(test_queries)} queries successful")
    return successful_queries == len(test_queries)

def main():
    """Test both SQL Labs"""
    print("🧪 TESTING SQL LAB FUNCTIONALITY")
    print("="*50)
    
    metabase_ok = test_metabase_sql()
    superset_ok = test_superset_sql()
    
    print("\n" + "="*50)
    print("📊 RESUMEN DE SQL LAB TESTS:")
    print(f"   {'✅' if metabase_ok else '❌'} Metabase SQL Lab: {'FUNCIONAL' if metabase_ok else 'CON PROBLEMAS'}")
    print(f"   {'✅' if superset_ok else '❌'} Superset SQL Lab: {'FUNCIONAL' if superset_ok else 'CON PROBLEMAS'}")
    
    if metabase_ok and superset_ok:
        print("\n🎉 ¡AMBOS SQL LABS FUNCIONAN!")
        print("   El problema puede ser de UI o configuración del usuario.")
    elif metabase_ok or superset_ok:
        print(f"\n⚠️  Solo {'Metabase' if metabase_ok else 'Superset'} funciona correctamente.")
    else:
        print("\n💥 AMBOS SQL LABS TIENEN PROBLEMAS")
        print("   Verificar conectividad y configuraciones.")

if __name__ == "__main__":
    main()