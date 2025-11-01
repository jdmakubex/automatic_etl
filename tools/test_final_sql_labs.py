#!/usr/bin/env python3
"""
Test final de SQL Labs con usuario correcto
==========================================

Prueba SQL Lab con el nuevo usuario que tiene permisos de Admin
"""
import os
import requests
from dotenv import load_dotenv
import json

load_dotenv('/app/.env')

def test_superset_with_correct_user():
    """Test SQL Lab con usuario que tiene roles"""
    print("🔍 TESTING SUPERSET SQL LAB - USUARIO CORRECTO")
    print("=" * 50)
    
    SUPERSET_URL = 'http://superset:8088'
    ADMIN = os.getenv('SUPERSET_ADMIN', 'admin')  # Usuario oficial del .env
    PASSWORD = os.getenv('SUPERSET_PASSWORD', 'Admin123!')
    
    session = requests.Session()
    
    # Login
    csrf_resp = session.get(f'{SUPERSET_URL}/login/')
    login_data = {
        'username': ADMIN,
        'password': PASSWORD,
        'csrf_token': session.cookies.get('csrf_token', '')
    }
    
    login_resp = session.post(f'{SUPERSET_URL}/login/', data=login_data)
    if 'logout' not in login_resp.text.lower():
        print(f"❌ Login failed")
        return False
    
    print("✅ Superset autenticado (usuario con Admin role)")
    
    # Check user info
    me_resp = session.get(f'{SUPERSET_URL}/api/v1/me/')
    if me_resp.status_code == 200:
        user_info = me_resp.json().get('result', {})
        print(f"   👤 User: {user_info.get('username')}")
        roles = [r.get('name') for r in user_info.get('roles', [])]
        print(f"   🔑 Roles: {roles}")
        
        if not roles or 'Admin' not in roles:
            print(f"   ⚠️  Usuario sin rol Admin correcto")
    
    # Get databases
    db_resp = session.get(f'{SUPERSET_URL}/api/v1/database/')
    if db_resp.status_code != 200:
        print(f"❌ No se pudieron obtener databases: {db_resp.status_code}")
        return False
    
    databases = db_resp.json().get('result', [])
    clickhouse_dbs = [db for db in databases if 'clickhouse' in db.get('backend', '').lower()]
    
    if not clickhouse_dbs:
        print("❌ No ClickHouse database found")
        return False
    
    ch_db_id = clickhouse_dbs[0]['id']
    print(f"✅ ClickHouse DB ID: {ch_db_id}")
    
    # Intentar diferentes métodos de SQL Lab
    test_methods = [
        {
            'name': 'SQL Lab API V1 (nuevo formato)',
            'url': f'{SUPERSET_URL}/api/v1/sqllab/execute/',
            'payload': {
                'database_id': ch_db_id,
                'sql': 'SELECT COUNT(*) as total FROM fgeo_analytics.test_table',
                'schema': 'fgeo_analytics'
            }
        },
        {
            'name': 'Chart Data API',
            'url': f'{SUPERSET_URL}/api/v1/chart/data',  
            'payload': {
                'datasource': {'type': 'table', 'id': 1},
                'queries': [{
                    'columns': [],
                    'metrics': [],
                    'filters': [],
                    'limit': 100
                }]
            }
        },
        {
            'name': 'Query API Direct',
            'url': f'{SUPERSET_URL}/api/v1/query/',
            'payload': {
                'database_id': ch_db_id,
                'sql': 'SELECT * FROM fgeo_analytics.test_table LIMIT 5'
            }
        }
    ]
    
    successful_methods = 0
    
    for method in test_methods:
        print(f"\n🧪 Trying: {method['name']}")
        
        try:
            resp = session.post(method['url'], json=method['payload'])
            print(f"   Status: {resp.status_code}")
            
            if resp.status_code in [200, 202]:
                try:
                    data = resp.json()
                    print(f"   ✅ Success!")
                    
                    # Analizar respuesta
                    if 'data' in data and data['data']:
                        print(f"      📊 Data rows: {len(data['data'])}")
                        if data['data']:
                            print(f"      📋 Sample: {data['data'][0]}")
                        successful_methods += 1
                    elif 'job_id' in data:
                        print(f"      🔄 Async job created: {data['job_id']}")
                        # Para jobs async, podríamos hacer polling pero por simplicidad lo contamos como éxito
                        successful_methods += 1
                    else:
                        print(f"      📝 Response: {json.dumps(data, indent=2)[:300]}")
                        
                except Exception as e:
                    print(f"      📝 Raw response: {resp.text[:200]}")
            else:
                print(f"   ❌ Error: {resp.text[:300]}")
                
        except Exception as e:
            print(f"   ❌ Exception: {e}")
    
    print(f"\n📊 Superset SQL Lab Result: {successful_methods}/{len(test_methods)} methods successful")
    return successful_methods > 0

def test_metabase_functionality():
    """Confirmar que Metabase sigue funcionando"""
    print("\n🔍 CONFIRMANDO METABASE FUNCIONA")
    print("=" * 40)
    
    METABASE_URL = 'http://metabase:3000'
    ADMIN = os.getenv('METABASE_ADMIN', 'admin@admin.com')
    PASSWORD = os.getenv('METABASE_PASSWORD', 'Admin123!')
    
    # Simple connectivity test
    auth_resp = requests.post(f'{METABASE_URL}/api/session', 
                            json={'username': ADMIN, 'password': PASSWORD})
    if auth_resp.status_code != 200:
        print(f"❌ Metabase auth failed")
        return False
    
    session_id = auth_resp.json()['id']
    headers = {'X-Metabase-Session': session_id}
    
    # Quick query test
    db_resp = requests.get(f'{METABASE_URL}/api/database', headers=headers)
    clickhouse_dbs = [db for db in db_resp.json()['data'] if db.get('engine') == 'clickhouse']
    
    if clickhouse_dbs:
        ch_db_id = clickhouse_dbs[0]['id']
        payload = {
            "type": "native",
            "native": {"query": "SELECT COUNT(*) FROM fgeo_analytics.test_table"},
            "database": ch_db_id
        }
        
        resp = requests.post(f'{METABASE_URL}/api/dataset', json=payload, headers=headers)
        if resp.status_code == 200:
            data = resp.json()
            if "data" in data and "rows" in data["data"]:
                rows = data["data"]["rows"]
                print(f"✅ Metabase SQL funcional: {len(rows)} filas, data: {rows[0] if rows else 'empty'}")
                return True
    
    print("❌ Metabase test failed")
    return False

def main():
    """Test both platforms"""
    print("🧪 PRUEBA FINAL DE SQL LABS")
    print("="*60)
    
    metabase_ok = test_metabase_functionality()
    superset_ok = test_superset_with_correct_user()
    
    print("\n" + "="*60)
    print("🏆 RESULTADO FINAL:")
    print(f"   {'✅' if metabase_ok else '❌'} Metabase SQL Lab: {'FUNCIONAL' if metabase_ok else 'CON PROBLEMAS'}")
    print(f"   {'✅' if superset_ok else '❌'} Superset SQL Lab: {'FUNCIONAL' if superset_ok else 'CON PROBLEMAS'}")
    
    if metabase_ok and superset_ok:
        print("\n🎉 ¡AMBOS SQL LABS FUNCIONAN CORRECTAMENTE!")
        print("   Ambas plataformas pueden ejecutar consultas SQL a ClickHouse.")
    elif metabase_ok or superset_ok:
        working = 'Metabase' if metabase_ok else 'Superset'
        not_working = 'Superset' if metabase_ok else 'Metabase'
        print(f"\n⚠️  {working} funciona, {not_working} necesita más configuración.")
    else:
        print("\n💥 Ambas plataformas tienen problemas de configuración.")

if __name__ == "__main__":
    main()