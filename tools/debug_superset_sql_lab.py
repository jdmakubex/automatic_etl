#!/usr/bin/env python3
"""
Verificar endpoints de Superset SQL Lab
======================================

Investigar si el problema es de endpoint o configuración de Superset
"""
import os
import requests
from dotenv import load_dotenv
import json

load_dotenv('/app/.env')

def check_superset_endpoints():
    """Verificar endpoints disponibles de Superset"""
    print("🔍 CHECKING SUPERSET ENDPOINTS")
    print("=" * 40)
    
    SUPERSET_URL = 'http://superset:8088'
    ADMIN = os.getenv('SUPERSET_ADMIN', 'admin')
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
    print("✅ Superset autenticado")
    
    # Test diferentes endpoints de SQL Lab
    endpoints_to_test = [
        '/superset/sql_json/',
        '/api/v1/sqllab/execute/',
        '/api/v1/query/',
        '/superset/sqllab_viz/',
        '/api/v1/database/1/select_star/fgeo_analytics/test_table/',
        '/api/v1/database/1/table/test_table/fgeo_analytics/'
    ]
    
    for endpoint in endpoints_to_test:
        print(f"\n📡 Testing: {endpoint}")
        
        # Test different methods
        for method in ['GET', 'POST']:
            try:
                if method == 'GET':
                    resp = session.get(f'{SUPERSET_URL}{endpoint}')
                else:
                    test_payload = {
                        'database_id': 1,
                        'sql': 'SELECT 1 as test',
                        'schema': 'fgeo_analytics'
                    }
                    resp = session.post(f'{SUPERSET_URL}{endpoint}', json=test_payload)
                
                print(f"   {method}: {resp.status_code}")
                
                if resp.status_code == 200:
                    content = resp.text[:200] if len(resp.text) > 200 else resp.text
                    print(f"      ✅ Response: {content}")
                elif resp.status_code == 404:
                    print(f"      ❌ Endpoint no existe")
                elif resp.status_code == 405:
                    print(f"      ⚠️  Método no permitido")
                else:
                    error_content = resp.text[:100] if len(resp.text) > 100 else resp.text
                    print(f"      ❌ Error: {error_content}")
                    
            except Exception as e:
                print(f"   ❌ Exception {method}: {e}")

def check_superset_config():
    """Verificar configuración de Superset"""
    print("\n🔧 CHECKING SUPERSET CONFIG")
    print("=" * 40)
    
    SUPERSET_URL = 'http://superset:8088'
    ADMIN = os.getenv('SUPERSET_ADMIN', 'admin')
    PASSWORD = os.getenv('SUPERSET_PASSWORD', 'Admin123!')
    
    session = requests.Session()
    
    # Login again
    csrf_resp = session.get(f'{SUPERSET_URL}/login/')
    login_data = {
        'username': ADMIN,
        'password': PASSWORD, 
        'csrf_token': session.cookies.get('csrf_token', '')
    }
    session.post(f'{SUPERSET_URL}/login/', data=login_data)
    
    # Check config endpoints
    config_endpoints = [
        '/api/v1/menu/',
        '/api/v1/security/roles/',
        '/api/v1/me/',
        '/health',
        '/static/appbuilder/js/'
    ]
    
    for endpoint in config_endpoints:
        resp = session.get(f'{SUPERSET_URL}{endpoint}')
        print(f"📊 {endpoint}: {resp.status_code}")
        
        if resp.status_code == 200 and endpoint == '/api/v1/me/':
            try:
                data = resp.json()
                if 'result' in data:
                    user_info = data['result']
                    print(f"   👤 User: {user_info.get('username')}")
                    print(f"   🔑 Roles: {[r.get('name') for r in user_info.get('roles', [])]}")
            except:
                pass

def test_direct_clickhouse_query():
    """Test directo a ClickHouse desde Superset usando otro método"""
    print("\n💾 TESTING DIRECT CLICKHOUSE FROM SUPERSET")
    print("=" * 40)
    
    SUPERSET_URL = 'http://superset:8088'
    ADMIN = os.getenv('SUPERSET_ADMIN', 'admin')
    PASSWORD = os.getenv('SUPERSET_PASSWORD', 'Admin123!')
    
    session = requests.Session()
    
    # Login
    csrf_resp = session.get(f'{SUPERSET_URL}/login/')
    login_data = {
        'username': ADMIN,
        'password': PASSWORD,
        'csrf_token': session.cookies.get('csrf_token', '')
    }
    session.post(f'{SUPERSET_URL}/login/', data=login_data)
    
    # Try different SQL execution methods
    methods_to_test = [
        {
            'name': 'SQL Lab Execute',
            'endpoint': '/api/v1/sqllab/execute/',
            'payload': {
                'database_id': 1,
                'sql': 'SELECT COUNT(*) FROM fgeo_analytics.test_table',
                'schema': 'fgeo_analytics',
                'limit': 1000,
                'async': False
            }
        },
        {
            'name': 'Chart Data',
            'endpoint': '/api/v1/chart/data',
            'payload': {
                'datasource': {
                    'type': 'table',
                    'id': 1
                },
                'queries': [{
                    'metrics': [],
                    'groupby': [],
                    'filters': [],
                    'limit': 100
                }]
            }
        }
    ]
    
    for method in methods_to_test:
        print(f"\n🧪 Trying: {method['name']}")
        try:
            resp = session.post(f"{SUPERSET_URL}{method['endpoint']}", json=method['payload'])
            print(f"   Status: {resp.status_code}")
            
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    print(f"   ✅ Success: {json.dumps(data, indent=2)[:300]}")
                except:
                    print(f"   ✅ Success (no JSON): {resp.text[:200]}")
            else:
                print(f"   ❌ Error: {resp.text[:200]}")
        except Exception as e:
            print(f"   ❌ Exception: {e}")

def main():
    """Run all tests"""
    check_superset_endpoints()
    check_superset_config() 
    test_direct_clickhouse_query()

if __name__ == "__main__":
    main()