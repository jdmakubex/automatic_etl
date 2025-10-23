#!/usr/bin/env python3
"""
Script para crear un chart de prueba y verificar que funciona correctamente
"""
import requests
import json
import os

SUPERSET_URL = os.getenv("SUPERSET_URL", "http://localhost:8088")
SUPERSET_ADMIN = os.getenv("SUPERSET_ADMIN", "admin")
SUPERSET_PASSWORD = os.getenv("SUPERSET_PASSWORD", "Admin123!")

def get_token():
    """Obtener token de autenticación"""
    response = requests.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json={"username": SUPERSET_ADMIN, "password": SUPERSET_PASSWORD, "provider": "db", "refresh": True},
        headers={"Content-Type": "application/json"}
    )
    if response.status_code == 200:
        return response.json().get("access_token")
    return None

def create_simple_chart(token, dataset_id):
    """Crear un chart simple de conteo por fecha"""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    
    # Configuración de un chart tipo Table (simple)
    chart_data = {
        "slice_name": "Test - Registros por fecha",
        "viz_type": "table",
        "datasource_id": dataset_id,
        "datasource_type": "table",
        "params": json.dumps({
            "datasource": f"{dataset_id}__table",
            "viz_type": "table",
            "metrics": ["count"],
            "groupby": ["fecha"],
            "row_limit": 100,
            "time_grain_sqla": None,  # ← ESTO ES CRÍTICO: No aplicar Time Grain
            "order_desc": True,
            "include_time": False
        })
    }
    
    response = requests.post(
        f"{SUPERSET_URL}/api/v1/chart/",
        json=chart_data,
        headers=headers
    )
    
    if response.status_code in [200, 201]:
        chart_id = response.json().get("id")
        print(f"✅ Chart creado exitosamente (ID: {chart_id})")
        print(f"   URL: {SUPERSET_URL}/explore/?form_data=%7B%22slice_id%22%3A{chart_id}%7D")
        return chart_id
    else:
        print(f"❌ Error creando chart: {response.status_code}")
        print(f"   Response: {response.text[:500]}")
        return None

def test_chart_data(token, chart_id):
    """Probar ejecutar la query del chart"""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    
    response = requests.get(
        f"{SUPERSET_URL}/api/v1/chart/{chart_id}/data",
        headers=headers
    )
    
    if response.status_code == 200:
        print("✅ Chart ejecutó correctamente")
        data = response.json()
        print(f"   Registros retornados: {len(data.get('result', [{}])[0].get('data', []))}")
        return True
    else:
        print(f"❌ Error ejecutando chart: {response.status_code}")
        print(f"   Response: {response.text[:500]}")
        return False

if __name__ == "__main__":
    print("🧪 Creando chart de prueba...")
    
    token = get_token()
    if not token:
        print("❌ No se pudo obtener token")
        exit(1)
    
    # Dataset ID 14 = bitacora (tiene columna fecha)
    chart_id = create_simple_chart(token, 14)
    
    if chart_id:
        print("\n🧪 Probando ejecución del chart...")
        test_chart_data(token, chart_id)
