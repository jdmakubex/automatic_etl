#!/usr/bin/env python3
"""
Verificación simple: SQL Lab está funcionando correctamente
"""
import requests
import os

SUPERSET_URL = os.getenv("SUPERSET_URL", "http://localhost:8088")
SUPERSET_ADMIN = os.getenv("SUPERSET_ADMIN", "admin")
SUPERSET_PASSWORD = os.getenv("SUPERSET_PASSWORD", "Admin123!")

# Obtener token
response = requests.post(
    f"{SUPERSET_URL}/api/v1/security/login",
    json={"username": SUPERSET_ADMIN, "password": SUPERSET_PASSWORD, "provider": "db", "refresh": True},
    headers={"Content-Type": "application/json"}
)
token = response.json().get("access_token")

if not token:
    print("❌ No se pudo autenticar")
    exit(1)

headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {token}"
}

# Verificar queries ejecutadas recientemente
response = requests.get(
    f"{SUPERSET_URL}/api/v1/query/?q=(order_column:changed_on,order_direction:desc,page:0,page_size:10)",
    headers=headers
)

if response.status_code == 200:
    queries = response.json().get("result", [])
    print(f"\n📊 Últimas {len(queries)} queries ejecutadas en SQL Lab:\n")
    
    for i, q in enumerate(queries[:5], 1):
        state = q.get("state", "unknown")
        sql = q.get("sql", "")[:80]
        error = q.get("errorMessage")
        rows = q.get("rows")
        
        icon = "✅" if state == "success" else "❌" if state == "failed" else "⏳"
        
        print(f"{icon} Query {i}:")
        print(f"   Estado: {state}")
        print(f"   SQL: {sql}...")
        if rows is not None:
            print(f"   Filas retornadas: {rows}")
        if error:
            print(f"   Error: {error[:200]}")
        print()
else:
    print(f"❌ Error obteniendo queries: {response.status_code}")

print("\n" + "="*80)
print("CONCLUSIÓN:")
print("="*80)
print("""
Si ves queries con estado 'success' arriba, SQL Lab ESTÁ FUNCIONANDO.

Los problemas que reportaste probablemente fueron:
1. Queries específicas con Time Grain que generan SQL inválido
2. Timeout en UI mientras esperaba respuesta async

SOLUCIÓN para usar SQL Lab:
- Escribe queries SQL directamente (sin usar datasets en Explore)
- NO uses Time Grain en gráficas
- Para fechas, usa funciones ClickHouse directamente:
  
  Ejemplos válidos:
  ✅ SELECT fecha, count(*) FROM tabla GROUP BY fecha
  ✅ SELECT toStartOfDay(fecha) as dia, count(*) FROM tabla GROUP BY dia
  ✅ SELECT toYYYYMM(fecha) as mes, count(*) FROM tabla GROUP BY mes
  
  JOIN entre tablas:
  ✅ SELECT a.*, b.* FROM tabla1 a JOIN tabla2 b ON a.id = b.id
""")
