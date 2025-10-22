#!/bin/bash
# Script de verificación automática de Superset
# Verifica admin, conexiones, datasets y guarda resultados en logs

set -e

TIMESTAMP=$(date '+%Y-%m-%d_%H-%M-%S')
LOG_DIR="${LOG_DIR:-/app/logs}"
OUTPUT_FILE="$LOG_DIR/superset_verify_${TIMESTAMP}.log"
JSON_FILE="$LOG_DIR/superset_verify_${TIMESTAMP}.json"

# Configuración desde variables de entorno
SUPERSET_URL="${SUPERSET_URL:-http://superset:8088}"
ADMIN_USER="${SUPERSET_ADMIN:-admin}"
ADMIN_PASS="${SUPERSET_PASSWORD:-Admin123!}"

echo "🔍 VERIFICACIÓN DE SUPERSET" | tee "$OUTPUT_FILE"
echo "========================================" | tee -a "$OUTPUT_FILE"
echo "⏰ Timestamp: $(date -Iseconds)" | tee -a "$OUTPUT_FILE"
echo "🌐 URL: $SUPERSET_URL" | tee -a "$OUTPUT_FILE"
echo "👤 Usuario: $ADMIN_USER" | tee -a "$OUTPUT_FILE"
echo "" | tee -a "$OUTPUT_FILE"

# Crear script Python inline para verificaciones
python3 << 'PYTHON_SCRIPT' | tee -a "$OUTPUT_FILE"
import requests
import json
import os
import sys
from datetime import datetime

SUPERSET_URL = os.getenv('SUPERSET_URL', 'http://superset:8088')
ADMIN_USER = os.getenv('SUPERSET_ADMIN', 'admin')
ADMIN_PASS = os.getenv('SUPERSET_PASSWORD', 'Admin123!')
LOG_DIR = os.getenv('LOG_DIR', '/app/logs')
JSON_FILE = os.getenv('JSON_FILE')

results = {
    "timestamp": datetime.now().isoformat(),
    "url": SUPERSET_URL,
    "checks": {}
}

def log(msg, level="INFO"):
    print(f"[{level}] {msg}")

# 1. Verificar disponibilidad
log("📡 1. VERIFICANDO DISPONIBILIDAD...")
try:
    resp = requests.get(f"{SUPERSET_URL}/health", timeout=10)
    if resp.status_code == 200:
        log("   ✅ Superset está disponible", "SUCCESS")
        results["checks"]["availability"] = {"status": "ok", "code": 200}
    else:
        log(f"   ⚠️  Superset responde pero con código {resp.status_code}", "WARNING")
        results["checks"]["availability"] = {"status": "warning", "code": resp.status_code}
except Exception as e:
    log(f"   ❌ Superset no disponible: {e}", "ERROR")
    results["checks"]["availability"] = {"status": "error", "message": str(e)}
    sys.exit(1)

print()

# 2. Verificar autenticación
log("🔐 2. VERIFICANDO AUTENTICACIÓN...")
token = None
try:
    login_resp = requests.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json={"username": ADMIN_USER, "password": ADMIN_PASS, "provider": "db", "refresh": True},
        headers={"Content-Type": "application/json"},
        timeout=10
    )
    if login_resp.status_code == 200:
        token = login_resp.json().get("access_token")
        if token:
            log(f"   ✅ Autenticación exitosa (token: {token[:20]}...)", "SUCCESS")
            results["checks"]["authentication"] = {"status": "ok", "user": ADMIN_USER}
        else:
            log("   ⚠️  Login OK pero sin token", "WARNING")
            results["checks"]["authentication"] = {"status": "warning", "message": "no token"}
    else:
        log(f"   ❌ Autenticación falló: {login_resp.status_code}", "ERROR")
        log(f"      Response: {login_resp.text[:200]}")
        results["checks"]["authentication"] = {"status": "error", "code": login_resp.status_code}
        sys.exit(1)
except Exception as e:
    log(f"   ❌ Error en autenticación: {e}", "ERROR")
    results["checks"]["authentication"] = {"status": "error", "message": str(e)}
    sys.exit(1)

print()

if not token:
    log("❌ No se pudo obtener token, abortando verificaciones", "ERROR")
    sys.exit(1)

headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# 3. Verificar bases de datos configuradas
log("📊 3. VERIFICANDO BASES DE DATOS CONFIGURADAS...")
try:
    db_resp = requests.get(f"{SUPERSET_URL}/api/v1/database/", headers=headers, timeout=10)
    if db_resp.status_code == 200:
        databases = db_resp.json().get("result", [])
        log(f"   ✅ Se encontraron {len(databases)} bases de datos", "SUCCESS")
        results["checks"]["databases"] = {"status": "ok", "count": len(databases), "list": []}
        for db in databases:
            db_name = db.get("database_name", "")
            db_id = db.get("id", 0)
            log(f"      - {db_name} (ID: {db_id})")
            results["checks"]["databases"]["list"].append({"name": db_name, "id": db_id})
    else:
        log(f"   ⚠️  Error obteniendo bases de datos: {db_resp.status_code}", "WARNING")
        results["checks"]["databases"] = {"status": "error", "code": db_resp.status_code}
except Exception as e:
    log(f"   ❌ Error consultando bases de datos: {e}", "ERROR")
    results["checks"]["databases"] = {"status": "error", "message": str(e)}

print()

# 4. Verificar datasets configurados
log("📋 4. VERIFICANDO DATASETS CONFIGURADOS...")
try:
    ds_resp = requests.get(f"{SUPERSET_URL}/api/v1/dataset/", headers=headers, timeout=10)
    if ds_resp.status_code == 200:
        datasets = ds_resp.json().get("result", [])
        log(f"   ✅ Se encontraron {len(datasets)} datasets", "SUCCESS")
        results["checks"]["datasets"] = {"status": "ok", "count": len(datasets), "list": []}
        for ds in datasets[:20]:  # Mostrar primeros 20
            schema = ds.get("schema", "")
            table = ds.get("table_name", "")
            ds_id = ds.get("id", 0)
            log(f"      - {schema}.{table} (ID: {ds_id})")
            results["checks"]["datasets"]["list"].append({
                "id": ds_id,
                "schema": schema,
                "table": table
            })
        if len(datasets) > 20:
            log(f"      ... y {len(datasets) - 20} más")
    else:
        log(f"   ⚠️  Error obteniendo datasets: {ds_resp.status_code}", "WARNING")
        results["checks"]["datasets"] = {"status": "error", "code": ds_resp.status_code}
except Exception as e:
    log(f"   ❌ Error consultando datasets: {e}", "ERROR")
    results["checks"]["datasets"] = {"status": "error", "message": str(e)}

print()

# 5. Verificar dataset esperado (test_table)
log("🎯 5. VERIFICANDO DATASET ESPERADO (test_table)...")
try:
    # Buscar test_table en fgeo_analytics
    found_test_table = False
    for ds in datasets:
        if ds.get("table_name") == "test_table" and ds.get("schema") == "fgeo_analytics":
            found_test_table = True
            log(f"   ✅ Dataset test_table encontrado (ID: {ds.get('id')})", "SUCCESS")
            results["checks"]["expected_dataset"] = {"status": "ok", "id": ds.get("id")}
            break
    
    if not found_test_table:
        log("   ⚠️  Dataset test_table NO encontrado", "WARNING")
        results["checks"]["expected_dataset"] = {"status": "warning", "message": "not found"}
except Exception as e:
    log(f"   ❌ Error verificando dataset esperado: {e}", "ERROR")
    results["checks"]["expected_dataset"] = {"status": "error", "message": str(e)}

print()

# Guardar resultados en JSON
log("📄 Guardando resultados en JSON...")
results["status"] = "completed"
try:
    with open(JSON_FILE, 'w') as f:
        json.dump(results, f, indent=2)
    log(f"   ✅ JSON guardado en: {JSON_FILE}", "SUCCESS")
except Exception as e:
    log(f"   ⚠️  Error guardando JSON: {e}", "WARNING")

# Resumen final
print()
print("========================================")
log("✅ VERIFICACIÓN COMPLETADA", "SUCCESS")

# Determinar estado general
all_ok = all(
    check.get("status") == "ok" 
    for check in results["checks"].values()
)

if all_ok:
    log("🎉 Todos los checks pasaron exitosamente", "SUCCESS")
    sys.exit(0)
else:
    log("⚠️  Algunos checks fallaron - revisar detalles arriba", "WARNING")
    sys.exit(1)

PYTHON_SCRIPT

EXIT_CODE=$?

echo "" | tee -a "$OUTPUT_FILE"
echo "========================================" | tee -a "$OUTPUT_FILE"
echo "📄 Log guardado en: $OUTPUT_FILE" | tee -a "$OUTPUT_FILE"
echo "📊 JSON guardado en: $JSON_FILE" | tee -a "$OUTPUT_FILE"

# Crear symlink al último reporte
ln -sf "$(basename "$OUTPUT_FILE")" "$LOG_DIR/superset_verify_latest.log"
ln -sf "$(basename "$JSON_FILE")" "$LOG_DIR/superset_verify_latest.json"

exit $EXIT_CODE
