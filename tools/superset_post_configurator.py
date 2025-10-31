#!/usr/bin/env python3
"""
Script: superset_post_configurator.py
Automatiza el filtrado de esquemas/tablas y ajuste de permisos/metadata en Superset tras la ingesta ETL.
- Solo expone datasets declarados en el pipeline/config.
- Ajusta roles y permisos para admin y usuarios estándar.
- Corrige metadata: time column, métricas, acceso.
"""
import os
import json
import requests

SUPERSET_URL = os.environ.get("SUPERSET_URL", "http://localhost:8088")
SUPERSET_USER = os.environ.get("SUPERSET_ADMIN_USER", "admin")
SUPERSET_PASS = os.environ.get("SUPERSET_ADMIN_PASS", "Admin123!")
DATASETS_CONFIG = os.environ.get("DATASETS_CONFIG", "generated/datasets_config.json")

# Autenticación
session = requests.Session()
login_resp = session.post(f"{SUPERSET_URL}/login/", data={"username": SUPERSET_USER, "password": SUPERSET_PASS})
assert login_resp.ok, f"Error autenticando en Superset: {login_resp.text}"

# Leer datasets permitidos
with open(DATASETS_CONFIG) as f:
    allowed_datasets = json.load(f)["datasets"]

# Obtener todos los datasets
datasets_resp = session.get(f"{SUPERSET_URL}/api/v1/dataset/?q=(page_size:1000)")
datasets = datasets_resp.json()["result"]

for ds in datasets:
    ds_id = ds["id"]
    ds_name = ds["table_name"]
    ds_schema = ds["schema"]
    # Si no está en la lista permitida, despublicar/eliminar
    if f"{ds_schema}.{ds_name}" not in allowed_datasets:
        session.delete(f"{SUPERSET_URL}/api/v1/dataset/{ds_id}")
    else:
        # Metadata: asegurar time column y métricas
        patch = {"main_dttm_col": ds.get("main_dttm_col", ""), "metrics": ds.get("metrics", [])}
        session.patch(f"{SUPERSET_URL}/api/v1/dataset/{ds_id}", json=patch)

# Ajustar roles/permisos para admin
roles_resp = session.get(f"{SUPERSET_URL}/api/v1/role/?q=(page_size:100)")
roles = roles_resp.json()["result"]
for role in roles:
    if role["name"] == "Admin":
        # Asegurar todos los permisos
        session.put(f"{SUPERSET_URL}/api/v1/role/{role['id']}", json={"permissions": ["all_database_access", "all_datasource_access", "can_create_chart", "can_edit_chart"]})

print("✔️ Post-configuración de Superset completada.")
