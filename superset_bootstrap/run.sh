#!/usr/bin/env bash
set -euo pipefail

echo "== BOOTSTRAP =="

echo "[ls /bootstrap]"
ls -l /bootstrap || true

echo "[superset --version]"
/app/.venv/bin/superset --version

# 1) Migraciones + init (idempotente)
echo "[db upgrade + init]"
/app/.venv/bin/superset db upgrade
/app/.venv/bin/superset init

# 2) Prepara YAML (si no existe en HOME, cópialo desde /bootstrap)
/bin/test -f /app/superset_home/clickhouse_db.yaml || \
  cp -n /bootstrap/clickhouse_db.yaml /app/superset_home/clickhouse_db.yaml

# 3) Importa datasources
echo "[import-datasources]"
/app/.venv/bin/superset import-datasources \
  -p /app/superset_home/clickhouse_db.yaml \
  -u "${SUPERSET_ADMIN:-admin}"

echo "BOOTSTRAP_OK"
