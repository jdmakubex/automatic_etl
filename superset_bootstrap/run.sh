#!/usr/bin/env bash

set -euo pipefail


echo "== BOOTSTRAP =="


echo "[ls /bootstrap]"
ls -l /bootstrap || true


echo "[superset --version]"
/app/.venv/bin/superset --version


# 0) Limpia la base de datos de Superset si existe (para evitar problemas de cifrado)
echo "[limpiando superset.db]"
rm -f /app/superset_home/superset.db

# 1) Migraciones + init (idempotente)
echo "[db upgrade + init]"
/app/.venv/bin/superset db upgrade
/app/.venv/bin/superset init


# 2) Prepara ZIP (si no existe en HOME, cópialo desde /bootstrap)
/bin/test -f /app/superset_home/clickhouse_db.zip || \
  cp -n /bootstrap/clickhouse_db.zip /app/superset_home/clickhouse_db.zip


# 3) Importa datasources
echo "[import-datasources]"
/app/.venv/bin/superset import-datasources \
  -p /app/superset_home/clickhouse_db.zip


echo "BOOTSTRAP_OK"
