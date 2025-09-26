#!/usr/bin/env bash
set -euo pipefail

python -m venv /app/.venv
/app/.venv/bin/python -m ensurepip --upgrade
/app/.venv/bin/python -m pip install --upgrade pip setuptools wheel
/app/.venv/bin/pip install clickhouse-connect==0.7.19 clickhouse-sqlalchemy==0.2.6
chown -R 1000:1000 /app/.venv

/app/.venv/bin/python - <<'PY'
import clickhouse_connect, sqlalchemy
print("OK drivers")
PY

echo "VENV_OK"
