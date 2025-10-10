#!/usr/bin/env bash

# Cargar variables de entorno desde .env si existe
if [ -f .env ]; then
	export $(grep -v '^#' .env | xargs)
	echo "[ETL] Variables de entorno cargadas desde .env"
else
	echo "[ETL] Advertencia: No se encontró archivo .env, algunas variables pueden faltar."
fi

set -euo pipefail

python -m venv /app/.venv
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
