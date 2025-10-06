#!/usr/bin/env bash
set -euo pipefail

# Instala dependencias si faltan
python -m pip install --no-cache-dir -q python-dotenv requests clickhouse-connect SQLAlchemy PyMySQL

# Ejecuta el script principal
python /app/tools/render_from_env.py

# Si ocurre un error, aborta y nunca lanza el intérprete interactivo
exit $?