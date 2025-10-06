#!/usr/bin/env bash
set -euo pipefail
export PATH="/app/.venv/bin:${PATH}"

echo ">> Configurando CH fgeo_archivos"
superset set-database-uri --database-name "CH fgeo_archivos" --uri "clickhousedb+connect://etl:Et1Ingest!@clickhouse:8123/fgeo_archivos" --extra "{}" --allow-csv-upload false || true
