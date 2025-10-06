#!/usr/bin/env bash
set -euo pipefail

# Script maestro para lanzar todo el pipeline ETL y validar cada paso
# Mensajes y comprobaciones en español



# 1. Limpieza y preparación
printf "\n[ETL] Limpiando contenedores, volúmenes y red...\n"
docker compose down -v || true
# Elimina todos los contenedores activos (forzado)
docker ps -aq | xargs -r docker rm -f || true
docker network rm etl_prod_etl_net || true
docker network create etl_prod_etl_net || true
docker volume prune -f || true

# 2. Lanzar servicios base
printf "\n[ETL] Lanzando servicios principales...\n"
docker compose up -d clickhouse superset superset-venv-setup superset-init kafka connect
sleep 10

# 3. Comprobar estado de servicios
printf "\n[ETL] Comprobando estado de servicios...\n"
docker ps --format 'table {{.Names}}\t{{.Status}}'


# 4. Ejecutar pipeline de ingesta y creación de tablas
printf "\n[ETL] Ejecutando pipeline de ingesta y creación de tablas...\n"



# Mostrar contenido de /app y /app/tools en pipeline-gen
echo "[ETL] Contenido de /app en pipeline-gen:"
docker compose run --rm pipeline-gen ls -l /app
echo "[ETL] Contenido de /app/tools en pipeline-gen:"
docker compose run --rm pipeline-gen ls -l /app/tools

# Mostrar contenido de /app y /app/tools en etl-tools
echo "[ETL] Contenido de /app en etl-tools:"
docker compose run --rm etl-tools ls -l /app
echo "[ETL] Contenido de /app/tools en etl-tools:"
docker compose run --rm etl-tools ls -l /app/tools

# Verificar existencia y copiar si falta

echo "[ETL] Verificando existencia de /app/tools/gen_pipeline.py en pipeline-gen..."
docker compose run --rm -T pipeline-gen bash -c "if [ -f /app/tools/gen_pipeline.py ]; then echo '[OK] gen_pipeline.py encontrado.'; else echo '[ERROR] gen_pipeline.py NO encontrado. Copiando...'; cp /app/tools/gen_pipeline.py /app/tools/gen_pipeline.py; fi"

echo "[ETL] Generando pipeline y creando tablas en ClickHouse..."
docker compose run --rm pipeline-gen python /app/tools/gen_pipeline.py
docker compose run --rm pipeline-gen bash /app/generated/default/ch_create_raw_pipeline.sh

# Ejecutar ingesta de datos en ClickHouse
echo "[ETL] Ejecutando ingesta de datos en ClickHouse..."
docker compose run --rm etl-tools python tools/ingest_runner.py --source-url=mysql+pymysql://juan.marcos:123456@172.21.61.53:3306/archivos --ch-database=fgeo_analytics --ch-prefix=src__default__ --schemas archivos --chunksize 50000 --truncate-before-load --dedup none

# Validar tablas y filas en ClickHouse
printf "\n[ETL] Tablas en ClickHouse (fgeo_analytics):\n"
docker exec clickhouse clickhouse-client -q "SHOW TABLES FROM fgeo_analytics"

TABLA=$(docker exec clickhouse clickhouse-client -q "SHOW TABLES FROM fgeo_analytics" | head -n 1)
if [ -n "$TABLA" ]; then
  printf "\n[ETL] SELECT count() FROM fgeo_analytics.$TABLA:\n"
  docker exec clickhouse clickhouse-client -q "SELECT count() FROM fgeo_analytics.$TABLA"
else
  printf "\n[ETL] No se encontraron tablas en fgeo_analytics.\n"
fi

# Exportar y listar datasets de Superset
printf "\n[ETL] Exportando datasets de Superset...\n"
docker exec superset superset export-datasources -f /app/superset_home/exported_dbs.zip
docker cp superset:/app/superset_home/exported_dbs.zip ./superset_bootstrap/exported_dbs.zip
unzip -l ./superset_bootstrap/exported_dbs.zip

printf "\n[ETL] Proceso completo. Revisa los resultados arriba.\n"
