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
docker compose run --rm configurator python tools/gen_pipeline.py
sleep 2
docker compose run --rm configurator bash generated/default/ch_create_raw_pipeline.sh
sleep 2

# 5. Comprobar tablas en ClickHouse
printf "\n[ETL] Tablas en ClickHouse (fgeo_analytics):\n"
docker exec clickhouse clickhouse-client -q "SHOW TABLES FROM fgeo_analytics"

# 6. Comprobar filas en una tabla (si existe)
TABLA=$(docker exec clickhouse clickhouse-client -q "SHOW TABLES FROM fgeo_analytics" | head -n 1)
if [ -n "$TABLA" ]; then
  printf "\n[ETL] SELECT count() FROM fgeo_analytics.$TABLA:\n"
  docker exec clickhouse clickhouse-client -q "SELECT count() FROM fgeo_analytics.$TABLA"
else
  printf "\n[ETL] No se encontraron tablas en fgeo_analytics.\n"
fi

# 7. Comprobar datasets en Superset
printf "\n[ETL] Exportando datasets de Superset...\n"
docker exec superset superset export-datasources -f /app/superset_home/exported_dbs.zip

docker cp superset:/app/superset_home/exported_dbs.zip ./superset_bootstrap/exported_dbs.zip
unzip -l ./superset_bootstrap/exported_dbs.zip

printf "\n[ETL] Proceso completo. Revisa los resultados arriba.\n"
