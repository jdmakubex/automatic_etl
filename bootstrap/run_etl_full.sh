#!/usr/bin/env bash
set -euo pipefail

# Script maestro para lanzar todo el pipeline ETL y validar cada paso
# Mensajes y comprobaciones en español




# 1. Limpieza y preparación
printf "\n[ETL] Limpiando contenedores, volúmenes y red...\n" | tee -a logs/etl_full.log
docker compose down -v | tee -a logs/etl_full.log || true
# Elimina todos los contenedores activos (forzado)
docker ps -aq | xargs -r docker rm -f | tee -a logs/etl_full.log || true
docker network rm etl_prod_etl_net | tee -a logs/etl_full.log || true
docker network create etl_prod_etl_net | tee -a logs/etl_full.log || true
docker volume prune -f | tee -a logs/etl_full.log || true

# 2. Construcción de imagen
echo "[ETL] Construyendo imagen pipeline-gen con dependencias actualizadas..." | tee -a logs/etl_full.log
docker compose build pipeline-gen | tee -a logs/etl_full.log

# 3. Lanzar servicios base
printf "\n[ETL] Lanzando servicios principales...\n" | tee -a logs/etl_full.log
docker compose up -d clickhouse superset superset-venv-setup superset-init kafka connect | tee -a logs/etl_full.log
sleep 10

# 4. Comprobar estado de servicios
printf "\n[ETL] Comprobando estado de servicios...\n" | tee -a logs/etl_full.log

# Validar salud de servicios principales
printf "\n[ETL] Verificando salud de servicios principales...\n" | tee -a logs/etl_full.log
for SVC in clickhouse superset kafka connect pipeline-gen etl-tools configurator cdc-bootstrap cdc superset-venv-setup; do
  STATUS=$(docker inspect --format='{{.State.Health.Status}}' $SVC 2>/dev/null || echo "no-healthcheck")
  echo "[ETL] Servicio $SVC: Estado de salud = $STATUS" | tee -a logs/etl_full.log
done

docker ps --format 'table {{.Names}}\t{{.Status}}' | tee -a logs/etl_full.log

# 2. Lanzar servicios base
printf "\n[ETL] Lanzando servicios principales...\n"
docker compose up -d clickhouse superset superset-venv-setup superset-init kafka connect
sleep 10

# 3. Comprobar estado de servicios
printf "\n[ETL] Comprobando estado de servicios...\n"
docker ps --format 'table {{.Names}}\t{{.Status}}'


# 4. Ejecutar pipeline de ingesta y creación de tablas
printf "\n[ETL] Ejecutando pipeline de ingesta y creación de tablas...\n"




echo "[ETL] Verificando archivos clave en /app/tools dentro de pipeline-gen..." | tee -a logs/etl_full.log
docker compose run --rm pipeline-gen bash -c 'ls -l /app/tools && [ -f /app/tools/gen_pipeline.py ] && [ -f /app/tools/requirements.txt ] && [ -f /app/tools/ingest_runner.py ]' | tee -a logs/etl_full.log
if [ $? -eq 0 ]; then
  echo "[ETL] ✔ Archivos clave presentes en /app/tools dentro de pipeline-gen." | tee -a logs/etl_full.log
else
  echo "[ETL] [ERROR] Faltan archivos clave en /app/tools dentro de pipeline-gen. Abortando." | tee -a logs/etl_full.log
  exit 1
fi

# Mostrar contenido de /app y /app/tools en etl-tools
echo "[ETL] Contenido de /app en etl-tools:" | tee -a logs/etl_full.log
docker compose run --rm etl-tools ls -l /app | tee -a logs/etl_full.log
echo "[ETL] Contenido de /app/tools en etl-tools:" | tee -a logs/etl_full.log
docker compose run --rm etl-tools ls -l /app/tools | tee -a logs/etl_full.log




# Copia y verificación automática de todos los archivos y subcarpetas de tools/ en todos los contenedores relevantes



# Con volumen compartido, no es necesario copiar archivos manualmente.
printf "\n[ETL] Todos los contenedores relevantes ya tienen acceso a los scripts y archivos de tools/ mediante el volumen compartido.\n" | tee -a logs/etl_full.log





printf "\n[ETL] Generando pipeline y creando tablas en ClickHouse...\n" | tee -a logs/etl_full.log

# Validar sintaxis de gen_pipeline.py
printf "[ETL] Validando sintaxis de gen_pipeline.py...\n" | tee -a logs/etl_full.log
docker compose run --rm pipeline-gen python3 -m py_compile /app/tools/gen_pipeline.py | tee -a logs/etl_full.log && \
  echo "[ETL] ✔ Sintaxis válida en gen_pipeline.py" | tee -a logs/etl_full.log || { echo "[ETL] [ERROR] Error de sintaxis en gen_pipeline.py" | tee -a logs/etl_full.log; exit 1; }

# Verificar dependencias Python
printf "[ETL] Verificando dependencias Python (pymysql, dotenv)...\n" | tee -a logs/etl_full.log
docker compose run --rm pipeline-gen python3 -c "import pymysql, dotenv" | tee -a logs/etl_full.log && \
  echo "[ETL] ✔ Dependencias presentes" | tee -a logs/etl_full.log || { echo "[ETL] [ERROR] Faltan dependencias Python" | tee -a logs/etl_full.log; exit 1; }

# Validar variables de entorno críticas
printf "[ETL] Verificando variables de entorno críticas...\n" | tee -a logs/etl_full.log
if [ -z "${DB_CONNECTIONS:-}" ]; then
  echo "[ETL] [ERROR] La variable DB_CONNECTIONS no está definida en .env. Abortando." | tee -a logs/etl_full.log; exit 1;
else
  echo "[ETL] ✔ DB_CONNECTIONS definida" | tee -a logs/etl_full.log
fi

# Ejecutar gen_pipeline.py con control de errores y log
printf "[ETL] Ejecutando gen_pipeline.py...\n" | tee -a logs/etl_full.log
if docker compose run --rm pipeline-gen python3 -u /app/tools/gen_pipeline.py | tee -a logs/etl_full.log; then
  echo "[ETL] ✔ gen_pipeline.py ejecutado correctamente." | tee -a logs/etl_full.log
else
  echo "[ETL] [ERROR] Falló la ejecución de gen_pipeline.py." | tee -a logs/etl_full.log; exit 1;
fi

# Validar archivo generado de script de ClickHouse
printf "[ETL] Validando existencia de ch_create_raw_pipeline.sh...\n" | tee -a logs/etl_full.log
if docker compose run --rm pipeline-gen bash -c "test -f /app/generated/default/ch_create_raw_pipeline.sh" | tee -a logs/etl_full.log; then
  echo "[ETL] ✔ ch_create_raw_pipeline.sh generado correctamente." | tee -a logs/etl_full.log
else
  echo "[ETL] [ERROR] No se generó ch_create_raw_pipeline.sh. Abortando." | tee -a logs/etl_full.log; exit 1;
fi

# Ejecutar script de creación de tablas en ClickHouse
printf "[ETL] Ejecutando script de creación de tablas en ClickHouse...\n" | tee -a logs/etl_full.log
docker compose run --rm pipeline-gen bash /app/generated/default/ch_create_raw_pipeline.sh | tee -a logs/etl_full.log

# Ejecutar ingesta de datos en ClickHouse
echo "[ETL] Ejecutando ingesta de datos en ClickHouse..." | tee -a logs/etl_full.log
docker compose run --rm etl-tools python tools/ingest_runner.py --source-url=mysql+pymysql://juan.marcos:123456@172.21.61.53:3306/archivos --ch-database=fgeo_analytics --ch-prefix=src__default__ --schemas archivos --chunksize 50000 --truncate-before-load --dedup none | tee -a logs/etl_full.log

# Validar tablas y filas en ClickHouse
printf "\n[ETL] Tablas en ClickHouse (fgeo_analytics):\n" | tee -a logs/etl_full.log
docker exec clickhouse clickhouse-client -q "SHOW TABLES FROM fgeo_analytics" | tee -a logs/etl_full.log

TABLA=$(docker exec clickhouse clickhouse-client -q "SHOW TABLES FROM fgeo_analytics" | head -n 1)
if [ -n "$TABLA" ]; then
  printf "\n[ETL] SELECT count() FROM fgeo_analytics.$TABLA:\n" | tee -a logs/etl_full.log
  docker exec clickhouse clickhouse-client -q "SELECT count() FROM fgeo_analytics.$TABLA" | tee -a logs/etl_full.log
else
  printf "\n[ETL] No se encontraron tablas en fgeo_analytics.\n" | tee -a logs/etl_full.log
fi

# Exportar y listar datasets de Superset
printf "\n[ETL] Exportando datasets de Superset...\n" | tee -a logs/etl_full.log
docker exec superset superset export-datasources -f /app/superset_home/exported_dbs.zip | tee -a logs/etl_full.log
docker cp superset:/app/superset_home/exported_dbs.zip ./superset_bootstrap/exported_dbs.zip | tee -a logs/etl_full.log
unzip -l ./superset_bootstrap/exported_dbs.zip | tee -a logs/etl_full.log

printf "\n[ETL] Proceso completo. Revisa los resultados arriba y en logs/etl_full.log.\n" | tee -a logs/etl_full.log
