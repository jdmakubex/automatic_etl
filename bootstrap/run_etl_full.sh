#!/usr/bin/env bash
set -euo pipefail

# Script maestro para lanzar todo el pipeline ETL y validar cada paso
# Mensajes y comprobaciones en español

# Función para logging con timestamp y nivel
log_info() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] [INFO] $*" | tee -a logs/etl_full.log
}

log_error() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] [ERROR] $*" | tee -a logs/etl_full.log >&2
}

log_warning() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] [WARNING] $*" | tee -a logs/etl_full.log
}

# Función para manejo de errores fatales
handle_fatal_error() {
    log_error "Error fatal: $1"
    log_error "Consulta docs/ERROR_RECOVERY.md para soluciones"
    exit 1
}

# Función para manejo de errores recuperables
handle_recoverable_error() {
    log_warning "Error recuperable: $1"
    log_warning "Intentando continuar..."
    return 0
}

# Crear directorio de logs si no existe
mkdir -p logs

log_info "=== Iniciando pipeline ETL completo ==="

# Validar variables de entorno críticas antes de comenzar
log_info "Validando variables de entorno..."
if [ -z "${DB_CONNECTIONS:-}" ]; then
  handle_fatal_error "La variable DB_CONNECTIONS no está definida en .env. Abortando."
fi
log_info "✓ DB_CONNECTIONS definida"

# Validar que existe archivo .env
if [ ! -f .env ]; then
  log_warning "Archivo .env no encontrado. Usando valores por defecto."
else
  log_info "✓ Archivo .env encontrado"
fi

# NUEVO: Ejecutar validaciones completas si están habilitadas
ENABLE_VALIDATION=${ENABLE_VALIDATION:-true}
if [ "$ENABLE_VALIDATION" = "true" ]; then
  log_info "Ejecutando validaciones de entorno y dependencias..."
  if docker compose run --rm etl-tools python tools/validators.py 2>&1 | tee -a logs/etl_full.log; then
    log_info "✓ Validaciones de entorno pasaron"
  else
    handle_recoverable_error "Algunas validaciones fallaron, pero continuando..."
  fi
else
  log_info "⊝ Validaciones deshabilitadas (ENABLE_VALIDATION=false)"
fi


# 1. Limpieza y preparación
log_info "Limpiando contenedores, volúmenes y red..."
docker compose down -v 2>&1 | tee -a logs/etl_full.log || handle_recoverable_error "docker compose down falló"
# Elimina todos los contenedores activos (forzado)
docker ps -aq | xargs -r docker rm -f 2>&1 | tee -a logs/etl_full.log || handle_recoverable_error "No hay contenedores para eliminar"
docker network rm etl_prod_etl_net 2>&1 | tee -a logs/etl_full.log || handle_recoverable_error "Red no existe"
docker network create etl_prod_etl_net 2>&1 | tee -a logs/etl_full.log || handle_fatal_error "No se pudo crear la red"
docker volume prune -f 2>&1 | tee -a logs/etl_full.log || handle_recoverable_error "No hay volúmenes para limpiar"
log_info "✓ Limpieza completada"

# 2. Construcción de imagen
log_info "Construyendo imagen pipeline-gen con dependencias actualizadas..."
if ! docker compose build pipeline-gen 2>&1 | tee -a logs/etl_full.log; then
  handle_fatal_error "No se pudo construir la imagen pipeline-gen"
fi
log_info "✓ Imagen construida exitosamente"

# 3. Lanzar servicios base
log_info "Lanzando servicios principales..."
if ! docker compose up -d clickhouse superset superset-venv-setup superset-init kafka connect 2>&1 | tee -a logs/etl_full.log; then
  handle_fatal_error "No se pudieron iniciar los servicios principales"
fi
log_info "Esperando 10 segundos para que los servicios inicien..."
sleep 10
log_info "✓ Servicios principales iniciados"

# 4. Comprobar estado de servicios
log_info "Comprobando estado de servicios..."

# Validar salud de servicios principales
log_info "Verificando salud de servicios principales..."
UNHEALTHY_SERVICES=()
for SVC in clickhouse superset kafka connect pipeline-gen etl-tools configurator cdc-bootstrap cdc superset-venv-setup; do
  STATUS=$(docker inspect --format='{{.State.Health.Status}}' $SVC 2>/dev/null || echo "no-healthcheck")
  if [ "$STATUS" = "unhealthy" ]; then
    UNHEALTHY_SERVICES+=("$SVC")
    log_error "Servicio $SVC: Estado de salud = $STATUS"
  else
    log_info "Servicio $SVC: Estado de salud = $STATUS"
  fi
done

# Si hay servicios no saludables, advertir pero continuar
if [ ${#UNHEALTHY_SERVICES[@]} -gt 0 ]; then
  log_warning "Servicios no saludables detectados: ${UNHEALTHY_SERVICES[*]}"
  log_warning "Continuando de todas formas..."
fi

docker ps --format 'table {{.Names}}\t{{.Status}}' | tee -a logs/etl_full.log
log_info "✓ Verificación de servicios completada"

# NUEVO: Verificar dependencias y esperar a que servicios estén listos
ENABLE_DEPENDENCY_VERIFICATION=${ENABLE_DEPENDENCY_VERIFICATION:-true}
if [ "$ENABLE_DEPENDENCY_VERIFICATION" = "true" ]; then
  log_info "Verificando que servicios estén listos..."
  if docker compose run --rm etl-tools python tools/verify_dependencies.py 2>&1 | tee -a logs/etl_full.log; then
    log_info "✓ Todos los servicios están listos"
  else
    handle_recoverable_error "Algunos servicios no están completamente listos, pero continuando..."
  fi
else
  log_info "⊝ Verificación de dependencias deshabilitada (ENABLE_DEPENDENCY_VERIFICATION=false)"
fi

# NUEVO: Probar permisos antes de iniciar ingesta
ENABLE_PERMISSION_TESTS=${ENABLE_PERMISSION_TESTS:-false}
if [ "$ENABLE_PERMISSION_TESTS" = "true" ]; then
  log_info "Ejecutando pruebas de permisos..."
  if docker compose run --rm etl-tools python tools/test_permissions.py 2>&1 | tee -a logs/etl_full.log; then
    log_info "✓ Todas las pruebas de permisos pasaron"
  else
    log_warning "⚠ Algunas pruebas de permisos fallaron (puede ser normal si CDC no está configurado)"
  fi
else
  log_info "⊝ Pruebas de permisos deshabilitadas (ENABLE_PERMISSION_TESTS=false)"
fi

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
log_info "Exportando datasets de Superset..."
if docker exec superset superset export-datasources -f /app/superset_home/exported_dbs.zip 2>&1 | tee -a logs/etl_full.log; then
  docker cp superset:/app/superset_home/exported_dbs.zip ./superset_bootstrap/exported_dbs.zip 2>&1 | tee -a logs/etl_full.log || handle_recoverable_error "No se pudo copiar archivo exportado"
  unzip -l ./superset_bootstrap/exported_dbs.zip 2>&1 | tee -a logs/etl_full.log || handle_recoverable_error "No se pudo listar contenido del archivo"
  log_info "✓ Datasets exportados correctamente"
else
  handle_recoverable_error "No se pudo exportar datasets de Superset"
fi

# Ejecutar validaciones automáticas
log_info ""
log_info "=== Ejecutando validaciones automáticas ==="

# Validar ClickHouse
log_info "Validando ClickHouse..."
if docker compose run --rm etl-tools python tools/validate_clickhouse.py 2>&1 | tee -a logs/etl_full.log; then
  log_info "✓ Validación de ClickHouse exitosa"
else
  log_warning "⚠ Validación de ClickHouse falló (ver logs/clickhouse_validation.json)"
fi

# Validar Superset
log_info "Validando Superset..."
if docker compose run --rm configurator python tools/validate_superset.py 2>&1 | tee -a logs/etl_full.log; then
  log_info "✓ Validación de Superset exitosa"
else
  log_warning "⚠ Validación de Superset falló (ver logs/superset_validation.json)"
fi

log_info ""
log_info "=== Proceso completo ==="
log_info "Revisa los resultados arriba y en logs/etl_full.log"
log_info "Para solucionar problemas, consulta: docs/ERROR_RECOVERY.md"
log_info ""
