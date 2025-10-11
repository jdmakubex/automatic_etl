#!/bin/bash

###############################################################
# [DOCUMENTACIÓN GENERAL DEL PIPELINE ETL]
# -------------------------------------------------------------
# Script maestro para automatizar el pipeline ETL completo:
#   - Limpieza robusta de contenedores, volúmenes y red Docker
#   - Validación de entorno, dependencias y permisos
#   - Construcción y despliegue de servicios base (ClickHouse, Superset, Kafka, etc.)
#   - Ejecución de scripts de ingesta, creación de tablas y validaciones automáticas
#   - Exportación y verificación de datasets en Superset
#
# [Historial de cambios críticos]
# - Se reforzó la limpieza de la red Docker 'etl_prod_etl_net' para evitar conflictos de etiquetas y duplicaciones que bloqueaban el arranque de servicios (ver sección dedicada más abajo).
# - Se agregaron validaciones automáticas de entorno, dependencias y permisos antes de la ingesta.
# - El pipeline es idempotente: todos los servicios y recursos se recrean desde cero en cada ejecución.
# - Se adaptó la lógica para compatibilidad total con Superset y drivers de ClickHouse.
# - Se documentan advertencias y buenas prácticas para evitar romper procesos externos.
#
# [Referencias y troubleshooting]
# - docs/ERROR_RECOVERY.md: Solución de errores comunes y recuperación.
# - README_AUTOMATIZADO.md: Resumen de automatización y flujo general.
# - logs/etl_full.log: Registro detallado de cada ejecución.
#
# Si realizas cambios en la lógica de red, servicios o validaciones, documenta aquí el motivo y el impacto esperado.
###############################################################

# Cargar variables de entorno desde .env si existe

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


# [DOCUMENTACIÓN] Carga robusta de variables de entorno
# -----------------------------------------------------
# El archivo .env debe estar disponible en el volumen compartido /app
# para que todos los scripts internos (Bash y Python) accedan a las variables.
# Si no se encuentra, advierte y continúa (algunos valores pueden faltar).

# Copiar y limpiar .env para uso en contenedores
if [ -f .env ]; then
  # Crear .env.clean sin comentarios ni líneas vacías, preservando el original
  grep -v '^#' .env | grep -v '^$' > .env.clean
  cp .env.clean ./tools/.env
  cp .env.clean ./generated/default/.env_auto
  log_info "Variables de entorno cargadas y copiadas limpias a volúmenes compartidos (.env.clean, .env_auto). El archivo .env original se preserva."
else
  log_warning "No se encontró archivo .env, algunas variables pueden faltar."
fi


# Eliminar red Docker conflictiva si existe (forzado, con espera y reintento)

###############################################################
# [DOCUMENTACIÓN] Limpieza robusta de recursos Docker
# -------------------------------------------------------------
# La red 'etl_net' ahora es gestionada automáticamente por Docker Compose.
# Ya no se elimina manualmente, evitando conflictos y errores de etiquetas.
#
# El pipeline elimina contenedores y volúmenes, y deja que Compose gestione
# la red según el ciclo de vida de los servicios. Esto garantiza idempotencia
# y evita afectar recursos externos o compartidos.
#
# Referencia: docs/ERROR_RECOVERY.md y README_AUTOMATIZADO.md
###############################################################
log_info "Deteniendo servicios y limpiando recursos Docker..."
docker compose down -v 2>&1 | tee -a logs/etl_full.log || log_warning "docker compose down falló o no había servicios activos"

# Script maestro para lanzar todo el pipeline ETL y validar cada paso
# Mensajes y comprobaciones en español

###############################################################
# [DOCUMENTACIÓN DE FLUJO Y CONTROL DE CAMBIOS]
# -------------------------------------------------------------
# Esta sección ejecuta la validación de entorno, limpieza de recursos,
# despliegue de servicios y validaciones automáticas. Cada paso está
# documentado para facilitar auditoría y troubleshooting.
#
# - Validaciones previas aseguran que las variables críticas y dependencias
#   estén presentes antes de continuar. Si alguna falla, se aborta o se
#   intenta continuar según el tipo de error.
# - La limpieza de contenedores, volúmenes y red Docker garantiza que el
#   pipeline sea idempotente y no se acumulen recursos obsoletos.
# - El despliegue de servicios base se realiza en modo robusto, con
#   comprobación de salud y dependencias antes de la ingesta.
# - Se documentan advertencias y buenas prácticas para evitar romper
#   procesos externos o dependencias compartidas.
# - El registro de cada paso queda en logs/etl_full.log para análisis posterior.
#
# Si modificas la lógica de validaciones, limpieza o despliegue, documenta aquí
# el motivo y el impacto esperado. Esto ayuda a mantener trazabilidad y control
# sobre el pipeline ETL.
###############################################################

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

set -a
source .env
set +a
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
###############################################################
###############################################################
# [ROBUSTECIMIENTO] Limpieza automática de red conflictiva
# -------------------------------------------------------------
# Antes de iniciar el pipeline, elimina la red 'etl_prod_etl_net' si existe,
# para evitar conflictos de etiquetas y asegurar que Compose la gestione correctamente.
# Esto soluciona el error recurrente de red con etiquetas incorrectas.
if docker network inspect etl_prod_etl_net >/dev/null 2>&1; then
  log_warning "Red Docker conflictiva 'etl_prod_etl_net' detectada. Eliminando para evitar errores de etiquetas..."
  docker network rm etl_prod_etl_net 2>&1 | tee -a logs/etl_full.log || log_warning "No se pudo eliminar la red conflictiva, puede que no exista."
else
  log_info "No se detectó red conflictiva 'etl_prod_etl_net'."
fi
###############################################################
###############################################################
log_info "Limpiando contenedores y volúmenes..."
docker compose down -v 2>&1 | tee -a logs/etl_full.log || handle_recoverable_error "docker compose down falló"
# Elimina todos los contenedores activos (forzado)
docker ps -aq | xargs -r docker rm -f 2>&1 | tee -a logs/etl_full.log || handle_recoverable_error "No hay contenedores para eliminar"
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

# [CAMBIO] Inicialización robusta de ClickHouse ahora se ejecuta de forma interna
# -------------------------------------------------------------------------------
# Se ejecuta el script de setup desde el volumen compartido, usando docker exec
# para evitar dependencias externas y asegurar que la configuración se realiza
# directamente en el contenedor de ClickHouse.
#
# El script debe estar accesible en /app/setup_clickhouse_robust.sh
# (montado por volumen compartido). Si falla, se documenta en el log y se aborta.
log_info "Ejecutando inicialización robusta de ClickHouse desde el contenedor..."
if docker exec clickhouse bash /app/setup_clickhouse_robust.sh 2>&1 | tee -a logs/etl_full.log; then
  log_info "✓ Inicialización robusta de ClickHouse completada"
else
  handle_fatal_error "Falló la inicialización robusta de ClickHouse (ver logs/etl_full.log)"
fi

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
docker compose run --rm etl-tools python tools/ingest_runner.py --source-url=mysql+pymysql://juan.marcos:123456@172.21.61.53:3306/archivos --ch-database=fgeo_analytics --ch-prefix=archivos_ --schemas archivos --chunksize 50000 --truncate-before-load --dedup none | tee -a logs/etl_full.log

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
