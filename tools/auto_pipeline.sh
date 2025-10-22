#!/bin/bash
# Orquestador automático sin dependencias de Docker socket
# Se ejecuta automáticamente al iniciar los contenedores

set -e
set -o pipefail

# Configuración de logs detallados
LOG_FILE="/app/logs/auto_pipeline_detailed.log"
mkdir -p /app/logs

# Función de logging
log_message() {
    local level=$1
    local message=$2
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] [$level] $message" | tee -a "$LOG_FILE"
}

# Función para ejecutar comandos con logs (salida visible y persistente)
execute_with_log() {
    local description=$1
    local command=$2
    
    log_message "INFO" "🚧 INICIANDO: $description"
    
    if eval "$command" 2>&1 | tee -a "$LOG_FILE"; then
        log_message "SUCCESS" "✅ COMPLETADO: $description"
        return 0
    else
        log_message "ERROR" "❌ FALLÓ: $description"
        return 1
    fi
}

echo "🚀 INICIANDO PIPELINE ETL AUTOMÁTICO..."
echo "⏰ $(date)"
log_message "INFO" "🚀 INICIANDO PIPELINE ETL AUTOMÁTICO - $(date)"

# Verificar estado limpio del sistema (solo si ClickHouse está disponible)
log_message "INFO" "🔍 FASE 0: Verificación de estado limpio del sistema"
if command -v clickhouse-client &> /dev/null; then
    if python3 tools/verify_clean_state.py 2>&1 | tee -a "$LOG_FILE"; then
        log_message "SUCCESS" "✅ Verificación de estado limpio completada"
    else
        log_message "WARNING" "⚠️  Sistema tiene datos antiguos - se recomienda limpieza"
    fi
else
    log_message "INFO" "ℹ️  Verificación de estado limpio omitida (clickhouse-client no disponible en este contenedor)"
fi

# Función para esperar servicios
wait_for_service() {
    local service_name=$1
    local host=$2
    local port=$3
    local max_attempts=30
    local attempt=1
    
    log_message "INFO" "⏳ Esperando $service_name en $host:$port..."
    echo "⏳ Esperando $service_name en $host:$port..."
    
    while [ $attempt -le $max_attempts ]; do
        if nc -z $host $port 2>/dev/null; then
            log_message "SUCCESS" "✅ $service_name está listo después de $attempt intentos"
            echo "✅ $service_name está listo"
            return 0
        fi
        if [ $((attempt % 5)) -eq 0 ]; then
            log_message "INFO" "   Esperando $service_name... intento $attempt/$max_attempts"
        fi
        echo "   Intento $attempt/$max_attempts..."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    log_message "ERROR" "❌ $service_name no respondió después de $max_attempts intentos"
    echo "❌ $service_name no respondió después de $max_attempts intentos"
    return 1
}

# Esperar servicios críticos
log_message "INFO" "📋 FASE 1: Verificación de servicios críticos"
wait_for_service "ClickHouse" "clickhouse" "8123" || exit 1
# Validar autenticación HTTP a ClickHouse con las credenciales del entorno
if ! python3 tools/test_clickhouse_auth.py; then
    log_message "ERROR" "❌ Falló autenticación HTTP a ClickHouse. Revisa CLICKHOUSE_* o CH_* en .env"
    exit 1
fi
wait_for_service "Kafka Connect" "connect" "8083" || exit 1

# Pequeña pausa adicional para estabilización
log_message "INFO" "⏳ Esperando estabilización de servicios (15s)..."
echo "⏳ Esperando estabilización de servicios..."
sleep 15
log_message "SUCCESS" "✅ Servicios estabilizados"

log_message "INFO" "📋 FASE 2: Ingesta automática de datos"
echo "🎯 EJECUTANDO INGESTA AUTOMÁTICA DE DATOS..."

# Ejecutar ingesta multi-database con logs detallados
log_message "INFO" "🔄 Iniciando ingesta multi-database desde DB_CONNECTIONS..."
log_message "INFO" "   - Parseando configuración desde .env"
log_message "INFO" "   - Procesando todas las conexiones definidas"
log_message "INFO" "   - Chunk size: 50,000 por defecto"

if python3 tools/multi_database_ingest.py 2>&1 | tee -a "$LOG_FILE"; then
    
    # Verificar reporte multi-database
    log_message "INFO" "🔍 Verificando datos ingresados..."
    if [ -f "/app/logs/multi_database_ingest_report.json" ]; then
        TOTAL_ROWS=$(python3 -c "
import json
try:
    with open('/app/logs/multi_database_ingest_report.json', 'r') as f:
        report = json.load(f)
    print(report['summary']['total_records_processed'])
except:
    print('0')
" 2>/dev/null || echo "0")
        log_message "SUCCESS" "✅ Ingesta completada: $TOTAL_ROWS registros totales"
        
        # Verificar datos en ClickHouse
        log_message "INFO" "🔍 Verificando tablas y registros en ClickHouse..."
        if command -v clickhouse-client &> /dev/null; then
            echo "📊 Tablas creadas en ClickHouse:" | tee -a "$LOG_FILE"
            clickhouse-client --user="${CLICKHOUSE_USER:-default}" --password="${CLICKHOUSE_PASSWORD:-ClickHouse123!}" \
                --query="SELECT database, name, total_rows FROM system.tables WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA') AND name NOT LIKE '.inner%' ORDER BY database, name FORMAT PrettyCompact" \
                2>&1 | tee -a "$LOG_FILE" || true
        fi
    else
        TOTAL_ROWS="0"
        log_message "WARNING" "⚠️ Reporte multi-database no encontrado"
    fi
    # No bloquear por 0 filas: algunas tablas pueden no devolver datos en esta fase.
    INGESTION_SUCCESS=true
else
    log_message "ERROR" "❌ Error en la ingesta multi-database"
    INGESTION_SUCCESS=false
fi

if [ "$INGESTION_SUCCESS" = true ]; then
    log_message "SUCCESS" "✅ INGESTA DE DATOS COMPLETADA EXITOSAMENTE"
    echo "✅ INGESTA DE DATOS COMPLETADA EXITOSAMENTE"
    
    # NUEVA FASE: Esperar a que Debezium complete el snapshot inicial
    log_message "INFO" "📋 FASE 2.5: Esperando snapshot inicial de Debezium"
    echo "🔄 ESPERANDO SNAPSHOT INICIAL DE DEBEZIUM..."
    echo "ℹ️  Esto puede tardar varios minutos dependiendo del tamaño de las tablas..."
    
    if python3 tools/wait_for_debezium_snapshot.py 2>&1 | tee -a "$LOG_FILE"; then
        log_message "SUCCESS" "✅ Snapshot de Debezium completado"
        echo "✅ Snapshot de Debezium completado"
    else
        log_message "WARNING" "⚠️ Timeout esperando snapshot (continuará en background)"
        echo "⚠️ Timeout esperando snapshot (continuará en background)"
    fi

    # NUEVA FASE: Diagnóstico y auto-reparación CDC (Kafka-ClickHouse)
    log_message "INFO" "🛠️  FASE 2.6: Diagnóstico y ajuste del pipeline CDC"
    echo "🧪 Ejecutando diagnóstico CDC (ClickHouse-Kafka-MVs)..."
    if python3 tools/diagnose_and_fix_cdc_pipeline.py 2>&1 | tee -a "$LOG_FILE"; then
        log_message "SUCCESS" "✅ Diagnóstico CDC ejecutado"
    else
        log_message "WARNING" "⚠️ Diagnóstico CDC encontró problemas (se intentó auto-reparar)"
    fi
    echo "⏳ Esperando 20s para que las MVs reanuden consumo..."
    sleep 20
    
    # Verificación POST-ingesta de ClickHouse
    log_message "INFO" "📋 FASE 2.7: Verificación POST-ingesta de ClickHouse"
    echo "🔍 Ejecutando verificación de ClickHouse..."
    if bash tools/verificaciones/clickhouse_verify.sh 2>&1 | tee -a "$LOG_FILE"; then
        log_message "SUCCESS" "✅ Verificación de ClickHouse completada"
        echo "📊 Ver detalles en: logs/clickhouse_verify_latest.log"
    else
        log_message "WARNING" "⚠️ Verificación de ClickHouse encontró problemas"
    fi
    
    # Verificación de Kafka y Connect
    log_message "INFO" "📋 FASE 2.8: Verificación de Kafka y Connect"
    echo "🔍 Ejecutando verificación de Kafka..."
    if bash tools/verificaciones/kafka_verify.sh 2>&1 | tee -a "$LOG_FILE"; then
        log_message "SUCCESS" "✅ Verificación de Kafka completada"
        echo "📊 Ver detalles en: logs/kafka_verify_latest.log"
    else
        log_message "WARNING" "⚠️ Verificación de Kafka encontró problemas"
    fi
    
    # Configurar Superset datasets automáticamente
    log_message "INFO" "📋 FASE 3: Configuración automática de Superset"
    echo "📊 CONFIGURANDO SUPERSET AUTOMÁTICAMENTE..."
    
    # Esperar que Superset esté listo
    wait_for_service "Superset" "superset" "8088" || exit 1
    
    # Inicializar Superset automáticamente
    log_message "INFO" "🔧 Creando usuario administrador de Superset..."
    echo "🔧 Inicializando Superset..."
    
    # Crear/resetear usuario admin con contraseña estandarizada
    ADMIN_PASSWORD="${SUPERSET_PASSWORD:-Admin123!}"
    echo "🔐 Configurando admin con contraseña: ${ADMIN_PASSWORD:0:6}***"
    
    # Intentar crear admin (falla si ya existe, pero no importa)
    docker compose exec -T superset superset fab create-admin \
        --username "${SUPERSET_USERNAME:-admin}" \
        --firstname Admin \
        --lastname User \
        --email admin@example.com \
        --password "$ADMIN_PASSWORD" 2>&1 | tee -a "$LOG_FILE" || \
    log_message "INFO" "Admin ya existe, reseteando contraseña..."
    
    # Resetear contraseña para asegurar sincronización
    execute_with_log "Resetear contraseña del admin" \
        "docker compose exec -T superset superset fab reset-password --username \"${SUPERSET_USERNAME:-admin}\" --password \"$ADMIN_PASSWORD\""
    
    execute_with_log "Actualizar base de datos de Superset" \
        "docker compose exec -T superset superset db upgrade"
    
    execute_with_log "Inicializar roles y permisos de Superset" \
        "docker compose exec -T superset superset init"
    

    # Configurar ClickHouse en Superset automáticamente
    log_message "INFO" "🔗 Configurando conexión ClickHouse en Superset..."
    echo "📊 Configurando base de datos ClickHouse en Superset..."
    
    if python3 superset_bootstrap/configure_clickhouse_automatic.py 2>&1 | tee -a "$LOG_FILE"; then
        log_message "SUCCESS" "✅ Superset configurado correctamente con ClickHouse"
        echo "✅ Superset configurado correctamente con ClickHouse"
        SUPERSET_CONFIG_SUCCESS=true
    else
        log_message "WARNING" "⚠️ Superset disponible pero configuración manual requerida"
        echo "⚠️  Superset disponible pero configuración manual requerida"
        SUPERSET_CONFIG_SUCCESS=false
    fi

    # Limpieza automática de datasets del esquema base (solo analytics visibles)
    log_message "INFO" "🧹 Limpiando datasets del esquema base en Superset..."
    echo "🧹 Limpiando datasets del esquema base en Superset..."
    if python3 tools/cleanup_superset_base_datasets.py 2>&1 | tee -a "$LOG_FILE"; then
        log_message "SUCCESS" "✅ Datasets del esquema base eliminados"
        echo "✅ Datasets del esquema base eliminados"
    else
        log_message "WARNING" "⚠️ Error limpiando datasets del esquema base"
        echo "⚠️ Error limpiando datasets del esquema base"
    fi
    
    # Verificar datasets configurados en Superset
    log_message "INFO" "📊 Verificando datasets configurados en Superset..."
    echo "📊 Datasets configurados en Superset:" | tee -a "$LOG_FILE"
    python3 -c "
import requests
import os
import sys

url = os.getenv('SUPERSET_URL', 'http://superset:8088')
admin = os.getenv('SUPERSET_ADMIN', 'admin')
pwd = os.getenv('SUPERSET_PASSWORD', 'Admin123!')

try:
    # Login
    resp = requests.post(f'{url}/api/v1/security/login', json={'username': admin, 'password': pwd, 'provider': 'db', 'refresh': True}, timeout=10)
    if resp.status_code != 200:
        print(f'⚠️  No se pudo autenticar en Superset')
        sys.exit(0)
    
    token = resp.json().get('access_token')
    headers = {'Authorization': f'Bearer {token}'}
    
    # Obtener datasets
    ds_resp = requests.get(f'{url}/api/v1/dataset/', headers=headers, timeout=10)
    if ds_resp.status_code != 200:
        print(f'⚠️  Error obteniendo datasets: {ds_resp.status_code}')
        sys.exit(0)
    
    datasets = ds_resp.json().get('result', [])
    print(f'✅ Total de datasets: {len(datasets)}')
    for ds in datasets[:10]:  # Mostrar primeros 10
        print(f'   - {ds.get(\"schema\", \"\")}.{ds.get(\"table_name\", \"\")}')
    if len(datasets) > 10:
        print(f'   ... y {len(datasets) - 10} más')
except Exception as e:
    print(f'⚠️  Error verificando datasets: {e}')
" 2>&1 | tee -a "$LOG_FILE" || true
    
    # Verificación completa de Superset
    log_message "INFO" "📋 FASE 3.9: Verificación completa de Superset"
    echo "🔍 Ejecutando verificación completa de Superset..."
    if bash tools/verificaciones/superset_verify.sh 2>&1 | tee -a "$LOG_FILE"; then
        log_message "SUCCESS" "✅ Verificación de Superset completada"
        echo "📊 Ver detalles en: logs/superset_verify_latest.log"
    else
        log_message "WARNING" "⚠️ Verificación de Superset encontró problemas"
    fi
    
    # Validación final
    log_message "INFO" "📋 FASE 4: Validación final del pipeline"
    execute_with_log "Validación final del pipeline" \
        "python3 tools/validate_final_pipeline.py"
    
    # Verificación de automatización completa
    log_message "INFO" "📋 FASE 5: Verificación de automatización completa"
    execute_with_log "Verificación de que todo está automatizado" \
        "python3 tools/verify_automation.py"
    
    # Verificación consolidada de todos los componentes
    log_message "INFO" "📋 FASE 6: Verificación consolidada final"
    echo "🔍 Ejecutando verificación consolidada de todos los componentes..."
    if bash tools/run_verifications.sh 2>&1 | tee -a "$LOG_FILE"; then
        log_message "SUCCESS" "✅ Verificación consolidada completada - Ver logs/verificacion_consolidada_latest.log"
        echo "📊 Reporte consolidado: logs/verificacion_consolidada_latest.json"
    else
        log_message "WARNING" "⚠️ Algunas verificaciones fallaron - revisar logs/verificacion_consolidada_latest.log"
    fi
    
    log_message "SUCCESS" "🎉 PIPELINE ETL COMPLETADO AUTOMÁTICAMENTE"
    echo "🎉 PIPELINE ETL COMPLETADO AUTOMÁTICAMENTE"
    echo "📈 Superset disponible en: http://localhost:8088"
    echo "👤 Usuario: ${SUPERSET_USERNAME:-admin} / Contraseña: ${SUPERSET_PASSWORD:-Admin123!}"
    
    # Guardar estado de éxito detallado
    cat > /app/logs/auto_pipeline_status.json << EOF
{
  "status": "success",
  "timestamp": "$(date -Iseconds)",
  "message": "Pipeline ETL completado automáticamente",
  "details": {
    "ingestion_success": $INGESTION_SUCCESS,
    "superset_config_success": $SUPERSET_CONFIG_SUCCESS,
    "total_rows_ingested": $TOTAL_ROWS,
    "superset_url": "http://localhost:8088",
    "credentials": "${SUPERSET_USERNAME:-admin}/${SUPERSET_PASSWORD:-Admin123!}"
  }
}
EOF
    
else
    log_message "ERROR" "❌ ERROR EN LA INGESTA DE DATOS"
    echo "❌ ERROR EN LA INGESTA DE DATOS"
    
    cat > /app/logs/auto_pipeline_status.json << EOF
{
  "status": "error",
  "timestamp": "$(date -Iseconds)",
  "message": "Error en ingesta de datos",
  "details": {
    "ingestion_success": false,
    "error_phase": "data_ingestion"
  }
}
EOF
    exit 1
fi

log_message "SUCCESS" "🏁 ORQUESTACIÓN AUTOMÁTICA FINALIZADA - Duración: $SECONDS segundos"
echo "🏁 ORQUESTACIÓN AUTOMÁTICA FINALIZADA"
echo "📄 Logs detallados en: $LOG_FILE"
echo "📊 Estado del pipeline en: /app/logs/auto_pipeline_status.json"