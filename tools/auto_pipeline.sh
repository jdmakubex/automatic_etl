#!/bin/bash
# Orquestador automático sin dependencias de Docker socket
# Se ejecuta automáticamente al iniciar los contenedores

set -e

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

# Función para ejecutar comandos con logs
execute_with_log() {
    local description=$1
    local command=$2
    
    log_message "INFO" "� INICIANDO: $description"
    
    if eval "$command" >> "$LOG_FILE" 2>&1; then
        log_message "SUCCESS" "✅ COMPLETADO: $description"
        return 0
    else
        log_message "ERROR" "❌ FALLÓ: $description"
        return 1
    fi
}

echo "�🚀 INICIANDO PIPELINE ETL AUTOMÁTICO..."
echo "⏰ $(date)"
log_message "INFO" "🚀 INICIANDO PIPELINE ETL AUTOMÁTICO - $(date)"

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
    else
        TOTAL_ROWS="0"
        log_message "WARNING" "⚠️ Reporte multi-database no encontrado"
    fi
    
    INGESTION_SUCCESS=true
else
    log_message "ERROR" "❌ Error en la ingesta multi-database"
    INGESTION_SUCCESS=false
fi

if [ "$INGESTION_SUCCESS" = true ]; then
    log_message "SUCCESS" "✅ INGESTA DE DATOS COMPLETADA EXITOSAMENTE"
    echo "✅ INGESTA DE DATOS COMPLETADA EXITOSAMENTE"
    
    # Configurar Superset datasets automáticamente
    log_message "INFO" "📋 FASE 3: Configuración automática de Superset"
    echo "📊 CONFIGURANDO SUPERSET AUTOMÁTICAMENTE..."
    
    # Esperar que Superset esté listo
    wait_for_service "Superset" "superset" "8088" || exit 1
    
    # Inicializar Superset automáticamente
    log_message "INFO" "🔧 Creando usuario administrador de Superset..."
    echo "🔧 Inicializando Superset..."
    
    execute_with_log "Crear usuario admin en Superset" \
        "docker compose exec -T superset superset fab create-admin --username admin --firstname Admin --lastname User --email admin@example.com --password admin"
    
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
    
    # Validación final
    log_message "INFO" "📋 FASE 4: Validación final del pipeline"
    execute_with_log "Validación final del pipeline" \
        "python3 tools/validate_final_pipeline.py"
    
    # Verificación de automatización completa
    log_message "INFO" "📋 FASE 5: Verificación de automatización completa"
    execute_with_log "Verificación de que todo está automatizado" \
        "python3 tools/verify_automation.py"
    
    log_message "SUCCESS" "🎉 PIPELINE ETL COMPLETADO AUTOMÁTICAMENTE"
    echo "🎉 PIPELINE ETL COMPLETADO AUTOMÁTICAMENTE"
    echo "📈 Superset disponible en: http://localhost:8088"
    echo "👤 Usuario: admin / Contraseña: admin"
    
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
    "credentials": "admin/admin"
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