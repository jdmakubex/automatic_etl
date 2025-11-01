#!/bin/bash
"""
Script: setup_metabase_dynamic.sh
Automatiza el despliegue y configuración dinámica de Metabase
Parte del pipeline ETL, se ejecuta después del despliegue de ClickHouse
"""
set -e

echo "🚀 CONFIGURACIÓN DINÁMICA DE METABASE"
echo "======================================"

# Función para logging con timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Función para esperar que un servicio esté disponible
wait_for_service() {
    local service_name=$1
    local url=$2
    local max_attempts=${3:-30}
    local attempt=1
    
    log "⏳ Esperando que $service_name esté disponible en $url..."
    
    while [ $attempt -le $max_attempts ]; do
        if curl -s "$url" > /dev/null 2>&1; then
            log "✅ $service_name está disponible"
            return 0
        fi
        
        log "   Intento $attempt/$max_attempts - $service_name no disponible aún..."
        sleep 5
        attempt=$((attempt + 1))
    done
    
    log "❌ $service_name no respondió después de $max_attempts intentos"
    return 1
}

# Variables de entorno con defaults
METABASE_URL=${METABASE_URL:-"http://metabase:3000"}
CLICKHOUSE_URL=${CLICKHOUSE_URL:-"http://clickhouse:8123"}
MAX_RETRIES=${MAX_RETRIES:-3}

log "📍 Configuración:"
log "   Metabase: $METABASE_URL"
log "   ClickHouse: $CLICKHOUSE_URL"
log "   Max reintentos: $MAX_RETRIES"

# 1. Verificar que ClickHouse esté disponible
if ! wait_for_service "ClickHouse" "$CLICKHOUSE_URL/ping"; then
    log "💥 ClickHouse no está disponible. Abortando configuración de Metabase."
    exit 1
fi

# 2. Verificar que Metabase esté disponible
if ! wait_for_service "Metabase" "$METABASE_URL/api/health"; then
    log "💥 Metabase no está disponible. Abortando configuración."
    exit 1
fi

log "🔧 Ambos servicios están disponibles. Iniciando configuración dinámica..."

# 3. Ejecutar configurador dinámico con reintentos
attempt=1
while [ $attempt -le $MAX_RETRIES ]; do
    log "🎯 Intento de configuración $attempt/$MAX_RETRIES"
    
    if python3 /app/tools/metabase_dynamic_configurator.py; then
        log "✅ ¡Configuración dinámica de Metabase completada exitosamente!"
        
        # Guardar timestamp de última configuración exitosa
        echo "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > /app/logs/metabase_last_config.timestamp
        
        # Mostrar resumen de acceso
        log "📊 INFORMACIÓN DE ACCESO:"
        log "   URL: $METABASE_URL"
        log "   Usuario: ${METABASE_ADMIN:-admin}"
        log "   Password: ${METABASE_PASSWORD:-Metabase123!}"
        log "   Configurado dinámicamente desde DB_CONNECTIONS"
        
        exit 0
    else
        log "❌ Error en configuración (intento $attempt/$MAX_RETRIES)"
        
        if [ $attempt -lt $MAX_RETRIES ]; then
            log "⏳ Esperando 30 segundos antes del siguiente intento..."
            sleep 30
        fi
        
        attempt=$((attempt + 1))
    fi
done

log "💥 Falló la configuración después de $MAX_RETRIES intentos"

# Generar reporte de error
ERROR_LOG="/app/logs/metabase_config_error.json"
cat > "$ERROR_LOG" << EOF
{
    "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
    "error": "Configuración dinámica de Metabase falló",
    "attempts": $MAX_RETRIES,
    "services": {
        "metabase_url": "$METABASE_URL",
        "clickhouse_url": "$CLICKHOUSE_URL"
    },
    "next_steps": [
        "Verificar logs de Metabase",
        "Validar variables de entorno",
        "Confirmar conectividad entre servicios"
    ]
}
EOF

log "📝 Reporte de error guardado en: $ERROR_LOG"
exit 1