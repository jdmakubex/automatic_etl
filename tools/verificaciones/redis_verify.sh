#!/bin/bash
# Verificación completa de Redis para el stack asíncrono

set -e

TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
LOG_FILE="/app/logs/redis_verify_${TIMESTAMP}.log"
LATEST_LOG="/app/logs/redis_verify_latest.log"
JSON_REPORT="/app/logs/redis_verify_${TIMESTAMP}.json"

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Función de logging
log_and_print() {
    local level=$1
    local message=$2
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    echo "[$timestamp] [$level] $message" | tee -a "$LOG_FILE"
    
    case $level in
        "ERROR")
            echo -e "${RED}❌ $message${NC}"
            ;;
        "SUCCESS")
            echo -e "${GREEN}✅ $message${NC}"
            ;;
        "WARNING")
            echo -e "${YELLOW}⚠️  $message${NC}"
            ;;
        "INFO")
            echo -e "${BLUE}ℹ️  $message${NC}"
            ;;
    esac
}

echo "========================================"
echo "🔍 VERIFICACIÓN DE REDIS"
echo "========================================"
echo ""

# Inicializar contadores
TOTAL_CHECKS=0
PASSED_CHECKS=0
FAILED_CHECKS=0
WARNING_CHECKS=0

# Array para almacenar resultados
declare -a RESULTS

# Función para registrar resultado
record_result() {
    local check_name=$1
    local status=$2
    local message=$3
    
    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
    
    case $status in
        "PASS")
            PASSED_CHECKS=$((PASSED_CHECKS + 1))
            log_and_print "SUCCESS" "$check_name: $message"
            ;;
        "FAIL")
            FAILED_CHECKS=$((FAILED_CHECKS + 1))
            log_and_print "ERROR" "$check_name: $message"
            ;;
        "WARNING")
            WARNING_CHECKS=$((WARNING_CHECKS + 1))
            log_and_print "WARNING" "$check_name: $message"
            ;;
    esac
    
    RESULTS+=("{\"check\":\"$check_name\",\"status\":\"$status\",\"message\":\"$message\"}")
}

# 1. Verificar que Redis esté corriendo
log_and_print "INFO" "1. Verificando conectividad a Redis..."
if docker exec superset-redis redis-cli ping > /dev/null 2>&1; then
    REDIS_VERSION=$(docker exec superset-redis redis-cli INFO server | grep redis_version | cut -d: -f2 | tr -d '\r')
    record_result "Redis Connectivity" "PASS" "Redis respondiendo (versión: $REDIS_VERSION)"
else
    record_result "Redis Connectivity" "FAIL" "Redis no responde al comando PING"
fi

# 2. Verificar memoria de Redis
log_and_print "INFO" "2. Verificando memoria de Redis..."
USED_MEMORY=$(docker exec superset-redis redis-cli INFO memory | grep "used_memory_human:" | cut -d: -f2 | tr -d '\r')
MAX_MEMORY=$(docker exec superset-redis redis-cli INFO memory | grep "maxmemory_human:" | cut -d: -f2 | tr -d '\r')
if [ -n "$USED_MEMORY" ]; then
    record_result "Redis Memory" "PASS" "Uso de memoria: $USED_MEMORY (Max: ${MAX_MEMORY:-unlimited})"
else
    record_result "Redis Memory" "WARNING" "No se pudo obtener información de memoria"
fi

# 3. Verificar bases de datos Redis (DB 0-4 para Superset)
log_and_print "INFO" "3. Verificando bases de datos Redis..."
declare -A DB_PURPOSES=(
    [0]="Celery Broker"
    [1]="Results Backend"
    [2]="General Cache"
    [3]="Data Cache"
    [4]="Async Queries"
)

TOTAL_KEYS=0
for db in {0..4}; do
    DB_SIZE=$(docker exec superset-redis redis-cli -n $db DBSIZE 2>/dev/null | grep -o '[0-9]*' || echo "0")
    TOTAL_KEYS=$((TOTAL_KEYS + DB_SIZE))
    PURPOSE="${DB_PURPOSES[$db]}"
    
    if [ "$DB_SIZE" -ge 0 ]; then
        if [ "$DB_SIZE" -gt 0 ]; then
            record_result "Redis DB $db ($PURPOSE)" "PASS" "$DB_SIZE claves almacenadas"
        else
            record_result "Redis DB $db ($PURPOSE)" "PASS" "DB vacía (normal si sistema recién iniciado)"
        fi
    else
        record_result "Redis DB $db ($PURPOSE)" "FAIL" "No se pudo consultar DB $db"
    fi
done

log_and_print "INFO" "   Total de claves en Redis: $TOTAL_KEYS"

# 4. Verificar persistencia (AOF)
log_and_print "INFO" "4. Verificando persistencia (AOF)..."
AOF_ENABLED=$(docker exec superset-redis redis-cli CONFIG GET appendonly | tail -1 | tr -d '\r')
if [ "$AOF_ENABLED" = "yes" ]; then
    record_result "Redis Persistence" "PASS" "AOF habilitado (persistencia activa)"
else
    record_result "Redis Persistence" "WARNING" "AOF deshabilitado (datos en memoria volátil)"
fi

# 5. Verificar clientes conectados
log_and_print "INFO" "5. Verificando clientes conectados..."
CONNECTED_CLIENTS=$(docker exec superset-redis redis-cli INFO clients | grep "connected_clients:" | cut -d: -f2 | tr -d '\r')
if [ "$CONNECTED_CLIENTS" -gt 0 ]; then
    record_result "Redis Clients" "PASS" "$CONNECTED_CLIENTS clientes conectados"
else
    record_result "Redis Clients" "WARNING" "No hay clientes conectados (Superset/Celery podrían no estar usando Redis)"
fi

# 6. Verificar comandos procesados
log_and_print "INFO" "6. Verificando actividad de Redis..."
TOTAL_COMMANDS=$(docker exec superset-redis redis-cli INFO stats | grep "total_commands_processed:" | cut -d: -f2 | tr -d '\r')
if [ "$TOTAL_COMMANDS" -gt 0 ]; then
    record_result "Redis Activity" "PASS" "$TOTAL_COMMANDS comandos procesados"
else
    record_result "Redis Activity" "WARNING" "No se han procesado comandos (sistema recién iniciado)"
fi

# 7. Verificar conectividad desde Superset (si está corriendo)
log_and_print "INFO" "7. Verificando conectividad desde Superset..."
if docker exec superset bash -c "timeout 2 bash -c 'cat < /dev/null > /dev/tcp/redis/6379'" 2>/dev/null; then
    record_result "Superset->Redis" "PASS" "Superset puede conectarse a Redis"
else
    if docker ps | grep -q "superset"; then
        record_result "Superset->Redis" "FAIL" "Superset no puede conectarse a Redis"
    else
        record_result "Superset->Redis" "WARNING" "Superset no está corriendo (no se puede verificar conectividad)"
    fi
fi

# 8. Verificar conectividad desde Worker (si está corriendo)
log_and_print "INFO" "8. Verificando conectividad desde Worker..."
if docker exec superset-worker bash -c "timeout 2 bash -c 'cat < /dev/null > /dev/tcp/redis/6379'" 2>/dev/null; then
    record_result "Worker->Redis" "PASS" "Worker puede conectarse a Redis"
else
    if docker ps | grep -q "superset-worker"; then
        record_result "Worker->Redis" "FAIL" "Worker no puede conectarse a Redis"
    else
        record_result "Worker->Redis" "WARNING" "Worker no está corriendo (no se puede verificar conectividad)"
    fi
fi

# 9. Verificar uptime
log_and_print "INFO" "9. Verificando uptime de Redis..."
UPTIME_SECONDS=$(docker exec superset-redis redis-cli INFO server | grep "uptime_in_seconds:" | cut -d: -f2 | tr -d '\r')
UPTIME_DAYS=$(docker exec superset-redis redis-cli INFO server | grep "uptime_in_days:" | cut -d: -f2 | tr -d '\r')
if [ -n "$UPTIME_SECONDS" ]; then
    record_result "Redis Uptime" "PASS" "$UPTIME_DAYS días (${UPTIME_SECONDS}s)"
else
    record_result "Redis Uptime" "WARNING" "No se pudo obtener uptime"
fi

# 10. Verificar healthcheck de Docker
log_and_print "INFO" "10. Verificando healthcheck de Docker..."
HEALTH_STATUS=$(docker inspect --format='{{.State.Health.Status}}' superset-redis 2>/dev/null || echo "no-health")
if [ "$HEALTH_STATUS" = "healthy" ]; then
    record_result "Docker Healthcheck" "PASS" "Redis marcado como healthy"
elif [ "$HEALTH_STATUS" = "no-health" ]; then
    record_result "Docker Healthcheck" "WARNING" "No hay healthcheck configurado"
else
    record_result "Docker Healthcheck" "FAIL" "Estado: $HEALTH_STATUS"
fi

# Resumen final
echo ""
echo "========================================"
echo "📊 RESUMEN DE VERIFICACIÓN"
echo "========================================"
echo ""
echo "Total de verificaciones: $TOTAL_CHECKS"
echo -e "${GREEN}✅ Exitosas: $PASSED_CHECKS${NC}"
echo -e "${YELLOW}⚠️  Advertencias: $WARNING_CHECKS${NC}"
echo -e "${RED}❌ Fallidas: $FAILED_CHECKS${NC}"
echo ""

# Determinar estado general
if [ $FAILED_CHECKS -eq 0 ]; then
    if [ $WARNING_CHECKS -eq 0 ]; then
        OVERALL_STATUS="SUCCESS"
        log_and_print "SUCCESS" "Redis está completamente operativo"
    else
        OVERALL_STATUS="WARNING"
        log_and_print "WARNING" "Redis operativo pero con advertencias"
    fi
else
    OVERALL_STATUS="FAILED"
    log_and_print "ERROR" "Redis tiene problemas críticos"
fi

# Generar reporte JSON
cat > "$JSON_REPORT" << EOF
{
  "timestamp": "$(date -Iseconds)",
  "service": "Redis",
  "overall_status": "$OVERALL_STATUS",
  "summary": {
    "total_checks": $TOTAL_CHECKS,
    "passed": $PASSED_CHECKS,
    "warnings": $WARNING_CHECKS,
    "failed": $FAILED_CHECKS
  },
  "details": {
    "version": "$REDIS_VERSION",
    "used_memory": "$USED_MEMORY",
    "total_keys": $TOTAL_KEYS,
    "connected_clients": $CONNECTED_CLIENTS,
    "uptime_days": $UPTIME_DAYS
  },
  "checks": [
    $(IFS=,; echo "${RESULTS[*]}")
  ]
}
EOF

# Copiar a latest
cp "$LOG_FILE" "$LATEST_LOG"

echo ""
echo "📄 Logs guardados en:"
echo "   - $LOG_FILE"
echo "   - $LATEST_LOG"
echo "   - $JSON_REPORT"
echo ""

# Exit code basado en resultado
if [ "$OVERALL_STATUS" = "FAILED" ]; then
    exit 1
elif [ "$OVERALL_STATUS" = "WARNING" ]; then
    exit 0  # Warnings no son fatales
else
    exit 0
fi
