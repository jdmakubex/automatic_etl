#!/bin/bash
# Agente de Verificación Automática del Sistema ETL
# Ejecuta verificaciones cada hora para monitorear el estado del pipeline

set -e

LOG_DIR="/mnt/c/proyectos/etl_prod/logs"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
REPORT_FILE="$LOG_DIR/system_verification_$(date '+%Y%m%d').log"

# Crear directorio de logs si no existe
mkdir -p "$LOG_DIR"

echo "[$TIMESTAMP] 🤖 INICIANDO VERIFICACIÓN AUTOMÁTICA DEL SISTEMA" | tee -a "$REPORT_FILE"
echo "========================================================" | tee -a "$REPORT_FILE"

# Función para logging
log_status() {
    local status="$1"
    local message="$2"
    echo "[$TIMESTAMP] $status $message" | tee -a "$REPORT_FILE"
}

# Función para verificar servicio HTTP
check_http_service() {
    local service_name="$1"
    local url="$2"
    local expected_code="$3"
    
    if curl -s -o /dev/null -w "%{http_code}" "$url" | grep -q "$expected_code"; then
        log_status "✅" "$service_name: HEALTHY ($url)"
        return 0
    else
        log_status "❌" "$service_name: FAILED ($url)"
        return 1
    fi
}

# Función para verificar contenedor Docker
check_docker_container() {
    local container_name="$1"
    
    if docker ps --filter "name=$container_name" --filter "status=running" | grep -q "$container_name"; then
        local health=$(docker inspect --format='{{.State.Health.Status}}' "$container_name" 2>/dev/null || echo "no-health")
        if [ "$health" = "healthy" ] || [ "$health" = "no-health" ]; then
            log_status "✅" "Contenedor $container_name: RUNNING"
            return 0
        else
            log_status "⚠️" "Contenedor $container_name: RUNNING pero $health"
            return 1
        fi
    else
        log_status "❌" "Contenedor $container_name: NOT RUNNING"
        return 1
    fi
}

# Función para verificar datos en ClickHouse
check_clickhouse_data() {
    local database="$1"
    local expected_min="$2"
    
    local count=$(docker exec clickhouse clickhouse-client --query "SELECT COUNT(*) FROM system.tables WHERE database='$database'" 2>/dev/null || echo "0")
    
    if [ "$count" -ge "$expected_min" ]; then
        log_status "✅" "ClickHouse $database: $count tablas (>= $expected_min esperadas)"
        
        # Verificar datos específicos
        local total_rows=$(docker exec clickhouse clickhouse-client --query "SELECT SUM(total_rows) FROM system.tables WHERE database='$database'" 2>/dev/null || echo "0")
        log_status "📊" "ClickHouse $database: $total_rows filas totales"
        return 0
    else
        log_status "❌" "ClickHouse $database: Solo $count tablas (< $expected_min esperadas)"
        return 1
    fi
}

cd /mnt/c/proyectos/etl_prod

# 1. VERIFICAR SERVICIOS PRINCIPALES
log_status "🔍" "Verificando servicios principales..."

services_ok=0
total_services=4

check_http_service "ClickHouse" "http://localhost:8123/" "200" && ((services_ok++))
check_http_service "Superset" "http://localhost:8088/login/" "200" && ((services_ok++))  
check_http_service "Kafka Connect" "http://localhost:8083/connectors" "200" && ((services_ok++))

# Verificar Kafka (no HTTP)
if docker exec kafka kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1; then
    log_status "✅" "Kafka: HEALTHY (localhost:9092)"
    ((services_ok++))
else
    log_status "❌" "Kafka: FAILED"
fi

log_status "📊" "Servicios principales: $services_ok/$total_services HEALTHY"

# 2. VERIFICAR CONTENEDORES DOCKER
log_status "🔍" "Verificando contenedores Docker..."

containers_ok=0
total_containers=4

check_docker_container "clickhouse" && ((containers_ok++))
check_docker_container "superset" && ((containers_ok++))
check_docker_container "kafka" && ((containers_ok++))
check_docker_container "connect" && ((containers_ok++))

log_status "📊" "Contenedores: $containers_ok/$total_containers RUNNING"

# 3. VERIFICAR DATOS EN CLICKHOUSE
log_status "🔍" "Verificando datos en ClickHouse..."

data_ok=0
total_dbs=2

check_clickhouse_data "demo1_analytics" "3" && ((data_ok++))
check_clickhouse_data "demo2_analytics" "3" && ((data_ok++))

log_status "📊" "Bases de datos: $data_ok/$total_dbs con datos"

# 4. VERIFICAR CONECTIVIDAD MYSQL
log_status "🔍" "Verificando conectividad MySQL..."

mysql_ok=0
if docker run --rm --network host mysql:8.0 mysql -h172.21.61.53 -ujuan.marcos -p123456 -e "SELECT 1" >/dev/null 2>&1; then
    log_status "✅" "MySQL: Conectividad OK"
    
    # Contar registros en MySQL
    demo1_count=$(docker run --rm --network host mysql:8.0 mysql -h172.21.61.53 -ujuan.marcos -p123456 -e "SELECT COUNT(*) FROM demo1.empleados" -s -N 2>/dev/null || echo "0")
    demo2_count=$(docker run --rm --network host mysql:8.0 mysql -h172.21.61.53 -ujuan.marcos -p123456 -e "SELECT COUNT(*) FROM demo2.empleados" -s -N 2>/dev/null || echo "0")
    
    log_status "📊" "MySQL demo1.empleados: $demo1_count registros"
    log_status "📊" "MySQL demo2.empleados: $demo2_count registros"
    mysql_ok=1
else
    log_status "❌" "MySQL: Sin conectividad"
fi

# 5. RESUMEN FINAL
echo "" | tee -a "$REPORT_FILE"
log_status "🏁" "RESUMEN DE VERIFICACIÓN:"
echo "----------------------------------------" | tee -a "$REPORT_FILE"

overall_score=$((services_ok + containers_ok + data_ok + mysql_ok))
max_score=11

if [ $overall_score -ge 9 ]; then
    status="🟢 EXCELENTE"
elif [ $overall_score -ge 7 ]; then
    status="🟡 ACEPTABLE"  
else
    status="🔴 NECESITA ATENCIÓN"
fi

log_status "$status" "Sistema funcionando: $overall_score/$max_score componentes OK"

# Recomendaciones basadas en el estado
if [ $services_ok -lt 4 ]; then
    log_status "💡" "RECOMENDACIÓN: Reiniciar servicios con: docker compose restart"
fi

if [ $data_ok -lt 2 ]; then
    log_status "💡" "RECOMENDACIÓN: Ejecutar ingesta manual con: bash bootstrap/run_etl_full.sh"
fi

if [ $mysql_ok -eq 0 ]; then
    log_status "💡" "RECOMENDACIÓN: Verificar conectividad de red a MySQL 172.21.61.53"
fi

echo "" | tee -a "$REPORT_FILE"
log_status "📝" "Reporte guardado en: $REPORT_FILE"
log_status "⏰" "Próxima verificación en 1 hora"

# Retornar código basado en el estado general
if [ $overall_score -ge 7 ]; then
    exit 0
else
    exit 1
fi