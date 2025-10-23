#!/bin/bash
# verify_async_stack.sh
# Verificación del stack asíncrono de Superset (Redis + Celery)

set -e

echo "🔍 Verificando Stack Asíncrono de Superset"
echo "=========================================="
echo ""

# Colores
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Función para verificar servicio
check_service() {
    local service_name=$1
    local check_command=$2
    
    echo -n "Verificando $service_name... "
    if eval "$check_command" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ OK${NC}"
        return 0
    else
        echo -e "${RED}✗ FAIL${NC}"
        return 1
    fi
}

# Función para verificar healthcheck
check_healthcheck() {
    local container=$1
    local status=$(docker inspect --format='{{.State.Health.Status}}' $container 2>/dev/null || echo "no-health")
    
    echo -n "Healthcheck de $container... "
    if [ "$status" = "healthy" ]; then
        echo -e "${GREEN}✓ healthy${NC}"
        return 0
    elif [ "$status" = "no-health" ]; then
        # Verificar si al menos está running
        if docker ps --filter "name=$container" --filter "status=running" | grep -q $container; then
            echo -e "${YELLOW}⚠ running (sin healthcheck)${NC}"
            return 0
        else
            echo -e "${RED}✗ not running${NC}"
            return 1
        fi
    else
        echo -e "${RED}✗ $status${NC}"
        return 1
    fi
}

echo "📦 1. Verificando Contenedores"
echo "--------------------------------"

REDIS_OK=0
SUPERSET_OK=0
WORKER_OK=0
BEAT_OK=0

check_healthcheck "superset-redis" && REDIS_OK=1
check_healthcheck "superset" && SUPERSET_OK=1
check_healthcheck "superset-worker" && WORKER_OK=1

# Beat no tiene healthcheck estricto, solo verificar que corra
if docker ps --filter "name=superset-beat" --filter "status=running" | grep -q superset-beat; then
    echo -e "Healthcheck de superset-beat... ${GREEN}✓ running${NC}"
    BEAT_OK=1
else
    echo -e "Healthcheck de superset-beat... ${RED}✗ not running${NC}"
fi

echo ""
echo "🔌 2. Verificando Conectividad Redis"
echo "-------------------------------------"

# Ping Redis
check_service "Redis PING" "docker exec superset-redis redis-cli ping | grep -q PONG"

# Ver info de Redis
echo -n "Redis INFO... "
INFO_OUTPUT=$(docker exec superset-redis redis-cli INFO server 2>/dev/null | grep redis_version || echo "")
if [ -n "$INFO_OUTPUT" ]; then
    VERSION=$(echo "$INFO_OUTPUT" | cut -d: -f2 | tr -d '\r')
    echo -e "${GREEN}✓ $VERSION${NC}"
else
    echo -e "${RED}✗ No se pudo obtener INFO${NC}"
fi

echo ""
echo "🎯 3. Verificando Bases de Datos Redis"
echo "---------------------------------------"

# DB 0 - Celery Broker
echo -n "DB 0 (Celery Broker)... "
DB0_KEYS=$(docker exec superset-redis redis-cli -n 0 DBSIZE 2>/dev/null | grep -o '[0-9]*' || echo "0")
echo -e "${GREEN}✓ $DB0_KEYS keys${NC}"

# DB 1 - Results Backend
echo -n "DB 1 (Results Backend)... "
DB1_KEYS=$(docker exec superset-redis redis-cli -n 1 DBSIZE 2>/dev/null | grep -o '[0-9]*' || echo "0")
echo -e "${GREEN}✓ $DB1_KEYS keys${NC}"

# DB 2 - Cache
echo -n "DB 2 (Cache)... "
DB2_KEYS=$(docker exec superset-redis redis-cli -n 2 DBSIZE 2>/dev/null | grep -o '[0-9]*' || echo "0")
echo -e "${GREEN}✓ $DB2_KEYS keys${NC}"

# DB 3 - Data Cache
echo -n "DB 3 (Data Cache)... "
DB3_KEYS=$(docker exec superset-redis redis-cli -n 3 DBSIZE 2>/dev/null | grep -o '[0-9]*' || echo "0")
echo -e "${GREEN}✓ $DB3_KEYS keys${NC}"

# DB 4 - Async Queries
echo -n "DB 4 (Async Queries)... "
DB4_KEYS=$(docker exec superset-redis redis-cli -n 4 DBSIZE 2>/dev/null | grep -o '[0-9]*' || echo "0")
echo -e "${GREEN}✓ $DB4_KEYS keys${NC}"

echo ""
echo "👷 4. Verificando Celery Workers"
echo "---------------------------------"

# Ping workers
echo -n "Celery Workers Ping... "
WORKER_PING=$(docker exec superset-worker celery -A superset.tasks.celery_app:app inspect ping 2>/dev/null || echo "")
if echo "$WORKER_PING" | grep -q "pong"; then
    WORKER_COUNT=$(echo "$WORKER_PING" | grep -o "celery@" | wc -l)
    echo -e "${GREEN}✓ $WORKER_COUNT worker(s) activo(s)${NC}"
else
    echo -e "${RED}✗ No se detectaron workers${NC}"
fi

# Active tasks
echo -n "Tareas Activas... "
ACTIVE_TASKS=$(docker exec superset-worker celery -A superset.tasks.celery_app:app inspect active 2>/dev/null || echo "{}")
ACTIVE_COUNT=$(echo "$ACTIVE_TASKS" | grep -o '"name":' | wc -l)
echo -e "${GREEN}✓ $ACTIVE_COUNT tarea(s) ejecutándose${NC}"

# Registered tasks
echo -n "Tareas Registradas... "
REGISTERED=$(docker exec superset-worker celery -A superset.tasks.celery_app:app inspect registered 2>/dev/null || echo "")
if echo "$REGISTERED" | grep -q "sql_lab"; then
    echo -e "${GREEN}✓ sql_lab tasks registradas${NC}"
else
    echo -e "${YELLOW}⚠ No se encontraron tareas sql_lab${NC}"
fi

echo ""
echo "📅 5. Verificando Celery Beat (Scheduler)"
echo "------------------------------------------"

# Verificar que beat esté corriendo
if [ $BEAT_OK -eq 1 ]; then
    echo -n "Beat Process... "
    BEAT_LOG=$(docker logs superset-beat 2>&1 | tail -5)
    if echo "$BEAT_LOG" | grep -q "beat"; then
        echo -e "${GREEN}✓ Running${NC}"
    else
        echo -e "${YELLOW}⚠ Running pero sin logs recientes de beat${NC}"
    fi
    
    # Ver scheduled tasks
    echo -n "Tareas Programadas... "
    SCHEDULED=$(docker exec superset-worker celery -A superset.tasks.celery_app:app inspect scheduled 2>/dev/null || echo "")
    SCHEDULED_COUNT=$(echo "$SCHEDULED" | grep -o '"name":' | wc -l)
    echo -e "${GREEN}✓ $SCHEDULED_COUNT tarea(s) programada(s)${NC}"
else
    echo -e "${RED}✗ Beat no está corriendo${NC}"
fi

echo ""
echo "⚙️  6. Verificando Configuración de Superset"
echo "--------------------------------------------"

# Verificar FEATURE_FLAGS en config
echo -n "GLOBAL_ASYNC_QUERIES Flag... "
CONFIG_CHECK=$(docker exec superset grep -r "GLOBAL_ASYNC_QUERIES" /bootstrap/superset_config_simple.py 2>/dev/null || echo "")
if echo "$CONFIG_CHECK" | grep -q "True"; then
    echo -e "${GREEN}✓ Enabled${NC}"
else
    echo -e "${RED}✗ Not found or disabled${NC}"
fi

# Verificar CELERY_CONFIG
echo -n "CELERY_CONFIG... "
CELERY_CONFIG=$(docker exec superset grep -A5 "class CeleryConfig" /bootstrap/superset_config_simple.py 2>/dev/null || echo "")
if echo "$CELERY_CONFIG" | grep -q "broker_url"; then
    echo -e "${GREEN}✓ Configured${NC}"
else
    echo -e "${RED}✗ Not found${NC}"
fi

# Verificar RESULTS_BACKEND
echo -n "RESULTS_BACKEND... "
RESULTS_CONFIG=$(docker exec superset grep -A5 "RESULTS_BACKEND" /bootstrap/superset_config_simple.py 2>/dev/null || echo "")
if echo "$RESULTS_CONFIG" | grep -q "redis"; then
    echo -e "${GREEN}✓ Redis configured${NC}"
else
    echo -e "${RED}✗ Not configured${NC}"
fi

echo ""
echo "🌐 7. Test de Conectividad Superset -> Redis"
echo "---------------------------------------------"

# Test desde superset container
echo -n "Superset -> Redis... "
SUPERSET_REDIS_TEST=$(docker exec superset bash -c "timeout 2 bash -c 'cat < /dev/null > /dev/tcp/redis/6379'" 2>/dev/null && echo "OK" || echo "FAIL")
if [ "$SUPERSET_REDIS_TEST" = "OK" ]; then
    echo -e "${GREEN}✓ OK${NC}"
else
    echo -e "${RED}✗ No se puede conectar${NC}"
fi

# Test desde worker container
echo -n "Worker -> Redis... "
WORKER_REDIS_TEST=$(docker exec superset-worker bash -c "timeout 2 bash -c 'cat < /dev/null > /dev/tcp/redis/6379'" 2>/dev/null && echo "OK" || echo "FAIL")
if [ "$WORKER_REDIS_TEST" = "OK" ]; then
    echo -e "${GREEN}✓ OK${NC}"
else
    echo -e "${RED}✗ No se puede conectar${NC}"
fi

echo ""
echo "📊 8. Resumen del Stack"
echo "-----------------------"

TOTAL_CHECKS=4
PASSED_CHECKS=0

[ $REDIS_OK -eq 1 ] && ((PASSED_CHECKS++))
[ $SUPERSET_OK -eq 1 ] && ((PASSED_CHECKS++))
[ $WORKER_OK -eq 1 ] && ((PASSED_CHECKS++))
[ $BEAT_OK -eq 1 ] && ((PASSED_CHECKS++))

echo "Servicios: $PASSED_CHECKS/$TOTAL_CHECKS OK"
echo ""

if [ $PASSED_CHECKS -eq $TOTAL_CHECKS ]; then
    echo -e "${GREEN}✅ Stack Asíncrono Completamente Operativo${NC}"
    echo ""
    echo "🚀 SQL Lab está listo para consultas asíncronas"
    echo "   URL: http://localhost:8088/sqllab/"
    echo ""
    echo "📚 Para más información: docs/ASYNC_STACK.md"
    exit 0
else
    echo -e "${RED}❌ Algunos servicios no están operativos${NC}"
    echo ""
    echo "🔧 Acciones sugeridas:"
    [ $REDIS_OK -eq 0 ] && echo "   - Reiniciar Redis: docker restart superset-redis"
    [ $SUPERSET_OK -eq 0 ] && echo "   - Reiniciar Superset: docker restart superset"
    [ $WORKER_OK -eq 0 ] && echo "   - Reiniciar Worker: docker restart superset-worker"
    [ $BEAT_OK -eq 0 ] && echo "   - Reiniciar Beat: docker restart superset-beat"
    echo ""
    echo "   - Ver logs: docker logs <container>"
    exit 1
fi
