#!/bin/bash

# Cargar variables de entorno desde .env si existe
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
    echo "[ETL] Variables de entorno cargadas desde .env"
else
    echo "[ETL] Advertencia: No se encontró archivo .env, algunas variables pueden faltar."
fi

# Levanta automáticamente todo el pipeline ETL con inicialización completa
# 
# USO:
#   ./start_etl_pipeline.sh           # Inicio normal con orquestación automática
#   ./start_etl_pipeline.sh --clean   # Inicio forzando limpieza completa
#   ./start_etl_pipeline.sh --manual  # Inicio sin orquestación (modo manual)
#   ./start_etl_pipeline.sh --help    # Mostrar ayuda
#!/bin/bash
# Levanta automáticamente todo el pipeline ETL con inicialización completa
#
# USO:
#   ./start_etl_pipeline.sh           # Inicio normal con orquestación automática
#   ./start_etl_pipeline.sh --clean   # Inicio forzando limpieza completa
#   ./start_etl_pipeline.sh --manual  # Inicio sin orquestación (modo manual)
#   ./start_etl_pipeline.sh --help    # Mostrar ayuda
#
# Este script:
# 1. Verifica dependencias
# 2. Levanta servicios Docker Compose
# 3. Ejecuta orquestación automática (limpieza, configuración, despliegue)
# 4. Muestra estado final del pipeline

set -euo pipefail

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Función para imprimir con colores
print_header() {
    echo -e "${PURPLE}================================================================================================${NC}"
    echo -e "${PURPLE} $1${NC}"
    echo -e "${PURPLE}================================================================================================${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
ENV_FILE="$SCRIPT_DIR/.env"
LOGS_DIR="$SCRIPT_DIR/logs"

# Flags
FORCE_CLEAN=false
MANUAL_MODE=false
SHOW_HELP=false

# Parsear argumentos
while [[ $# -gt 0 ]]; do
    case $1 in
        --clean)
            FORCE_CLEAN=true
            shift
            ;;
        --manual)
            MANUAL_MODE=true
            shift
            ;;
        --help|-h)
            SHOW_HELP=true
            shift
            ;;
        *)
            print_error "Argumento desconocido: $1"
            SHOW_HELP=true
            shift
            ;;
    esac
done

# Mostrar ayuda si se solicita
if [ "$SHOW_HELP" = true ]; then
    echo -e "${CYAN}"
    cat << 'EOF'
🚀 PIPELINE ETL - SCRIPT DE INICIO AUTOMÁTICO

DESCRIPCIÓN:
  Este script levanta completamente el pipeline ETL con inicialización automática.
  Incluye limpieza, configuración de usuarios, descubrimiento de esquemas, 
  creación de modelos, aplicación de conectores y validación completa.

USO:
  ./start_etl_pipeline.sh [OPCIONES]

OPCIONES:
  --clean     Forzar limpieza completa antes de inicializar
  --manual    Modo manual sin orquestación automática  
  --help, -h  Mostrar esta ayuda

EJEMPLOS:
  ./start_etl_pipeline.sh           # Inicio normal
  ./start_etl_pipeline.sh --clean   # Inicio con limpieza forzada
  ./start_etl_pipeline.sh --manual  # Inicio manual (solo servicios)

FASES DE INICIALIZACIÓN AUTOMÁTICA:
  1. 🧹 Limpieza completa de datos existentes
  2. 👥 Configuración de usuarios y permisos  
  3. 🔍 Descubrimiento automático de esquemas MySQL
  4. 🏗️  Creación de modelos optimizados en ClickHouse
  5. 🔌 Despliegue automático de conectores Debezium
  6. ✅ Validación completa del pipeline

SERVICIOS INCLUIDOS:
  - ClickHouse (base de datos analítica)
  - Kafka + Kafka Connect (streaming de datos)
  - Superset (visualización)
  - Orquestador automático (inicialización)

LOGS:
  Los logs detallados se guardan en ./logs/
  
PUERTO DE ACCESO:
  - Superset: http://localhost:8088 (admin/Admin123!)
  - ClickHouse: http://localhost:8123
  - Kafka Connect: http://localhost:8083

EOF
    echo -e "${NC}"
    exit 0
fi

# Función para verificar dependencias
check_dependencies() {
    print_info "Verificando dependencias..."
    
    # Verificar Docker
    if ! command -v docker &> /dev/null; then
        print_error "Docker no está instalado"
        exit 1
    fi
    
    # Verificar Docker Compose
    if ! command -v docker compose &> /dev/null; then
        print_error "Docker Compose no está disponible"
        exit 1
    fi
    
    # Verificar archivo docker-compose.yml
    if [ ! -f "$COMPOSE_FILE" ]; then
        print_error "Archivo docker-compose.yml no encontrado en $COMPOSE_FILE"
        exit 1
    fi
    
    # Verificar archivo .env
    if [ ! -f "$ENV_FILE" ]; then
        print_warning "Archivo .env no encontrado, se usarán valores por defecto"
    fi
    
    print_success "Todas las dependencias verificadas"
}

# Función para asegurar red Docker (delegar a Compose y evitar etiquetas inválidas)
ensure_docker_network() {
    print_info "Verificando red Docker..."
    NETWORK_NAME="etl_prod_etl_net"
    # Si existe una red previa sin etiquetas de compose correctas, eliminarla
    if docker network inspect "$NETWORK_NAME" &> /dev/null; then
        print_info "Eliminando red existente para que Docker Compose la recree con etiquetas correctas..."
        docker network rm "$NETWORK_NAME" >/dev/null 2>&1 || true
    fi
    print_success "Docker Compose creará la red automáticamente"
}

# Función para limpiar servicios previos si es necesario
cleanup_previous_services() {
    if [ "$FORCE_CLEAN" = true ]; then
        print_info "Limpieza forzada solicitada..."
        
        print_info "Deteniendo servicios existentes..."
        docker compose -f "$COMPOSE_FILE" down --remove-orphans -v 2>/dev/null || true
        
        print_info "Limpiando imágenes de herramientas ETL..."
        docker rmi etl-tools:latest pipeline-gen:latest 2>/dev/null || true
        
        print_success "Limpieza previa completada"
    fi
}

# Función para crear directorio de logs
ensure_logs_directory() {
    if [ ! -d "$LOGS_DIR" ]; then
        mkdir -p "$LOGS_DIR"
        print_success "Directorio de logs creado: $LOGS_DIR"
    fi
}

# Función para levantar servicios
start_services() {
    print_info "Iniciando servicios Docker Compose..."
    
    # Determinar servicios a levantar
    if [ "$MANUAL_MODE" = true ]; then
        print_info "Modo manual: levantando solo servicios base"
        SERVICES="clickhouse kafka connect superset superset-init"
    else
        print_info "Modo automático: levantando servicios con orquestador"
        SERVICES="" # Levantar todos los servicios
    fi
    
    # Levantar servicios
    if [ -n "$SERVICES" ]; then
        docker compose -f "$COMPOSE_FILE" up -d $SERVICES
    else
        docker compose -f "$COMPOSE_FILE" up -d
    fi
    
    print_success "Servicios iniciados"
}

# Función para esperar que los servicios estén listos
wait_for_services() {
    print_info "Esperando a que los servicios estén listos..."
    
    # Esperar ClickHouse
    print_info "Esperando ClickHouse..."
    # Diagnóstico: healthcheck, ping HTTP y query simple, con reintentos
    local ch_deadline=$((SECONDS + 240))
    local ch_ok=false
    while [ $SECONDS -lt $ch_deadline ]; do
        # 1) Verificar health de contenedor
        if docker compose ps --format 'table {{.Name}}\t{{.State}}' | grep -q '^clickhouse\s\+running$'; then
            # 2) Verificar ping HTTP
            if curl -sf http://localhost:8123/ping >/dev/null 2>&1; then
                # 3) Verificar query nativa
                if docker compose exec clickhouse clickhouse-client --query "SELECT 1" >/dev/null 2>&1; then
                    ch_ok=true
                    break
                fi
            fi
        fi
        sleep 2
    done
    if [ "$ch_ok" = true ]; then
        print_success "ClickHouse listo"
    else
        print_error "ClickHouse no respondió en tiempo esperado (health/ping/query). Revisa logs: logs/clickhouse_setup.log"
        return 1
    fi
    
        # Esperar Kafka (health + operación básica)
        print_info "Esperando Kafka..."
        timeout 240 bash -c '
            cid=$(docker compose ps -q kafka)
            until [ -n "$cid" ]; do sleep 2; cid=$(docker compose ps -q kafka); done
            until [ "$(docker inspect -f "{{.State.Health.Status}}" "$cid")" = "healthy" ]; do sleep 2; done
            # Validar que kafka-topics funciona dentro del contenedor
            for i in $(seq 1 10); do
                if docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
                    exit 0
                fi
                sleep 2
            done
            exit 1
        ' || {
                print_error "Kafka no respondió en tiempo esperado"
                return 1
        }
        print_success "Kafka listo"
    
    # Esperar Kafka Connect
    print_info "Esperando Kafka Connect..."
    timeout 240 bash -c 'until curl -sf http://localhost:8083/connectors &>/dev/null; do sleep 3; done' || {
        print_error "Kafka Connect no respondió en tiempo esperado"
        return 1
    }
    print_success "Kafka Connect listo"
    
    print_success "Todos los servicios base están listos"
}

# Función para mostrar estado de la orquestación
show_orchestration_status() {
    if [ "$MANUAL_MODE" = true ]; then
        print_info "Modo manual activo - No hay orquestación automática"
        return 0
    fi
    
    print_info "Verificando estado de la orquestación automática..."
    
    # Esperar a que el orquestador termine (máximo 20 minutos)
    local timeout=1200
    local elapsed=0
    local interval=10
    
    while [ $elapsed -lt $timeout ]; do
        # Obtener estado del contenedor del orquestador (puede no haber arrancado aún)
        local cid
        cid=$(docker compose -f "$COMPOSE_FILE" ps -q etl-orchestrator 2>/dev/null || true)
        local status=""
        if [ -n "$cid" ]; then
            status=$(docker inspect -f '{{.State.Status}}' "$cid" 2>/dev/null || true)
        fi

        # Mostrar progreso cada minuto desde el log detallado, si existe
        if [ $((elapsed % 60)) -eq 0 ]; then
            local minutes=$((elapsed / 60))
            if [ $elapsed -eq 0 ]; then
                print_info "⏳ Esperando orquestación (el orquestador arranca cuando Superset está healthy)..."
            else
                print_info "⏳ Orquestación en progreso... (${minutes} minutos)"
            fi
            if [ -f "$LOGS_DIR/auto_pipeline_detailed.log" ]; then
                echo -e "${CYAN}📋 Últimas actividades:${NC}"
                tail -n 5 "$LOGS_DIR/auto_pipeline_detailed.log" | sed 's/^/   /'
            fi
        fi

        # Si hay archivo de estado y marca SUCCESS o FAILURE, salimos
        if [ -f "$LOGS_DIR/auto_pipeline_status.json" ]; then
            if grep -q '"overall"[[:space:]]*:[[:space:]]*"SUCCESS"' "$LOGS_DIR/auto_pipeline_status.json"; then
                print_success "🎉 Orquestación completada exitosamente"
                return 0
            fi
            if grep -q '"overall"[[:space:]]*:[[:space:]]*"FAIL' "$LOGS_DIR/auto_pipeline_status.json"; then
                print_warning "⚠️  Orquestación completada con errores"
                return 1
            fi
        fi

        # Si el contenedor del orquestador ya terminó (exited) y no hay estado, detenemos espera
        if [ "$status" = "exited" ] || [ "$status" = "dead" ]; then
            break
        fi

        sleep $interval
        elapsed=$((elapsed + interval))
    done

    # Resultado final por archivos de estado
    if [ -f "$LOGS_DIR/auto_pipeline_status.json" ]; then
        if grep -q '"overall"[[:space:]]*:[[:space:]]*"SUCCESS"' "$LOGS_DIR/auto_pipeline_status.json"; then
            print_success "🎉 Orquestación completada exitosamente"
            return 0
        elif grep -q '"overall"[[:space:]]*:[[:space:]]*"' "$LOGS_DIR/auto_pipeline_status.json"; then
            print_warning "⚠️  Orquestación completada con advertencias"
            return 1
        fi
    fi

    print_warning "⏰ Orquestación en progreso o estado indeterminado"
    return 1
}

# Función para mostrar estado final
show_final_status() {
    print_header "ESTADO FINAL DEL PIPELINE ETL"
    
    # Ejecutar validador de estado si es posible
    if docker ps --format "table {{.Names}}" | grep -q "etl-orchestrator"; then
        print_info "Ejecutando validación final..."
        docker compose exec etl-orchestrator python tools/pipeline_status.py 2>/dev/null || {
            print_warning "No se pudo ejecutar validación automática"
        }
    elif [ -f "$SCRIPT_DIR/tools/pipeline_status.py" ]; then
        print_info "Ejecutando validación externa..."
        cd "$SCRIPT_DIR" && python3 tools/pipeline_status.py 2>/dev/null || {
            print_warning "No se pudo ejecutar validación externa"
        }
    fi
    
    # Mostrar servicios activos
    print_info "Servicios activos:"
    docker compose -f "$COMPOSE_FILE" ps --format "table {{.Service}}\t{{.State}}\t{{.Ports}}"
    
    # Información de acceso
    echo -e "${CYAN}"
    echo "🌐 ACCESO A SERVICIOS:"
    echo "   📊 Superset:      http://localhost:8088  (admin/Admin123!)"
    echo "   🏠 ClickHouse:    http://localhost:8123"
    echo "   🔌 Kafka Connect: http://localhost:8083/connectors"
    echo ""
    echo "📋 LOGS DETALLADOS:"
    echo "   📁 Directorio:    $LOGS_DIR"
    echo "   🎯 Orquestador:   $LOGS_DIR/auto_pipeline_detailed.log"
    echo "   📈 Estado JSON:   $LOGS_DIR/auto_pipeline_status.json"
    echo ""
    echo "🛠️  COMANDOS ÚTILES:"
    echo "   Ver logs:         docker compose logs -f [servicio]"
    echo "   Estado:           python3 tools/pipeline_status.py"
    echo "   Detener:          docker compose down"
    echo -e "${NC}"

    # Mostrar una pequeña muestra del log del orquestador si existe
    if [ -f "$LOGS_DIR/auto_pipeline_detailed.log" ]; then
        print_info "Últimas 10 líneas del orquestador:"
        tail -n 10 "$LOGS_DIR/auto_pipeline_detailed.log" | sed 's/^/   /'
    fi
}

# Función principal
main() {
    print_header "🚀 INICIANDO PIPELINE ETL AUTOMÁTICO"

    print_header "🧹 FASE 1: LIMPIEZA Y DEPENDENCIAS"
    check_dependencies
    ensure_docker_network
    ensure_logs_directory
    cleanup_previous_services


    print_header "🏗️  FASE 2: LEVANTANDO SERVICIOS DOCKER"
    # Levantar servicios, ignorando errores benignos (como imágenes ya existentes)
    set +e
    start_services || print_warning "Algunos servicios ya estaban creados o hubo warnings, continuando..."
    set -e
    print_info "Estado de los contenedores tras el build:"
    docker compose ps --format 'table {{.Name}}\t{{.State}}\t{{.Status}}' || print_warning "No se pudo mostrar el estado de los contenedores, continuando..."

    print_header "🔍 FASE 3: ESPERANDO SERVICIOS BASE"
    wait_for_services

    print_header "🤖 FASE 4: ORQUESTACIÓN AUTOMÁTICA Y PROGRESO"
    if show_orchestration_status; then
        print_success "🎉 Pipeline ETL iniciado exitosamente"
    else
        print_warning "⚠️  Pipeline ETL iniciado con advertencias"
    fi

    print_header "📊 FASE 5: ESTADO FINAL Y ACCESO"
    show_final_status

    # Ejecutar validación automática del pipeline y mostrar resultado
    if [ -f "$SCRIPT_DIR/tools/pipeline_status.py" ]; then
        print_info "Validando estado final del pipeline (datos, vistas, conectores, Superset)..."
        python3 "$SCRIPT_DIR/tools/pipeline_status.py" || print_warning "No se pudo ejecutar validación automática"
    fi

    print_header "✅ INICIO COMPLETADO"
}

# Trap para cleanup en caso de interrupción
trap 'print_warning "Script interrumpido por el usuario"; exit 1' INT TERM

# Ejecutar función principal
main "$@"