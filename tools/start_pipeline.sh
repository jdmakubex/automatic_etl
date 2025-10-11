#!/bin/bash
set -e

# =============================================================================
# 🚀 SCRIPT DE INICIALIZACIÓN COMPLETA DEL PIPELINE ETL
# =============================================================================
# Este script ejecuta todo el pipeline ETL usando las configuraciones 
# centralizadas del archivo .env
#
# Uso:
#   ./tools/start_pipeline.sh [--clean] [--validate-only]
#
# Opciones:
#   --clean        : Limpia todos los contenedores y volúmenes antes de iniciar
#   --validate-only: Solo valida la configuración sin ejecutar el pipeline
# =============================================================================

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Funciones de utilidad
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_section() {
    echo
    echo "============================================================"
    echo "🎯 $1"
    echo "============================================================"
}

# Verificar que estamos en el directorio correcto
if [[ ! -f "docker-compose.yml" ]] || [[ ! -f ".env" ]]; then
    log_error "Este script debe ejecutarse desde el directorio raíz del proyecto"
    log_error "Asegúrate de que existan docker-compose.yml y .env"
    exit 1
fi

# Cargar variables de entorno
if [[ -f ".env" ]]; then
    log_info "Cargando configuración desde .env..."
    export $(grep -v '^#' .env | xargs)
else
    log_error "Archivo .env no encontrado"
    exit 1
fi

# Procesar argumentos
CLEAN_MODE=false
VALIDATE_ONLY=false

for arg in "$@"; do
    case $arg in
        --clean)
            CLEAN_MODE=true
            shift
            ;;
        --validate-only)
            VALIDATE_ONLY=true
            shift
            ;;
        *)
            log_error "Argumento desconocido: $arg"
            echo "Uso: $0 [--clean] [--validate-only]"
            exit 1
            ;;
    esac
done

# Función de validación de configuración
validate_configuration() {
    print_section "VALIDACIÓN DE CONFIGURACIÓN"
    
    if [[ -f "tools/validate_config.py" ]]; then
        log_info "Ejecutando validador de configuración..."
        python3 tools/validate_config.py
        if [[ $? -ne 0 ]]; then
            log_error "Validación de configuración falló"
            exit 1
        fi
    else
        log_warning "Script de validación no encontrado, saltando..."
    fi
}

# Función de limpieza
clean_environment() {
    print_section "LIMPIEZA DEL ENTORNO"
    
    log_info "Deteniendo todos los servicios..."
    docker compose down --remove-orphans || true
    
    log_info "Eliminando volúmenes..."
    docker compose down -v || true
    
    log_info "Limpiando imágenes no utilizadas..."
    docker system prune -f || true
    
    log_info "Limpiando directorio generated..."
    rm -rf generated/* || true
    mkdir -p generated/default
    
    log_info "Limpiando logs antiguos..."
    rm -rf logs/* || true
    mkdir -p logs
    
    log_success "Entorno limpiado correctamente"
}

# Función principal de despliegue
deploy_pipeline() {
    print_section "DESPLIEGUE DEL PIPELINE ETL"
    
    log_info "Construyendo imágenes..."
    docker compose build --no-cache
    
    log_info "Iniciando servicios en orden de dependencias..."
    docker compose up -d
    
    log_info "Esperando a que todos los servicios estén saludables..."
    local max_wait=300  # 5 minutos
    local wait_time=0
    
    while [[ $wait_time -lt $max_wait ]]; do
        local unhealthy=$(docker compose ps --format json | jq -r 'select(.Health != "healthy" and .Health != "") | .Service' | wc -l)
        
        if [[ $unhealthy -eq 0 ]]; then
            log_success "Todos los servicios están saludables"
            break
        fi
        
        log_info "Esperando servicios... ($wait_time/${max_wait}s)"
        sleep 10
        wait_time=$((wait_time + 10))
    done
    
    if [[ $wait_time -ge $max_wait ]]; then
        log_error "Timeout esperando servicios saludables"
        docker compose ps
        exit 1
    fi
}

# Función de verificación post-despliegue
verify_pipeline() {
    print_section "VERIFICACIÓN DEL PIPELINE"
    
    log_info "Verificando conectividad de servicios..."
    
    # Verificar ClickHouse
    log_info "Verificando ClickHouse..."
    if docker compose exec clickhouse clickhouse-client --query "SELECT 1" >/dev/null 2>&1; then
        log_success "✅ ClickHouse respondiendo"
    else
        log_error "❌ ClickHouse no responde"
        return 1
    fi
    
    # Verificar Kafka
    log_info "Verificando Kafka..."
    if docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list >/dev/null 2>&1; then
        log_success "✅ Kafka respondiendo"
    else
        log_error "❌ Kafka no responde"
        return 1
    fi
    
    # Verificar Kafka Connect
    log_info "Verificando Kafka Connect..."
    if docker compose exec connect curl -s http://connect:8083/connectors >/dev/null 2>&1; then
        log_success "✅ Kafka Connect respondiendo"
    else
        log_error "❌ Kafka Connect no responde"
        return 1
    fi
    
    # Verificar Superset
    log_info "Verificando Superset..."
    if docker compose exec superset curl -s http://superset:8088/health >/dev/null 2>&1; then
        log_success "✅ Superset respondiendo"
    else
        log_warning "⚠️ Superset podría no estar completamente listo"
    fi
    
    log_success "Verificación de servicios completada"
}

# Función de configuración automática
auto_configure() {
    print_section "CONFIGURACIÓN AUTOMÁTICA"
    
    log_info "Generando configuración de pipeline..."
    docker compose exec pipeline-gen python3 tools/gen_pipeline.py || {
        log_error "Error generando configuración de pipeline"
        return 1
    }
    
    log_info "Aplicando conectores Debezium..."
    docker compose exec etl-tools python3 tools/apply_connectors_auto.py || {
        log_error "Error aplicando conectores"
        return 1
    }
    
    log_info "Configurando Superset..."
    docker compose exec etl-tools python3 tools/superset_auto_configurator.py || {
        log_warning "Configuración de Superset falló (podría necesitar configuración manual)"
    }
    
    log_success "Configuración automática completada"
}

# Función de reporte final
final_report() {
    print_section "REPORTE FINAL"
    
    echo "🎯 SERVICIOS DISPONIBLES:"
    echo "  📊 ClickHouse:    http://localhost:8123"
    echo "  🔗 Kafka Connect: http://localhost:8083"
    echo "  📈 Superset:      http://localhost:8088"
    echo "  🐘 Kafka:         localhost:19092"
    
    echo
    echo "👤 CREDENCIALES:"
    echo "  Superset Admin:   ${SUPERSET_ADMIN:-admin} / ${SUPERSET_PASSWORD:-Admin123!}"
    echo "  ClickHouse ETL:   ${CH_USER:-etl} / ${CH_PASSWORD:-Et1Ingest!}"
    
    echo
    echo "📋 COMANDOS ÚTILES:"
    echo "  Ver logs:         docker compose logs -f"
    echo "  Estado servicios: docker compose ps"
    echo "  Ejecutar scripts: docker compose exec etl-tools python3 tools/[script].py"
    echo "  Validar pipeline: docker compose exec etl-tools python3 tools/pipeline_status.py"
    
    echo
    log_success "🎉 ¡PIPELINE ETL INICIADO CORRECTAMENTE!"
}

# =============================================================================
# EJECUCIÓN PRINCIPAL
# =============================================================================

print_section "INICIO DEL PIPELINE ETL"
log_info "Configuración desde: $(pwd)/.env"
log_info "Proyecto: ${COMPOSE_PROJECT_NAME:-etl_prod}"

# 1. Validar configuración
validate_configuration

# Si solo validación, terminar aquí
if [[ "$VALIDATE_ONLY" == true ]]; then
    log_success "Validación completada. Use --clean para limpiar y desplegar."
    exit 0
fi

# 2. Limpiar si se solicitó
if [[ "$CLEAN_MODE" == true ]]; then
    clean_environment
fi

# 3. Desplegar servicios
deploy_pipeline

# 4. Verificar servicios
verify_pipeline

# 5. Configurar automáticamente
auto_configure

# 6. Reporte final
final_report

log_success "🚀 Pipeline ETL completamente operativo"