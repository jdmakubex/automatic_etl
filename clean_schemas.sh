#!/bin/bash

# Script independiente para ejecutar solo la depuración de esquemas
# Uso: ./clean_schemas.sh

set -euo pipefail

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Funciones de utilidad
print_header() {
    echo -e "${CYAN}===========================================${NC}"
    echo -e "${CYAN}$1${NC}"
    echo -e "${CYAN}===========================================${NC}"
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

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# Función principal
main() {
    print_header "🧹 DEPURACIÓN INDEPENDIENTE DE ESQUEMAS"
    
    # Verificar que los servicios estén corriendo
    if ! docker ps --format "{{.Names}}" | grep -q "clickhouse"; then
        print_error "ClickHouse no está corriendo. Inicia el pipeline primero."
        exit 1
    fi
    
    if ! docker ps --format "{{.Names}}" | grep -q "superset"; then
        print_error "Superset no está corriendo. Inicia el pipeline primero."
        exit 1
    fi
    
    print_info "Servicios detectados correctamente"
    
    # Ejecutar depuración
    if [ -f "$SCRIPT_DIR/tools/schema_cleaner.py" ]; then
        print_info "Iniciando depuración automática..."
        
        cd "$SCRIPT_DIR"
        if python3 tools/schema_cleaner.py; then
            print_success "Depuración completada exitosamente"
            
            # Mostrar resumen si existe el reporte
            if [ -f "$SCRIPT_DIR/logs/schema_cleanup_report.json" ]; then
                print_info "Resumen de depuración:"
                
                # Extraer información clave del JSON usando grep y sed
                USEFUL_TABLES=$(grep '"useful_tables_kept"' logs/schema_cleanup_report.json | sed 's/.*: *\([0-9]*\).*/\1/')
                REMOVED_TABLES=$(grep '"tables_removed"' logs/schema_cleanup_report.json | sed 's/.*: *\([0-9]*\).*/\1/')
                REMOVED_DBS=$(grep '"databases_removed"' logs/schema_cleanup_report.json | sed 's/.*: *\([0-9]*\).*/\1/')
                TOTAL_RECORDS=$(grep '"total_records_kept"' logs/schema_cleanup_report.json | sed 's/.*: *\([0-9]*\).*/\1/')
                
                echo -e "   📊 Tablas útiles mantenidas: ${GREEN}${USEFUL_TABLES:-0}${NC}"
                echo -e "   🗑️  Tablas eliminadas: ${YELLOW}${REMOVED_TABLES:-0}${NC}"
                echo -e "   🗑️  Bases de datos eliminadas: ${YELLOW}${REMOVED_DBS:-0}${NC}"
                echo -e "   💾 Registros conservados: ${GREEN}${TOTAL_RECORDS:-0}${NC}"
            fi
            
            # Información de acceso
            echo ""
            print_info "Acceso a herramientas limpias:"
            echo -e "   📊 Superset: ${CYAN}http://localhost:8088${NC} (admin/admin)"
            echo -e "   📈 Metabase: ${CYAN}http://localhost:3000${NC}"
            echo -e "   📋 Guía Metabase: ${CYAN}logs/metabase_clean_guide.md${NC}"
            
        else
            print_error "Falló la depuración automática"
            exit 1
        fi
    else
        print_error "Script de depuración no encontrado: tools/schema_cleaner.py"
        exit 1
    fi
    
    print_header "🎉 DEPURACIÓN COMPLETADA"
}

# Verificar argumentos de ayuda
if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    echo -e "${CYAN}"
    cat << 'EOF'
🧹 DEPURACIÓN INDEPENDIENTE DE ESQUEMAS

DESCRIPCIÓN:
  Este script ejecuta únicamente la depuración de esquemas de ClickHouse,
  eliminando tablas vacías, bases de datos innecesarias y configurando
  automáticamente Superset y Metabase con solo las tablas útiles.

USO:
  ./clean_schemas.sh

REQUISITOS:
  - El pipeline ETL debe estar corriendo (./start_etl_pipeline.sh)
  - ClickHouse y Superset deben estar disponibles

QUÉ HACE:
  1. 🔍 Identifica tablas y bases de datos vacías o innecesarias
  2. 🗑️  Elimina esquemas que causan confusión
  3. 📊 Configura datasets automáticamente en Superset
  4. 📈 Genera guía limpia para Metabase
  5. 📋 Crea reporte detallado de la depuración

ARCHIVOS GENERADOS:
  - logs/schema_cleanup_report.json (reporte detallado)
  - logs/metabase_clean_guide.md (guía para Metabase)

EJEMPLOS:
  ./clean_schemas.sh                # Ejecutar depuración
  ./clean_schemas.sh --help         # Mostrar esta ayuda

EOF
    echo -e "${NC}"
    exit 0
fi

# Ejecutar función principal
main "$@"