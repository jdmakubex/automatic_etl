#!/usr/bin/env bash
# -*- coding: utf-8 -*-
# Script para ejecutar todas las pruebas y validaciones del pipeline ETL

set -euo pipefail

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Función para logging
log_info() {
    echo -e "${GREEN}[INFO]${NC} $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*" >&2
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $*"
}

# Crear directorio de logs
mkdir -p logs

log_info "=== Ejecutando suite de pruebas ETL ==="
echo ""

# Variables de control
ENABLE_UNIT_TESTS=${ENABLE_UNIT_TESTS:-true}
ENABLE_PERMISSION_TESTS=${ENABLE_PERMISSION_TESTS:-true}
ENABLE_DEPENDENCY_VERIFICATION=${ENABLE_DEPENDENCY_VERIFICATION:-true}
ENABLE_VALIDATION=${ENABLE_VALIDATION:-true}

TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# 1. Pruebas unitarias
if [ "$ENABLE_UNIT_TESTS" = "true" ]; then
    log_info "[1/4] Ejecutando pruebas unitarias..."
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    if python -m unittest discover tests -v 2>&1 | tee logs/unit_tests.log; then
        log_info "✓ Pruebas unitarias: PASADAS"
        PASSED_TESTS=$((PASSED_TESTS + 1))
    else
        log_error "✗ Pruebas unitarias: FALLIDAS"
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi
    echo ""
else
    log_warning "[1/4] Pruebas unitarias deshabilitadas (ENABLE_UNIT_TESTS=false)"
    echo ""
fi

# 2. Validación de entorno
if [ "$ENABLE_VALIDATION" = "true" ]; then
    log_info "[2/4] Validando entorno y dependencias..."
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    if python tools/validators.py 2>&1 | tee logs/validation.log; then
        log_info "✓ Validación de entorno: PASADA"
        PASSED_TESTS=$((PASSED_TESTS + 1))
    else
        log_error "✗ Validación de entorno: FALLIDA"
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi
    echo ""
else
    log_warning "[2/4] Validación de entorno deshabilitada (ENABLE_VALIDATION=false)"
    echo ""
fi

# 3. Pruebas de permisos
if [ "$ENABLE_PERMISSION_TESTS" = "true" ]; then
    log_info "[3/4] Ejecutando pruebas de permisos..."
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    if python tools/test_permissions.py 2>&1 | tee logs/permission_tests.log; then
        log_info "✓ Pruebas de permisos: PASADAS"
        PASSED_TESTS=$((PASSED_TESTS + 1))
    else
        log_warning "⚠ Pruebas de permisos: FALLIDAS (puede ser normal si servicios no están corriendo)"
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi
    echo ""
else
    log_warning "[3/4] Pruebas de permisos deshabilitadas (ENABLE_PERMISSION_TESTS=false)"
    echo ""
fi

# 4. Verificación de dependencias
if [ "$ENABLE_DEPENDENCY_VERIFICATION" = "true" ]; then
    log_info "[4/4] Verificando dependencias y secuencialidad..."
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    if python tools/verify_dependencies.py 2>&1 | tee logs/dependency_verification.log; then
        log_info "✓ Verificación de dependencias: PASADA"
        PASSED_TESTS=$((PASSED_TESTS + 1))
    else
        log_warning "⚠ Verificación de dependencias: FALLIDA (puede ser normal si servicios no están corriendo)"
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi
    echo ""
else
    log_warning "[4/4] Verificación de dependencias deshabilitada (ENABLE_DEPENDENCY_VERIFICATION=false)"
    echo ""
fi

# Resumen
echo "=============================================="
log_info "Resumen de Pruebas:"
echo "  Total ejecutadas: $TOTAL_TESTS"
echo "  Pasadas: $PASSED_TESTS"
echo "  Fallidas: $FAILED_TESTS"
echo ""

if [ $FAILED_TESTS -eq 0 ] && [ $TOTAL_TESTS -gt 0 ]; then
    log_info "✓ Todas las pruebas pasaron exitosamente"
    echo ""
    log_info "Logs guardados en:"
    ls -1 logs/*.log logs/*.json 2>/dev/null || true
    exit 0
elif [ $TOTAL_TESTS -eq 0 ]; then
    log_warning "⚠ No se ejecutaron pruebas (todas deshabilitadas)"
    exit 0
else
    log_error "✗ Algunas pruebas fallaron"
    echo ""
    log_info "Logs guardados en:"
    ls -1 logs/*.log logs/*.json 2>/dev/null || true
    echo ""
    log_info "Para más información, consulta:"
    log_info "  - docs/ERROR_RECOVERY.md"
    log_info "  - docs/DEPENDENCIES.md"
    exit 1
fi
