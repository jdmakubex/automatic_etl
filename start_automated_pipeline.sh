#!/bin/bash
# Script maestro para iniciar el pipeline ETL automatizado
# Incluye monitoreo en tiempo real y validaciones

set -e

echo "🚀 INICIANDO PIPELINE ETL COMPLETAMENTE AUTOMATIZADO"
echo "=" * 70
echo "⏰ $(date)"
echo

# Función para limpiar recursos al salir
cleanup() {
    echo
    echo "🧹 Limpiando recursos..."
    # Aquí podrías agregar comandos de limpieza si es necesario
}
trap cleanup EXIT

# Verificar que Docker Compose esté disponible
echo "🔍 Verificando prerrequisitos..."
if ! command -v docker &> /dev/null; then
    echo "❌ Docker no está instalado o no está en PATH"
    exit 1
fi

if ! docker compose version &> /dev/null; then
    echo "❌ Docker Compose no está disponible"
    exit 1
fi

echo "✅ Docker y Docker Compose disponibles"

# Validar configuración de variables de entorno
echo "🔍 Validando configuración de seguridad..."
if docker compose exec etl-tools python3 tools/validate_environment.py > /dev/null 2>&1; then
    echo "✅ Variables de entorno configuradas correctamente"
else
    echo "❌ Problema en configuración de variables de entorno"
    echo "    Ejecuta: docker compose exec etl-tools python3 tools/validate_environment.py"
    echo "    para ver detalles del problema"
    exit 1
fi
echo

# Mostrar estado actual
echo "📊 Estado actual de contenedores:"
docker compose ps || echo "No hay contenedores ejecutándose"
echo

# Preguntar si continuar
read -p "¿Deseas iniciar el pipeline automatizado? (s/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Ss]$ ]]; then
    echo "👋 Cancelado por el usuario"
    exit 0
fi

echo "🚀 Iniciando todos los servicios..."
echo "   Esto incluye:"
echo "   • ClickHouse (base de datos analítica)"
echo "   • Kafka + Debezium Connect (CDC)"
echo "   • Configuración automática de permisos ETL"
echo "   • Superset (interfaz de dashboards)"
echo "   • Orquestador ETL (automatización)"
echo

# Iniciar servicios en segundo plano
docker compose up -d

echo "✅ Servicios iniciados"
echo "⏳ El orquestador comenzará automáticamente..."
echo

# Esperar un momento para que los servicios se estabilicen
echo "⏳ Esperando estabilización inicial (15s)..."
sleep 15

# CONFIGURACIÓN AUTOMÁTICA DE PERMISOS ETL
echo "🔧 Configurando permisos ETL automáticamente..."
if docker compose exec etl-tools python3 tools/etl_permissions_setup.py > /dev/null 2>&1; then
    echo "✅ Permisos ETL configurados correctamente"
else
    echo "⚠️  Advertencia: Algunos permisos ETL pueden requerir configuración manual"
    echo "    Revisa logs/etl_permissions_setup.log para detalles"
fi

echo "⏳ Esperando estabilización final (15s)..."
sleep 15

echo "📊 Iniciando monitor en tiempo real..."
echo "   (Puedes presionar Ctrl+C para detener el monitor sin afectar el pipeline)"
echo

# Iniciar monitor
python3 tools/monitor_pipeline.py

echo
echo "🏁 Monitor finalizado"
echo

# Mostrar estado final
echo "📋 ESTADO FINAL:"
echo "=" * 40

# Ejecutar validación final
if docker compose exec etl-tools python3 tools/validate_final_pipeline.py 2>/dev/null; then
    echo "✅ Validación final exitosa"
else
    echo "⚠️  Ejecutando validación final desde host..."
    python3 tools/validate_final_pipeline.py
fi

echo
echo "🔗 ACCESOS RÁPIDOS:"
echo "   📊 Superset: http://localhost:8088 (admin/admin)"
echo "   📄 Logs orquestador: docker logs etl_prod-etl-orchestrator-1"
echo "   📋 Estado servicios: docker compose ps"
echo "   🔍 Monitor: python3 tools/monitor_pipeline.py"

echo
echo "🎉 ¡Pipeline ETL automatizado completado!"