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
echo "   • Superset (interfaz de dashboards)"
echo "   • Orquestador ETL (automatización)"
echo

# Iniciar servicios en segundo plano
docker compose up -d

echo "✅ Servicios iniciados"
echo "⏳ El orquestador comenzará automáticamente..."
echo

# Esperar un momento para que los servicios se estabilicen
echo "⏳ Esperando estabilización inicial (30s)..."
sleep 30

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