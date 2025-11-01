#!/bin/bash
"""
Script para reiniciar completamente Metabase y configurarlo desde cero
"""

echo "🔄 REINICIO COMPLETO DE METABASE"
echo "================================="

# 1. Detener Metabase
echo "⏹️  Deteniendo Metabase..."
docker-compose stop metabase

# 2. Eliminar volumen de Metabase para reset completo
echo "🗑️  Eliminando datos de Metabase (reset completo)..."
docker volume rm etl_prod_metabase-data 2>/dev/null || true
docker volume rm $(docker-compose config --services | grep metabase | head -1)_metabase-data 2>/dev/null || true

# 3. Eliminar contenedor
echo "🧹 Eliminando contenedor de Metabase..."
docker-compose rm -f metabase

# 4. Recrear y iniciar Metabase
echo "🚀 Recreando y iniciando Metabase..."
docker-compose up -d metabase

# 5. Esperar que Metabase esté disponible
echo "⏳ Esperando que Metabase inicie (esto puede tomar varios minutos)..."
for i in {1..30}; do
    if curl -s http://localhost:3000/api/health > /dev/null 2>&1; then
        echo "✅ Metabase disponible"
        break
    fi
    echo "   Intento $i/30 - esperando..."
    sleep 10
done

# 6. Verificar estado
if curl -s http://localhost:3000/api/health > /dev/null 2>&1; then
    echo ""
    echo "🎉 METABASE REINICIADO EXITOSAMENTE"
    echo "=================================="
    echo "🔗 URL: http://localhost:3000"
    echo "📝 Estado: Listo para configuración inicial"
    echo ""
    echo "Ahora ejecuta el setup automático:"
    echo "python3 tools/metabase_initial_setup.py"
else
    echo "❌ Error: Metabase no responde después del reinicio"
    echo "Verifica los logs: docker-compose logs metabase"
fi