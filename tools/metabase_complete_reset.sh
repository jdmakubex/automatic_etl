#!/bin/bash
# Script para reset completo y limpio de Metabase

echo "🔄 RESET COMPLETO DE METABASE - ELIMINACIÓN TOTAL"
echo "================================================="

# 1. Detener todos los servicios relacionados
echo "⏹️  Deteniendo servicios..."
docker-compose stop metabase metabase-db

# 2. Eliminar contenedores
echo "🗑️  Eliminando contenedores..."
docker-compose rm -f metabase metabase-db

# 3. Eliminar TODOS los volúmenes de Metabase
echo "🧹 Eliminando volúmenes de Metabase..."
docker volume rm etl_prod_metabase_data 2>/dev/null || true
docker volume rm etl_prod_metabase_db_data 2>/dev/null || true

# 4. Limpiar cualquier volumen huérfano
echo "🧽 Limpiando volúmenes huérfanos..."
docker volume prune -f

# 5. Recrear servicios completamente
echo "🚀 Recreando servicios Metabase desde cero..."
docker-compose up -d metabase-db
sleep 5
docker-compose up -d metabase

# 6. Esperar que Metabase esté completamente disponible
echo "⏳ Esperando que Metabase esté completamente disponible..."
echo "   (Esto puede tomar varios minutos para la configuración inicial)"

for i in {1..60}; do
    if curl -s http://localhost:3000/api/health > /dev/null 2>&1; then
        # Verificar que realmente esté listo para setup
        if curl -s http://localhost:3000/api/session/properties | grep -q "setup-token"; then
            echo "✅ Metabase disponible y listo para setup inicial"
            break
        fi
    fi
    echo "   Intento $i/60 - esperando inicialización completa..."
    sleep 10
done

# 7. Verificar estado final
if curl -s http://localhost:3000/api/health > /dev/null 2>&1; then
    if curl -s http://localhost:3000/api/session/properties | grep -q "setup-token"; then
        echo ""
        echo "🎉 METABASE COMPLETAMENTE REINICIADO"
        echo "==================================="
        echo "✅ Estado: Limpio y listo para configuración inicial"
        echo "🔗 URL: http://localhost:3000"
        echo "📝 Setup token disponible"
        echo ""
        echo "Ejecuta ahora el setup automático:"
        echo "python3 tools/metabase_direct_setup.py"
    else
        echo "⚠️  Metabase iniciado pero puede no estar en estado inicial limpio"
        echo "Intenta acceder manualmente: http://localhost:3000"
    fi
else
    echo "❌ Error: Metabase no responde después del reset completo"
    echo "Verifica logs: docker-compose logs metabase"
fi