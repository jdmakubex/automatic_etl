#!/bin/bash
# Script de limpieza TOTAL del sistema ETL
# Elimina: servicios, volúmenes, redes, logs, datos generados

set -e

echo "🧹 LIMPIEZA TOTAL DEL SISTEMA ETL"
echo "=================================================="
echo ""

# Detener y eliminar servicios
echo "⏹️  Deteniendo servicios..."
docker compose down 2>/dev/null || true

# Eliminar volúmenes
echo "🗑️  Eliminando volúmenes..."
docker compose down -v 2>/dev/null || true

# Eliminar volúmenes específicos del proyecto (por si acaso)
echo "🗑️  Eliminando volúmenes específicos del proyecto..."
docker volume rm etl_prod_ch_data 2>/dev/null || echo "     ✅ ch_data ya eliminado"
docker volume rm etl_prod_kafka_data 2>/dev/null || echo "     ✅ kafka_data ya eliminado"
docker volume rm etl_prod_connect_data 2>/dev/null || echo "     ✅ connect_data ya eliminado"
docker volume rm etl_prod_superset_home 2>/dev/null || echo "     ✅ superset_home ya eliminado"
docker volume rm etl_prod_etl_logs 2>/dev/null || echo "     ✅ etl_logs ya eliminado"

# Limpiar volúmenes huérfanos
echo "🗑️  Limpiando volúmenes huérfanos..."
docker volume prune -f

# Eliminar redes huérfanas
echo "🌐 Limpiando redes huérfanas..."
docker network prune -f

# Limpiar logs
echo "📝 Limpiando logs..."
rm -rf logs/*.json logs/*.log 2>/dev/null || true
touch logs/.gitkeep

# Limpiar archivos generados
echo "📁 Limpiando archivos generados..."
rm -rf generated/*/schemas/*.json 2>/dev/null || true
rm -rf generated/fiscalizacion generated/archivos generated/mysql generated/information_schema 2>/dev/null || true
find generated/ -type f -name "*.json" -delete 2>/dev/null || true

# Verificar estado final
echo ""
echo "✅ Limpieza completada"
echo ""
echo "📊 Estado final:"
echo "   - Volúmenes Docker:"
docker volume ls | grep etl_prod || echo "     ✅ No hay volúmenes de etl_prod"

echo "   - Redes Docker:"
docker network ls | grep etl_prod || echo "     ✅ No hay redes de etl_prod"

echo "   - Archivos logs:"
ls -1 logs/*.json logs/*.log 2>/dev/null | wc -l | xargs -I {} echo "     {} archivos de log"

echo "   - Directorios generated:"
find generated/ -mindepth 1 -maxdepth 1 -type d 2>/dev/null | wc -l | xargs -I {} echo "     {} directorios"

echo ""
echo "🎯 Sistema completamente limpio y listo para nueva ejecución"
