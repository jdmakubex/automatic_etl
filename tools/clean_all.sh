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
docker volume rm etl_prod_redis_data 2>/dev/null || echo "     ✅ redis_data ya eliminado"
docker volume rm etl_prod_generated_data 2>/dev/null || echo "     ✅ generated_data ya eliminado"

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

# --- RECREAR CARPETAS NECESARIAS (para evitar fallos de mount en Docker/WSL) ---
echo "🔧 Recreando carpetas necesarias (vacías) para próximos despliegues..."
# Lista de rutas relativas que deben existir para los bind mounts en docker-compose
REQUIRED_DIRS=(
	"generated/default/schemas"
	"generated/default"
	"generated"
	"logs"
)

for d in "${REQUIRED_DIRS[@]}"; do
	if [ ! -d "$d" ]; then
		echo "   - Creando $d"
		mkdir -p "$d"
	else
		echo "   - Existe $d"
	fi
	# asegurar un .gitkeep para mantener la estructura en git y evitar que quede vacía
	if [ ! -f "$d/.gitkeep" ]; then
		touch "$d/.gitkeep"
	fi
done

# Asegurar permisos amplios (útil en WSL/Docker Desktop) para evitar problemas de acceso
chmod -R 0777 generated logs || true


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
