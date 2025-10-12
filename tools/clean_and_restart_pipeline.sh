#!/usr/bin/env bash
# clean_and_restart_pipeline.sh
# Apaga, limpia y reinicia toda la infraestructura ETL automáticamente
set -e

# 1. Apagar todos los contenedores
if docker compose ps | grep -q 'Up'; then
  echo "🛑 Apagando contenedores..."
  docker compose down --remove-orphans
else
  echo "ℹ️ No hay contenedores activos."
fi

# 2. Limpiar volúmenes y archivos generados (sin borrar archivos críticos)
echo "🧹 Limpiando volúmenes y datos generados..."
docker volume prune -f
docker system prune -f --volumes
# Solo limpiar logs y datos generados, nunca .env, archivos de configuración, ni plantillas
find logs/ -type f ! -name '*.gitkeep' -delete
find generated/ -type f -delete
# No borrar .env, mysql_simple_connector.json, bootstrap/*.sql, bootstrap/*.yaml, etc.
# No borrar archivos en bootstrap/ ni superset_bootstrap/

# 3. Reiniciar contenedores
echo "🚀 Iniciando contenedores..."
docker compose up -d --build

# 4. Ejecutar orquestador maestro
echo "🎯 Ejecutando orquestador maestro..."
docker compose exec etl-orchestrator python3 tools/master_orchestrator.py

echo "✅ Proceso de reinicio y orquestación completo. Revisa los logs para resultados."
