#!/bin/bash
# Configuración automática de permisos ETL
# Se ejecuta automáticamente durante el pipeline

echo "🔧 Configurando permisos ETL automáticamente..."
docker compose exec etl-tools python3 tools/etl_permissions_setup.py

if [ $? -eq 0 ]; then
    echo "✅ Permisos ETL configurados correctamente"
else
    echo "❌ Error configurando permisos ETL"
    exit 1
fi
