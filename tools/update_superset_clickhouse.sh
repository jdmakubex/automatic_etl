#!/bin/bash
# Script para actualizar Superset con nueva versión de clickhouse-sqlalchemy

set -e

echo "🔧 Actualizando Superset con clickhouse-sqlalchemy 0.3.2..."
echo "   (Esto corrige el error 'dict has no attribute set')"
echo ""

# Detener servicios de Superset
echo "1️⃣ Deteniendo servicios de Superset..."
docker compose stop superset superset-init superset-datasets superset-worker superset-beat

# Reconstruir imagen de Superset
echo ""
echo "2️⃣ Reconstruyendo imagen de Superset..."
docker compose build superset

# Reiniciar servicios
echo ""
echo "3️⃣ Iniciando servicios actualizados..."
docker compose up -d superset superset-worker superset-beat

# Esperar a que Superset esté listo
echo ""
echo "4️⃣ Esperando a que Superset esté disponible..."
sleep 10

# Re-ejecutar configuración de datasets (opcional, pero recomendado)
echo ""
echo "5️⃣ Re-aplicando configuración de datasets..."
docker compose up superset-datasets --force-recreate --no-deps

echo ""
echo "✅ Actualización completada!"
echo ""
echo "🧪 Para verificar:"
echo "   1. Ve a http://localhost:8088/sqllab"
echo "   2. Ejecuta tu query de prueba:"
echo "      SELECT id, cieps_id, formatovic_id"
echo "      FROM fiscalizacion.src__fiscalizacion__fiscalizacion__cieps_formatovic"
echo "      LIMIT 100"
echo ""
echo "   3. Debería funcionar sin el error 'dict has no attribute set'"
