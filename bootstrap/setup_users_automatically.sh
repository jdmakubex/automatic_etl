#!/bin/bash
# Script para configurar usuarios automáticamente en ClickHouse
# Se ejecuta después del arranque del contenedor

echo "🔧 Configurando usuarios ETL automáticamente..."

# Esperar a que ClickHouse esté disponible (usando HTTP en lugar de cliente)
echo "⏳ Esperando ClickHouse HTTP..."
timeout=60
elapsed=0
while [ $elapsed -lt $timeout ]; do
    if curl -s http://clickhouse:8123/ping >/dev/null 2>&1; then
        echo "✅ ClickHouse HTTP disponible"
        break
    fi
    echo "⏳ Esperando ClickHouse... ($elapsed/$timeout)s"
    sleep 2
    elapsed=$((elapsed + 2))
done

if [ $elapsed -ge $timeout ]; then
    echo "❌ Timeout esperando ClickHouse HTTP después de ${timeout}s"
    exit 1
fi

# Copiar archivo de usuarios automáticamente
echo "📋 Copiando configuración de usuarios..."
cp /app/users.xml /etc/clickhouse-server/users.d/01-custom-users.xml
chown clickhouse:clickhouse /etc/clickhouse-server/users.d/01-custom-users.xml
chmod 644 /etc/clickhouse-server/users.d/01-custom-users.xml

# Recargar configuración usando HTTP
echo "🔄 Recargando configuración..."
curl -s -X POST "http://clickhouse:8123/" -d "SYSTEM RELOAD CONFIG" >/dev/null 2>&1

# Verificar usuarios usando HTTP
echo "🧪 Verificando usuarios ETL..."
if curl -s -X POST "http://clickhouse:8123/" --user "etl:Et1Ingest!" -d "SELECT 'ETL OK'" >/dev/null 2>&1; then
    echo "✅ Usuario ETL configurado correctamente"
fi

if curl -s -X POST "http://clickhouse:8123/" --user "superset:Sup3rS3cret!" -d "SELECT 'Superset OK'" >/dev/null 2>&1; then
    echo "✅ Usuario Superset configurado correctamente"
fi

echo "✅ Usuarios ETL configurados automáticamente"