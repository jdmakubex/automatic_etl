#!/bin/bash
# Script para configurar usuarios automáticamente en ClickHouse
# Se ejecuta después del arranque del contenedor

echo "🔧 Configurando usuarios ETL automáticamente..."

# Esperar a que ClickHouse esté disponible
while ! clickhouse-client --query "SELECT 1" >/dev/null 2>&1; do
    echo "⏳ Esperando ClickHouse..."
    sleep 2
done

# Copiar archivo de usuarios automáticamente
echo "📋 Copiando configuración de usuarios..."
cp /app/users.xml /etc/clickhouse-server/users.d/01-custom-users.xml
chown clickhouse:clickhouse /etc/clickhouse-server/users.d/01-custom-users.xml
chmod 644 /etc/clickhouse-server/users.d/01-custom-users.xml

# Recargar configuración
echo "🔄 Recargando configuración..."
clickhouse-client --query "SYSTEM RELOAD CONFIG"

# Verificar usuarios
echo "🧪 Verificando usuarios ETL..."
if clickhouse-client --user etl --password Et1Ingest! --query "SELECT 'ETL OK'" >/dev/null 2>&1; then
    echo "✅ Usuario ETL configurado correctamente"
fi

if clickhouse-client --user superset --password Sup3rS3cret! --query "SELECT 'Superset OK'" >/dev/null 2>&1; then
    echo "✅ Usuario Superset configurado correctamente"
fi

echo "✅ Usuarios ETL configurados automáticamente"