#!/bin/bash

set -euo pipefail

echo "🚀 INICIANDO CONFIGURACIÓN ROBUSTA DE CLICKHOUSE"
echo "================================================="

# Función para ejecutar comando en ClickHouse con reintentos

execute_clickhouse_command() {
    local query="$1"
    local max_retries=5
    local retry=0
    while [ $retry -lt $max_retries ]; do
        if docker exec clickhouse clickhouse-client --password="$CLICKHOUSE_PASSWORD" --query="$query" 2>/dev/null; then
            return 0
        fi
        retry=$((retry + 1))
        echo "⚠️  Intento $retry/$max_retries falló, reintentando en 2 segundos..."
        sleep 2
    done
    echo "❌ Comando falló después de $max_retries intentos: $query"
    return 1
}


## Eliminado: Espera HTTP con curl (no disponible en contenedor)

# Espera robusta a que ClickHouse esté listo (cliente interno, desde contenedor)
for i in {1..20}; do
    if docker exec clickhouse clickhouse-client --password="$CLICKHOUSE_PASSWORD" --query="SELECT 1" 2>/dev/null; then
        echo "✅ ClickHouse está listo (cliente interno)"
        break
    fi
    echo "⏳ Esperando ClickHouse (cliente interno)... [$i/20]"
    sleep 2
    if [ "$i" -eq 20 ]; then
        echo "❌ Timeout esperando ClickHouse (cliente interno)"
        exit 1
    fi
done

# Crear base de datos
echo "📊 Creando base de datos fgeo_analytics..."
execute_clickhouse_command "CREATE DATABASE IF NOT EXISTS fgeo_analytics"


# Lógica robusta de recreación de usuarios y permisos
echo "🧹 Eliminando usuarios existentes para recreación..."
execute_clickhouse_command "DROP USER IF EXISTS etl" || true
execute_clickhouse_command "DROP USER IF EXISTS superset" || true

echo "👥 Creando usuario ETL..."
execute_clickhouse_command "CREATE USER etl IDENTIFIED BY 'Et1Ingest!'"

echo "👥 Creando usuario Superset..."
execute_clickhouse_command "CREATE USER superset IDENTIFIED BY 'Sup3rS3cret!'"


# Asignar permisos robustamente
echo "🔐 Asignando permisos al usuario ETL..."
execute_clickhouse_command "REVOKE ALL ON fgeo_analytics.* FROM etl" || true
execute_clickhouse_command "GRANT SELECT, INSERT, CREATE, ALTER, DROP ON fgeo_analytics.* TO etl WITH GRANT OPTION"

echo "🔐 Asignando permisos al usuario Superset..."
execute_clickhouse_command "REVOKE ALL ON fgeo_analytics.* FROM superset" || true
execute_clickhouse_command "GRANT SELECT ON fgeo_analytics.* TO superset"
execute_clickhouse_command "GRANT SELECT ON system.* TO superset"

# Verificar usuarios creados
echo "✅ Verificando usuarios creados..."
if execute_clickhouse_command "SELECT name FROM system.users WHERE name IN ('etl', 'superset')"; then
    echo "✅ Usuarios verificados correctamente"
else
    echo "❌ Error al verificar usuarios"
    exit 1
fi

echo "🎉 CONFIGURACIÓN DE CLICKHOUSE COMPLETADA"
echo "========================================"