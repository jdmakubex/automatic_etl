#!/bin/bash

# [DOCUMENTACIÓN] Configuración robusta de ClickHouse
# ---------------------------------------------------
# Este script configura ClickHouse de forma robusta para el pipeline ETL,
# incluyendo carga de variables de entorno, creación de bases de datos,
# usuarios y tablas necesarias para la ingesta.

echo "🚀 INICIANDO CONFIGURACIÓN ROBUSTA DE CLICKHOUSE"
echo "================================================="

# [PASO 1] Carga robusta de variables de entorno
echo "🔧 Cargando variables de entorno..."

if [ -f /app/.env.clean ]; then
    set -a
    source /app/.env.clean
    set +a
    echo "[ETL] ✅ Variables de entorno cargadas desde /app/.env.clean (limpio)"
elif [ -f /app/.env ]; then
    set -a
    source /app/.env
    set +a
    echo "[ETL] ✅ Variables de entorno cargadas desde /app/.env"
elif [ -f .env ]; then
    set -a
    source .env
    set +a
    echo "[ETL] ✅ Variables de entorno cargadas desde ./env (directorio actual)"
else
    echo "[ETL] ⚠️  Advertencia: No se encontró archivo .env, algunas variables pueden faltar."
fi

# [PASO 2] Validar variables críticas
echo "🔍 Validando variables críticas..."
REQUIRED_VARS=(CLICKHOUSE_HTTP_HOST CLICKHOUSE_HTTP_PORT CLICKHOUSE_PASSWORD)
MISSING_VARS=()

for VAR in "${REQUIRED_VARS[@]}"; do
    if [ -z "${!VAR}" ]; then
        MISSING_VARS+=("$VAR")
        echo "❌ Variable faltante: $VAR"
    else
        echo "✅ Variable definida: $VAR"
    fi
done

if [ ${#MISSING_VARS[@]} -gt 0 ]; then
    echo "❌ Faltan variables críticas: ${MISSING_VARS[*]}"
    exit 1
fi

# [PASO 3] Función para ejecutar comandos ClickHouse
execute_clickhouse_command() {
    local query="$1"
    echo "🔧 Ejecutando: $query"
    # Usar curl HTTP para ejecutar comandos en ClickHouse
    local user="${CLICKHOUSE_USER:-default}"
    local pass="${CLICKHOUSE_PASSWORD:-}"
    local host="${CLICKHOUSE_HTTP_HOST:-clickhouse}"
    local port="${CLICKHOUSE_HTTP_PORT:-8123}"
    local url="http://$host:$port/"
    if [ -n "$pass" ]; then
        curl -s -u "$user:$pass" -X POST "$url" -d "$query"
    else
        curl -s -X POST "$url" -d "$query"
    fi
    local exit_code=$?
    if [ $exit_code -eq 0 ]; then
        echo "✅ Comando ejecutado exitosamente"
        return 0
    else
        echo "❌ Error ejecutando comando (código: $exit_code)"
        return $exit_code
    fi
}

# [PASO 4] Verificar conectividad
echo "🔗 Verificando conectividad con ClickHouse..."
if ! execute_clickhouse_command "SELECT 1"; then
    echo "❌ No se puede conectar a ClickHouse via HTTP"
    exit 1
fi

# [PASO 5] Crear bases de datos
echo "🏗️  Creando bases de datos..."
execute_clickhouse_command "CREATE DATABASE IF NOT EXISTS fgeo_default"
execute_clickhouse_command "CREATE DATABASE IF NOT EXISTS fgeo_analytics"

# [PASO 6] Crear usuarios si no existen
echo "👥 Configurando usuarios..."

# Usuario ETL
execute_clickhouse_command "
CREATE USER IF NOT EXISTS etl 
IDENTIFIED WITH plaintext_password BY 'Et1Ingest!' 
SETTINGS profile = 'default'
"

# Usuario Superset
execute_clickhouse_command "
CREATE USER IF NOT EXISTS superset 
IDENTIFIED WITH plaintext_password BY 'Sup3rS3cret!' 
SETTINGS profile = 'default'
"

# [PASO 7] Otorgar permisos
echo "🔐 Configurando permisos..."

# Permisos GLOBALES para usuario ETL (para crear cualquier base de datos dinámicamente)
execute_clickhouse_command "GRANT ALL ON *.* TO etl WITH GRANT OPTION"

# Permisos para usuario Superset  
execute_clickhouse_command "GRANT SELECT ON fgeo_default.* TO superset"
execute_clickhouse_command "GRANT SELECT ON fgeo_analytics.* TO superset"
execute_clickhouse_command "GRANT SELECT ON *.* TO superset"

# [PASO 8] Parsear y procesar conexiones de base de datos
echo "📊 Procesando conexiones de bases de datos..."
if [ -f /app/bootstrap/parse_db_connections.py ]; then
    echo "🐍 Ejecutando parser de conexiones..."
    cd /app && python3 bootstrap/parse_db_connections.py
    
    if [ $? -eq 0 ]; then
        echo "✅ Conexiones procesadas exitosamente"
    else
        echo "⚠️  Parser de conexiones falló, continuando..."
    fi
else
    echo "⚠️  No se encontró parse_db_connections.py, omitiendo..."
fi

# [PASO 9] Verificar usuarios creados
echo "✅ Verificando usuarios creados..."
if execute_clickhouse_command "SELECT name FROM system.users WHERE name IN ('etl', 'superset')"; then
    echo "✅ Usuarios verificados correctamente"
else
    echo "⚠️  No se pudo verificar usuarios (permisos insuficientes), pero continuando..."
fi

# [PASO 10] Crear tabla de metadatos para seguimiento
echo "📋 Creando tabla de metadatos..."
execute_clickhouse_command "
CREATE TABLE IF NOT EXISTS fgeo_analytics.etl_metadata (
    connection_name String,
    source_type String,
    source_host String,
    source_database String,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (connection_name, created_at)
"

echo "🎉 CONFIGURACIÓN DE CLICKHOUSE COMPLETADA"
echo "========================================"
echo "✅ Bases de datos: fgeo_default, fgeo_analytics"
echo "✅ Usuarios: etl, superset"
echo "✅ Permisos configurados"
echo "✅ Tabla de metadatos creada"
echo ""