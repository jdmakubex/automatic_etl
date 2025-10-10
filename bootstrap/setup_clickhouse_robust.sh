#!/bin/bash

# [DOCUMENTACIÓN] Carga robusta de variables de entorno
# -----------------------------------------------------
# El script busca el archivo .env en /app (volumen compartido),
# para garantizar que las variables estén disponibles tanto para Bash como para Python.
# IMPORTANTE: Se debe cargar ANTES de validar las variables.

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
    echo "[ETL] ⚠️  Advertencia: No se encontró archivo /app/.env.clean, /app/.env ni ./env, algunas variables pueden faltar."
fi

# Validar que las variables críticas están definidas DESPUÉS de cargarlas
echo "🔍 Validando variables críticas..."
REQUIRED_VARS=(CLICKHOUSE_HTTP_HOST CLICKHOUSE_HTTP_PORT CLICKHOUSE_PASSWORD)
MISSING_VARS=()
for VAR in "${REQUIRED_VARS[@]}"; do
    echo "🚀 INICIANDO CONFIGURACIÓN ROBUSTA DE CLICKHOUSE (delegado a Python)"
    echo "================================================="

    python3 /app/parse_db_connections.py
    ORDER BY (connection_name, created_at)
    "
    
    # Insertar metadatos de la conexión
    execute_clickhouse_command "
    INSERT INTO fgeo_analytics.$metadata_table 
    (connection_name, source_type, source_host, source_database)
    VALUES ('$connection_name', '$connection_type', '$connection_host', '$connection_db')
    "
    
    echo "  ✅ Esquema creado para conexión: $connection_name"
done

echo "🎯 Esquemas de conexiones creados exitosamente"








# Verificar usuarios creados
echo "✅ Verificando usuarios creados..."
if execute_clickhouse_command "SELECT name FROM system.users WHERE name IN ('etl', 'superset')"; then
    echo "✅ Usuarios verificados correctamente"
else
    echo "⚠️  No se pudo verificar usuarios (permisos insuficientes), pero continuando..."
    echo "ℹ️  Los usuarios se crean automáticamente en el proceso de inicialización"
fi

echo "🎉 CONFIGURACIÓN DE CLICKHOUSE COMPLETADA"
echo "========================================"