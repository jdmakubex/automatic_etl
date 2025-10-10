#!/bin/bash

# ClickHouse Multi-Database Setup Script
# ======================================
# Este script configura ClickHouse para múltiples bases de datos
# basándose en el JSON de conexiones MySQL definido en DB_CONNECTIONS

echo "🚀 INICIANDO CONFIGURACIÓN MULTI-DATABASE DE CLICKHOUSE"
echo "======================================================="

# Función para ejecutar comandos ClickHouse
execute_clickhouse_command() {
    local query="$1"
    local timeout=30
    local attempt=0
    local max_attempts=10
    
    while [ $attempt -lt $max_attempts ]; do
        if curl -s --max-time $timeout \
            --data "$query" \
            "http://${CLICKHOUSE_HTTP_HOST:-clickhouse}:${CLICKHOUSE_HTTP_PORT:-8123}/" \
            2>/dev/null; then
            return 0
        fi
        
        attempt=$((attempt + 1))
        echo "⏳ Intento $attempt/$max_attempts falló, reintentando en 5 segundos..."
        sleep 5
    done
    
    echo "❌ Error: No se pudo ejecutar comando ClickHouse después de $max_attempts intentos"
    return 1
}

# Esperar que ClickHouse esté listo
echo "⏳ Esperando que ClickHouse esté disponible..."
for i in {1..30}; do
    if execute_clickhouse_command "SELECT 1" > /dev/null 2>&1; then
        echo "✅ ClickHouse está disponible"
        break
    fi
    echo "   Intento $i/30: ClickHouse no disponible, esperando..."
    sleep 2
done

# Verificar disponibilidad final
if ! execute_clickhouse_command "SELECT 1" > /dev/null 2>&1; then
    echo "❌ Error: ClickHouse no está disponible después de 60 segundos"
    exit 1
fi

# Generar configuración multi-database
echo "🏗️  Generando configuración multi-database..."
if [ -f "/bootstrap/generate_multi_databases.py" ]; then
    python3 /bootstrap/generate_multi_databases.py
    if [ $? -eq 0 ]; then
        echo "✅ Configuración multi-database generada"
    else
        echo "❌ Error generando configuración multi-database"
        exit 1
    fi
else
    echo "⚠️  Archivo generate_multi_databases.py no encontrado, usando configuración por defecto"
fi

# Ejecutar SQL generado
echo "📊 Aplicando configuración de bases de datos..."
if [ -f "/bootstrap/clickhouse_multi_init.sql" ]; then
    echo "🔧 Ejecutando clickhouse_multi_init.sql..."
    if execute_clickhouse_command "$(cat /bootstrap/clickhouse_multi_init.sql)"; then
        echo "✅ Configuración multi-database aplicada exitosamente"
    else
        echo "❌ Error aplicando configuración multi-database"
        # No salir con error, puede ser porque ya existe
    fi
else
    echo "⚠️  Archivo clickhouse_multi_init.sql no encontrado, usando configuración por defecto"
    
    # Configuración fallback
    echo "🔧 Aplicando configuración fallback..."
    execute_clickhouse_command "
    CREATE USER IF NOT EXISTS etl IDENTIFIED BY 'Et1Ingest!';
    CREATE USER IF NOT EXISTS superset IDENTIFIED BY 'Sup3rS3cret!';
    CREATE DATABASE IF NOT EXISTS fgeo_default;
    GRANT SELECT, INSERT, CREATE, ALTER, DROP ON fgeo_default.* TO etl WITH GRANT OPTION;
    GRANT SELECT ON fgeo_default.* TO superset;
    "
fi

# Verificar configuración
echo "🔍 Verificando bases de datos creadas..."
databases=$(execute_clickhouse_command "SHOW DATABASES" | grep "fgeo_" || true)
if [ -n "$databases" ]; then
    echo "✅ Bases de datos ClickHouse detectadas:"
    echo "$databases" | while read -r db; do
        echo "   📊 $db"
    done
else
    echo "⚠️  No se detectaron bases de datos fgeo_*, verificando configuración..."
fi

# Verificar usuarios
echo "🔍 Verificando usuarios..."
if execute_clickhouse_command "SELECT name FROM system.users WHERE name IN ('etl', 'superset')" > /dev/null 2>&1; then
    echo "✅ Usuarios etl y superset verificados"
else
    echo "⚠️  No se pudo verificar usuarios (puede ser por permisos), pero continuando..."
fi

echo "🎉 CONFIGURACIÓN MULTI-DATABASE DE CLICKHOUSE COMPLETADA"
echo "========================================================"