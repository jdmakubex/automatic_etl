#!/bin/bash
set -e

echo "🚀 Iniciando configuración completa de Superset..."

# Variables de entorno con valores por defecto
SUPERSET_ADMIN=${SUPERSET_ADMIN:-admin}
SUPERSET_PASSWORD=${SUPERSET_PASSWORD:-Admin123!}

echo "📊 Verificando/creando usuario administrador..."

# Verificar si el usuario ya existe
if superset fab list-users | grep -q "username:$SUPERSET_ADMIN"; then
    echo "👤 Usuario $SUPERSET_ADMIN ya existe, eliminando para recrear..."
    superset fab delete-user --username "$SUPERSET_ADMIN" || echo "⚠️  No se pudo eliminar usuario existente"
fi

echo "➕ Creando usuario administrador..."
superset fab create-admin \
    --username "$SUPERSET_ADMIN" \
    --firstname Admin \
    --lastname User \
    --email admin@example.com \
    --password "$SUPERSET_PASSWORD"

if [ $? -eq 0 ]; then
    echo "✅ Usuario administrador creado exitosamente"
else
    echo "❌ Error al crear usuario administrador"
    exit 1
fi

echo "🔧 Actualizando base de datos de Superset..."
superset db upgrade

echo "🔑 Inicializando roles y permisos..."
superset init

echo "🔌 Importando configuración de ClickHouse..."
if [ -f /bootstrap/clickhouse_db.yaml ]; then
    echo "📋 Contenido del archivo de configuración:"
    cat /bootstrap/clickhouse_db.yaml
    echo ""
    echo "🔄 Ejecutando importación..."
    superset import-datasources -p /bootstrap/clickhouse_db.yaml
    if [ $? -eq 0 ]; then
        echo "✅ Configuración de ClickHouse importada exitosamente"
    else
        echo "❌ Error al importar configuración de ClickHouse"
    fi
else
    echo "❌ No se encontró archivo de configuración de ClickHouse"
fi

# Sincronizar metadatos y refrescar esquemas/tablas
echo "🔄 Sincronizando metadatos de bases de datos..."
superset db upgrade
superset init

echo "📋 Listando usuarios creados:"
superset fab list-users

echo "🎉 Inicialización de Superset completada!"
echo "📝 Credenciales de acceso:"
echo "   Usuario: $SUPERSET_ADMIN"
echo "   Contraseña: $SUPERSET_PASSWORD"
echo "   URL: http://localhost:8088"