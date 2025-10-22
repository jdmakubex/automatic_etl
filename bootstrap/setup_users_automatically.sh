#!/bin/bash
# Script para configurar usuarios automáticamente en ClickHouse
# Se ejecuta después del arranque del contenedor

set -euo pipefail

echo "🔧 Configurando usuarios de ClickHouse desde variables de entorno..."

# Cargar variables con valores por defecto
CH_DEFAULT_USER=${CLICKHOUSE_USER:-${CLICKHOUSE_DEFAULT_USER:-default}}
CH_DEFAULT_PASSWORD=${CLICKHOUSE_PASSWORD:-${CLICKHOUSE_DEFAULT_PASSWORD:-ClickHouse123!}}
CH_ETL_USER=${CLICKHOUSE_ETL_USER:-etl}
CH_ETL_PASSWORD=${CLICKHOUSE_ETL_PASSWORD:-Et1Ingest!}
CH_SUPERSET_USER=${CLICKHOUSE_SUPERSET_USER:-superset_ro}
CH_SUPERSET_PASSWORD=${CLICKHOUSE_SUPERSET_PASSWORD:-Sup3rS3cret!}
CH_AUDITOR_USER=${CLICKHOUSE_AUDITOR_USER:-auditor}
CH_AUDITOR_PASSWORD=${CLICKHOUSE_AUDITOR_PASSWORD:-Audit0r123!}

echo "🔎 Usuarios: default=${CH_DEFAULT_USER}, etl=${CH_ETL_USER}, superset=${CH_SUPERSET_USER}, auditor=${CH_AUDITOR_USER}"

# Esperar a que ClickHouse esté disponible usando cliente nativo
echo "⏳ Esperando ClickHouse TCP..."
timeout=90
elapsed=0
while [ $elapsed -lt $timeout ]; do
        if clickhouse-client --query "SELECT 1" >/dev/null 2>&1; then
                echo "✅ ClickHouse TCP disponible"
                break
        fi
        echo "⏳ Esperando ClickHouse... ($elapsed/$timeout)s"
        sleep 3
        elapsed=$((elapsed + 3))
done

if [ $elapsed -ge $timeout ]; then
        echo "❌ Timeout esperando ClickHouse TCP después de ${timeout}s"
        exit 1
fi

# Renderizar XML de usuarios dinámicamente desde variables de entorno
cat >/etc/clickhouse-server/users.d/01-custom-users.xml <<XML
<clickhouse>
    <users>
        <${CH_DEFAULT_USER}>
            <password>${CH_DEFAULT_PASSWORD}</password>
            <access_management>1</access_management>
            <profile>default</profile>
            <networks><ip>::/0</ip></networks>
        </${CH_DEFAULT_USER}>

        <${CH_ETL_USER}>
            <password>${CH_ETL_PASSWORD}</password>
            <access_management>1</access_management>
            <profile>default</profile>
            <networks><ip>::/0</ip></networks>
        </${CH_ETL_USER}>

        <${CH_SUPERSET_USER}>
            <password>${CH_SUPERSET_PASSWORD}</password>
            <access_management>1</access_management>
            <profile>default</profile>
            <networks><ip>::/0</ip></networks>
        </${CH_SUPERSET_USER}>

        <${CH_AUDITOR_USER}>
            <password>${CH_AUDITOR_PASSWORD}</password>
            <access_management>1</access_management>
            <profile>default</profile>
            <networks><ip>::/0</ip></networks>
        </${CH_AUDITOR_USER}>
    </users>
</clickhouse>
XML

chown clickhouse:clickhouse /etc/clickhouse-server/users.d/01-custom-users.xml
chmod 644 /etc/clickhouse-server/users.d/01-custom-users.xml

# Recargar configuración
echo "🔄 Recargando configuración de usuarios..."
clickhouse-client --query "SYSTEM RELOAD CONFIG" >/dev/null 2>&1 || true

# Verificación básica de credenciales
echo "🧪 Verificando usuarios..."
if clickhouse-client --user "${CH_ETL_USER}" --password "${CH_ETL_PASSWORD}" --query "SELECT 'ETL OK'" >/dev/null 2>&1; then
    echo "✅ Usuario ${CH_ETL_USER} listo"
else
    echo "❌ Usuario ${CH_ETL_USER} no autenticó"
    exit 1
fi

if clickhouse-client --user "${CH_SUPERSET_USER}" --password "${CH_SUPERSET_PASSWORD}" --query "SELECT 'SS OK'" >/dev/null 2>&1; then
    echo "✅ Usuario ${CH_SUPERSET_USER} listo"
else
    echo "⚠️ Usuario ${CH_SUPERSET_USER} no autenticó (no bloqueante)"
fi

echo "✅ Usuarios configurados desde entorno"