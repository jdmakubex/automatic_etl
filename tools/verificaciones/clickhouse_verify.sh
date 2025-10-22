#!/bin/bash
# Script de verificación automática de ClickHouse
# Ejecuta checks post-ingesta y guarda resultados en logs

set -e

TIMESTAMP=$(date '+%Y-%m-%d_%H-%M-%S')
LOG_DIR="${LOG_DIR:-/app/logs}"
OUTPUT_FILE="$LOG_DIR/clickhouse_verify_${TIMESTAMP}.log"
JSON_FILE="$LOG_DIR/clickhouse_verify_${TIMESTAMP}.json"

# Credenciales desde variables de entorno
CH_USER="${CLICKHOUSE_DEFAULT_USER:-default}"
CH_PASS="${CLICKHOUSE_DEFAULT_PASSWORD:-ClickHouse123!}"
CH_HOST="${CLICKHOUSE_HTTP_HOST:-clickhouse}"
CH_PORT="${CLICKHOUSE_HTTP_PORT:-8123}"

echo "🔍 VERIFICACIÓN DE CLICKHOUSE" | tee "$OUTPUT_FILE"
echo "========================================" | tee -a "$OUTPUT_FILE"
echo "⏰ Timestamp: $(date -Iseconds)" | tee -a "$OUTPUT_FILE"
echo "🖥️  Host: $CH_HOST:$CH_PORT" | tee -a "$OUTPUT_FILE"
echo "" | tee -a "$OUTPUT_FILE"

# Inicializar JSON
echo "{" > "$JSON_FILE"
echo "  \"timestamp\": \"$(date -Iseconds)\"," >> "$JSON_FILE"
echo "  \"host\": \"$CH_HOST:$CH_PORT\"," >> "$JSON_FILE"

# 1. Verificar conectividad
echo "📡 1. VERIFICANDO CONECTIVIDAD..." | tee -a "$OUTPUT_FILE"
if clickhouse-client --host="$CH_HOST" --port=9000 --user="$CH_USER" --password="$CH_PASS" --query="SELECT 1" &>/dev/null; then
    echo "   ✅ Conexión exitosa" | tee -a "$OUTPUT_FILE"
    echo "  \"connectivity\": {\"status\": \"ok\"}," >> "$JSON_FILE"
else
    echo "   ❌ Error de conexión" | tee -a "$OUTPUT_FILE"
    echo "  \"connectivity\": {\"status\": \"error\"}," >> "$JSON_FILE"
    echo "}" >> "$JSON_FILE"
    exit 1
fi
echo "" | tee -a "$OUTPUT_FILE"

# 2. Listar bases de datos
echo "📊 2. BASES DE DATOS EXISTENTES..." | tee -a "$OUTPUT_FILE"
clickhouse-client --host="$CH_HOST" --port=9000 --user="$CH_USER" --password="$CH_PASS" \
    --query="SELECT name FROM system.databases WHERE name NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA') ORDER BY name" \
    --format=PrettyCompact | tee -a "$OUTPUT_FILE"

# Guardar en JSON
DB_LIST=$(clickhouse-client --host="$CH_HOST" --port=9000 --user="$CH_USER" --password="$CH_PASS" \
    --query="SELECT name FROM system.databases WHERE name NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA') ORDER BY name" \
    --format=JSONCompact | jq -c '.data')
echo "  \"databases\": $DB_LIST," >> "$JSON_FILE"
echo "" | tee -a "$OUTPUT_FILE"

# 3. Listar tablas con conteo de registros
echo "📋 3. TABLAS Y CONTEO DE REGISTROS..." | tee -a "$OUTPUT_FILE"
clickhouse-client --host="$CH_HOST" --port=9000 --user="$CH_USER" --password="$CH_PASS" \
    --query="SELECT database, name, total_rows, formatReadableSize(total_bytes) as size 
             FROM system.tables 
             WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA') 
             AND name NOT LIKE '.inner%' 
             ORDER BY database, name" \
    --format=PrettyCompact | tee -a "$OUTPUT_FILE"

# Guardar en JSON
TABLES_JSON=$(clickhouse-client --host="$CH_HOST" --port=9000 --user="$CH_USER" --password="$CH_PASS" \
    --query="SELECT database, name, total_rows, total_bytes 
             FROM system.tables 
             WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA') 
             AND name NOT LIKE '.inner%' 
             ORDER BY database, name" \
    --format=JSONCompact | jq -c '.data')
echo "  \"tables\": $TABLES_JSON," >> "$JSON_FILE"
echo "" | tee -a "$OUTPUT_FILE"

# 4. Calcular totales
echo "📈 4. ESTADÍSTICAS GENERALES..." | tee -a "$OUTPUT_FILE"
TOTAL_ROWS=$(clickhouse-client --host="$CH_HOST" --port=9000 --user="$CH_USER" --password="$CH_PASS" \
    --query="SELECT SUM(total_rows) FROM system.tables WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA') AND name NOT LIKE '.inner%'" | tr -d '\n')
TOTAL_TABLES=$(clickhouse-client --host="$CH_HOST" --port=9000 --user="$CH_USER" --password="$CH_PASS" \
    --query="SELECT COUNT(*) FROM system.tables WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA') AND name NOT LIKE '.inner%'" | tr -d '\n')
TOTAL_SIZE=$(clickhouse-client --host="$CH_HOST" --port=9000 --user="$CH_USER" --password="$CH_PASS" \
    --query="SELECT formatReadableSize(SUM(total_bytes)) FROM system.tables WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA') AND name NOT LIKE '.inner%'" | tr -d '\n')

echo "   📊 Total de tablas: $TOTAL_TABLES" | tee -a "$OUTPUT_FILE"
echo "   📈 Total de registros: $TOTAL_ROWS" | tee -a "$OUTPUT_FILE"
echo "   💾 Tamaño total: $TOTAL_SIZE" | tee -a "$OUTPUT_FILE"

echo "  \"summary\": {" >> "$JSON_FILE"
echo "    \"total_tables\": $TOTAL_TABLES," >> "$JSON_FILE"
echo "    \"total_rows\": $TOTAL_ROWS," >> "$JSON_FILE"
echo "    \"total_size\": \"$TOTAL_SIZE\"" >> "$JSON_FILE"
echo "  }," >> "$JSON_FILE"
echo "" | tee -a "$OUTPUT_FILE"

# 5. Verificar tablas esperadas (test_table debe existir en fgeo_analytics)
echo "🎯 5. VERIFICANDO TABLAS ESPERADAS..." | tee -a "$OUTPUT_FILE"
if clickhouse-client --host="$CH_HOST" --port=9000 --user="$CH_USER" --password="$CH_PASS" \
    --query="EXISTS TABLE fgeo_analytics.test_table" | grep -q "1"; then
    echo "   ✅ Tabla test_table existe en fgeo_analytics" | tee -a "$OUTPUT_FILE"
    TEST_ROWS=$(clickhouse-client --host="$CH_HOST" --port=9000 --user="$CH_USER" --password="$CH_PASS" \
        --query="SELECT COUNT(*) FROM fgeo_analytics.test_table" | tr -d '\n')
    echo "   📊 Registros en test_table: $TEST_ROWS" | tee -a "$OUTPUT_FILE"
    echo "  \"expected_tables\": {\"test_table\": {\"exists\": true, \"rows\": $TEST_ROWS}}," >> "$JSON_FILE"
else
    echo "   ⚠️  Tabla test_table NO existe en fgeo_analytics" | tee -a "$OUTPUT_FILE"
    echo "  \"expected_tables\": {\"test_table\": {\"exists\": false, \"rows\": 0}}," >> "$JSON_FILE"
fi
echo "" | tee -a "$OUTPUT_FILE"

# Finalizar JSON
echo "  \"status\": \"completed\"" >> "$JSON_FILE"
echo "}" >> "$JSON_FILE"

# Resumen final
echo "========================================" | tee -a "$OUTPUT_FILE"
echo "✅ VERIFICACIÓN COMPLETADA" | tee -a "$OUTPUT_FILE"
echo "📄 Log guardado en: $OUTPUT_FILE" | tee -a "$OUTPUT_FILE"
echo "📊 JSON guardado en: $JSON_FILE" | tee -a "$OUTPUT_FILE"

# Crear symlink al último reporte
ln -sf "$(basename "$OUTPUT_FILE")" "$LOG_DIR/clickhouse_verify_latest.log"
ln -sf "$(basename "$JSON_FILE")" "$LOG_DIR/clickhouse_verify_latest.json"

exit 0
