#!/usr/bin/env bash
#
# cdc_monitor.sh
# Monitoreo continuo del estado CDC (conectores, tópicos, tablas RAW)
# Actualiza logs/cdc_extended_report.json cada N minutos
#

set -euo pipefail

# Configuración
REPORT_PATH="${REPORT_PATH:-/app/logs/cdc_extended_report.json}"
CONNECT_URL="${CONNECT_URL:-http://connect:8083}"
CH_HOST="${CLICKHOUSE_HTTP_HOST:-clickhouse}"
CH_PORT="${CLICKHOUSE_HTTP_PORT:-8123}"
CH_USER="${CH_USER:-etl}"
CH_PASS="${CH_PASSWORD:-Et1Ingest!}"
INTERVAL_SEC="${CDC_MONITOR_INTERVAL:-300}"  # 5 min por defecto

echo "🔍 CDC Monitor iniciado"
echo "   Reporte: $REPORT_PATH"
echo "   Intervalo: ${INTERVAL_SEC}s"

# Función para consultar ClickHouse vía HTTP
ch_query() {
    curl -sSf -u "$CH_USER:$CH_PASS" "http://$CH_HOST:$CH_PORT/" --data-binary "$1"
}

# Función para generar el reporte
generate_report() {
    local timestamp
    timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    
    # 1. Conectores
    local connectors_json
    connectors_json=$(curl -sSf "$CONNECT_URL/connectors" 2>/dev/null || echo "[]")
    local connector_count
    connector_count=$(echo "$connectors_json" | jq -r 'length' 2>/dev/null || echo 0)
    
    # 2. Tópicos Kafka (requiere acceso al contenedor kafka, simplificado aquí)
    local topics_count=0
    
    # 3. Tablas RAW en ClickHouse
    local raw_tables_query="SELECT name, total_rows FROM system.tables WHERE database='fgeo_analytics' AND match(name,'_raw$') ORDER BY 1 FORMAT JSONCompact"
    local raw_tables_json
    raw_tables_json=$(ch_query "$raw_tables_query" 2>/dev/null || echo '{"data":[]}')
    
    # 4. Kafka/MV counts
    local kafka_mv_query="SELECT engine, count() as cnt FROM system.tables WHERE database='ext' AND engine IN ('Kafka','MaterializedView') GROUP BY engine FORMAT JSONCompact"
    local kafka_mv_json
    kafka_mv_json=$(ch_query "$kafka_mv_query" 2>/dev/null || echo '{"data":[]}')
    
    # Construir JSON
    cat > "$REPORT_PATH" <<EOF
{
  "timestamp": "$timestamp",
  "connectors": {
    "count": $connector_count,
    "list": $connectors_json
  },
  "clickhouse": {
    "raw_tables": $raw_tables_json,
    "engines": $kafka_mv_json
  },
  "overall": "MONITORING"
}
EOF
    
    echo "[$(date -u +"%Y-%m-%d %H:%M:%S")] ✅ Reporte CDC actualizado: $connector_count conectores"
}

# Loop principal
while true; do
    generate_report || echo "⚠️  Error generando reporte CDC"
    sleep "$INTERVAL_SEC"
done
