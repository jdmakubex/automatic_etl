#!/bin/bash
# Script de verificación automática de Kafka y Connect
# Verifica topics, conectores y guarda resultados en logs

set -e

TIMESTAMP=$(date '+%Y-%m-%d_%H-%M-%S')
LOG_DIR="${LOG_DIR:-/app/logs}"
OUTPUT_FILE="$LOG_DIR/kafka_verify_${TIMESTAMP}.log"
JSON_FILE="$LOG_DIR/kafka_verify_${TIMESTAMP}.json"

# Configuración desde variables de entorno
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
CONNECT_URL="${CONNECT_URL:-http://connect:8083}"

echo "🔍 VERIFICACIÓN DE KAFKA Y CONNECT" | tee "$OUTPUT_FILE"
echo "========================================" | tee -a "$OUTPUT_FILE"
echo "⏰ Timestamp: $(date -Iseconds)" | tee -a "$OUTPUT_FILE"
echo "🌐 Kafka Bootstrap: $KAFKA_BOOTSTRAP" | tee -a "$OUTPUT_FILE"
echo "🌐 Connect URL: $CONNECT_URL" | tee -a "$OUTPUT_FILE"
echo "" | tee -a "$OUTPUT_FILE"

# Inicializar JSON
cat > "$JSON_FILE" << 'EOF'
{
  "timestamp": "",
  "kafka_bootstrap": "",
  "connect_url": "",
  "checks": {}
}
EOF

# Actualizar timestamp y URLs en JSON
python3 << PYTHON << 'PYTHON_END' | tee -a "$OUTPUT_FILE"
import json
import os
from datetime import datetime

JSON_FILE = os.getenv('JSON_FILE')
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092')
CONNECT_URL = os.getenv('CONNECT_URL', 'http://connect:8083')

with open(JSON_FILE, 'r') as f:
    data = json.load(f)

data['timestamp'] = datetime.now().isoformat()
data['kafka_bootstrap'] = KAFKA_BOOTSTRAP
data['connect_url'] = CONNECT_URL

with open(JSON_FILE, 'w') as f:
    json.dump(data, f, indent=2)

PYTHON_END

# 1. Verificar conectividad a Kafka
echo "📡 1. VERIFICANDO CONECTIVIDAD A KAFKA..." | tee -a "$OUTPUT_FILE"
if echo "" | timeout 5 nc -z ${KAFKA_BOOTSTRAP%:*} ${KAFKA_BOOTSTRAP#*:} &>/dev/null; then
    echo "   ✅ Kafka está disponible" | tee -a "$OUTPUT_FILE"
    python3 -c "
import json
with open('$JSON_FILE', 'r') as f: data = json.load(f)
data['checks']['kafka_connectivity'] = {'status': 'ok'}
with open('$JSON_FILE', 'w') as f: json.dump(data, f, indent=2)
" 2>/dev/null || true
else
    echo "   ❌ Kafka no está disponible" | tee -a "$OUTPUT_FILE"
    python3 -c "
import json
with open('$JSON_FILE', 'r') as f: data = json.load(f)
data['checks']['kafka_connectivity'] = {'status': 'error'}
with open('$JSON_FILE', 'w') as f: json.dump(data, f, indent=2)
" 2>/dev/null || true
    echo "}" >> "$JSON_FILE"
    exit 1
fi
echo "" | tee -a "$OUTPUT_FILE"

# 2. Listar topics de Kafka
echo "📋 2. LISTANDO TOPICS DE KAFKA..." | tee -a "$OUTPUT_FILE"
if command -v kafka-topics &> /dev/null; then
    TOPICS=$(kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP" --list 2>/dev/null || echo "")
    if [ -n "$TOPICS" ]; then
        echo "$TOPICS" | while read topic; do
            if [[ ! "$topic" =~ ^__ ]]; then
                echo "   📌 $topic" | tee -a "$OUTPUT_FILE"
            fi
        done
        TOPIC_COUNT=$(echo "$TOPICS" | grep -v "^__" | wc -l)
        echo "   ✅ Total de topics (sin sistema): $TOPIC_COUNT" | tee -a "$OUTPUT_FILE"
        
        # Guardar en JSON
        TOPICS_JSON=$(echo "$TOPICS" | jq -R -s -c 'split("\n") | map(select(length > 0))')
        python3 -c "
import json
with open('$JSON_FILE', 'r') as f: data = json.load(f)
data['checks']['kafka_topics'] = {'status': 'ok', 'count': $TOPIC_COUNT, 'topics': $TOPICS_JSON}
with open('$JSON_FILE', 'w') as f: json.dump(data, f, indent=2)
" 2>/dev/null || true
    else
        echo "   ℹ️  No se encontraron topics (o kafka-topics no disponible)" | tee -a "$OUTPUT_FILE"
        python3 -c "
import json
with open('$JSON_FILE', 'r') as f: data = json.load(f)
data['checks']['kafka_topics'] = {'status': 'warning', 'message': 'no topics or command unavailable'}
with open('$JSON_FILE', 'w') as f: json.dump(data, f, indent=2)
" 2>/dev/null || true
    fi
else
    echo "   ℹ️  Comando kafka-topics no disponible en este contenedor" | tee -a "$OUTPUT_FILE"
    python3 -c "
import json
with open('$JSON_FILE', 'r') as f: data = json.load(f)
data['checks']['kafka_topics'] = {'status': 'skipped', 'message': 'command not available'}
with open('$JSON_FILE', 'w') as f: json.dump(data, f, indent=2)
" 2>/dev/null || true
fi
echo "" | tee -a "$OUTPUT_FILE"

# 3. Verificar Kafka Connect
echo "🔌 3. VERIFICANDO KAFKA CONNECT..." | tee -a "$OUTPUT_FILE"
CONNECT_RESP=$(curl -s -o /dev/null -w "%{http_code}" "$CONNECT_URL/" 2>/dev/null || echo "000")
if [ "$CONNECT_RESP" = "200" ]; then
    echo "   ✅ Kafka Connect está disponible" | tee -a "$OUTPUT_FILE"
    python3 -c "
import json
with open('$JSON_FILE', 'r') as f: data = json.load(f)
data['checks']['connect_connectivity'] = {'status': 'ok', 'code': 200}
with open('$JSON_FILE', 'w') as f: json.dump(data, f, indent=2)
" 2>/dev/null || true
else
    echo "   ⚠️  Kafka Connect no disponible (código: $CONNECT_RESP)" | tee -a "$OUTPUT_FILE"
    python3 -c "
import json
with open('$JSON_FILE', 'r') as f: data = json.load(f)
data['checks']['connect_connectivity'] = {'status': 'warning', 'code': $CONNECT_RESP}
with open('$JSON_FILE', 'w') as f: json.dump(data, f, indent=2)
" 2>/dev/null || true
fi
echo "" | tee -a "$OUTPUT_FILE"

# 4. Listar conectores
echo "🔗 4. LISTANDO CONECTORES DE KAFKA CONNECT..." | tee -a "$OUTPUT_FILE"
if [ "$CONNECT_RESP" = "200" ]; then
    CONNECTORS=$(curl -s "$CONNECT_URL/connectors" 2>/dev/null || echo "[]")
    CONNECTOR_COUNT=$(echo "$CONNECTORS" | jq 'length' 2>/dev/null || echo "0")
    
    if [ "$CONNECTOR_COUNT" -gt 0 ]; then
        echo "$CONNECTORS" | jq -r '.[]' 2>/dev/null | while read connector; do
            echo "   📌 $connector" | tee -a "$OUTPUT_FILE"
            
            # Obtener estado del conector
            STATUS=$(curl -s "$CONNECT_URL/connectors/$connector/status" 2>/dev/null || echo "{}")
            STATE=$(echo "$STATUS" | jq -r '.connector.state' 2>/dev/null || echo "UNKNOWN")
            echo "      Estado: $STATE" | tee -a "$OUTPUT_FILE"
        done
        echo "   ✅ Total de conectores: $CONNECTOR_COUNT" | tee -a "$OUTPUT_FILE"
        
        # Guardar en JSON
        python3 -c "
import json
with open('$JSON_FILE', 'r') as f: data = json.load(f)
data['checks']['connect_connectors'] = {'status': 'ok', 'count': $CONNECTOR_COUNT, 'connectors': $CONNECTORS}
with open('$JSON_FILE', 'w') as f: json.dump(data, f, indent=2)
" 2>/dev/null || true
    else
        echo "   ℹ️  No hay conectores configurados" | tee -a "$OUTPUT_FILE"
        python3 -c "
import json
with open('$JSON_FILE', 'r') as f: data = json.load(f)
data['checks']['connect_connectors'] = {'status': 'ok', 'count': 0, 'connectors': []}
with open('$JSON_FILE', 'w') as f: json.dump(data, f, indent=2)
" 2>/dev/null || true
    fi
else
    echo "   ⚠️  No se pueden listar conectores (Connect no disponible)" | tee -a "$OUTPUT_FILE"
    python3 -c "
import json
with open('$JSON_FILE', 'r') as f: data = json.load(f)
data['checks']['connect_connectors'] = {'status': 'skipped', 'message': 'connect unavailable'}
with open('$JSON_FILE', 'w') as f: json.dump(data, f, indent=2)
" 2>/dev/null || true
fi
echo "" | tee -a "$OUTPUT_FILE"

# Finalizar JSON con estado
python3 -c "
import json
with open('$JSON_FILE', 'r') as f: data = json.load(f)
data['status'] = 'completed'
with open('$JSON_FILE', 'w') as f: json.dump(data, f, indent=2)
" 2>/dev/null || true

# Resumen final
echo "========================================" | tee -a "$OUTPUT_FILE"
echo "✅ VERIFICACIÓN COMPLETADA" | tee -a "$OUTPUT_FILE"
echo "📄 Log guardado en: $OUTPUT_FILE" | tee -a "$OUTPUT_FILE"
echo "📊 JSON guardado en: $JSON_FILE" | tee -a "$OUTPUT_FILE"

# Crear symlink al último reporte
ln -sf "$(basename "$OUTPUT_FILE")" "$LOG_DIR/kafka_verify_latest.log"
ln -sf "$(basename "$JSON_FILE")" "$LOG_DIR/kafka_verify_latest.json"

exit 0
