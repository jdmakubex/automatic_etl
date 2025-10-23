#!/bin/bash
# Orquestador de verificaciones automáticas
# Ejecuta scripts de verificación y consolida resultados

set -e

TIMESTAMP=$(date '+%Y-%m-%d_%H-%M-%S')
LOG_DIR="${LOG_DIR:-/app/logs}"
VERIFY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/verificaciones"
CONSOLIDATED_LOG="$LOG_DIR/verificacion_consolidada_${TIMESTAMP}.log"
CONSOLIDATED_JSON="$LOG_DIR/verificacion_consolidada_${TIMESTAMP}.json"

echo "╔════════════════════════════════════════════════════════════╗" | tee "$CONSOLIDATED_LOG"
echo "║       🔍 ORQUESTADOR DE VERIFICACIONES AUTOMÁTICAS         ║" | tee -a "$CONSOLIDATED_LOG"
echo "╚════════════════════════════════════════════════════════════╝" | tee -a "$CONSOLIDATED_LOG"
echo "" | tee -a "$CONSOLIDATED_LOG"
echo "⏰ Timestamp: $(date -Iseconds)" | tee -a "$CONSOLIDATED_LOG"
echo "📂 Directorio de verificaciones: $VERIFY_DIR" | tee -a "$CONSOLIDATED_LOG"
echo "" | tee -a "$CONSOLIDATED_LOG"

# Inicializar JSON consolidado
cat > "$CONSOLIDATED_JSON" << 'EOF'
{
  "timestamp": "",
  "components": {}
}
EOF

python3 -c "
import json
from datetime import datetime
with open('$CONSOLIDATED_JSON', 'r') as f: data = json.load(f)
data['timestamp'] = datetime.now().isoformat()
with open('$CONSOLIDATED_JSON', 'w') as f: json.dump(data, f, indent=2)
"

# Función para ejecutar verificación con reintentos
run_verification() {
    local component=$1
    local script=$2
    local max_retries=${3:-1}
    local retry_delay=${4:-5}
    
    echo "════════════════════════════════════════════════════════════" | tee -a "$CONSOLIDATED_LOG"
    echo "🔍 VERIFICANDO: $component" | tee -a "$CONSOLIDATED_LOG"
    echo "════════════════════════════════════════════════════════════" | tee -a "$CONSOLIDATED_LOG"
    
    if [ ! -f "$script" ]; then
        echo "⚠️  Script no encontrado: $script" | tee -a "$CONSOLIDATED_LOG"
        python3 -c "
import json
with open('$CONSOLIDATED_JSON', 'r') as f: data = json.load(f)
data['components']['$component'] = {'status': 'skipped', 'message': 'script not found'}
with open('$CONSOLIDATED_JSON', 'w') as f: json.dump(data, f, indent=2)
"
        echo "" | tee -a "$CONSOLIDATED_LOG"
        return 1
    fi
    
    local attempt=1
    local success=false
    
    while [ $attempt -le $max_retries ]; do
        echo "📍 Intento $attempt de $max_retries..." | tee -a "$CONSOLIDATED_LOG"
        
        if bash "$script" 2>&1 | tee -a "$CONSOLIDATED_LOG"; then
            echo "✅ Verificación de $component exitosa" | tee -a "$CONSOLIDATED_LOG"
            success=true
            
            # Intentar copiar el JSON generado al consolidado
            local json_latest="$LOG_DIR/${component}_verify_latest.json"
            if [ -f "$json_latest" ]; then
                python3 -c "
import json
try:
    with open('$CONSOLIDATED_JSON', 'r') as f: 
        consolidated = json.load(f)
    with open('$json_latest', 'r') as f:
        component_data = json.load(f)
    consolidated['components']['$component'] = component_data
    with open('$CONSOLIDATED_JSON', 'w') as f:
        json.dump(consolidated, f, indent=2)
except Exception as e:
    print(f'⚠️  Error consolidando JSON: {e}')
" 2>&1 | tee -a "$CONSOLIDATED_LOG"
            fi
            
            break
        else
            echo "❌ Verificación de $component falló (intento $attempt/$max_retries)" | tee -a "$CONSOLIDATED_LOG"
            
            if [ $attempt -lt $max_retries ]; then
                echo "⏳ Esperando ${retry_delay}s antes de reintentar..." | tee -a "$CONSOLIDATED_LOG"
                sleep $retry_delay
                # Incrementar delay para backoff
                retry_delay=$((retry_delay * 2))
            fi
            
            attempt=$((attempt + 1))
        fi
    done
    
    if [ "$success" = false ]; then
        echo "💔 Verificación de $component FALLÓ después de $max_retries intentos" | tee -a "$CONSOLIDATED_LOG"
        python3 -c "
import json
with open('$CONSOLIDATED_JSON', 'r') as f: data = json.load(f)
data['components']['$component'] = {'status': 'error', 'message': 'failed after $max_retries retries'}
with open('$CONSOLIDATED_JSON', 'w') as f: json.dump(data, f, indent=2)
"
    fi
    
    echo "" | tee -a "$CONSOLIDATED_LOG"
    return $([ "$success" = true ] && echo 0 || echo 1)
}

# Array de verificaciones a ejecutar
# Formato: "nombre:script:max_retries:retry_delay"
VERIFICATIONS=(
    "clickhouse:$VERIFY_DIR/clickhouse_verify.sh:2:5"
    "kafka:$VERIFY_DIR/kafka_verify.sh:1:3"
    "redis:$VERIFY_DIR/redis_verify.sh:1:3"
    "superset:$VERIFY_DIR/superset_verify.sh:3:10"
)

# Ejecutar todas las verificaciones
TOTAL=0
SUCCESS=0
FAILED=0

for verification in "${VERIFICATIONS[@]}"; do
    IFS=':' read -r component script retries delay <<< "$verification"
    
    TOTAL=$((TOTAL + 1))
    
    if run_verification "$component" "$script" "$retries" "$delay"; then
        SUCCESS=$((SUCCESS + 1))
    else
        FAILED=$((FAILED + 1))
    fi
done

# Resumen final
echo "════════════════════════════════════════════════════════════" | tee -a "$CONSOLIDATED_LOG"
echo "📊 RESUMEN DE VERIFICACIONES" | tee -a "$CONSOLIDATED_LOG"
echo "════════════════════════════════════════════════════════════" | tee -a "$CONSOLIDATED_LOG"
echo "   Total: $TOTAL" | tee -a "$CONSOLIDATED_LOG"
echo "   ✅ Exitosas: $SUCCESS" | tee -a "$CONSOLIDATED_LOG"
echo "   ❌ Fallidas: $FAILED" | tee -a "$CONSOLIDATED_LOG"
echo "" | tee -a "$CONSOLIDATED_LOG"

# Actualizar JSON consolidado con resumen
python3 -c "
import json
with open('$CONSOLIDATED_JSON', 'r') as f: data = json.load(f)
data['summary'] = {
    'total': $TOTAL,
    'success': $SUCCESS,
    'failed': $FAILED,
    'all_passed': $FAILED == 0
}
with open('$CONSOLIDATED_JSON', 'w') as f: json.dump(data, f, indent=2)
"

if [ $FAILED -eq 0 ]; then
    echo "🎉 ✅ TODAS LAS VERIFICACIONES PASARON" | tee -a "$CONSOLIDATED_LOG"
    EXIT_CODE=0
else
    echo "⚠️  ALGUNAS VERIFICACIONES FALLARON" | tee -a "$CONSOLIDATED_LOG"
    echo "" | tee -a "$CONSOLIDATED_LOG"
    echo "💡 Sugerencias:" | tee -a "$CONSOLIDATED_LOG"
    echo "   1. Revisar logs individuales en: $LOG_DIR/*_verify_latest.log" | tee -a "$CONSOLIDATED_LOG"
    echo "   2. Verificar que los servicios estén disponibles" | tee -a "$CONSOLIDATED_LOG"
    echo "   3. Revisar credenciales en .env" | tee -a "$CONSOLIDATED_LOG"
    echo "   4. Ejecutar limpieza total: ./tools/clean_all.sh" | tee -a "$CONSOLIDATED_LOG"
    EXIT_CODE=1
fi

echo "" | tee -a "$CONSOLIDATED_LOG"
echo "📄 Log consolidado: $CONSOLIDATED_LOG" | tee -a "$CONSOLIDATED_LOG"
echo "📊 JSON consolidado: $CONSOLIDATED_JSON" | tee -a "$CONSOLIDATED_LOG"

# Crear symlinks a los últimos reportes
ln -sf "$(basename "$CONSOLIDATED_LOG")" "$LOG_DIR/verificacion_consolidada_latest.log"
ln -sf "$(basename "$CONSOLIDATED_JSON")" "$LOG_DIR/verificacion_consolidada_latest.json"

echo "" | tee -a "$CONSOLIDATED_LOG"
echo "╔════════════════════════════════════════════════════════════╗" | tee -a "$CONSOLIDATED_LOG"
if [ $EXIT_CODE -eq 0 ]; then
    echo "║              ✅ VERIFICACIONES COMPLETADAS                 ║" | tee -a "$CONSOLIDATED_LOG"
else
    echo "║           ⚠️  VERIFICACIONES CON ERRORES                  ║" | tee -a "$CONSOLIDATED_LOG"
fi
echo "╚════════════════════════════════════════════════════════════╝" | tee -a "$CONSOLIDATED_LOG"

exit $EXIT_CODE
