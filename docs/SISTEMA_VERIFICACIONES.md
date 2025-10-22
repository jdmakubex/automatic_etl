# Sistema de Verificaciones Automáticas - Documentación Completa

## Índice
1. [Visión General](#visión-general)
2. [Scripts de Verificación](#scripts-de-verificación)
3. [Integración con el Pipeline](#integración-con-el-pipeline)
4. [Uso Manual](#uso-manual)
5. [Formato de Reportes](#formato-de-reportes)
6. [Troubleshooting](#troubleshooting)

---

## Visión General

### Problema que Resuelve
Antes, el usuario tenía que ejecutar múltiples comandos manualmente para verificar:
- ¿Qué tablas se crearon en ClickHouse?
- ¿Cuántos registros tiene cada tabla?
- ¿Qué datasets configuró Superset?
- ¿Hay datos de ejecuciones anteriores?

### Solución Implementada
Sistema automatizado de verificaciones que:
- ✅ Se ejecuta automáticamente después de cada fase del pipeline
- ✅ Genera logs legibles y archivos JSON para análisis
- ✅ Incluye reintentos con backoff exponencial
- ✅ Consolida resultados de múltiples componentes
- ✅ Persiste resultados con timestamps y symlinks a "latest"

---

## Scripts de Verificación

### 1. `clickhouse_verify.sh`

**Ubicación**: `tools/verificaciones/clickhouse_verify.sh`

**Qué verifica**:
1. ✅ Conectividad a ClickHouse
2. 📊 Bases de datos existentes (lista)
3. 📋 Tablas con conteo de registros y tamaño
4. 📈 Estadísticas generales (total tablas, registros, tamaño)
5. 🎯 Verifica que `test_table` existe en `fgeo_analytics`

**Variables de entorno usadas**:
```bash
CLICKHOUSE_DEFAULT_USER=default
CLICKHOUSE_DEFAULT_PASSWORD=ClickHouse123!
CLICKHOUSE_HTTP_HOST=clickhouse
CLICKHOUSE_HTTP_PORT=8123
LOG_DIR=/app/logs
```

**Salida**:
- Log: `logs/clickhouse_verify_TIMESTAMP.log`
- JSON: `logs/clickhouse_verify_TIMESTAMP.json`
- Symlink: `logs/clickhouse_verify_latest.{log,json}`

**Ejemplo de JSON generado**:
```json
{
  "timestamp": "2025-10-22T00:30:15Z",
  "host": "clickhouse:8123",
  "connectivity": {"status": "ok"},
  "databases": [["fgeo_analytics"], ["default"]],
  "tables": [
    ["fgeo_analytics", "test_table", 6, 1024]
  ],
  "summary": {
    "total_tables": 1,
    "total_rows": 6,
    "total_size": "1.00 KiB"
  },
  "expected_tables": {
    "test_table": {"exists": true, "rows": 6}
  },
  "status": "completed"
}
```

---

### 2. `superset_verify.sh`

**Ubicación**: `tools/verificaciones/superset_verify.sh`

**Qué verifica**:
1. 📡 Disponibilidad del endpoint `/health`
2. 🔐 Autenticación con admin (obtiene token)
3. 📊 Bases de datos configuradas (lista con IDs)
4. 📋 Datasets configurados (lista con esquema y tabla)
5. 🎯 Verifica que el dataset `test_table` existe

**Variables de entorno usadas**:
```bash
SUPERSET_URL=http://superset:8088
SUPERSET_ADMIN=admin
SUPERSET_PASSWORD=Admin123!
LOG_DIR=/app/logs
```

**Salida**:
- Log: `logs/superset_verify_TIMESTAMP.log`
- JSON: `logs/superset_verify_TIMESTAMP.json`
- Symlink: `logs/superset_verify_latest.{log,json}`

**Ejemplo de JSON generado**:
```json
{
  "timestamp": "2025-10-22T00:30:20Z",
  "url": "http://superset:8088",
  "checks": {
    "availability": {"status": "ok", "code": 200},
    "authentication": {"status": "ok", "user": "admin"},
    "databases": {
      "status": "ok",
      "count": 1,
      "list": [
        {"name": "ClickHouse ETL Database", "id": 1}
      ]
    },
    "datasets": {
      "status": "ok",
      "count": 1,
      "list": [
        {"id": 1, "schema": "fgeo_analytics", "table": "test_table"}
      ]
    },
    "expected_dataset": {"status": "ok", "id": 1}
  },
  "status": "completed"
}
```

---

### 3. `kafka_verify.sh`

**Ubicación**: `tools/verificaciones/kafka_verify.sh`

**Qué verifica**:
1. 📡 Conectividad a Kafka
2. 📋 Topics existentes (excluyendo topics del sistema)
3. 🔌 Disponibilidad de Kafka Connect
4. 🔗 Conectores configurados con sus estados

**Variables de entorno usadas**:
```bash
KAFKA_BOOTSTRAP=kafka:9092
CONNECT_URL=http://connect:8083
LOG_DIR=/app/logs
```

**Salida**:
- Log: `logs/kafka_verify_TIMESTAMP.log`
- JSON: `logs/kafka_verify_TIMESTAMP.json`
- Symlink: `logs/kafka_verify_latest.{log,json}`

**Ejemplo de JSON generado**:
```json
{
  "timestamp": "2025-10-22T00:30:18Z",
  "kafka_bootstrap": "kafka:9092",
  "connect_url": "http://connect:8083",
  "checks": {
    "kafka_connectivity": {"status": "ok"},
    "kafka_topics": {
      "status": "ok",
      "count": 0,
      "topics": []
    },
    "connect_connectivity": {"status": "ok", "code": 200},
    "connect_connectors": {
      "status": "ok",
      "count": 0,
      "connectors": []
    }
  },
  "status": "completed"
}
```

---

### 4. `run_verifications.sh` (Orquestador)

**Ubicación**: `tools/run_verifications.sh`

**Qué hace**:
1. 🎯 Ejecuta todos los scripts de verificación en orden
2. 🔄 Maneja reintentos con backoff exponencial
3. 📊 Consolida resultados en un solo reporte
4. 💾 Genera log y JSON consolidados
5. ✅ Retorna código de salida apropiado

**Configuración de reintentos**:
```bash
VERIFICATIONS=(
    "clickhouse:$VERIFY_DIR/clickhouse_verify.sh:2:5"
    #            script                          ^  ^
    #                                            |  |
    #                                    max_retries |
    #                                           retry_delay (segundos)
    
    "kafka:$VERIFY_DIR/kafka_verify.sh:1:3"
    "superset:$VERIFY_DIR/superset_verify.sh:3:10"
)
```

**Backoff exponencial**:
- Intento 1: espera `delay` segundos
- Intento 2: espera `delay * 2` segundos
- Intento 3: espera `delay * 4` segundos
- ...

**Salida**:
- Log: `logs/verificacion_consolidada_TIMESTAMP.log`
- JSON: `logs/verificacion_consolidada_TIMESTAMP.json`
- Symlink: `logs/verificacion_consolidada_latest.{log,json}`

**Ejemplo de JSON consolidado**:
```json
{
  "timestamp": "2025-10-22T00:30:25Z",
  "components": {
    "clickhouse": {
      "timestamp": "2025-10-22T00:30:15Z",
      "summary": {"total_tables": 1, "total_rows": 6},
      "status": "completed"
    },
    "kafka": {
      "timestamp": "2025-10-22T00:30:18Z",
      "checks": {...},
      "status": "completed"
    },
    "superset": {
      "timestamp": "2025-10-22T00:30:20Z",
      "checks": {...},
      "status": "completed"
    }
  },
  "summary": {
    "total": 3,
    "success": 3,
    "failed": 0,
    "all_passed": true
  }
}
```

---

## Integración con el Pipeline

### Modificaciones en `auto_pipeline.sh`

#### FASE 0: Verificación Pre-Ingesta
```bash
log_message "INFO" "🔍 FASE 0: Verificación de estado limpio"
if python3 tools/verify_clean_state.py; then
    log_message "SUCCESS" "✅ Sistema limpio"
else
    log_message "WARNING" "⚠️  Datos antiguos detectados"
fi
```

#### FASE 2: POST-Ingesta
```bash
# Después de multi_database_ingest.py
log_message "INFO" "🔍 Verificando datos ingresados..."

# Mostrar tablas y conteos
clickhouse-client --query="
    SELECT database, name, total_rows 
    FROM system.tables 
    WHERE ...
    FORMAT PrettyCompact
" | tee -a "$LOG_FILE"
```

#### FASE 3: POST-Configuración de Superset
```bash
# Después de configurar datasets
log_message "INFO" "📊 Verificando datasets configurados..."
python3 -c "
import requests, os
# Obtener lista de datasets vía API
# Mostrar primeros 10
" | tee -a "$LOG_FILE"
```

#### FASE 5: Verificación Final Completa
```bash
log_message "INFO" "🔍 Ejecutando verificación completa de todos los componentes..."
if bash tools/run_verifications.sh; then
    log_message "SUCCESS" "✅ Todas las verificaciones pasaron"
else
    log_message "WARNING" "⚠️  Algunas verificaciones fallaron"
fi
```

---

## Uso Manual

### Ejecutar Verificación Individual

#### ClickHouse
```bash
# Desde el host
docker compose exec etl-orchestrator bash tools/verificaciones/clickhouse_verify.sh

# Ver resultado
cat logs/clickhouse_verify_latest.log
cat logs/clickhouse_verify_latest.json | jq .
```

#### Superset
```bash
docker compose exec etl-orchestrator bash tools/verificaciones/superset_verify.sh
cat logs/superset_verify_latest.log
```

#### Kafka
```bash
docker compose exec etl-orchestrator bash tools/verificaciones/kafka_verify.sh
cat logs/kafka_verify_latest.log
```

### Ejecutar Todas las Verificaciones
```bash
# Desde el host
docker compose exec etl-orchestrator bash tools/run_verifications.sh

# Ver resultado consolidado
cat logs/verificacion_consolidada_latest.log
cat logs/verificacion_consolidada_latest.json | jq .
```

### Ejecutar con Variables Personalizadas
```bash
docker compose exec \
  -e CLICKHOUSE_DEFAULT_USER=myuser \
  -e CLICKHOUSE_DEFAULT_PASSWORD=mypass \
  etl-orchestrator bash tools/verificaciones/clickhouse_verify.sh
```

---

## Formato de Reportes

### Estructura de Directorios de Logs
```
logs/
├── clickhouse_verify_2025-10-22_00-30-15.log
├── clickhouse_verify_2025-10-22_00-30-15.json
├── clickhouse_verify_latest.log → clickhouse_verify_2025-10-22_00-30-15.log
├── clickhouse_verify_latest.json → clickhouse_verify_2025-10-22_00-30-15.json
│
├── superset_verify_2025-10-22_00-30-20.log
├── superset_verify_2025-10-22_00-30-20.json
├── superset_verify_latest.log
├── superset_verify_latest.json
│
├── kafka_verify_2025-10-22_00-30-18.log
├── kafka_verify_2025-10-22_00-30-18.json
├── kafka_verify_latest.log
├── kafka_verify_latest.json
│
├── verificacion_consolidada_2025-10-22_00-30-25.log
├── verificacion_consolidada_2025-10-22_00-30-25.json
├── verificacion_consolidada_latest.log
├── verificacion_consolidada_latest.json
│
├── auto_pipeline_detailed.log
├── auto_pipeline_status.json
└── clean_state_verification.json
```

### Convenciones de Nombrado
- **Timestamp**: `YYYY-MM-DD_HH-MM-SS`
- **Formato**: `{componente}_verify_{timestamp}.{log|json}`
- **Symlink latest**: `{componente}_verify_latest.{log|json}`

### Campos Comunes en JSON
Todos los JSONs tienen:
```json
{
  "timestamp": "ISO 8601 format",
  "status": "completed|error|warning",
  ...
}
```

---

## Troubleshooting

### Problema: Script no encuentra comando
**Síntoma**:
```
⚠️  Comando kafka-topics no disponible en este contenedor
```

**Solución**:
- Normal. El script detecta y marca como "skipped"
- Si es crítico, ejecutar desde contenedor que tenga el comando:
  ```bash
  docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
  ```

### Problema: Error de conexión
**Síntoma**:
```json
{"connectivity": {"status": "error"}}
```

**Solución**:
1. Verificar que el servicio esté corriendo:
   ```bash
   docker compose ps
   ```
2. Verificar credenciales en `.env`
3. Verificar red Docker:
   ```bash
   docker network inspect etl_prod_etl_net
   ```

### Problema: Timeout en autenticación
**Síntoma**:
```
❌ No se pudo obtener token tras reintentos
```

**Solución**:
1. Verificar que `superset-init` haya completado:
   ```bash
   docker compose ps superset-init
   ```
2. Resetear contraseña manualmente:
   ```bash
   docker exec superset superset fab reset-password \
     --username admin --password Admin123!
   ```
3. Verificar variables en `.env`:
   ```bash
   grep SUPERSET .env
   ```

### Problema: Datos antiguos detectados
**Síntoma**:
```
⚠️  Sistema con datos antiguos detectados
```

**Solución**:
```bash
# Limpieza total
./tools/clean_all.sh

# Reiniciar pipeline
docker compose up -d
```

### Problema: Verificación falla después de reintentos
**Síntoma**:
```
💔 Verificación de superset FALLÓ después de 3 intentos
```

**Solución**:
1. Ver log detallado:
   ```bash
   cat logs/superset_verify_latest.log
   ```
2. Ver JSON para detalles programáticos:
   ```bash
   cat logs/superset_verify_latest.json | jq '.checks'
   ```
3. Ejecutar manualmente para debugging:
   ```bash
   docker compose exec etl-orchestrator bash -x tools/verificaciones/superset_verify.sh
   ```

---

## Mejoras Futuras

### Configurabilidad
- [ ] Timeouts configurables vía env vars
- [ ] Reintentos configurables por componente
- [ ] Umbrales de alerta personalizables

### Notificaciones
- [ ] Enviar email/slack en caso de fallo
- [ ] Webhook para integración con herramientas de monitoreo
- [ ] Dashboard web para visualizar reportes

### Métricas
- [ ] Exportar métricas a Prometheus
- [ ] Historial de verificaciones con tendencias
- [ ] Alertas basadas en patrones (ej: tablas creciendo demasiado rápido)

---

## Resumen de Archivos

| Archivo | Propósito | Cuándo se ejecuta |
|---------|-----------|-------------------|
| `clickhouse_verify.sh` | Verifica ClickHouse | POST-ingesta, verificación manual |
| `superset_verify.sh` | Verifica Superset | POST-config, verificación manual |
| `kafka_verify.sh` | Verifica Kafka/Connect | POST-config, verificación manual |
| `run_verifications.sh` | Orquestador | FASE 5 (final), verificación manual |
| `verify_clean_state.py` | Estado limpio | FASE 0 (pre-ingesta) |
| `clean_all.sh` | Limpieza total | Antes de nueva ejecución |

---

## Comandos Rápidos

```bash
# Ver última verificación de ClickHouse
cat logs/clickhouse_verify_latest.log

# Ver última verificación consolidada
cat logs/verificacion_consolidada_latest.log

# Ejecutar solo verificación de Superset
docker compose exec etl-orchestrator bash tools/verificaciones/superset_verify.sh

# Ejecutar todas las verificaciones manualmente
docker compose exec etl-orchestrator bash tools/run_verifications.sh

# Ver resumen de todas las verificaciones en JSON
cat logs/verificacion_consolidada_latest.json | jq '.summary'

# Buscar errores en logs
grep -r "ERROR\|❌" logs/*.log

# Listar todos los reportes generados hoy
ls -lh logs/*$(date +%Y-%m-%d)*.{log,json}
```
