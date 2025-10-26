# Registro de Cambios - Sistema de Robustez y Automatización

## 2025-10-24

### Validación de ingesta ETL y datos en ClickHouse

- Se revisaron los logs de ingesta (`logs/ingest_runner.log`, `logs/ingest_status.json`) y se confirmó éxito: más de 34,000 filas procesadas y 11 tablas actualizadas.
- Validación directa en ClickHouse:
  - `fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora`: 513,344 filas
  - `fiscalizacion.src__fiscalizacion__fiscalizacion__ofeindisdup`: 209,171 filas
  - `fiscalizacion.src__fiscalizacion__fiscalizacion__cieps`: 66 filas
  - `fiscalizacion.src__fiscalizacion__fiscalizacion__formatovic`: 106 filas
  - `fiscalizacion.src__fiscalizacion__fiscalizacion__cieps_formatovic`: 129 filas
  - `fiscalizacion.src__fiscalizacion__fiscalizacion__agrupadoai`: 18 filas
  - `fiscalizacion.src__fiscalizacion__fiscalizacion__altoimpacto`: 44 filas
- No se detectaron errores ni advertencias relevantes en la ingesta.
- Siguiente paso: Validar creación de gráficas y queries en Superset UI usando estos datos.

---

## 2025-10-23

### Cambios Implementados

#### 1. Superset init: actualización robusta de conexión ClickHouse

Ubicación: `tools/superset_auto_configurator.py`

- Al actualizar la base de datos “ClickHouse ETL Database”, ahora se intenta primero un PATCH con campos seguros (expose_in_sqllab, allow_ctas, allow_cvas, allow_run_async, allow_dml, extra).
- Si el PATCH no está permitido (405), se hace fallback a PUT completo y se registran errores detallados.
- Resiliencia: si la actualización falla con 422, el script detecta la BD oficial existente y continúa con su ID para no bloquear el pipeline.

Resultado: `superset-init` ya no se detiene por 422 y el pipeline avanza a la configuración de datasets.

#### 2. Post-config de datasets y preferencias de SQL Lab

Ubicación: `superset_bootstrap/run_post_config.sh` y scripts asociados

- Se verificó la creación/configuración automática de 19 datasets en 2 esquemas.
- Se aplica Time Grain por defecto = None y métrica COUNT(*).
- La activación “Run Async” vía API no se pudo forzar por diferencia de endpoint (404/405); se confía en GLOBAL_ASYNC_QUERIES y la configuración UI local. Pendiente: compatibilidad de endpoint para forzar la preferencia a nivel usuario.

Archivos de reporte generados:
- `logs/dataset_candidates.json`
- `logs/dataset_time_mapping.json`

### Próximos Pasos

1. Ajustar el endpoint/API para establecer "Run Async" por defecto a nivel usuario (Superset 3.1).
2. Validar en UI: creación de gráficas por admin y ausencia de errores con columnas fecha.

#### 3. Validación automatizada de Superset UI

Ubicación: `tools/validate_superset_ui.py`

- Script de validación automatizada que verifica:
  - Autenticación admin funcional
  - Base de datos ClickHouse conectada y configurada
  - 19 datasets creados y accesibles
  - Columnas temporales detectadas en datasets relevantes
  - Permisos de admin para crear charts
  - SQL Lab con async habilitado globalmente

Resultado: ✅ Todas las validaciones automatizadas pasaron.

**Estado del sistema**: Listo para validación manual en UI.

Documentación creada:
- `docs/VALIDATION_CHECKLIST.md`: Checklist completo de validación manual con pasos detallados para verificar creación de charts, SQL Lab async, y manejo de columnas fecha.

Reporte generado:
- `logs/superset_ui_validation.json`: Resultados completos de validación automatizada.

### Pendientes

1. **Validación Manual UI** (15-20 min):
   - Crear charts con columnas de fecha
   - Verificar SQL Lab ejecuta en modo async
   - Confirmar ausencia de errores GROUP BY

2. **Ajuste opcional**: Forzar "Run Async" por defecto a nivel usuario vía API (el endpoint /api/v1/me/ retorna 401; la funcionalidad async está disponible pero el toggle no se marca automáticamente).

## 2025-10-22

### Cambios Implementados

#### 1. Robustez de Autenticación (configure_datasets.py)

**Ubicación**: `superset_bootstrap/configure_datasets.py`

**Cambios realizados**:

- Superset datasets bootstrap now reliably runs via corrected docker-compose command (array form and multi-line bash script).
- Automatic dataset column configuration: mark DateTime/Timestamp columns as temporal (is_dttm=True) to avoid GROUP BY errors.
- For existing datasets (422 on create), we now look them up and apply the same configuration.
- Default Explore Time Grain is set to None by writing default_form_data.time_grain_sqla = null to dataset.extra.

#### 2. Dependencias en Docker Compose

**Ubicación**: `docker-compose.yml`

**Cambio**:

- The /api/v1/database/{id}/refresh endpoint returns 404 on current Superset build; behavior is tolerated and does not block table listing.
- Temporary debug logs were added to verify the execution flow; they will be removed after validation.


## Fecha: 2025-10-22

### Problema Principal Identificado
El usuario reportó que:
1. El pipeline se "atora" en obtención de tokens (401 Not authorized)
2. Hay verificaciones manuales que deberían ser automáticas
3. Los datos de ejecuciones anteriores no se limpiaban correctamente
4. Falta trazabilidad de qué está pasando en cada momento

### Cambios Implementados

#### 1. Robustez de Autenticación (configure_datasets.py)

**Ubicación**: `superset_bootstrap/configure_datasets.py`

**Cambios realizados**:

```python
# AGREGADO: Espera activa para confirmar que Superset está completamente inicializado
def wait_for_superset(url, timeout=300):
    """Esperar a que Superset esté disponible y completamente inicializado"""
    # Antes: solo verificaba /health
    # Ahora: verifica /health + espera adicional de 10s para inicialización completa
    
# AGREGADO: Nueva función para esperar al admin
def wait_for_admin_ready(url, username, max_wait=180):
    """Esperar a que el admin user esté realmente creado y disponible.
    
    Intenta login periódicamente hasta que funcione.
    - Reintenta cada 3s
    - Log de progreso cada 5 intentos
    - Prueba contraseñas alternativas si encuentra 401
    """
    # Esta función asegura que superset-init haya terminado
    # antes de que superset-datasets intente autenticarse

# MEJORADO: Backoff exponencial en get_auth_token
def get_auth_token(..., initial_backoff=3, max_backoff=30):
    """Obtener token con reintentos inteligentes
    
    Cambios:
    - Backoff exponencial: 3s → 4.5s → 6.75s → 10s → 15s → 22.5s → 30s
    - Log de progreso cada 5 intentos
    - Prueba múltiples contraseñas automáticamente
    - Mensajes de error más descriptivos
    """

# MEJORADO: authed_request ya existía pero se robustece
# Ahora maneja mejor los 401 y actualiza el token_ref globalmente
```

**Razón del cambio**: 
- El servicio `superset-datasets` se ejecutaba antes de que el admin existiera
- Los reintentos constantes saturaban el servidor
- No había visibilidad de qué estaba fallando

---

#### 2. Dependencias en Docker Compose

**Ubicación**: `docker-compose.yml`

**Cambio**:
```yaml
superset-datasets:
  depends_on:
    superset:
      condition: service_healthy
    superset-init:
      condition: service_completed_successfully  # ⬅️ NUEVO
```

**Razón**: 
- Antes: `superset-datasets` iniciaba apenas Superset estaba "healthy"
- Ahora: espera a que `superset-init` termine de crear el admin
- Elimina race condition entre creación de admin y configuración de datasets

---

#### 3. Estandarización de Contraseñas

**Ubicación**: `tools/auto_pipeline.sh`

**Antes**:
```bash
execute_with_log "Crear usuario admin en Superset" \
    "docker compose exec -T superset superset fab create-admin ... --password admin"
```

**Ahora**:
```bash
# Usar variable de entorno estandarizada
ADMIN_PASSWORD="${SUPERSET_PASSWORD:-Admin123!}"

# Crear admin
docker compose exec -T superset superset fab create-admin \
    --password "$ADMIN_PASSWORD"

# NUEVO: Siempre resetear para sincronizar
execute_with_log "Resetear contraseña del admin" \
    "docker compose exec -T superset superset fab reset-password \
        --username admin --password \"$ADMIN_PASSWORD\""
```

**Razón**:
- Antes: admin se creaba con "admin", pero otros servicios esperaban "Admin123!"
- Desincronización causaba 401s persistentes
- Ahora: contraseña consistente en todos los servicios desde el inicio

---

#### 4. Sistema de Limpieza Total

**Ubicación**: `tools/clean_all.sh` (NUEVO)

**Propósito**: Script unificado para limpiar TODO el sistema

**Funcionalidad**:
```bash
#!/bin/bash
# Detener servicios
docker compose down

# Eliminar volúmenes (ClickHouse, Kafka, Superset)
docker compose down -v
docker volume prune -f

# Eliminar redes huérfanas
docker network prune -f

# Limpiar logs y archivos generados
rm -rf logs/*.json logs/*.log
rm -rf generated/*/schemas/*.json

# Mostrar resumen del estado final
```

**Razón**:
- Usuario reportó confusión por datos antiguos
- Antes: múltiples comandos manuales necesarios
- Ahora: un solo comando limpia TODO

---

#### 5. Verificación Automática de Estado Limpio

**Ubicación**: `tools/verify_clean_state.py` (NUEVO)

**Propósito**: Detectar automáticamente datos de ejecuciones anteriores

**Funcionalidad**:
```python
def check_clickhouse_databases():
    """Verifica que solo existan las bases esperadas"""
    # Esperadas: fgeo_analytics, default
    # Cualquier otra → ALERTA

def check_clickhouse_tables():
    """Verifica que solo exista test_table"""
    # Esperada: fgeo_analytics.test_table
    # Cualquier otra → ALERTA

def check_kafka_topics():
    """Verifica que solo existan topics del sistema"""
    # Esperados: connect_configs, connect_offsets, connect_statuses
    # Cualquier otro → ALERTA

def check_superset_datasets():
    """Verifica que solo exista test_table en datasets"""
    # Esperado: fgeo_analytics.test_table
    # Cualquier otro → ALERTA
```

**Salida**: Genera `logs/clean_state_verification.json` con:
```json
{
  "status": "clean|dirty",
  "timestamp": "...",
  "results": {
    "clickhouse_databases": {"status": "clean", "databases": [...]},
    "clickhouse_tables": {"status": "clean", "tables": [...]},
    ...
  }
}
```

**Integración**: Se ejecuta automáticamente en **FASE 0** del pipeline

**Razón**:
- Antes: verificación manual con múltiples comandos
- Usuario perdía tiempo debuggeando datos antiguos
- Ahora: detección automática con reporte persistente

---

#### 6. Verificaciones POST-Proceso Integradas

**Ubicación**: `tools/auto_pipeline.sh`

**FASE 0 (NUEVA)**:
```bash
log_message "INFO" "🔍 FASE 0: Verificación de estado limpio del sistema"
if python3 tools/verify_clean_state.py; then
    log_message "SUCCESS" "✅ Sistema limpio"
else
    log_message "WARNING" "⚠️  Datos antiguos detectados"
fi
```

**FASE 2 - POST-Ingesta (MEJORADO)**:
```bash
# Antes: solo mostraba total de registros
# Ahora: también lista todas las tablas con conteos
if command -v clickhouse-client &> /dev/null; then
    echo "📊 Tablas creadas en ClickHouse:"
    clickhouse-client --query="
        SELECT database, name, total_rows 
        FROM system.tables 
        WHERE ...
        FORMAT PrettyCompact
    " | tee -a "$LOG_FILE"
fi
```

**FASE 3 - POST-Configuración de Superset (NUEVO)**:
```bash
log_message "INFO" "📊 Verificando datasets configurados en Superset..."
python3 -c "
import requests, os
# Login a Superset
# Obtener lista de datasets
# Mostrar primeros 10
" | tee -a "$LOG_FILE"
```

**Razón**:
- Antes: usuario ejecutaba comandos manuales para ver estado
- No había trazabilidad de qué se creó en cada fase
- Ahora: verificación automática con log persistente

---

#### 7. Variables de Entorno Unificadas

**Ubicación**: `docker-compose.yml`, `tools/auto_pipeline.sh`

**Estandarización**:
```yaml
# Todos los servicios ahora usan:
- SUPERSET_ADMIN=${SUPERSET_ADMIN:-admin}
- SUPERSET_PASSWORD=${SUPERSET_PASSWORD:-Admin123!}
- SUPERSET_URL=http://superset:8088
```

**En scripts**:
```bash
ADMIN_PASSWORD="${SUPERSET_PASSWORD:-Admin123!}"
echo "👤 Usuario: ${SUPERSET_USERNAME:-admin} / Contraseña: $ADMIN_PASSWORD"
```

**Razón**:
- Antes: valores hardcodeados en múltiples lugares
- Inconsistencias causaban 401s
- Ahora: una sola fuente de verdad (.env)

---

### Documentación Creada

#### docs/TOKEN_ROBUSTNESS.md
**Contenido**:
- Explicación detallada de cada mejora de autenticación
- Diagrama de flujo completo del proceso
- Comandos de troubleshooting
- Variables de entorno
- Estrategias de rollback

#### docs/AUTOMATED_VERIFICATIONS.md
**Contenido**:
- Descripción de cada script de verificación
- Flujo automatizado completo (FASE 0-5)
- Reportes generados y su ubicación
- Comparación antes/después
- Ejemplos de uso

---

#### 4. Automatización post-ingesta: filtrado y permisos en Superset

Ubicación: `tools/superset_post_configurator.py`

- Script que, tras la ingesta ETL, filtra los esquemas/tablas visibles en Superset para mostrar solo los modelos declarados en el pipeline/config.
- Ajusta roles y permisos para el usuario admin y roles estándar, asegurando acceso total y posibilidad de crear charts.
- Corrige la metadata de los datasets (time column, métricas, acceso).
- El flujo es replicable para cualquier base de datos declarada en el pipeline.

---

### Próximos Cambios Necesarios

Según feedback del usuario, aún faltan:

1. **Scripts de Verificación Internos en Contenedores**
   - Convertir comandos manuales en scripts ejecutables
   - Ejemplo: `verificar_clickhouse.sh`, `verificar_superset.sh`
   - Que se ejecuten automáticamente post-proceso

2. **Persistencia de Resultados de Verificación**
   - Todos los resultados deben guardarse en `logs/`
   - Formato JSON para análisis programático

3. **Reintentos y Esperas Configurables**
   - Actualmente algunos timeouts son fijos
   - Deberían ser configurables vía env vars

4. **Alertas Claras de Fallos**
   - Mensajes más descriptivos cuando algo falla
   - Sugerencias de qué revisar/hacer

---

### Archivos Modificados

```
Modificados:
- superset_bootstrap/configure_datasets.py
- docker-compose.yml
- tools/auto_pipeline.sh

Creados:
- tools/clean_all.sh
- tools/verify_clean_state.py
- docs/TOKEN_ROBUSTNESS.md
- docs/AUTOMATED_VERIFICATIONS.md
- docs/CHANGELOG.md (este archivo)

Pendientes de crear:
- tools/verificaciones/clickhouse_verify.sh
- tools/verificaciones/superset_verify.sh
- tools/verificaciones/kafka_verify.sh
- tools/orchestration_helper.sh (wrapper para ejecutar verificaciones)
```

---

### Contexto para Modelos Futuros

**Si retomas este proyecto**, ten en cuenta:

1. **Problema de Autenticación**: 
   - Era causado por race condition entre superset-init y superset-datasets
   - Solucionado con depends_on + wait_for_admin_ready + password reset

2. **Limpieza de Datos**:
   - Usuario quiere estado LIMPIO sin datos antiguos
   - clean_all.sh elimina TODO
   - verify_clean_state.py detecta datos antiguos automáticamente

3. **Filosofía de Diseño**:
   - TODO debe ser automático
   - TODO debe loggearse
   - TODO debe ser verificable
   - NO comandos manuales del usuario

4. **Siguiente Fase**:
   - Crear scripts de verificación por componente
   - Persistir todos los resultados en logs/
   - Hacer reintentos/timeouts configurables
   - Mejorar mensajes de error con sugerencias

---

### Testing Realizado

- ✅ Limpieza total verificada (volúmenes, redes, logs eliminados)
- ⏳ Pipeline limpio con mejoras aún no ejecutado (pendiente de aprobación del usuario)
- ✅ Scripts de verificación creados y documentados
- ✅ Documentación completa generada

---

### Notas Técnicas

**Backoff Exponencial**:
```
Intento 1: 3s
Intento 2: 4.5s (3 * 1.5)
Intento 3: 6.75s (4.5 * 1.5)
Intento 4: 10.125s (6.75 * 1.5)
Intento 5: 15.1875s (10.125 * 1.5)
Intento 6: 22.78s (15.1875 * 1.5)
Intento 7+: 30s (límite máximo)
```

**Contraseñas Intentadas** (en orden):
1. Contraseña proporcionada al script
2. Variable SUPERSET_PASSWORD del entorno
3. "Admin123!" (estándar actual)
4. "admin" (legacy, por compatibilidad)

**Orden de Ejecución de Servicios**:
```
1. clickhouse (healthy)
2. kafka (healthy)
3. connect (healthy)
4. superset (healthy)
5. superset-venv-setup (completed)
6. etl-orchestrator (FASE 0-2: ingesta)
7. superset-init (completed) → crea admin
8. superset-datasets (completed) → configura datasets
```

---

### Estado y checklist de depuración Superset UI (2025-10-24)

- Validación automatizada: admin puede autenticarse, ver 19 datasets y acceder al formulario de creación de charts.
- Configuración async global habilitada y base ClickHouse ETL correctamente registrada.
- Problemas detectados:
  - Aparecen esquemas/tablas no deseados en la UI (más allá de los declarados en el JSON/env).
  - Íconos de advertencia en las tablas (posibles incidencias de metadata, time column, permisos o acceso).
  - El usuario admin está limitado para crear charts libremente.
- No hay errores críticos en los logs de Superset, solo advertencias menores de configuración.

Checklist de depuración:
1. Revisar y ajustar permisos/roles en Superset para admin y otros usuarios.
2. Validar que los datasets expuestos en Superset correspondan solo a los modelos/tablas deseados.
3. Revisar la metadata de los datasets: columnas clave, time column, métricas y acceso.
4. Analizar los íconos de advertencia en la UI y buscar detalles en los logs.
5. Documentar cualquier cambio y resultado en este CHANGELOG.

---
