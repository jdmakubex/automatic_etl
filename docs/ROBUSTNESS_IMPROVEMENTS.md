# Mejoras de Robustez del Pipeline ETL

Este documento resume todas las mejoras implementadas para hacer el pipeline ETL más robusto, observable y fácil de mantener.

## Resumen de Cambios

### 1. Healthchecks en Docker Compose ✅

**Problema anterior**: Los servicios no tenían healthchecks, dificultando detectar cuándo estaban listos.

**Solución**: Se agregaron healthchecks a todos los servicios:

```yaml
# Ejemplo: ClickHouse
healthcheck:
  test: ["CMD-SHELL", "wget -qO- http://localhost:8123/ping | grep -q 'Ok'"]
  interval: 10s
  timeout: 5s
  retries: 12

# Ejemplo: Superset
healthcheck:
  test: ["CMD-SHELL", "curl -sf http://localhost:8088/health || exit 1"]
  interval: 10s
  timeout: 5s
  retries: 30

# Ejemplo: Servicios auxiliares (etl-tools, pipeline-gen, etc.)
healthcheck:
  test: ["CMD-SHELL", "test -d /app/tools && python3 --version || exit 1"]
  interval: 10s
  timeout: 5s
  retries: 3
```

**Beneficios**:
- Docker Compose espera a que servicios estén saludables antes de iniciar dependientes
- `docker compose ps` muestra estado de salud de cada servicio
- Detección automática de servicios fallidos

---

### 2. Validación de Variables de Entorno ✅

**Problema anterior**: Errores crípticos cuando faltaban variables o tenían formato incorrecto.

**Solución**: Nuevo módulo `tools/validators.py` que valida:

- Variables de entorno críticas (DB_CONNECTIONS, CLICKHOUSE_DATABASE, etc.)
- Formato JSON de DB_CONNECTIONS
- Dependencias Python requeridas
- Conectividad a servicios (ClickHouse, Kafka Connect)

**Uso**:
```bash
# Validar todo el entorno
docker compose run --rm etl-tools python tools/validators.py

# O directamente en Python
python tools/validators.py
```

**Beneficios**:
- Errores claros y accionables
- Detecta problemas antes de ejecutar el pipeline
- Guía paso a paso para corregir errores

---

### 3. Manejo de Errores Diferenciado ✅

**Problema anterior**: Cualquier error abortaba todo el proceso, incluso si era recuperable.

**Solución**: Clasificación de errores en tres tipos:

1. **Errores Recuperables** (`RecoverableError`):
   - Ejemplo: Una tabla falla pero otras continúan
   - Ejemplo: Timeout temporal de red
   - Acción: Se registra, se advierte, pero el proceso continúa

2. **Errores Fatales** (`FatalError`):
   - Ejemplo: Credenciales inválidas
   - Ejemplo: Servicio no disponible
   - Acción: Se aborta inmediatamente con mensaje claro

3. **Errores Inesperados**:
   - Bugs o condiciones no previstas
   - Acción: Se registra con stack trace completo

**Implementado en**:
- `ingest_runner.py`: Continúa con siguiente tabla si una falla
- `apply_connectors.py`: Continúa con siguiente conector si uno falla
- `cdc_bootstrap.py`: Registra errores pero completa lo que puede

**Beneficios**:
- Pipeline más resiliente
- No se pierde trabajo por un error puntual
- Errores fatales se identifican claramente

---

### 4. Logging Estructurado JSON ✅

**Problema anterior**: Logs en formato texto difíciles de parsear y analizar.

**Solución**: Soporte para logging estructurado JSON:

**Formato texto (default)**:
```
2025-01-06 12:00:00 [INFO] Conectando a ClickHouse...
2025-01-06 12:00:01 [ERROR] No se pudo conectar: Connection refused
```

**Formato JSON**:
```json
{
  "timestamp": "2025-01-06T12:00:00Z",
  "level": "ERROR",
  "logger": "ingest_runner",
  "message": "No se pudo conectar: Connection refused",
  "module": "ingest_runner",
  "function": "get_clickhouse_client",
  "line": 408,
  "exception": "..."
}
```

**Uso**:
```bash
# Habilitar logs JSON
export LOG_FORMAT=json
python tools/ingest_runner.py

# O inline
LOG_FORMAT=json python tools/ingest_runner.py
```

**Beneficios**:
- Fácil de parsear con herramientas (jq, logstash, etc.)
- Información estructurada (timestamp, nivel, módulo, función, línea)
- Contexto completo en cada log
- Stack traces incluidos en errores

---

### 5. Scripts de Validación Automática ✅

**Problema anterior**: Validación manual tediosa y propensa a errores.

**Solución**: Scripts automatizados para validar el sistema:

#### `validate_clickhouse.py`
Valida:
- Base de datos existe
- Tablas existen (cantidad mínima)
- Datos en tablas
- Esquema de tablas (columnas, tipos)

```bash
# Ejecutar validación
docker compose run --rm etl-tools python tools/validate_clickhouse.py

# Con logs JSON
LOG_FORMAT=json docker compose run --rm etl-tools python tools/validate_clickhouse.py

# Genera: logs/clickhouse_validation.json
```

#### `validate_superset.py`
Valida:
- Superset responde (health endpoint)
- Autenticación funciona
- Bases de datos configuradas
- Datasets disponibles
- (Opcional) Conectividad de datasets

```bash
# Ejecutar validación
docker compose run --rm configurator python tools/validate_superset.py

# Genera: logs/superset_validation.json
```

**Salida ejemplo**:
```json
{
  "timestamp": "2025-01-06T12:00:00Z",
  "component": "clickhouse",
  "database": "fgeo_analytics",
  "tests": [
    {
      "test": "database_exists",
      "status": "PASS",
      "details": {"found": true}
    },
    {
      "test": "tables_exist",
      "status": "PASS",
      "details": {
        "count": 5,
        "tables": [...]
      }
    }
  ],
  "summary": {
    "total": 10,
    "passed": 10,
    "failed": 0,
    "errors": 0
  }
}
```

**Beneficios**:
- Validación automática post-deployment
- Reportes JSON analizables
- Exit codes para CI/CD
- Detecta problemas temprano

---

### 6. Mejoras en `run_etl_full.sh` ✅

**Problema anterior**: Script monolítico sin manejo de errores.

**Solución**: Refactorización completa:

**Nuevas funciones**:
```bash
log_info()       # Log con timestamp y nivel INFO
log_error()      # Log con timestamp y nivel ERROR
log_warning()    # Log con timestamp y nivel WARNING
handle_fatal_error()       # Manejo de errores fatales
handle_recoverable_error() # Manejo de errores recuperables
```

**Mejoras**:
- Validación de variables de entorno al inicio
- Healthchecks antes de continuar
- Manejo de errores en cada paso
- Validaciones automáticas al final
- Referencias a documentación de errores

**Beneficios**:
- Script más robusto
- Errores claros y accionables
- Proceso no se rompe por errores menores
- Validaciones post-ejecución

---

### 7. Documentación de Errores y Recuperación ✅

**Problema anterior**: No había guía para resolver errores comunes.

**Solución**: Guía completa en `docs/ERROR_RECOVERY.md`:

**Contenido**:
1. **Tipos de errores**: Recuperables vs fatales
2. **Errores comunes**: 10+ escenarios con soluciones
   - Servicios no inician
   - Variables no definidas
   - Servicios no responden
   - Permisos insuficientes
   - Espacio en disco
   - Puertos ocupados
   - Y más...
3. **Procedimientos de recuperación**:
   - Recuperación total (reset completo)
   - Recuperación parcial (solo datos)
   - Recuperación de configuración
4. **Logs y diagnóstico**:
   - Ubicación de logs
   - Formato JSON
   - Comandos de diagnóstico

**Ejemplo de entrada**:
```markdown
### 3. ClickHouse no responde

#### Error
FatalError: No se pudo conectar a ClickHouse en clickhouse:8123

#### Causa
- Servicio ClickHouse no está corriendo
- Puerto no accesible
- Healthcheck fallando

#### Diagnóstico
docker ps -a | grep clickhouse
docker logs clickhouse
docker inspect clickhouse | grep -A 10 Health

#### Solución
docker compose restart clickhouse
curl http://localhost:8123/ping

# Si persiste, recrear contenedor
docker compose down clickhouse
docker compose up -d clickhouse
```

**Beneficios**:
- Soluciones paso a paso
- Ejemplos reales
- Comandos copy-paste
- Diagnóstico guiado

---

## Uso de las Nuevas Características

### Flujo Típico con Validaciones

```bash
# 1. Validar entorno antes de empezar
docker compose run --rm etl-tools python tools/validators.py

# 2. Ejecutar pipeline
bash bootstrap/run_etl_full.sh

# 3. Validar resultados
docker compose run --rm etl-tools python tools/validate_clickhouse.py
docker compose run --rm configurator python tools/validate_superset.py

# 4. Si hay errores, consultar guía
cat docs/ERROR_RECOVERY.md
```

### Habilitar Logs JSON

```bash
# En variables de entorno
export LOG_FORMAT=json

# O inline
LOG_FORMAT=json python tools/ingest_runner.py

# En scripts bash (run_etl_full.sh)
# Los logs ya incluyen timestamps y niveles
```

### Validaciones en CI/CD

```yaml
# Ejemplo: GitHub Actions
- name: Validate environment
  run: docker compose run --rm etl-tools python tools/validators.py

- name: Run ETL
  run: bash bootstrap/run_etl_full.sh

- name: Validate ClickHouse
  run: docker compose run --rm etl-tools python tools/validate_clickhouse.py

- name: Validate Superset
  run: docker compose run --rm configurator python tools/validate_superset.py
```

---

## Checklist de Verificación

Después de aplicar estos cambios, verifica:

- [x] Todos los servicios tienen healthchecks configurados
- [x] `validators.py` ejecuta sin errores
- [x] Scripts Python manejan errores recuperables
- [x] Logs JSON funcionan con `LOG_FORMAT=json`
- [x] `validate_clickhouse.py` genera reporte JSON
- [x] `validate_superset.py` genera reporte JSON
- [x] `docs/ERROR_RECOVERY.md` está completo y útil
- [x] README documenta nuevas características
- [ ] Pipeline completo ejecuta sin errores fatales
- [ ] Validaciones post-ejecución pasan

---

## Próximos Pasos Recomendados

1. **Integración con CI/CD**: Agregar validaciones a pipeline de CI/CD
2. **Monitoreo**: Usar logs JSON con herramientas de monitoreo (Datadog, ELK, etc.)
3. **Alertas**: Configurar alertas basadas en healthchecks
4. **Métricas**: Extraer métricas de reportes JSON de validación
5. **Documentación adicional**: Agregar runbooks para operaciones comunes

---

## Soporte

Para problemas o preguntas:

1. Consulta `docs/ERROR_RECOVERY.md`
2. Revisa logs con `docker logs <servicio>`
3. Ejecuta validaciones automáticas
4. Verifica healthchecks con `docker compose ps`
5. Abre un issue en GitHub con:
   - Logs relevantes
   - Reporte de validaciones
   - Comandos ejecutados
   - Variables de entorno (sin credenciales)

---

## Changelog

- **2025-01-06**: Implementación inicial de mejoras de robustez
  - Healthchecks en todos los servicios
  - Validadores de entorno
  - Manejo de errores diferenciado
  - Logging JSON
  - Scripts de validación automática
  - Documentación de errores
