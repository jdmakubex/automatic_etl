# Variables de Entorno - Guía Completa

Este documento describe todas las variables de entorno disponibles para configurar y controlar el pipeline ETL.

## Variables de Configuración Principal

### DB_CONNECTIONS (Requerida)
Configuración de las bases de datos origen en formato JSON.

**Formato:**
```json
[
  {
    "type": "mysql",
    "name": "nombre_conexion",
    "host": "hostname_o_ip",
    "port": 3306,
    "user": "usuario",
    "pass": "contraseña",
    "db": "nombre_base_datos"
  }
]
```

**Ejemplo con múltiples conexiones:**
```json
[
  {"type":"mysql","name":"fiscalizacion","host":"host.docker.internal","port":3306,"user":"etl_user","pass":"secret","db":"fiscalizacion"},
  {"type":"mysql","name":"sipoa","host":"host.docker.internal","port":3306,"user":"etl_user","pass":"secret","db":"sipoa"}
]
```

**Uso:**
```bash
export DB_CONNECTIONS='[{"type":"mysql","name":"test","host":"localhost","port":3306,"user":"root","pass":"pwd","db":"test"}]'
```

### CLICKHOUSE_DATABASE
Nombre de la base de datos destino en ClickHouse.

**Default:** `fgeo_analytics`

**Uso:**
```bash
export CLICKHOUSE_DATABASE=mi_base_datos
```

### CLICKHOUSE_HOST
Hostname del servidor ClickHouse.

**Default:** `clickhouse`

**Uso:**
```bash
export CLICKHOUSE_HOST=localhost
# O desde fuera de Docker:
export CLICKHOUSE_HOST=host.docker.internal
```

### CLICKHOUSE_PORT
Puerto HTTP de ClickHouse.

**Default:** `8123`

**Uso:**
```bash
export CLICKHOUSE_PORT=8123
```

### CLICKHOUSE_USER
Usuario de ClickHouse.

**Default:** `default`

**Uso:**
```bash
export CLICKHOUSE_USER=admin
```

### CLICKHOUSE_PASSWORD
Contraseña de ClickHouse.

**Default:** `` (vacío)

**Uso:**
```bash
export CLICKHOUSE_PASSWORD=mi_contraseña
```

## Variables de Servicios

### CONNECT_URL
URL del servicio Kafka Connect.

**Default:** `http://connect:8083`

**Uso:**
```bash
export CONNECT_URL=http://localhost:8083
```

### Variables Debezium (ajustes soportados)
Parámetros de ajuste del conector Debezium. Estas variables controlan el comportamiento del conector, pero no las credenciales.

Soportadas:
- DBZ_SERVER_NAME_PREFIX (default: dbserver)
- DBZ_SNAPSHOT_MODE (default: initial)
- DBZ_DECIMAL_MODE (default: string)
- DBZ_BINARY_MODE (default: base64)
- DBZ_TIME_PRECISION (default: connect)
- DBZ_HISTORY_TOPIC (default: schema-changes)

No usar (deprecadas, serán ignoradas):
- DBZ_DATABASE_HOSTNAME
- DBZ_DATABASE_PORT
- DBZ_DATABASE_USER
- DBZ_DATABASE_PASSWORD

Motivo: Las credenciales/host/puerto de MySQL provienen exclusivamente de DB_CONNECTIONS (fuente de verdad única). Mantener variables duplicadas ocasiona inconsistencias.

### SUPERSET_URL
URL del servicio Superset.

**Default:** `http://superset:8088`

**Uso:**
```bash
export SUPERSET_URL=http://localhost:8088
```

### SUPERSET_ADMIN
Usuario administrador de Superset.

**Default:** `admin`

**Uso:**
```bash
export SUPERSET_ADMIN=admin
```

### SUPERSET_PASSWORD
Contraseña del administrador de Superset.

**Default:** `Admin123!`

**Uso:**
```bash
export SUPERSET_PASSWORD=SuperSecretPassword
```

## Variables de Control de Funcionalidad

### ENABLE_VALIDATION
Habilita/deshabilita validaciones generales del sistema.

**Default:** `true`

**Valores:** `true` | `false`

**Uso:**
```bash
# Deshabilitar todas las validaciones
export ENABLE_VALIDATION=false

# Ejecutar script sin validaciones
ENABLE_VALIDATION=false python tools/validators.py
```

**Afecta a:**
- Validación de variables de entorno
- Validación de formato JSON
- Validación de conectividad a servicios

### ENABLE_PERMISSION_TESTS
Habilita/deshabilita pruebas de permisos en Debezium, Kafka y ClickHouse.

**Default:** `true`

**Valores:** `true` | `false`

**Uso:**
```bash
# Ejecutar pruebas de permisos
export ENABLE_PERMISSION_TESTS=true
python tools/test_permissions.py

# Omitir pruebas de permisos
ENABLE_PERMISSION_TESTS=false python tools/test_permissions.py
```

### ENABLE_DEPENDENCY_VERIFICATION
Habilita/deshabilita verificación de dependencias y secuencialidad.

**Default:** `true`

**Valores:** `true` | `false`

**Uso:**
```bash
# Verificar dependencias
export ENABLE_DEPENDENCY_VERIFICATION=true
python tools/verify_dependencies.py

# Omitir verificación de dependencias
ENABLE_DEPENDENCY_VERIFICATION=false python tools/verify_dependencies.py
```

### ENABLE_CDC
Habilita/deshabilita componentes de CDC (Change Data Capture).

**Default:** `false`

**Valores:** `true` | `false`

**Uso:**
```bash
# Habilitar CDC
export ENABLE_CDC=true

# Esto activa:
# - Verificación de Kafka Connect
# - Aplicación de conectores Debezium
# - Creación de pipeline CDC en ClickHouse
```

### ENABLE_SUPERSET
Habilita/deshabilita Superset y sus validaciones.

**Default:** `true`

**Valores:** `true` | `false`

**Uso:**
```bash
# Deshabilitar Superset
export ENABLE_SUPERSET=false

# Útil para pipelines de solo datos sin visualización
```

## Variables de Logging

### LOG_LEVEL
Nivel de detalle de los logs.

**Default:** `INFO`

**Valores:** `DEBUG` | `INFO` | `WARNING` | `ERROR` | `CRITICAL`

**Uso:**
```bash
# Logs detallados para debugging
export LOG_LEVEL=DEBUG
python tools/ingest_runner.py

# Solo errores críticos
LOG_LEVEL=ERROR python tools/ingest_runner.py
```

**Niveles:**
- `DEBUG`: Información muy detallada, útil para desarrollo
- `INFO`: Información general sobre el progreso
- `WARNING`: Advertencias que no detienen la ejecución
- `ERROR`: Errores que requieren atención
- `CRITICAL`: Errores fatales que detienen el proceso

### LOG_FORMAT
Formato de salida de los logs.

**Default:** `text`

**Valores:** `text` | `json`

**Uso:**
```bash
# Formato legible para humanos
export LOG_FORMAT=text
python tools/ingest_runner.py

# Formato JSON estructurado para parseo automático
export LOG_FORMAT=json
python tools/ingest_runner.py
```

**Formato JSON incluye:**
- `timestamp`: ISO 8601 UTC
- `level`: Nivel del log
- `logger`: Nombre del módulo
- `message`: Mensaje del log
- `module`, `function`, `line`: Información de código fuente
- `exception`: Stack trace en caso de errores

**Ejemplo de salida JSON:**
```json
{
  "timestamp": "2025-01-15T10:30:45.123456Z",
  "level": "INFO",
  "logger": "ingest_runner",
  "message": "Iniciando ingesta de datos",
  "module": "ingest_runner",
  "function": "main",
  "line": 123
}
```

## Variables de Validación

### MIN_TABLES
Número mínimo de tablas esperadas en ClickHouse para validación.

**Default:** `1`

**Uso:**
```bash
export MIN_TABLES=5
python tools/validate_clickhouse.py
```

### VALIDATE_DATA
Habilita validación de datos en tablas (no solo esquema).

**Default:** `true`

**Valores:** `true` | `false`

**Uso:**
```bash
# Validar que las tablas tengan datos
export VALIDATE_DATA=true

# Solo validar que las tablas existan
VALIDATE_DATA=false python tools/validate_clickhouse.py
```

### EXPECTED_DATABASE
Nombre de la base de datos esperada en validaciones de Superset.

**Default:** `fgeo_analytics`

**Uso:**
```bash
export EXPECTED_DATABASE=mi_base_datos
python tools/validate_superset.py
```

### MIN_DATASETS
Número mínimo de datasets esperados en Superset.

**Default:** `0`

**Uso:**
```bash
export MIN_DATASETS=10
python tools/validate_superset.py
```

### VALIDATE_CONNECTIVITY
Habilita pruebas de conectividad desde Superset a ClickHouse.

**Default:** `false`

**Valores:** `true` | `false`

**Uso:**
```bash
export VALIDATE_CONNECTIVITY=true
python tools/validate_superset.py
```

## Variables de Salida

### OUTPUT_FILE
Archivo de salida para resultados de validación de ClickHouse.

**Default:** `logs/clickhouse_validation.json`

**Uso:**
```bash
export OUTPUT_FILE=/tmp/my_validation.json
python tools/validate_clickhouse.py
```

### PERMISSION_TEST_OUTPUT
Archivo de salida para resultados de pruebas de permisos.

**Default:** `logs/permission_tests.json`

**Uso:**
```bash
export PERMISSION_TEST_OUTPUT=/tmp/permission_results.json
python tools/test_permissions.py
```

### DEPENDENCY_VERIFICATION_OUTPUT
Archivo de salida para resultados de verificación de dependencias.

**Default:** `logs/dependency_verification.json`

**Uso:**
```bash
export DEPENDENCY_VERIFICATION_OUTPUT=/tmp/deps.json
python tools/verify_dependencies.py
```

## Ejemplos de Uso Combinado

### Modo Desarrollo (Logs detallados, todas las validaciones)
```bash
export LOG_LEVEL=DEBUG
export LOG_FORMAT=text
export ENABLE_VALIDATION=true
export ENABLE_PERMISSION_TESTS=true
export ENABLE_DEPENDENCY_VERIFICATION=true
export VALIDATE_DATA=true

# Ejecutar pipeline
bash bootstrap/run_etl_full.sh
```

### Modo Producción (Logs JSON, validaciones esenciales)
```bash
export LOG_LEVEL=INFO
export LOG_FORMAT=json
export ENABLE_VALIDATION=true
export ENABLE_PERMISSION_TESTS=false
export ENABLE_DEPENDENCY_VERIFICATION=true
export VALIDATE_DATA=true

# Ejecutar pipeline
bash bootstrap/run_etl_full.sh
```

### Modo Rápido (Sin validaciones, solo ingesta)
```bash
export LOG_LEVEL=WARNING
export ENABLE_VALIDATION=false
export ENABLE_PERMISSION_TESTS=false
export ENABLE_DEPENDENCY_VERIFICATION=false

# Ejecutar solo ingesta
docker compose run --rm etl-tools python tools/ingest_runner.py
```

### Modo CDC Completo
```bash
export ENABLE_CDC=true
export ENABLE_SUPERSET=true
export LOG_LEVEL=INFO
export LOG_FORMAT=json

# Ejecutar pipeline completo
bash bootstrap/run_etl_full.sh
```

## Configuración en Archivo .env

Todas estas variables pueden configurarse en el archivo `.env`:

```bash
# .env example
DB_CONNECTIONS=[{"type":"mysql","name":"test","host":"localhost","port":3306,"user":"root","pass":"pwd","db":"test"}]
CLICKHOUSE_DATABASE=fgeo_analytics
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=

# Servicios
CONNECT_URL=http://connect:8083
SUPERSET_URL=http://superset:8088
SUPERSET_ADMIN=admin
SUPERSET_PASSWORD=Admin123!

# Control
ENABLE_VALIDATION=true
ENABLE_PERMISSION_TESTS=true
ENABLE_DEPENDENCY_VERIFICATION=true
ENABLE_CDC=false
ENABLE_SUPERSET=true

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=text

# Validación
MIN_TABLES=1
VALIDATE_DATA=true
EXPECTED_DATABASE=fgeo_analytics
MIN_DATASETS=0
VALIDATE_CONNECTIVITY=false

# Salidas
OUTPUT_FILE=logs/clickhouse_validation.json
PERMISSION_TEST_OUTPUT=logs/permission_tests.json
DEPENDENCY_VERIFICATION_OUTPUT=logs/dependency_verification.json
```

## Variables de Docker Compose

Algunas variables son específicas de Docker Compose (definidas en `docker-compose.yml`):

- `KAFKA_CLUSTER_ID`: UUID del clúster Kafka
- Variables de configuración de Kafka (brokers, controladores)
- Variables de configuración de servicios individuales

Consulta `docker-compose.yml` para más detalles.

## Precedencia de Variables

El orden de precedencia es:
1. Variables definidas en línea de comandos: `LOG_LEVEL=DEBUG python script.py`
2. Variables exportadas en la shell: `export LOG_LEVEL=DEBUG`
3. Variables en archivo `.env`
4. Valores por defecto en el código

## Referencias

- Documentación de dependencias: `docs/DEPENDENCIES.md`
- Guía de errores: `docs/ERROR_RECOVERY.md`
- Mejoras de robustez: `docs/ROBUSTNESS_IMPROVEMENTS.md`
