# Guía de Pruebas y Validación del Pipeline ETL

Esta guía describe cómo ejecutar y validar el pipeline ETL usando las herramientas de prueba y auditoría disponibles.

## Índice
- [Resumen de Herramientas](#resumen-de-herramientas)
- [Flujo de Validación Completo](#flujo-de-validación-completo)
- [Pruebas Individuales](#pruebas-individuales)
- [Interpretación de Resultados](#interpretación-de-resultados)
- [Integración con CI/CD](#integración-con-cicd)

## Resumen de Herramientas

### Test Runner Principal
**Script:** `tools/run_tests.sh`

Ejecuta todas las pruebas en secuencia y genera un reporte consolidado.

```bash
# Ejecutar todas las pruebas
bash tools/run_tests.sh

# Ejecutar con configuración personalizada
ENABLE_UNIT_TESTS=true \
ENABLE_PERMISSION_TESTS=true \
ENABLE_DEPENDENCY_VERIFICATION=true \
ENABLE_VALIDATION=true \
bash tools/run_tests.sh
```

### Herramientas de Prueba

| Herramienta | Propósito | Salida |
|-------------|-----------|--------|
| `unittest` | Pruebas unitarias de código Python | Terminal + logs/unit_tests.log |
| `tools/validators.py` | Validación de entorno y configuración | Terminal + resultados en consola |
| `tools/test_permissions.py` | Pruebas de permisos en tecnologías | Terminal + logs/permission_tests.json |
| `tools/verify_dependencies.py` | Verificación de dependencias y orden | Terminal + logs/dependency_verification.json |
| `tools/validate_clickhouse.py` | Validación de ClickHouse post-ingesta | Terminal + logs/clickhouse_validation.json |
| `tools/validate_superset.py` | Validación de Superset | Terminal + logs/superset_validation.json |

## Flujo de Validación Completo

### 1. Pre-Configuración (Antes de iniciar servicios)

```bash
# 1.1. Validar que el archivo .env existe
if [ ! -f .env ]; then
    echo "Error: Archivo .env no encontrado"
    echo "Copiar de ejemplo: cp .env.example .env"
    exit 1
fi

# 1.2. Validar sintaxis de DB_CONNECTIONS
source .env
echo "$DB_CONNECTIONS" | python -m json.tool > /dev/null
if [ $? -eq 0 ]; then
    echo "✓ DB_CONNECTIONS tiene sintaxis JSON válida"
else
    echo "✗ DB_CONNECTIONS tiene sintaxis inválida"
    exit 1
fi

# 1.3. Ejecutar validación de entorno
docker compose run --rm etl-tools python tools/validators.py
```

**Qué verifica:**
- Variables de entorno requeridas definidas
- Formato JSON de DB_CONNECTIONS correcto
- Dependencias Python instaladas (si servicios están corriendo)

### 2. Pre-Inicio (Antes de ingesta)

```bash
# 2.1. Iniciar servicios base
docker compose up -d clickhouse

# 2.2. Esperar y verificar dependencias
docker compose run --rm etl-tools python tools/verify_dependencies.py

# 2.3. Probar permisos (recomendado)
docker compose run --rm etl-tools python tools/test_permissions.py
```

**Qué verifica:**
- ClickHouse está listo y responde
- Usuario de ClickHouse tiene permisos necesarios
- Bases de datos origen accesibles (si están disponibles)
- Usuarios de MySQL tienen permisos de replicación (para CDC)
- Binlog habilitado en MySQL (para CDC)

### 3. Post-Ingesta (Después de cargar datos)

```bash
# 3.1. Ejecutar ingesta
docker compose run --rm etl-tools python tools/ingest_runner.py

# 3.2. Validar resultados en ClickHouse
docker compose run --rm etl-tools python tools/validate_clickhouse.py

# 3.3. Validar cantidad de datos
docker exec clickhouse clickhouse-client -q "
SELECT 
    table,
    sum(rows) as total_rows
FROM system.parts
WHERE database = 'fgeo_analytics'
GROUP BY table
ORDER BY table
"
```

**Qué verifica:**
- Base de datos existe
- Tablas creadas correctamente
- Tablas contienen datos
- Esquema de tablas es correcto (columnas y tipos)

### 4. Post-Configuración (Después de configurar Superset)

```bash
# 4.1. Iniciar Superset
docker compose up -d superset superset-venv-setup superset-init

# 4.2. Esperar a que esté listo
sleep 30

# 4.3. Validar Superset
docker compose run --rm configurator python tools/validate_superset.py
```

**Qué verifica:**
- Superset responde y está saludable
- Autenticación funciona
- Base de datos ClickHouse está registrada
- Datasets están disponibles (si se configuraron)

### 5. Validación Continua (Durante operación)

```bash
# Ejecutar suite completa periódicamente
bash tools/run_tests.sh

# O configurar como cron job
# 0 */6 * * * cd /path/to/etl && bash tools/run_tests.sh >> logs/scheduled_tests.log 2>&1
```

## Pruebas Individuales

### Pruebas Unitarias

```bash
# Todas las pruebas
python -m unittest discover tests -v

# Prueba específica
python -m unittest tests.test_validators -v

# Una sola prueba
python -m unittest tests.test_validators.TestValidators.test_validate_env_var_required_present -v
```

**Cobertura actual:**
- Validación de variables de entorno
- Validación de DB_CONNECTIONS
- Funciones de guardado de resultados

### Pruebas de Permisos

```bash
# Ejecutar todas las pruebas de permisos
python tools/test_permissions.py

# Con logs detallados
LOG_LEVEL=DEBUG python tools/test_permissions.py

# Guardar en archivo personalizado
PERMISSION_TEST_OUTPUT=/tmp/permissions.json python tools/test_permissions.py

# Deshabilitar pruebas de permisos
ENABLE_PERMISSION_TESTS=false python tools/test_permissions.py
```

**Casos probados:**
- **ClickHouse:**
  - Lectura de bases de datos (SHOW DATABASES)
  - Creación de base de datos (CREATE DATABASE)
  - Creación de tabla (CREATE TABLE)
  - Inserción de datos (INSERT)
  - Lectura de datos (SELECT)
  - Eliminación de base de datos (DROP DATABASE)

- **Kafka Connect:**
  - Listado de conectores (GET /connectors)
  - Listado de plugins (GET /connector-plugins)

- **MySQL (para Debezium):**
  - REPLICATION SLAVE
  - REPLICATION CLIENT
  - SELECT
  - Binlog habilitado (log_bin=ON)

### Verificación de Dependencias

```bash
# Verificar que servicios estén listos
python tools/verify_dependencies.py

# Con configuración personalizada
ENABLE_CDC=true \
ENABLE_SUPERSET=true \
python tools/verify_dependencies.py

# Cambiar reintentos y timeout
# (modificar en el script o vía variables si se implementan)
```

**Secuencia verificada:**
1. ClickHouse está listo
2. Base de datos existe (o puede ser creada)
3. Kafka Connect está listo (si ENABLE_CDC=true)
4. Superset está listo (si ENABLE_SUPERSET=true)
5. Dependencias Python instaladas

### Validación de ClickHouse

```bash
# Validación básica
python tools/validate_clickhouse.py

# Con parámetros personalizados
CLICKHOUSE_DATABASE=mi_base_datos \
MIN_TABLES=5 \
VALIDATE_DATA=true \
python tools/validate_clickhouse.py

# Logs en formato JSON
LOG_FORMAT=json python tools/validate_clickhouse.py
```

**Validaciones realizadas:**
- Base de datos existe
- Número mínimo de tablas
- Tablas tienen datos (si VALIDATE_DATA=true)
- Esquema de tablas correcto

### Validación de Superset

```bash
# Validación básica
python tools/validate_superset.py

# Con parámetros personalizados
SUPERSET_URL=http://localhost:8088 \
SUPERSET_ADMIN=admin \
SUPERSET_PASSWORD=Admin123! \
EXPECTED_DATABASE=fgeo_analytics \
MIN_DATASETS=5 \
python tools/validate_superset.py

# Con validación de conectividad
VALIDATE_CONNECTIVITY=true python tools/validate_superset.py
```

**Validaciones realizadas:**
- Superset responde (health check)
- Autenticación exitosa
- Base de datos ClickHouse registrada
- Número mínimo de datasets
- Conectividad desde Superset a ClickHouse (si VALIDATE_CONNECTIVITY=true)

## Interpretación de Resultados

### Formato de Salida JSON

Todos los scripts de validación generan JSON estructurado:

```json
{
  "timestamp": "2025-01-15T10:30:45.123456Z",
  "tests": {
    "test_name": {
      "passed": true,
      "message": "Descripción del resultado"
    }
  },
  "summary": {
    "total": 3,
    "passed": 2,
    "failed": 1
  },
  "duration_seconds": 1.234
}
```

### Códigos de Salida

| Código | Significado | Acción |
|--------|-------------|--------|
| 0 | Todas las pruebas pasaron | Continuar con siguiente paso |
| 1 | Algunas pruebas fallaron | Revisar logs y corregir |
| 2 | Error inesperado | Revisar stack trace y reportar bug |

### Interpretación de Logs

**Nivel INFO:** Progreso normal
```
[2025-01-15 10:30:45] [INFO] Iniciando pruebas de permisos
[2025-01-15 10:30:46] [INFO] ✓ ClickHouse: Usuario tiene todos los permisos necesarios
```

**Nivel WARNING:** Advertencia no crítica
```
[2025-01-15 10:30:47] [WARNING] ⚠ MySQL: Permisos faltantes: REPLICATION SLAVE
```
*Acción:* Verificar si es necesario CDC. Si no, ignorar.

**Nivel ERROR:** Error que requiere atención
```
[2025-01-15 10:30:48] [ERROR] ✗ ClickHouse no disponible: Connection refused
```
*Acción:* Verificar que ClickHouse esté corriendo.

### Archivos de Resultado

Todos los resultados se guardan en `logs/`:

```bash
# Ver resultados de permisos
cat logs/permission_tests.json | python -m json.tool

# Ver resultados de dependencias
cat logs/dependency_verification.json | python -m json.tool

# Ver logs de validación
tail -f logs/validation.log
```

## Integración con CI/CD

### GitHub Actions

```yaml
name: ETL Pipeline Tests

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v2
    
    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: '3.9'
    
    - name: Install dependencies
      run: |
        pip install -r requirements.txt
    
    - name: Run unit tests
      run: |
        python -m unittest discover tests -v
    
    - name: Validate environment (mock)
      run: |
        export DB_CONNECTIONS='[]'
        python tools/validators.py || true
    
    - name: Upload test results
      if: always()
      uses: actions/upload-artifact@v2
      with:
        name: test-results
        path: logs/
```

### GitLab CI

```yaml
stages:
  - test
  - validate

unit-tests:
  stage: test
  script:
    - pip install -r requirements.txt
    - python -m unittest discover tests -v
  artifacts:
    paths:
      - logs/
    when: always

validation:
  stage: validate
  script:
    - export DB_CONNECTIONS='[]'
    - python tools/validators.py || true
  artifacts:
    paths:
      - logs/
    when: always
```

### Jenkins

```groovy
pipeline {
    agent any
    
    stages {
        stage('Setup') {
            steps {
                sh 'pip install -r requirements.txt'
            }
        }
        
        stage('Unit Tests') {
            steps {
                sh 'python -m unittest discover tests -v'
            }
        }
        
        stage('Validation') {
            steps {
                sh '''
                    export DB_CONNECTIONS='[]'
                    python tools/validators.py || true
                '''
            }
        }
    }
    
    post {
        always {
            archiveArtifacts artifacts: 'logs/**/*', fingerprint: true
        }
    }
}
```

### Cron Job (Validación Periódica)

```bash
# Agregar a crontab
# crontab -e

# Ejecutar validaciones cada 6 horas
0 */6 * * * cd /path/to/automatic_etl && bash tools/run_tests.sh >> logs/scheduled_tests.log 2>&1

# Ejecutar solo verificación de dependencias cada hora
0 * * * * cd /path/to/automatic_etl && python tools/verify_dependencies.py >> logs/hourly_check.log 2>&1
```

## Mejores Prácticas

### Durante Desarrollo
- Ejecutar `python -m unittest discover tests -v` antes de cada commit
- Usar `LOG_LEVEL=DEBUG` para debugging
- Usar `LOG_FORMAT=text` para legibilidad

### Durante Despliegue
- Ejecutar `bash tools/run_tests.sh` antes de desplegar
- Verificar todos los logs en `logs/` antes de continuar
- Guardar logs de despliegue para auditoría

### En Producción
- Ejecutar validaciones periódicas con cron
- Usar `LOG_FORMAT=json` para parseo automático
- Monitorear archivos JSON en `logs/` para alertas
- Rotar logs regularmente

### Troubleshooting
- Consultar `docs/ERROR_RECOVERY.md` para errores comunes
- Revisar `docs/DEPENDENCIES.md` para orden de ejecución
- Verificar `docs/ENVIRONMENT_VARIABLES.md` para configuración

## Referencias

- **Documentación principal:** README.md
- **Guía de dependencias:** docs/DEPENDENCIES.md
- **Variables de entorno:** docs/ENVIRONMENT_VARIABLES.md
- **Recuperación de errores:** docs/ERROR_RECOVERY.md
- **Mejoras de robustez:** docs/ROBUSTNESS_IMPROVEMENTS.md
