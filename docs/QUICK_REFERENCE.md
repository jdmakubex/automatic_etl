# Referencia Rápida - Nuevas Características de Auditoría y Robustez

## Scripts Nuevos

### 1. Pruebas de Permisos (`tools/test_permissions.py`)
Valida permisos en todas las tecnologías del pipeline.

**Uso básico:**
```bash
python tools/test_permissions.py
```

**Configuración:**
```bash
# Deshabilitar
ENABLE_PERMISSION_TESTS=false python tools/test_permissions.py

# Personalizar salida
PERMISSION_TEST_OUTPUT=/tmp/permisos.json python tools/test_permissions.py

# Logs detallados
LOG_LEVEL=DEBUG python tools/test_permissions.py
```

**Resultado:** `logs/permission_tests.json`

---

### 2. Verificación de Dependencias (`tools/verify_dependencies.py`)
Verifica que servicios estén listos y en el orden correcto.

**Uso básico:**
```bash
python tools/verify_dependencies.py
```

**Configuración:**
```bash
# Deshabilitar
ENABLE_DEPENDENCY_VERIFICATION=false python tools/verify_dependencies.py

# Con CDC
ENABLE_CDC=true python tools/verify_dependencies.py

# Con Superset
ENABLE_SUPERSET=true python tools/verify_dependencies.py

# Personalizar salida
DEPENDENCY_VERIFICATION_OUTPUT=/tmp/deps.json python tools/verify_dependencies.py
```

**Resultado:** `logs/dependency_verification.json`

---

### 3. Test Runner (`tools/run_tests.sh`)
Ejecuta todas las pruebas y validaciones en secuencia.

**Uso básico:**
```bash
bash tools/run_tests.sh
```

**Configuración:**
```bash
# Control granular
ENABLE_UNIT_TESTS=true \
ENABLE_VALIDATION=true \
ENABLE_PERMISSION_TESTS=true \
ENABLE_DEPENDENCY_VERIFICATION=true \
bash tools/run_tests.sh
```

**Resultados:** 
- `logs/unit_tests.log`
- `logs/validation.log`
- `logs/permission_tests.log`
- `logs/dependency_verification.log`

---

## Pruebas Unitarias

### Ejecutar Todas las Pruebas
```bash
python -m unittest discover tests -v
```

### Ejecutar Prueba Específica
```bash
python -m unittest tests.test_validators
python -m unittest tests.test_permissions
python -m unittest tests.test_verify_dependencies
```

### Agregar Nueva Prueba
1. Crear archivo en `tests/test_*.py`
2. Importar `unittest`
3. Crear clase que hereda de `unittest.TestCase`
4. Nombrar métodos con prefijo `test_`

---

## Variables de Control

### Habilitar/Deshabilitar Funcionalidad

| Variable | Default | Descripción |
|----------|---------|-------------|
| `ENABLE_VALIDATION` | `true` | Validaciones generales |
| `ENABLE_PERMISSION_TESTS` | `true` | Pruebas de permisos |
| `ENABLE_DEPENDENCY_VERIFICATION` | `true` | Verificación de dependencias |
| `ENABLE_CDC` | `false` | Componentes CDC |
| `ENABLE_SUPERSET` | `true` | Superset |

### Logging

| Variable | Default | Valores | Descripción |
|----------|---------|---------|-------------|
| `LOG_LEVEL` | `INFO` | DEBUG, INFO, WARNING, ERROR | Nivel de detalle |
| `LOG_FORMAT` | `text` | text, json | Formato de salida |

### Archivos de Salida

| Variable | Default | Descripción |
|----------|---------|-------------|
| `OUTPUT_FILE` | `logs/clickhouse_validation.json` | Validación ClickHouse |
| `PERMISSION_TEST_OUTPUT` | `logs/permission_tests.json` | Pruebas de permisos |
| `DEPENDENCY_VERIFICATION_OUTPUT` | `logs/dependency_verification.json` | Verificación de dependencias |

---

## Flujos de Trabajo Comunes

### Pre-Despliegue (Validación Completa)
```bash
# 1. Validar configuración
python tools/validators.py

# 2. Ejecutar todas las pruebas
bash tools/run_tests.sh

# 3. Revisar resultados
ls -lh logs/

# 4. Si todo OK, desplegar
bash bootstrap/run_etl_full.sh
```

### Desarrollo Rápido (Sin Validaciones)
```bash
# Deshabilitar todo
export ENABLE_VALIDATION=false
export ENABLE_PERMISSION_TESTS=false
export ENABLE_DEPENDENCY_VERIFICATION=false

# Ejecutar pipeline
bash bootstrap/run_etl_full.sh
```

### Debugging (Logs Detallados)
```bash
# Habilitar logs detallados
export LOG_LEVEL=DEBUG
export LOG_FORMAT=text

# Ejecutar script específico
python tools/test_permissions.py
```

### Producción (Logs JSON)
```bash
# Configurar para producción
export LOG_LEVEL=INFO
export LOG_FORMAT=json
export ENABLE_VALIDATION=true
export ENABLE_DEPENDENCY_VERIFICATION=true
export ENABLE_PERMISSION_TESTS=false

# Ejecutar pipeline
bash bootstrap/run_etl_full.sh
```

### Validación Post-Ingesta
```bash
# 1. Verificar ClickHouse
python tools/validate_clickhouse.py

# 2. Verificar Superset (si aplica)
python tools/validate_superset.py

# 3. Revisar logs
cat logs/clickhouse_validation.json | python -m json.tool
```

---

## Interpretación Rápida de Resultados

### Código de Salida
- `0` = Éxito, continuar
- `1` = Fallos, revisar logs
- `2` = Error crítico, reportar

### Símbolos en Logs
- `✓` = Prueba pasó
- `✗` = Prueba falló
- `⚠` = Advertencia
- `⊝` = Omitido/Deshabilitado

### Logs JSON
```bash
# Ver resumen
cat logs/permission_tests.json | jq '.summary'

# Ver solo fallos
cat logs/permission_tests.json | jq '.tests | to_entries[] | select(.value.passed == false)'

# Ver duración
cat logs/dependency_verification.json | jq '.duration_seconds'
```

---

## Integración en Scripts

### En Bash
```bash
#!/bin/bash
set -e

# Validar antes de continuar
if ! python tools/verify_dependencies.py; then
    echo "Dependencias no listas, abortando"
    exit 1
fi

# Continuar con el proceso...
```

### En Python
```python
import subprocess
import sys

# Ejecutar validación
result = subprocess.run(
    ["python", "tools/validators.py"],
    capture_output=True
)

if result.returncode != 0:
    print("Validación falló")
    sys.exit(1)

# Continuar...
```

---

## Archivos de Logs

### Ubicación
Todos los logs se guardan en `logs/`:
```
logs/
├── .gitkeep
├── clickhouse_validation.json
├── dependency_verification.json
├── etl_full.log
├── permission_tests.json
├── superset_validation.json
├── unit_tests.log
└── validation.log
```

### Rotación Manual
```bash
# Crear backup con timestamp
timestamp=$(date +%Y%m%d_%H%M%S)
tar -czf logs_backup_${timestamp}.tar.gz logs/

# Limpiar logs antiguos
rm logs/*.json logs/*.log
```

---

## Documentación Completa

- **Guía de pruebas:** `docs/TESTING_GUIDE.md`
- **Variables de entorno:** `docs/ENVIRONMENT_VARIABLES.md`
- **Dependencias:** `docs/DEPENDENCIES.md`
- **Recuperación de errores:** `docs/ERROR_RECOVERY.md`
- **Mejoras de robustez:** `docs/ROBUSTNESS_IMPROVEMENTS.md`

---

## Soporte

Para problemas o dudas:
1. Revisar logs en `logs/`
2. Consultar `docs/ERROR_RECOVERY.md`
3. Verificar variables de entorno en `docs/ENVIRONMENT_VARIABLES.md`
4. Revisar orden de ejecución en `docs/DEPENDENCIES.md`
