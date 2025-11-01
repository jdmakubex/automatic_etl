# SEGURIDAD Y CONFIGURACIÓN DE VARIABLES DE ENTORNO

## Descripción

Este sistema garantiza que no existan valores hardcodeados (credenciales, URLs, etc.) en el código y que todas las variables de entorno estén configuradas correctamente.

## Principios de Seguridad

### ❌ NO hacer (valores hardcodeados):
```python
# INCORRECTO - valores hardcodeados
METABASE_URL = "http://metabase:3000" 
ADMIN_EMAIL = "admin@admin.com"
PASSWORD = "Admin123!"
```

### ✅ SÍ hacer (variables de entorno):
```python
# CORRECTO - variables desde .env
METABASE_URL = os.getenv("METABASE_URL")
ADMIN_EMAIL = os.getenv("METABASE_ADMIN") 
PASSWORD = os.getenv("METABASE_PASSWORD")
```

## Variables Requeridas en .env

### ClickHouse
```env
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_DEFAULT_USER=default
CLICKHOUSE_DEFAULT_PASSWORD=ClickHouse123!
CLICKHOUSE_ETL_USER=etl
CLICKHOUSE_ETL_PASSWORD=Et1Ingest!
CLICKHOUSE_DATABASE=fgeo_analytics
```

### Metabase
```env
METABASE_URL=http://metabase:3000
METABASE_ADMIN=admin@admin.com
METABASE_PASSWORD=Admin123!
```

### Superset  
```env
SUPERSET_URL=http://superset:8088
SUPERSET_ADMIN=admin
SUPERSET_PASSWORD=Admin123!
```

### Pipeline
```env
MODE_INGEST=cdc
KAFKA_BROKERS=kafka:9092
DB_CONNECTIONS=[{"name":"fiscalizacion","type":"mysql",...}]
```

## Validación Automática

### Script de Validación: `tools/validate_environment.py`

**Funciones:**
- ✅ Verifica que todas las variables requeridas estén configuradas
- ✅ Detecta valores hardcodeados en scripts críticos  
- ✅ Valida formato de configuraciones (URLs, emails, JSON)
- ✅ Genera reporte detallado de problemas

**Ejecución automática:**
```bash
# Se ejecuta automáticamente en:
./start_automated_pipeline.sh

# O manualmente:
docker compose exec etl-tools python3 tools/validate_environment.py
```

## Archivos Corregidos

### Scripts de Metabase (sin valores hardcodeados):
- `tools/metabase_query_test.py`
- `tools/metabase_diagnostic.py`
- `tools/metabase_create_admin.py`  
- `tools/metabase_create_dashboard.py`
- `tools/metabase_setup_ui.py`
- `tools/metabase_connect_clickhouse.py`

### Patrón aplicado:
```python
# Antes (hardcodeado)
METABASE_URL = os.getenv("METABASE_URL", "http://metabase:3000")

# Después (seguro)
METABASE_URL = os.getenv("METABASE_URL")
```

## Integración en Pipeline

### 1. Validación Automática en Inicio
```bash
# start_automated_pipeline.sh incluye:
echo "🔍 Validando configuración de seguridad..."
docker compose exec etl-tools python3 tools/validate_environment.py
```

### 2. Prevención de Errores
- El pipeline **no inicia** si hay problemas de configuración
- Mensaje claro sobre qué variables faltan o están mal configuradas
- Evita errores en tiempo de ejecución por credenciales faltantes

## Para Ambientes de Producción

### Paso 1: Configurar .env
```bash
# Copia el archivo de ejemplo
cp .env.example .env

# Edita con tus credenciales reales
nano .env
```

### Paso 2: Validar Configuración
```bash
# Ejecuta validación
docker compose exec etl-tools python3 tools/validate_environment.py

# Debería mostrar: "¡Configuración válida y segura!"
```

### Paso 3: Ejecutar Pipeline
```bash
# Ahora puedes ejecutar con confianza
./start_automated_pipeline.sh
```

## Beneficios del Sistema

✅ **Seguridad**: No hay credenciales en el código fuente  
✅ **Flexibilidad**: Fácil cambio de configuración sin tocar código  
✅ **Prevención**: Detecta problemas antes de ejecutar  
✅ **Auditoría**: Logs claros de qué está configurado  
✅ **Replicabilidad**: Funciona en cualquier ambiente con .env correcto  

## Solución de Problemas

### Error: "Variable METABASE_URL no configurada"
```bash
# Agregar al .env:
echo "METABASE_URL=http://metabase:3000" >> .env
```

### Error: "Valores hardcodeados detectados"
```bash
# Ver qué archivo tiene el problema:
docker compose exec etl-tools python3 tools/validate_environment.py

# Corregir reemplazando valores hardcodeados con os.getenv()
```

### Error: "JSON inválido en DB_CONNECTIONS"
```bash
# Validar JSON:
echo $DB_CONNECTIONS | jq .

# Corregir formato en .env
```

## Estado Actual

- ✅ Todos los scripts críticos corregidos
- ✅ Validación automática implementada
- ✅ Integración en pipeline principal
- ✅ Documentación completa
- ✅ Sistema de prevención de errores

**El sistema está listo para producción sin riesgos de seguridad.**