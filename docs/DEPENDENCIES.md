# Dependencias y Secuencialidad del Pipeline ETL

Este documento describe las dependencias entre componentes del pipeline ETL y el orden correcto de ejecución.

## Diagrama de Dependencias

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Servicios Base                                           │
│    - Docker y Docker Compose                                │
│    - Archivo .env con configuración                         │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. ClickHouse (Base de Datos)                               │
│    - Puerto 8123 disponible                                 │
│    - Healthcheck: http://localhost:8123/ping                │
│    DEBE estar listo antes de: Ingesta, CDC, Superset        │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ 3a. Ingesta Bulk (Opcional)                                 │
│     - ingest_runner.py                                      │
│     - Crea base de datos y tablas                           │
│     - Carga datos iniciales                                 │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ├──────────────────────┐
                 │                      │
                 ▼                      ▼
┌────────────────────────────┐  ┌───────────────────────────┐
│ 3b. Kafka + CDC (Opcional) │  │ 4. Superset (Opcional)    │
│     - Kafka brokers        │  │    - Depende de ClickHouse│
│     - Kafka Connect        │  │    - Puerto 8088          │
│     - Debezium connectors  │  │    - provision_superset   │
│     DEBE: Bases origen con │  └───────────────────────────┘
│           binlog habilitado│
└────────────────────────────┘
```

## Secuencia de Inicio Recomendada

### Modo Básico (Sin CDC)

1. **Preparación**
   ```bash
   # Validar archivo .env
   cat .env
   
   # Verificar dependencias
   docker compose run --rm etl-tools python tools/verify_dependencies.py
   ```

2. **Iniciar ClickHouse**
   ```bash
   docker compose up -d clickhouse
   
   # Esperar a que esté listo
   docker compose run --rm etl-tools python tools/verify_dependencies.py
   ```

3. **Ejecutar ingesta inicial**
   ```bash
   docker compose run --rm etl-tools python tools/ingest_runner.py
   ```

4. **Validar resultados**
   ```bash
   docker compose run --rm etl-tools python tools/validate_clickhouse.py
   ```

5. **Iniciar Superset (opcional)**
   ```bash
   docker compose up -d superset superset-venv-setup superset-init
   ```

### Modo Completo (Con CDC)

1. **Pasos 1-4 del modo básico** (para carga inicial)

2. **Iniciar Kafka**
   ```bash
   docker compose up -d kafka-controller-1 kafka-controller-2 kafka-controller-3
   docker compose up -d kafka-1 kafka-2 kafka-3
   
   # Esperar a que esté listo (30-60 segundos)
   sleep 30
   ```

3. **Iniciar Kafka Connect**
   ```bash
   docker compose up -d connect
   
   # Esperar a que esté listo
   docker compose run --rm etl-tools python tools/verify_dependencies.py
   ```

4. **Aplicar conectores Debezium**
   ```bash
   docker compose run --rm configurator python tools/apply_connectors.py
   ```

5. **Generar y aplicar pipeline CDC**
   ```bash
   docker compose run --rm configurator python tools/gen_pipeline.py
   docker compose run --rm configurator bash generated/ch_create_raw_pipeline.sh
   ```

## Dependencias de Scripts

### tools/validators.py
**Dependencias:**
- Python 3.7+
- Variables de entorno: DB_CONNECTIONS, CLICKHOUSE_DATABASE (opcionales)
- Paquetes: requests, clickhouse_connect (para validación de conexiones)

**Ejecutar antes de:** Cualquier otro script

### tools/test_permissions.py
**Dependencias:**
- tools/validators.py
- ClickHouse, Kafka Connect, MySQL (según configuración)
- Variables de entorno: DB_CONNECTIONS, CLICKHOUSE_HOST, CONNECT_URL

**Ejecutar antes de:** Scripts de ingesta o CDC

### tools/verify_dependencies.py
**Dependencias:**
- ClickHouse corriendo
- Kafka Connect corriendo (si ENABLE_CDC=true)
- Superset corriendo (si ENABLE_SUPERSET=true)

**Ejecutar:** Después de iniciar servicios, antes de scripts de ingesta

### tools/ingest_runner.py
**Dependencias:**
- ClickHouse listo
- DB_CONNECTIONS configurado
- Acceso a bases de datos origen

**Ejecutar después de:** ClickHouse está listo

### tools/apply_connectors.py
**Dependencias:**
- Kafka Connect listo
- DB_CONNECTIONS configurado
- Bases origen con binlog habilitado

**Ejecutar después de:** Kafka y Connect están listos

### tools/gen_pipeline.py
**Dependencias:**
- DB_CONNECTIONS configurado
- Acceso a bases de datos origen

**Ejecutar después de:** apply_connectors.py

### tools/validate_clickhouse.py
**Dependencias:**
- ClickHouse listo
- Base de datos creada (por ingest_runner o gen_pipeline)

**Ejecutar después de:** Ingesta o creación de pipeline

### tools/validate_superset.py
**Dependencias:**
- Superset listo
- ClickHouse listo y con datos

**Ejecutar después de:** Superset está listo y ClickHouse tiene datos

## Variables de Entorno para Control

### Validación y Verificación
- `ENABLE_VALIDATION=true|false` - Habilita/deshabilita validaciones generales
- `ENABLE_PERMISSION_TESTS=true|false` - Habilita/deshabilita pruebas de permisos
- `ENABLE_DEPENDENCY_VERIFICATION=true|false` - Habilita/deshabilita verificación de dependencias

### Componentes Opcionales
- `ENABLE_CDC=true|false` - Habilita/deshabilita CDC con Kafka
- `ENABLE_SUPERSET=true|false` - Habilita/deshabilita Superset

### Logging
- `LOG_LEVEL=DEBUG|INFO|WARNING|ERROR` - Nivel de logging
- `LOG_FORMAT=text|json` - Formato de logs

### Salidas
- `PERMISSION_TEST_OUTPUT=logs/permission_tests.json` - Archivo de resultados de permisos
- `DEPENDENCY_VERIFICATION_OUTPUT=logs/dependency_verification.json` - Archivo de verificación de dependencias

## Troubleshooting de Dependencias

### Error: "ClickHouse no está listo"
**Causa:** ClickHouse no ha iniciado completamente

**Solución:**
```bash
# Verificar logs
docker logs clickhouse

# Verificar healthcheck
docker ps | grep clickhouse

# Esperar más tiempo
docker compose run --rm etl-tools python tools/verify_dependencies.py
```

### Error: "Kafka Connect no disponible"
**Causa:** Connect no está listo o Kafka no está disponible

**Solución:**
```bash
# Verificar que Kafka esté corriendo
docker ps | grep kafka

# Verificar logs de Connect
docker logs connect

# Reintentar después de 30-60 segundos
sleep 30
docker compose run --rm etl-tools python tools/verify_dependencies.py
```

### Error: "Base de datos no existe"
**Causa:** No se ha ejecutado ingesta inicial

**Solución:**
```bash
# Ejecutar ingesta para crear base de datos
docker compose run --rm etl-tools python tools/ingest_runner.py
```

## Orden de Limpieza/Apagado

Para apagar el pipeline de forma ordenada:

```bash
# 1. Detener conectores CDC (si están activos)
# Los conectores se detienen automáticamente al apagar Connect

# 2. Apagar servicios de aplicación
docker compose stop superset connect

# 3. Apagar Kafka (si está activo)
docker compose stop kafka-1 kafka-2 kafka-3
docker compose stop kafka-controller-1 kafka-controller-2 kafka-controller-3

# 4. Apagar ClickHouse (último, para no perder datos)
docker compose stop clickhouse

# 5. Limpiar todo (opcional, elimina datos)
docker compose down -v
```

## Referencias

- Script maestro: `bootstrap/run_etl_full.sh` - Ejecuta secuencia completa
- Documentación de errores: `docs/ERROR_RECOVERY.md`
- Mejoras de robustez: `docs/ROBUSTNESS_IMPROVEMENTS.md`
