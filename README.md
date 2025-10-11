# etl_prod (FULL)
Producción: MySQL 5.5 → Debezium (Connect HA) → Kafka (KRaft RF=3) → ClickHouse → Superset.
Sigue el README del chat: pasos 1–7. Este paquete incluye el docker-compose COMPLETO y scripts listos.

## 🤖 Agente de Ciclo Automático ETL

**NUEVO:** Sistema completamente autónomo que ejecuta ciclos de validación, ajuste y ejecución del pipeline ETL hasta lograr éxito completo **sin intervención del usuario**.

```bash
# Ejecutar ciclo automático de validación y ajuste
python tools/auto_etl_cycle_agent.py

# Con opciones avanzadas
python tools/auto_etl_cycle_agent.py --max-iterations 10 --debug
```

**Características:**
- ✅ Commit automático del estado actual
- ✅ Limpieza completa de contenedores y datos
- ✅ Ejecución completa del orquestador ETL
- ✅ Validación automática de resultados y logs
- ✅ Detección y ajuste automático de errores
- ✅ Iteración hasta lograr éxito completo
- ✅ Reportes detallados en JSON

Ver documentación completa: [docs/AUTO_ETL_CYCLE_AGENT.md](docs/AUTO_ETL_CYCLE_AGENT.md)

---

# ETL FGEO — Stack ClickHouse + Superset (WSL/Windows)
**Fecha:** 2025-09-23

## Objetivo
Cargar datos **reales** desde múltiples BDs (MySQL 5.5/MariaDB/PostgreSQL) a ClickHouse (`fgeo_analytics`) y visualizarlos en Superset. Dos modos de ingesta:
1) **Bulk loader** (directo) — `tools/ingest_runner.py` (rápido para tener tablas hoy).
2) **CDC** con Debezium → Kafka → ClickHouse (para tiempo real).

> Recomendación: arranca con *Bulk loader* (modo 1). Cuando quede estable, activas CDC.

---

## 0) Requisitos
- Windows 11 + WSL2 Ubuntu (Docker Desktop ON)
- Puertos libres: 8123 (CH), 8088 (Superset), 9092/19092 (Kafka), 8083 (Connect)
- Acceso a tus BDs origen (VPN si aplican)
- **MySQL configurado correctamente** (ver sección 0.1)

---

## 0.1) Configuración MySQL Requerida

Para que el ETL funcione correctamente, MySQL debe estar configurado con los siguientes parámetros y permisos:

### Configuración del servidor MySQL (my.cnf)

**Para CDC con Debezium (tiempo real):**
```ini
[mysqld]
# Habilitar binary logging para CDC
log-bin=mysql-bin
binlog_format=ROW
binlog_row_image=FULL

# Server ID único (importante para replicación)
server_id=1

# Opcional: retención de logs
expire_logs_days=7
max_binlog_size=100M
```

**Para Bulk loader únicamente:**
- No requiere configuración especial del servidor

### Usuarios y permisos necesarios

Crear un usuario dedicado para el ETL con los permisos correctos:

```sql
-- Crear usuario ETL
CREATE USER 'etl_user'@'%' IDENTIFIED BY 'tu_password_seguro';

-- Permisos para Bulk loader (mínimos)
GRANT SELECT ON tu_base_datos.* TO 'etl_user'@'%';

-- Permisos adicionales para CDC (Debezium)
GRANT REPLICATION SLAVE ON *.* TO 'etl_user'@'%';
GRANT REPLICATION CLIENT ON *.* TO 'etl_user'@'%';
GRANT SELECT ON tu_base_datos.* TO 'etl_user'@'%';

-- Aplicar cambios
FLUSH PRIVILEGES;
```

### Verificar configuración

**Verificar binary logging (necesario para CDC):**
```sql
SHOW VARIABLES LIKE 'log_bin';
SHOW VARIABLES LIKE 'binlog_format';
SHOW VARIABLES LIKE 'binlog_row_image';
SHOW VARIABLES LIKE 'server_id';
```

**Verificar permisos del usuario:**
```sql
SHOW GRANTS FOR 'etl_user'@'%';
```

**Verificar que las tablas tengan claves primarias (recomendado para CDC):**
```sql
SELECT 
    table_schema,
    table_name,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM information_schema.table_constraints 
            WHERE constraint_type = 'PRIMARY KEY' 
            AND table_schema = t.table_schema 
            AND table_name = t.table_name
        ) THEN 'Sí' 
        ELSE 'No' 
    END as tiene_primary_key
FROM information_schema.tables t 
WHERE table_schema = 'tu_base_datos' 
AND table_type = 'BASE TABLE'
ORDER BY table_name;
```

### Troubleshooting MySQL

**Error: "The MySQL server is not configured as a replica"**
- Verificar que `log-bin` esté habilitado y `server_id` configurado

**Error: "Access denied for user"**
- Verificar permisos con `SHOW GRANTS FOR 'usuario'@'%'`
- Verificar conectividad de red desde contenedores Docker

**Error: "Table without primary key"**
- Debezium requiere claves primarias para CDC
- Agregar clave primaria o usar modo snapshot únicamente

### Configuración de ejemplo en DB_CONNECTIONS

```json
[{
    "type": "mysql",
    "name": "mi_base_datos",
   "host": "host.docker.internal",
    "port": 3306,
    "user": "etl_user",
    "pass": "tu_password_seguro",
    "db": "mi_base_datos"
}]
```

### Notas importantes

1. **Seguridad**: Usar siempre passwords seguros y limitar acceso por IP si es posible
2. **Performance**: El binary logging puede impactar performance, monitorear espacio en disco
3. **Backup**: Los binary logs consumen espacio, configurar rotación adecuada
4. **Red**: Verificar que los puertos MySQL (3306) sean accesibles desde Docker
5. **Versión**: Probado con MySQL 5.5+ y MariaDB 10.0+

---

## 1) .env
Edita `etl_prod/.env` (o copia desde `.env.example`) y define **DB_CONNECTIONS**. Ejemplo mínimo:
```json
[{"type":"mysql","name":"koko","host":"host.docker.internal","port":3306,"user":"user","pass":"***","db":"koko"},
 {"type":"mysql","name":"tobi","host":"host.docker.internal","port":3306,"user":"user","pass":"***","db":"tobi"}]
```
Otros:
```
CLICKHOUSE_DATABASE=fgeo_analytics
SUPERSET_ADMIN=admin
SUPERSET_PASSWORD=Admin123!
SUPERSET_URL=http://superset:8088
KAFKA_CLUSTER_ID=<uuid>
```

---

## 2) Arranque mínimo para trabajar HOY (Bulk loader directo)
```bash
cd etl_prod
docker compose up -d clickhouse superset superset-venv-setup superset-init
# Espera ~30-60s tras SUPERTSET_INIT_OK
docker compose run --rm ingestor
```
El contenedor `ingestor`:
- Instala requirements
- **Conecta a cada BD** de `DB_CONNECTIONS`
- Refleja esquema y crea/ajusta tablas en ClickHouse
- Inserta datos (idempotente por tabla)

### Smoke tests (ClickHouse)
```bash
# Listar tablas y filas (ClickHouse >= 24.3 usa total_rows)
docker exec -it clickhouse bash -lc "clickhouse-client -q \"SELECT name, total_rows FROM system.tables WHERE database='fgeo_analytics' ORDER BY name FORMAT PrettyCompact\""

# Contar filas de una tabla
docker exec -it clickhouse bash -lc "clickhouse-client -q \"SELECT count() FROM fgeo_analytics.<tu_tabla>\""
```

Si `total_rows` marca NULL, usa:
```bash
docker exec -it clickhouse bash -lc "clickhouse-client -q \"SELECT table, sum(rows) AS rows FROM system.parts WHERE database='fgeo_analytics' GROUP BY table ORDER BY table\""
```


### Smoke tests (Superset)
- Abre http://localhost:8088 (admin / Admin123!)
- La base de datos ClickHouse (`fgeo_analytics`) y los datasets se importan automáticamente al arrancar Superset, usando el archivo ZIP de configuración (`clickhouse_db.zip`).
- El proceso de arranque limpia la metadata interna de Superset para evitar problemas de cifrado y garantiza que la importación sea replicable y sin pasos manuales.
- Si necesitas reprovisionar, basta con reiniciar los contenedores: el flujo es 100% automático.

---

## 3) Si **no** estás en VPN (o no hay acceso a las BDs)
Levanta un **MySQL demo** y carga un dump local para probar el pipeline:
```bash
docker run -d --name mysqldemo -e MYSQL_ROOT_PASSWORD=pass -p 3307:3306 mysql:5.7
# Crea DB y carga CSV/SQL de prueba...
```
Luego en `DB_CONNECTIONS` usa `"host":"host.docker.internal","port":3307`.

---

## 4) Pasar a CDC (Debezium → Kafka → ClickHouse)
1. Levanta el clúster:
   ```bash
   docker compose up -d kafka-controller-1 kafka-controller-2 kafka-controller-3 kafka-1 kafka-2 kafka-3 connect configurator
   ```
2. Verifica brokers:
   ```bash
   docker exec -it kafka-1 bash -lc "kafka-topics --bootstrap-server kafka-1:9092 --list"
   ```
3. Aplica connector Debezium (desde `apply_connectors.py`):
   ```bash
   docker compose run --rm configurator python tools/apply_connectors.py
   ```
4. Genera objetos en ClickHouse para **cada tabla** (Kafka Engine + MVs a `*_raw`):
   ```bash
   docker compose run --rm configurator python tools/gen_pipeline.py
   docker compose run --rm configurator bash generated/ch_create_raw_pipeline.sh
   ```
5. Verifica consumo:
   ```bash
   docker exec -it clickhouse bash -lc "clickhouse-client -q \"SHOW TABLES FROM fgeo_analytics\""
   docker exec -it clickhouse bash -lc "clickhouse-client -q \"SELECT * FROM system.kafka_consumers FORMAT PrettyCompact\""
   docker exec -it clickhouse bash -lc "clickhouse-client -q \"SELECT table, sum(rows) FROM system.parts WHERE database='fgeo_analytics' GROUP BY table\""
   ```

> Nota WSL/Windows: asegúrate que `KAFKA_ADVERTISED_LISTENERS` en `docker-compose.yml` expone `PLAINTEXT_HOST://host.docker.internal:1909x` y que tus clientes fuera del clúster usan esos puertos `19092/19093/19094`.

---

docker compose up -d clickhouse superset superset-venv-setup superset-init
docker compose run --rm provision-superset
docker compose run --rm ingestor             # (bulk loader; opcional si usarás CDC)
docker compose up -d kafka-controller-1 kafka-controller-2 kafka-controller-3 kafka-1 kafka-2 kafka-3 connect configurator
docker compose run --rm configurator python tools/apply_connectors.py
docker compose run --rm configurator python tools/gen_pipeline.py
docker compose run --rm configurator bash generated/ch_create_raw_pipeline.sh

## 5) Orden recomendado (full)
```bash
docker compose up -d clickhouse superset superset-venv-setup superset-init
# La importación de la DB ClickHouse y datasets en Superset es automática (no requiere provision-superset manual)
docker compose run --rm ingestor             # (bulk loader; opcional si usarás CDC)
docker compose up -d kafka-controller-1 kafka-controller-2 kafka-controller-3 kafka-1 kafka-2 kafka-3 connect configurator
docker compose run --rm configurator python tools/apply_connectors.py
docker compose run --rm configurator python tools/gen_pipeline.py
docker compose run --rm configurator bash generated/ch_create_raw_pipeline.sh
```

---

## 6) Troubleshooting rápido
- **`Unknown identifier 'rows'`** ⇒ usa `total_rows` o consulta `system.parts`.
- **Superset vacío** ⇒ corre `provision-superset` y revisa credenciales CH.
- **No se conecta a MySQL 5.5** (CDC) ⇒ habilita `binlog_format=ROW`, `server_id`, `binlog_row_image=FULL`, usuario con `REPLICATION SLAVE, REPLICATION CLIENT`.
- **Kafka desde Windows host** ⇒ usa `host.docker.internal` y puertos 1909x.
- **Conexiones múltiples** ⇒ valida `DB_CONNECTIONS` (JSON válido) con `python tools/render_from_env.py --print` (desde `configurator`).

---

## 7) Comandos útiles
```bash
# Verificar ClickHouse vivo
curl -fsS http://localhost:8123/ping

# Inspeccionar logs de ingestor
docker logs -f etl_prod-ingestor-1

# Revisar tablas creadas por ingestor
docker exec -it clickhouse clickhouse-client -q "SHOW TABLES FROM fgeo_analytics"

# === NUEVOS: Validaciones automáticas ===

# Validar ClickHouse (tablas, datos, esquema)
docker compose run --rm etl-tools python tools/validate_clickhouse.py

# Validar Superset (configuración, bases de datos, datasets)
docker compose run --rm configurator python tools/validate_superset.py

# Validar variables de entorno y dependencias
docker compose run --rm etl-tools python tools/validators.py

# Ejecutar validaciones con logs JSON
LOG_FORMAT=json docker compose run --rm etl-tools python tools/validate_clickhouse.py
```

---

## 7.1) Características de robustez del pipeline

### Healthchecks
Todos los servicios tienen healthchecks configurados:
- ClickHouse: Verifica endpoint `/ping`
- Superset: Verifica endpoint `/health`
- Kafka: Verifica listado de topics
- Connect: Verifica endpoint REST `/connectors`
- Otros servicios: Verifican disponibilidad de Python y directorio de herramientas

### Validación de entorno
Antes de ejecutar scripts, se validan:
- Variables de entorno críticas (DB_CONNECTIONS, CLICKHOUSE_DATABASE, etc.)
- Dependencias Python requeridas
- Conectividad a servicios (ClickHouse, Kafka Connect)
- Formato de configuración JSON

### Manejo de errores
Los scripts diferencian entre:
- **Errores recuperables**: Se registran y el proceso continúa (ej: una tabla falla pero otras continúan)
- **Errores fatales**: Se aborta el proceso inmediatamente (ej: credenciales inválidas, servicio no disponible)
- **Errores inesperados**: Se registran con stack trace completo

### Logging estructurado
Soporta dos formatos de logging:
```bash
# Formato texto (default)
python tools/ingest_runner.py

# Formato JSON estructurado
LOG_FORMAT=json python tools/ingest_runner.py
```

Logs JSON incluyen:
- timestamp ISO 8601 UTC
- nivel (INFO, WARNING, ERROR)
- módulo y función
- mensaje
- contexto adicional
- stack trace en errores

### Validaciones automáticas
Scripts de validación para verificar el estado del sistema:
- `validate_clickhouse.py`: Valida base de datos, tablas, datos y esquemas
- `validate_superset.py`: Valida health, autenticación, bases de datos y datasets
- `validators.py`: Valida configuración de entorno
- `test_permissions.py`: **NUEVO** - Prueba permisos en ClickHouse, Kafka y MySQL
- `verify_dependencies.py`: **NUEVO** - Verifica dependencias y secuencialidad

Generan reportes JSON guardados en `logs/`:
- `logs/clickhouse_validation.json`
- `logs/superset_validation.json`
- `logs/permission_tests.json` - **NUEVO**
- `logs/dependency_verification.json` - **NUEVO**

### Pruebas de permisos
Valida que los usuarios tengan permisos correctos:
```bash
# Probar permisos en todas las tecnologías
docker compose run --rm etl-tools python tools/test_permissions.py

# Deshabilitar pruebas de permisos
ENABLE_PERMISSION_TESTS=false docker compose run --rm etl-tools python tools/test_permissions.py
```

Verifica:
- ClickHouse: CREATE DATABASE, CREATE TABLE, INSERT, SELECT, DROP
- Kafka Connect: Listado de conectores y plugins
- MySQL/Debezium: REPLICATION SLAVE, REPLICATION CLIENT, SELECT, binlog habilitado

### Verificación de dependencias
Valida la secuencia correcta de inicio:
```bash
# Verificar que todos los servicios estén listos
docker compose run --rm etl-tools python tools/verify_dependencies.py

# Deshabilitar verificación
ENABLE_DEPENDENCY_VERIFICATION=false docker compose run --rm etl-tools python tools/verify_dependencies.py
```

Asegura que:
1. ClickHouse esté listo antes de ingesta
2. Kafka Connect esté listo antes de aplicar conectores
3. Bases de datos existan antes de crear tablas
4. Dependencias Python estén instaladas

### Pruebas unitarias
El proyecto incluye pruebas unitarias para scripts principales:
```bash
# Ejecutar todas las pruebas
python -m unittest discover tests

# Ejecutar prueba específica
python -m unittest tests.test_validators
```

Ver `tests/README.md` para más detalles.

### Control por variables de entorno
Todas las validaciones y logs pueden controlarse con variables de entorno:

**Control de funcionalidad:**
- `ENABLE_VALIDATION=true|false` - Validaciones generales
- `ENABLE_PERMISSION_TESTS=true|false` - Pruebas de permisos
- `ENABLE_DEPENDENCY_VERIFICATION=true|false` - Verificación de dependencias
- `ENABLE_CDC=true|false` - Componentes CDC
- `ENABLE_SUPERSET=true|false` - Superset

**Logging:**
- `LOG_LEVEL=DEBUG|INFO|WARNING|ERROR` - Nivel de logging
- `LOG_FORMAT=text|json` - Formato de logs

Ver `docs/ENVIRONMENT_VARIABLES.md` para documentación completa.

### Documentación de errores
Ver `docs/ERROR_RECOVERY.md` para:
- Errores comunes y soluciones
- Procedimientos de recuperación
- Diagnóstico con logs
- Comandos de troubleshooting

---

## 8) Checklist de “Definición de listo”

---

## 9) Scripts principales y su función

### `tools/ingest_runner.py`
**Bulk loader**: Conecta a cada BD definida en `DB_CONNECTIONS`, refleja el esquema, crea/ajusta tablas en ClickHouse e inserta datos en bulk. Normaliza tipos, deduplica y permite opciones CLI para esquemas, tablas, modo de deduplicación y más.

### `tools/gen_pipeline.py`
**Generador de pipeline CDC**: Descubre tablas y columnas de cada BD origen, genera esquemas JSON, configuraciones de conectores Debezium y scripts de pipeline ClickHouse/Kafka. Salidas: `connector.json`, `tables.include.env`, `ch_create_raw_pipeline.sh` por conexión.

### `tools/render_from_env.py`
**Renderizador de entorno**: Lee `DB_CONNECTIONS` y genera SQL para creación de usuarios, bases y permisos en ClickHouse. También genera un script para registrar las DBs en Superset. Salidas: `clickhouse_init.sql`, `superset_create_dbs.sh`.

### `tools/provision_superset.py`
**Provisionador de Superset**: Espera a que Superset esté listo, inicia sesión vía API, crea/actualiza la conexión a ClickHouse y (opcional) registra un dataset demo. Usa variables de entorno para credenciales y conexión.

### `tools/cdc_bootstrap.py`
**Bootstrap CDC**: Instala dependencias Python (opcional), aplica conectores Debezium, genera objetos de pipeline en ClickHouse, aplica SQL generado vía HTTP y ejecuta scripts wrapper para CDC. Automatiza el setup end-to-end.

### `tools/apply_connectors.py`
**Aplicador de conectores Debezium**: Lee `DB_CONNECTIONS`, construye y valida configs de conectores Debezium para fuentes MySQL, los aplica vía API REST de Kafka Connect y espera a que estén en estado RUNNING.

---

## 11) Flujo recomendado (resumen)

1. Edita `.env` y define tus conexiones en `DB_CONNECTIONS`.
2. Arranca servicios base:
   ```bash
   docker compose up -d clickhouse superset superset-venv-setup superset-init
   ```
3. **NUEVO:** Valida configuración:
   ```bash
   docker compose run --rm etl-tools python tools/validators.py
   ```
4. Ingresa datos iniciales (bulk loader):
   ```bash
   docker compose run --rm ingestor
   ```
5. **NUEVO:** Valida ingesta:
   ```bash
   docker compose run --rm etl-tools python tools/validate_clickhouse.py
   docker compose run --rm configurator python tools/validate_superset.py
   ```
6. (Opcional) Levanta clúster CDC:
   ```bash
   docker compose up -d kafka-controller-1 kafka-controller-2 kafka-controller-3 kafka-1 kafka-2 kafka-3 connect configurator
   ```
7. Aplica conectores y genera pipeline CDC:
   ```bash
   docker compose run --rm configurator python tools/apply_connectors.py
   docker compose run --rm configurator python tools/gen_pipeline.py
   docker compose run --rm configurator bash generated/ch_create_raw_pipeline.sh
   ```
8. Verifica en ClickHouse y Superset que los datos y objetos estén presentes.
9. **NUEVO:** En caso de errores, consulta `docs/ERROR_RECOVERY.md`

---

## 12) Documentación y soporte

### Documentación Principal
- **README.md** (este archivo): Guía de inicio rápido y uso general
- **docs/DEPENDENCIES.md**: Dependencias y orden de ejecución de componentes
- **docs/ENVIRONMENT_VARIABLES.md**: Guía completa de variables de entorno
- **docs/ERROR_RECOVERY.md**: Guía de errores comunes y recuperación
- **docs/ROBUSTNESS_IMPROVEMENTS.md**: Resumen de mejoras de robustez

### Scripts Disponibles
- **tools/validators.py**: Validación de entorno y dependencias
- **tools/test_permissions.py**: Pruebas de permisos en tecnologías ETL
- **tools/verify_dependencies.py**: Verificación de dependencias y secuencialidad
- **tools/ingest_runner.py**: Ingesta bulk de datos
- **tools/apply_connectors.py**: Aplicación de conectores Debezium
- **tools/gen_pipeline.py**: Generación de pipeline CDC
- **tools/validate_clickhouse.py**: Validación de ClickHouse
- **tools/validate_superset.py**: Validación de Superset

### Pruebas
- **tests/**: Directorio con pruebas unitarias
- Ver `tests/README.md` para ejecutar pruebas

### Flujo de Trabajo Recomendado

1. **Configuración inicial:**
   ```bash
   # Copiar y editar archivo de configuración
   cp .env.example .env
   nano .env  # Editar DB_CONNECTIONS
   ```

2. **Validar entorno:**
   ```bash
   # Validar configuración
   docker compose run --rm etl-tools python tools/validators.py
   
   # Probar permisos
   docker compose run --rm etl-tools python tools/test_permissions.py
   ```

3. **Iniciar servicios:**
   ```bash
   # Iniciar servicios base
   docker compose up -d clickhouse
   
   # Verificar dependencias
   docker compose run --rm etl-tools python tools/verify_dependencies.py
   ```

4. **Ejecutar ingesta:**
   ```bash
   docker compose run --rm etl-tools python tools/ingest_runner.py
   ```

5. **Validar resultados:**
   ```bash
   docker compose run --rm etl-tools python tools/validate_clickhouse.py
   ```

6. **Consultar logs:**
   ```bash
   # Ver logs de validación
   cat logs/clickhouse_validation.json
   cat logs/permission_tests.json
   cat logs/dependency_verification.json
   ```

### Variables de Control

Controla el comportamiento del pipeline con variables de entorno:

```bash
# Ejemplo: Pipeline con todas las validaciones
export ENABLE_VALIDATION=true
export ENABLE_PERMISSION_TESTS=true
export ENABLE_DEPENDENCY_VERIFICATION=true
export LOG_LEVEL=INFO
export LOG_FORMAT=json

bash bootstrap/run_etl_full.sh
```

Ver `docs/ENVIRONMENT_VARIABLES.md` para lista completa.

### Soporte y Troubleshooting

- Para errores comunes: `docs/ERROR_RECOVERY.md`
- Para orden de ejecución: `docs/DEPENDENCIES.md`
- Para configuración: `docs/ENVIRONMENT_VARIABLES.md`
- Para dudas o problemas, revisa los logs en `logs/` y consulta la sección de troubleshooting.
