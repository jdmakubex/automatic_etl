# Guía de Errores y Recuperación del Pipeline ETL

## Tabla de Contenidos
- [Tipos de Errores](#tipos-de-errores)
- [Errores Comunes y Soluciones](#errores-comunes-y-soluciones)
- [Procedimientos de Recuperación](#procedimientos-de-recuperación)
- [Logs y Diagnóstico](#logs-y-diagnóstico)

---

## Tipos de Errores

### Errores Recuperables
Errores que permiten reintentos o continuación del proceso:

- **Timeout de conexión**: Reintentar después de esperar
- **Tabla temporal no disponible**: Recrear o esperar
- **Lock de base de datos**: Esperar y reintentar
- **Chunk de datos con filas inválidas**: Omitir y continuar con siguiente chunk

### Errores Fatales
Errores que requieren intervención manual:

- **Credenciales inválidas**: Corregir en `.env`
- **Base de datos no existe**: Crear base de datos
- **Puerto ocupado**: Liberar puerto o cambiar configuración
- **Falta espacio en disco**: Liberar espacio
- **Variables de entorno críticas faltantes**: Definir en `.env`

---

## Errores Comunes y Soluciones

### 1. Servicios de Docker no inician

#### Error
```
Error: Cannot connect to the Docker daemon
```

#### Causa
Docker no está corriendo o no tienes permisos.

#### Solución
```bash
# Iniciar Docker
sudo systemctl start docker

# Verificar estado
sudo systemctl status docker

# Agregar usuario al grupo docker (requiere logout/login)
sudo usermod -aG docker $USER
```

---

### 2. Variable DB_CONNECTIONS no definida

#### Error
```
[ERROR] Variable de entorno requerida no definida: DB_CONNECTIONS
```

#### Causa
El archivo `.env` no existe o no tiene la variable `DB_CONNECTIONS`.

#### Solución
```bash
# Copiar archivo de ejemplo
cp .env.example .env

# Editar y definir DB_CONNECTIONS
# Ejemplo:
# DB_CONNECTIONS=[{"name":"mydb","host":"localhost","port":3306,"user":"root","pass":"password","db":"database"}]
```

---

### 3. ClickHouse no responde

#### Error
```
FatalError: No se pudo conectar a ClickHouse en clickhouse:8123
```

#### Causa
- Servicio ClickHouse no está corriendo
- Puerto no accesible
- Healthcheck fallando

#### Diagnóstico
```bash
# Verificar estado del contenedor
docker ps -a | grep clickhouse

# Ver logs
docker logs clickhouse

# Verificar healthcheck
docker inspect clickhouse | grep -A 10 Health
```

#### Solución
```bash
# Reiniciar servicio
docker compose restart clickhouse

# Verificar conectividad
curl http://localhost:8123/ping

# Si persiste, recrear contenedor
docker compose down clickhouse
docker compose up -d clickhouse
```

---

### 4. Kafka Connect no está disponible

#### Error
```
FatalError: No se pudo conectar a Kafka Connect en http://connect:8083
```

#### Causa
- Kafka no ha terminado de iniciar
- Connect depende de Kafka que aún no está listo

#### Solución
```bash
# Verificar estado de Kafka
docker logs kafka

# Verificar estado de Connect
docker logs connect

# Esperar a que healthcheck pase
docker compose ps

# Si persiste después de 2-3 minutos, reiniciar
docker compose restart kafka connect
```

---

### 5. Error al ejecutar gen_pipeline.py

#### Error
```
[ERROR] Falló la ejecución de gen_pipeline.py
ModuleNotFoundError: No module named 'pymysql'
```

#### Causa
Dependencias Python no instaladas en el contenedor.

#### Solución
```bash
# Verificar Dockerfile tiene las dependencias
cat tools/Dockerfile.pipeline-gen

# Reconstruir imagen
docker compose build pipeline-gen

# O instalar manualmente en contenedor corriendo
docker compose run --rm pipeline-gen pip install pymysql python-dotenv
```

---

### 6. Superset no inicializa

#### Error
```
[ERROR] superset-init failed with exit code 1
```

#### Causa
- Base de datos de metadatos de Superset corrupta
- Configuración inválida
- Falta SECRET_KEY

#### Diagnóstico
```bash
# Ver logs detallados
docker logs superset-init

# Verificar variables de entorno
docker compose config | grep SUPERSET
```

#### Solución
```bash
# Limpiar y recrear Superset
docker compose down superset superset-init superset-venv-setup
docker volume rm etl_prod_superset_home
docker compose up -d superset-venv-setup superset-init

# Verificar que SECRET_KEY esté definido en docker-compose.yml
# SUPERSET_SECRET_KEY debe ser una cadena aleatoria larga
```

---

### 7. Tabla con datos corruptos o fechas inválidas

#### Error
```
[ERROR] Error ingiriendo table_name: Invalid date format '0000-00-00'
```

#### Causa
Datos en MySQL con fechas "cero" que ClickHouse no acepta.

#### Solución
El script `ingest_runner.py` ya maneja esto automáticamente convirtiendo fechas inválidas a `NULL`. Si persiste:

```python
# Verificar función coerce_datetime_columns en ingest_runner.py
# Las fechas '0000-00-00' se convierten a NaT (NULL)
```

---

### 8. Permisos insuficientes en MySQL

#### Error
```
sqlalchemy.exc.OperationalError: (1045, "Access denied for user 'user'@'host'")
```

#### Causa
Usuario MySQL no tiene permisos suficientes.

#### Solución
```sql
-- En MySQL, otorgar permisos
GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'user'@'%';
FLUSH PRIVILEGES;
```

---

### 9. Puerto ya en uso

#### Error
```
Error starting userland proxy: listen tcp 0.0.0.0:8123: bind: address already in use
```

#### Causa
Otro proceso está usando el puerto.

#### Diagnóstico
```bash
# Ver qué proceso usa el puerto
sudo lsof -i :8123
# o
sudo netstat -tulpn | grep 8123
```

#### Solución
```bash
# Opción 1: Matar el proceso
sudo kill <PID>

# Opción 2: Cambiar puerto en docker-compose.yml
# ports:
#   - "8124:8123"  # Usar 8124 en host en vez de 8123
```

---

### 10. Espacio en disco insuficiente

#### Error
```
no space left on device
```

#### Causa
Volúmenes de Docker ocupan todo el espacio.

#### Diagnóstico
```bash
# Ver uso de disco
df -h

# Ver tamaño de volúmenes Docker
docker system df -v
```

#### Solución
```bash
# Limpiar contenedores e imágenes no usadas
docker system prune -a --volumes

# Limpiar volúmenes específicos (¡CUIDADO! Borra datos)
docker volume rm etl_prod_ch_data
docker volume rm etl_prod_kafka_data

# Limpiar logs antiguos
find logs/ -type f -mtime +7 -delete
```

---

## Procedimientos de Recuperación

### Recuperación Total (Reset completo)

Cuando todo falla y necesitas empezar desde cero:

```bash
# 1. Detener todos los servicios
docker compose down -v

# 2. Limpiar todo
docker ps -aq | xargs -r docker rm -f
docker volume prune -f
docker network prune -f

# 3. Recrear red
docker network create etl_prod_etl_net

# 4. Reconstruir imágenes
docker compose build --no-cache

# 5. Iniciar servicios base
docker compose up -d clickhouse superset-venv-setup superset-init

# 6. Esperar healthchecks (2-3 min)
watch docker compose ps

# 7. Ejecutar pipeline
bash bootstrap/run_etl_full.sh
```

### Recuperación Parcial (Solo datos)

Cuando solo necesitas recargar datos:

```bash
# 1. Truncar tablas en ClickHouse
docker exec clickhouse clickhouse-client -q "TRUNCATE TABLE fgeo_analytics.*"

# 2. Ejecutar ingesta
docker compose run --rm etl-tools python tools/ingest_runner.py \
  --truncate-before-load \
  --chunksize 50000
```

### Recuperación de Configuración de Superset

Si solo la configuración de Superset está corrupta:

```bash
# 1. Detener Superset
docker compose down superset superset-init

# 2. Limpiar volumen de Superset
docker volume rm etl_prod_superset_home

# 3. Reiniciar Superset
docker compose up -d superset-venv-setup superset-init
```

---

## Logs y Diagnóstico

### Ubicación de Logs

```bash
# Logs de Docker Compose
logs/etl_full.log

# Logs de validación
logs/clickhouse_validation.json
logs/superset_validation.json

# Logs de contenedores
docker logs <nombre_contenedor>

# Logs en tiempo real
docker logs -f <nombre_contenedor>
```

### Formato de Logs JSON

Los scripts modernos usan logging JSON estructurado:

```json
{
  "timestamp": "2025-01-06T12:00:00Z",
  "level": "ERROR",
  "logger": "ingest_runner",
  "message": "Error conectando a MySQL",
  "module": "ingest_runner",
  "function": "get_source_engine",
  "line": 398,
  "exception": "..."
}
```

### Habilitar Logs JSON

```bash
# En variables de entorno
export LOG_FORMAT=json

# O en scripts Python
LOG_FORMAT=json python tools/ingest_runner.py

# Para ClickHouse validation
LOG_FORMAT=json python tools/validate_clickhouse.py
```

### Comandos de Diagnóstico

```bash
# Ver estado de todos los servicios
docker compose ps

# Ver healthchecks
docker inspect clickhouse | jq '.[0].State.Health'

# Ver últimas 100 líneas de logs de todos los servicios
docker compose logs --tail=100

# Ver logs de servicio específico
docker compose logs clickhouse

# Ejecutar comando en contenedor
docker compose exec clickhouse clickhouse-client -q "SELECT version()"

# Ver variables de entorno de un servicio
docker compose exec clickhouse env

# Ver recursos usados
docker stats

# Ver red
docker network inspect etl_prod_etl_net
```

---

## Validaciones Automáticas

### Ejecutar Validaciones

```bash
# Validar ClickHouse
python tools/validate_clickhouse.py

# Validar Superset
python tools/validate_superset.py

# Validar variables de entorno
python tools/validators.py

# Validar con logs JSON
LOG_FORMAT=json python tools/validate_clickhouse.py
```

### Interpretar Resultados

Los scripts de validación retornan:
- **Exit code 0**: Todas las validaciones pasaron
- **Exit code 1**: Algunas validaciones fallaron (recuperable)
- **Exit code 2**: Errores fatales (requiere corrección)
- **Exit code 3**: Error inesperado

---

## Contacto y Soporte

Para problemas no cubiertos en esta guía:

1. Revisa los logs detallados con `docker logs <servicio>`
2. Ejecuta las validaciones automáticas
3. Consulta la documentación de cada componente:
   - [ClickHouse](https://clickhouse.com/docs)
   - [Superset](https://superset.apache.org/docs)
   - [Kafka](https://kafka.apache.org/documentation/)
   - [Debezium](https://debezium.io/documentation/)

---

## Changelog

- **2025-01-06**: Versión inicial de la guía de recuperación
