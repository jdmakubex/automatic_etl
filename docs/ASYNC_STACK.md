# Stack Asíncrono de Superset - Redis + Celery

## 📋 Descripción General

Este documento describe la implementación del stack completo de consultas asíncronas en Superset utilizando Redis y Celery workers.

## 🏗️ Arquitectura

```
┌─────────────────┐
│   Superset UI   │ ← Usuario ejecuta query en SQL Lab
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Superset Server │ ← Recibe query y la envía a Celery
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Redis Broker   │ ← Cola de mensajes (tasks)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Celery Worker   │ ← Ejecuta query contra ClickHouse
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Redis Backend   │ ← Almacena resultados
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Superset UI   │ ← Polling obtiene resultados
└─────────────────┘
```

## 🔧 Componentes

### 1. Redis (superset-redis)
- **Puerto**: 6379
- **Función**: 
  - Broker de mensajes para Celery (DB 0)
  - Results Backend para queries (DB 1)
  - Cache general de Superset (DB 2)
  - Data cache (DB 3)
  - Async queries stream (DB 4)
- **Persistencia**: Volumen `redis_data` con AOF (Append-Only File)

### 2. Superset Worker (superset-worker)
- **Función**: Ejecuta queries SQL de forma asíncrona
- **Configuración**:
  - Pool: prefork
  - Concurrency: 4 workers
  - Fair scheduling (-O fair)
- **Healthcheck**: Celery ping via `celery inspect`

### 3. Superset Beat (superset-beat)
- **Función**: Scheduler para tareas periódicas
- **Tareas programadas**:
  - `reports.scheduler`: Cada minuto (generación de reportes)
  - `reports.prune_log`: Diariamente a medianoche (limpieza de logs)

### 4. Superset Server (superset)
- **Función**: Servidor web principal
- **Configuración async**: Habilitada vía `GLOBAL_ASYNC_QUERIES=True`

## ⚙️ Configuración

### Variables de Entorno
Todos los servicios de Superset reciben:
```bash
REDIS_HOST=redis
REDIS_PORT=6379
SUPERSET_SECRET_KEY=Sup3rS3cr3tK3yF0rPr0duct10nUs3
```

### Superset Config (superset_config_simple.py)

#### Celery
```python
class CeleryConfig:
    broker_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/0"
    result_backend = f"redis://{REDIS_HOST}:{REDIS_PORT}/0"
    imports = ("superset.sql_lab", "superset.tasks", "superset.tasks.scheduler")
```

#### Results Backend
```python
RESULTS_BACKEND = {
    "backend": "redis",
    "redis_host": REDIS_HOST,
    "redis_port": REDIS_PORT,
    "redis_db": 1,
}
```

#### Cache Configuration
```python
# General cache
CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_REDIS_DB': 2,
}

# Data cache (query results)
DATA_CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_REDIS_DB': 3,
    'CACHE_DEFAULT_TIMEOUT': 86400,  # 1 día
}
```

#### Global Async Queries
```python
FEATURE_FLAGS = {
    "GLOBAL_ASYNC_QUERIES": True,
    "SQLLAB_BACKEND_PERSISTENCE": True,
}

GLOBAL_ASYNC_QUERIES_REDIS_CONFIG = {
    "host": REDIS_HOST,
    "port": REDIS_PORT,
    "db": 4,
}
GLOBAL_ASYNC_QUERIES_TRANSPORT = "polling"
GLOBAL_ASYNC_QUERIES_POLLING_DELAY = 500  # ms
```

#### SQL Lab Timeouts
```python
SQLLAB_ASYNC_TIME_LIMIT_SEC = 300  # 5 minutos
SQLLAB_TIMEOUT = 300
SUPERSET_WEBSERVER_TIMEOUT = 300
```

## 🚀 Uso

### SQL Lab
1. Abre SQL Lab: http://localhost:8088/sqllab/
2. Selecciona la base de datos ClickHouse
3. Escribe tu query
4. Click en "Run" → La query se ejecuta de forma asíncrona
5. El UI hace polling cada 500ms hasta obtener resultados

### Explore/Charts
- Las consultas de dashboards y charts también usan async si están habilitadas
- `allow_run_async: True` en la configuración de la BD

## 🔍 Monitoreo

### Verificar Estado de Workers
```bash
# Entrar al contenedor worker
docker exec -it superset-worker bash

# Ver workers activos
celery -A superset.tasks.celery_app:app inspect active

# Ver estadísticas
celery -A superset.tasks.celery_app:app inspect stats

# Ver tareas programadas
celery -A superset.tasks.celery_app:app inspect scheduled
```

### Verificar Redis
```bash
# Conectar a Redis
docker exec -it superset-redis redis-cli

# Ver info
INFO

# Ver claves en DB 0 (broker)
SELECT 0
KEYS *

# Ver claves en DB 1 (results)
SELECT 1
KEYS *

# Monitor en tiempo real
MONITOR
```

### Logs
```bash
# Superset server
docker logs -f superset

# Worker
docker logs -f superset-worker

# Beat (scheduler)
docker logs -f superset-beat

# Redis
docker logs -f superset-redis
```

## 📊 Métricas y Rendimiento

### Capacidad
- **Workers**: 4 procesos concurrentes
- **Timeout**: 5 minutos por query
- **Cache**: Resultados cacheados 1 día (configurable)

### Escalado
Para aumentar capacidad de workers:
```yaml
# En docker-compose.yml
superset-worker:
  command: ["celery", "--app=superset.tasks.celery_app:app", "worker", 
            "--pool=prefork", "-O", "fair", "-c", "8"]  # 8 workers
```

O escalar horizontalmente:
```bash
docker-compose up -d --scale superset-worker=3
```

## 🛠️ Troubleshooting

### Worker no procesa queries
```bash
# Verificar worker está activo
docker ps | grep superset-worker

# Ver logs
docker logs superset-worker

# Restart worker
docker restart superset-worker
```

### Redis no responde
```bash
# Verificar healthcheck
docker inspect superset-redis | grep -A 10 Health

# Ping Redis
docker exec superset-redis redis-cli ping

# Restart Redis
docker restart superset-redis
```

### Queries se quedan "Running"
1. Verificar worker logs para errores
2. Verificar conexión ClickHouse
3. Revisar timeout settings (puede ser query muy lenta)
4. Cancelar query en SQL Lab

### Limpiar cache
```bash
# Flush todas las DBs de Redis
docker exec superset-redis redis-cli FLUSHALL

# Solo cache (DB 2)
docker exec superset-redis redis-cli -n 2 FLUSHDB

# Solo results (DB 1)
docker exec superset-redis redis-cli -n 1 FLUSHDB
```

## 🔐 Seguridad

### Redis
- No expuesto fuera de la red Docker (`etl_net`)
- Solo accesible por servicios de Superset
- Persistencia en volumen Docker (no en filesystem del host)

### Celery
- No autenticación (red interna trusted)
- SECRET_KEY compartida entre todos los servicios Superset

## 📈 Próximos Pasos

### Opcional: Flower (Celery Monitoring UI)
Agregar al `docker-compose.yml`:
```yaml
superset-flower:
  build:
    context: .
    dockerfile: ./superset.Dockerfile
  container_name: superset-flower
  command: ["celery", "--app=superset.tasks.celery_app:app", "flower"]
  ports:
    - "5555:5555"
  environment:
    - REDIS_HOST=redis
    - REDIS_PORT=6379
  depends_on:
    - redis
    - superset-worker
  networks:
    - etl_net
```

Luego acceder a: http://localhost:5555

### Opcional: Redis Sentinel (Alta Disponibilidad)
Para producción real con múltiples instancias Redis.

## ✅ Verificación del Stack

### Healthchecks Automáticos
Todos los servicios tienen healthchecks:
```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

Buscar `(healthy)` en:
- `superset-redis`
- `superset`
- `superset-worker`

### Test Manual
1. Abrir SQL Lab
2. Ejecutar query simple:
   ```sql
   SELECT count(*) FROM system.tables
   ```
3. Verificar aparece "Running..." y luego resultados
4. Revisar logs del worker:
   ```bash
   docker logs superset-worker | tail -20
   ```
   Debería mostrar la query ejecutándose

## 📚 Referencias

- [Superset Async Queries](https://superset.apache.org/docs/configuration/async-queries-celery/)
- [Celery Documentation](https://docs.celeryproject.org/)
- [Redis Documentation](https://redis.io/documentation)
