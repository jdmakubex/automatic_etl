# Guía de Despliegue del Stack Asíncrono

## 🚀 Despliegue Rápido

### Opción 1: Pipeline Completo (RECOMENDADO)

```bash
# 1. Limpieza completa del sistema anterior
bash tools/clean_all.sh

# 2. Levantar servicios con el nuevo stack
docker compose up -d

# 3. Verificar que todo esté funcionando
./tools/verify_async_stack.sh
```

⏱️ **Tiempo estimado**: 5-10 minutos

### Opción 2: Actualización Sin Limpieza

```bash
# 1. Detener servicios actuales
docker compose down

# 2. Reconstruir imágenes con nuevas dependencias
docker compose build superset superset-worker superset-beat

# 3. Levantar con el nuevo stack
docker compose up -d

# 4. Verificar
./tools/verify_async_stack.sh
```

⏱️ **Tiempo estimado**: 3-5 minutos

---

## 📋 Paso a Paso Detallado

### Paso 1: Preparación

#### 1.1 Verificar requisitos
```bash
# Docker y Docker Compose instalados
docker --version
docker compose version

# Puertos disponibles
# 6379 (Redis), 8088 (Superset), 8123 (ClickHouse)
```

#### 1.2 Actualizar configuración (si es necesario)
```bash
# Revisar .env
cat .env | grep -E "SUPERSET|REDIS"

# Variables esperadas (tienen defaults):
# REDIS_HOST=redis (en contenedores)
# REDIS_PORT=6379
```

### Paso 2: Limpieza (Opcional pero Recomendado)

```bash
# Detener todo
docker compose down

# Limpiar volúmenes antiguos (CUIDADO: elimina datos)
docker compose down -v

# O limpieza selectiva (mantener datos de ClickHouse)
docker volume rm etl_prod_superset_home 2>/dev/null || true
```

### Paso 3: Construcción

```bash
# Rebuild de imágenes Superset con Celery y Redis
docker compose build superset

# La imagen se usa para worker y beat también
```

### Paso 4: Despliegue

```bash
# Levantar todos los servicios
docker compose up -d

# O solo los servicios de Superset + Redis
docker compose up -d redis superset superset-worker superset-beat
```

### Paso 5: Verificación

#### 5.1 Healthchecks
```bash
# Ver estado de todos los servicios
docker compose ps

# Buscar (healthy) en:
# - superset-redis
# - superset
# - superset-worker
```

#### 5.2 Verificación automática
```bash
./tools/verify_async_stack.sh
```

**Salida esperada**:
```
✅ Stack Asíncrono Completamente Operativo
🚀 SQL Lab está listo para consultas asíncronas
```

#### 5.3 Verificación manual

**Redis**:
```bash
docker exec superset-redis redis-cli ping
# Debe responder: PONG
```

**Worker**:
```bash
docker exec superset-worker celery -A superset.tasks.celery_app:app inspect ping
# Debe responder con: celery@<hostname>: pong
```

**Superset UI**:
```bash
curl -s http://localhost:8088/health | jq
# Debe responder con: "healthy"
```

### Paso 6: Prueba Funcional

1. **Acceder a Superset**:
   ```
   http://localhost:8088
   ```

2. **Login**:
   - Usuario: `admin`
   - Password: valor de `SUPERSET_PASSWORD` en `.env` (default: `Admin123!`)

3. **Abrir SQL Lab**:
   ```
   http://localhost:8088/sqllab/
   ```

4. **Ejecutar query de prueba**:
   ```sql
   SELECT 
       name,
       engine,
       total_rows
   FROM system.tables 
   WHERE database = 'fgeo_analytics'
   LIMIT 10
   ```

5. **Verificar comportamiento asíncrono**:
   - Debe aparecer "Running..." brevemente
   - Luego mostrar resultados
   - En logs del worker debe aparecer la query

**Ver logs en tiempo real**:
```bash
docker logs -f superset-worker
```

---

## 🔍 Validación Post-Despliegue

### Checklist de Validación

- [ ] Redis responde a PING
- [ ] Worker de Celery responde a inspect ping
- [ ] Beat está corriendo (logs muestran "beat")
- [ ] Superset healthcheck es "healthy"
- [ ] SQL Lab permite ejecutar queries
- [ ] Queries muestran estado "Running" y luego resultados
- [ ] Logs de worker muestran ejecución de queries
- [ ] `verify_async_stack.sh` pasa todas las verificaciones

### Comandos de Validación

```bash
# 1. Todos los contenedores corriendo
docker compose ps | grep -E "redis|superset"

# 2. Logs sin errores críticos
docker logs superset 2>&1 | grep -i error | tail -20
docker logs superset-worker 2>&1 | grep -i error | tail -20

# 3. Redis tiene las 5 DBs configuradas
for i in {0..4}; do 
  echo "DB $i: $(docker exec superset-redis redis-cli -n $i DBSIZE) keys"
done

# 4. Worker tiene tareas registradas
docker exec superset-worker celery -A superset.tasks.celery_app:app inspect registered | grep sql_lab

# 5. Configuración cargada correctamente
docker logs superset 2>&1 | grep "GLOBAL_ASYNC_QUERIES: Enabled"
```

---

## 🐛 Solución de Problemas Durante Despliegue

### Problema: Redis no inicia

**Síntomas**:
```
superset-redis | Error: ...
```

**Solución**:
```bash
# Verificar puerto no ocupado
sudo lsof -i :6379

# Verificar volumen
docker volume inspect etl_prod_redis_data

# Recrear desde cero
docker compose down
docker volume rm etl_prod_redis_data
docker compose up -d redis
```

### Problema: Worker no puede conectarse a Redis

**Síntomas**:
```
superset-worker | Error: Cannot connect to redis://redis:6379
```

**Solución**:
```bash
# Verificar red
docker network inspect etl_prod_etl_net | grep -A 10 superset

# Reiniciar en orden
docker compose up -d redis
sleep 5
docker compose up -d superset-worker
```

### Problema: Superset no carga configuración async

**Síntomas**:
```
Superset logs no muestran "GLOBAL_ASYNC_QUERIES: Enabled"
```

**Solución**:
```bash
# Verificar archivo de config montado
docker exec superset cat /bootstrap/superset_config_simple.py | grep GLOBAL_ASYNC

# Verificar variable de entorno
docker exec superset env | grep SUPERSET_CONFIG_PATH

# Rebuild y restart
docker compose build superset
docker compose up -d superset
```

### Problema: Build falla por dependencias

**Síntomas**:
```
ERROR: Could not install packages: celery, redis
```

**Solución**:
```bash
# Limpiar cache de Docker
docker builder prune -a

# Build sin cache
docker compose build --no-cache superset

# Verificar conectividad a PyPI
docker run --rm python:3.11 pip install celery redis
```

---

## 🔄 Rollback (Si Algo Sale Mal)

### Rollback Completo

```bash
# 1. Detener todo
docker compose down

# 2. Volver a commit anterior
git log --oneline | head -5
git checkout <commit-antes-del-async>

# 3. Rebuild y levantar
docker compose build
docker compose up -d

# 4. Verificar
docker compose ps
```

### Rollback Solo de Superset (Mantener Datos)

```bash
# 1. Detener servicios de Superset
docker compose stop superset superset-worker superset-beat redis

# 2. Volver configuración anterior
git checkout HEAD~3 -- superset_bootstrap/superset_config_simple.py
git checkout HEAD~3 -- superset.Dockerfile

# 3. Rebuild solo Superset
docker compose build superset

# 4. Levantar sin async
docker compose up -d superset

# 5. Deshabilitar async en DB manualmente desde UI
# Data > Databases > ClickHouse > Advanced > SQL Lab
# Desmarcar "Run Async queries"
```

---

## 📊 Monitoreo Post-Despliegue

### Dashboard de Estado (Comando Rápido)

```bash
# Crear script de monitoreo
cat > /tmp/monitor_stack.sh << 'EOF'
#!/bin/bash
clear
echo "=== Stack Asíncrono - Estado ==="
echo ""
echo "Contenedores:"
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "NAME|redis|superset"
echo ""
echo "Redis DBs:"
for i in {0..4}; do 
  COUNT=$(docker exec superset-redis redis-cli -n $i DBSIZE 2>/dev/null | grep -o '[0-9]*' || echo "0")
  echo "  DB $i: $COUNT keys"
done
echo ""
echo "Celery Workers:"
docker exec superset-worker celery -A superset.tasks.celery_app:app inspect ping 2>/dev/null | grep pong || echo "  ⚠ No workers responding"
echo ""
echo "Últimos logs worker:"
docker logs superset-worker 2>&1 | tail -3
EOF

chmod +x /tmp/monitor_stack.sh
/tmp/monitor_stack.sh
```

### Logs Consolidados

```bash
# Ver logs de todos los servicios async
docker compose logs -f redis superset-worker superset-beat
```

### Métricas de Redis

```bash
# Info completa
docker exec superset-redis redis-cli INFO

# Solo stats relevantes
docker exec superset-redis redis-cli INFO stats | grep -E "total_commands|instantaneous"

# Clientes conectados
docker exec superset-redis redis-cli CLIENT LIST
```

---

## 🎓 Próximos Pasos Después del Despliegue

### 1. Configurar Alertas (Opcional)

Crear script de monitoreo continuo:
```bash
# /usr/local/bin/check_async_stack.sh
#!/bin/bash
if ! /mnt/c/proyectos/etl_prod/tools/verify_async_stack.sh > /dev/null 2>&1; then
    echo "ALERT: Async stack check failed" | mail -s "ETL Alert" admin@example.com
fi
```

Agregar a cron:
```bash
# Cada 10 minutos
*/10 * * * * /usr/local/bin/check_async_stack.sh
```

### 2. Optimizar Workers Según Carga

Monitorear uso durante 1 semana:
```bash
# Ver tareas activas cada hora
docker exec superset-worker celery -A superset.tasks.celery_app:app inspect active
```

Si workers están saturados (siempre con 4/4 tareas):
- Aumentar workers: `-c 8` en `docker-compose.yml`
- O escalar horizontalmente: `--scale superset-worker=2`

### 3. Configurar Backup de Redis (Opcional)

```bash
# Agregar a cron diario
docker exec superset-redis redis-cli BGSAVE
docker cp superset-redis:/data/dump.rdb /backup/redis/dump.$(date +%Y%m%d).rdb
```

### 4. Instalar Flower para Monitoreo Visual

Ver `docs/IMPLEMENTACION_STACK_ASINCRONO.md` sección "Próximos Pasos Opcionales".

---

## 📚 Documentación de Referencia

- **Guía completa**: `docs/ASYNC_STACK.md`
- **Resumen ejecutivo**: `docs/IMPLEMENTACION_STACK_ASINCRONO.md`
- **Script de verificación**: `tools/verify_async_stack.sh`
- **README principal**: `README.md` (sección Stack Asíncrono)

---

## ✅ Confirmación de Despliegue Exitoso

Si puedes confirmar **TODOS** estos puntos, el despliegue fue exitoso:

- [x] `docker compose ps` muestra todos los servicios "Up" y "(healthy)"
- [x] `./tools/verify_async_stack.sh` pasa con "✅ Stack Asíncrono Completamente Operativo"
- [x] SQL Lab carga sin errores en http://localhost:8088/sqllab/
- [x] Puedes ejecutar queries y ver resultados
- [x] `docker logs superset-worker` muestra queries ejecutándose
- [x] Redis responde: `docker exec superset-redis redis-cli ping` → PONG
- [x] No hay errores en logs: `docker logs superset 2>&1 | grep ERROR`

---

## 🎉 ¡Despliegue Completado!

Tu sistema ETL ahora incluye:
- ✅ Stack asíncrono completo (Redis + Celery)
- ✅ SQL Lab funcional sin errores
- ✅ Capacidad para queries largas (5 min)
- ✅ Ejecución concurrente (4 workers)
- ✅ Cache inteligente de resultados
- ✅ Monitoreo y verificación automatizada

**Siguiente paso**: Usar SQL Lab para explorar tus datos en ClickHouse 🚀
