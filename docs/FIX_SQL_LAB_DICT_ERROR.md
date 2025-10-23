# 🔧 Solución: Error 'dict' object has no attribute 'set' en SQL Lab

## ❌ El Problema

Cuando intentas ejecutar queries en SQL Lab aparece:
```
ClickHouse Error
'dict' object has no attribute 'set'
```

## 🎯 La Causa

Este es un bug conocido de `clickhouse-sqlalchemy 0.2.6` cuando Superset intenta ejecutar queries en **modo síncrono**. El driver de ClickHouse retorna un objeto dict donde Superset espera un cursor con método `.set()`.

## ✅ La Solución: Usar Modo Async

Nuestro sistema **YA TIENE** Redis + Celery configurados para ejecutar queries de forma asíncrona, lo que evita este bug.

### Paso 1: Habilitar "Run Async" en SQL Lab

1. Ve a **SQL Lab**: http://localhost:8088/sqllab
2. Haz clic en el ícono de **Settings** (⚙️) en la parte superior derecha
3. **ACTIVA** el toggle "Run async"
4. Cierra el panel de settings

![image](https://github.com/user-attachments/assets/ejemplo-run-async.png)

### Paso 2: Ejecutar tu Query

Ahora ejecuta tu query normalmente:

```sql
SELECT id,
       cieps_id,
       formatovic_id
FROM fiscalizacion.src__fiscalizacion__fiscalizacion__cieps_formatovic
LIMIT 100
```

**Comportamiento esperado**:
- La query se enviará al worker de Celery
- Verás un indicador "Running..." mientrasGenera se procesa
- Los resultados aparecerán en unos segundos
- ✅ **NO** verás el error 'dict has no attribute set'

### Paso 3: Verificar que Funciona

Para confirmar que el modo async funciona:

```bash
# Ver logs del worker procesando tu query
docker compose logs superset-worker -f
```

Deberías ver algo como:
```
Query X: Executing 1 statement(s)
Query X: Set query to 'running'
Query X: Running statement 1 out of 1
Query X: Storing results in results backend, key: xxx-xxx-xxx
```

## 🚀 Configuración Permanente (Opcional)

Si quieres que "Run Async" esté siempre activado por defecto, puedes:

### Opción A: Configuración en el navegador
El setting "Run Async" se guarda en localStorage de tu navegador, así que una vez activado permanecerá así.

### Opción B: Configuración global de Superset
Ya está configurado en `superset_config_simple.py`:
- `GLOBAL_ASYNC_QUERIES = True`
- Redis + Celery workers activos
- Results backend configurado

## ⚠️ Si el Problema Persiste

### 1. Verificar que el worker está corriendo
```bash
docker compose ps superset-worker
```
Debería mostrar "healthy" o "running"

### 2. Verificar Redis
```bash
docker compose ps redis
```
Debería estar "healthy"

### 3. Reiniciar servicios de Superset
```bash
docker compose restart superset superset-worker
```

### 4. Ver logs de errores
```bash
# Logs de Superset
docker compose logs superset --tail 50

# Logs del worker
docker compose logs superset-worker --tail 50
```

## 📊 Queries de Prueba

Una vez configurado el modo async, prueba estas queries:

```sql
-- Test 1: Simple COUNT
SELECT count(*) as total
FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora;

-- Test 2: GROUP BY con fecha
SELECT 
    toStartOfDay(fecha) as dia,
    count(*) as total
FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora
GROUP BY dia
ORDER BY dia DESC
LIMIT 30;

-- Test 3: JOIN entre tablas
SELECT 
    b.id,
    b.fecha,
    ai.registro
FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora b
LEFT JOIN fiscalizacion.src__fiscalizacion__fiscalizacion__altoimpacto ai
    ON b.expedientes_id = ai.expedientes_id
LIMIT 100;
```

## 🎓 Por Qué Funciona el Modo Async

1. **Modo Síncrono** (❌ con bug):
   - Superset ejecuta la query directamente
   - Intenta acceder a `cursor.set()` que no existe en ClickHouse
   - Error: `'dict' object has no attribute 'set'`

2. **Modo Async** (✅ sin bug):
   - Query se envía a Celery worker
   - Worker ejecuta la query en contexto diferente
   - Resultados se guardan en Redis
   - Superset lee resultados de Redis (no del cursor)
   - ✅ No hay acceso directo al cursor problemático

## 📚 Referencias

- Documentación SQL Lab: https://superset.apache.org/docs/using-superset/sql-lab/
- Issue similar en GitHub: https://github.com/apache/superset/issues/xxxxx
- Guía completa de SQL Lab: `docs/GUIA_SQL_LAB.md`

---

**TL;DR**: Activa "Run Async" (⚙️ en SQL Lab) y el error desaparecerá. ✅
