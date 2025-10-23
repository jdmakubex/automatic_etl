# Guía de Uso: SQL Lab en Superset + ClickHouse

## ✅ Estado Actual: SQL Lab está FUNCIONANDO

Las pruebas confirman que SQL Lab está operativo y puede ejecutar:
- ✅ Queries simples (COUNT, SELECT)
- ✅ Queries con GROUP BY y fechas
- ✅ JOINs entre múltiples tablas
- ✅ Procesamiento asíncrono via Redis + Celery

## 🎯 Cómo Usar SQL Lab Correctamente

### 1. Acceder a SQL Lab

```
Superset UI → SQL Lab → SQL Editor
```

**Base de datos**: Selecciona "ClickHouse ETL Database"  
**Schema**: Selecciona el schema que quieres consultar (ej: `fiscalizacion`, `archivos`)

### 2. Queries Básicas

#### Contar registros
```sql
SELECT count(*) as total 
FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora;
```

#### Seleccionar con límite
```sql
SELECT id, fecha, expedientes_id 
FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora 
LIMIT 100;
```

### 3. Trabajar con Fechas (IMPORTANTE)

❌ **NO uses Time Grain en la UI** - genera SQL inválido  
✅ **Usa funciones ClickHouse directamente en tu SQL**

#### Agrupar por día
```sql
SELECT 
    toStartOfDay(fecha) as dia, 
    count(*) as total
FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora
GROUP BY dia
ORDER BY dia DESC
LIMIT 30;
```

#### Agrupar por mes
```sql
SELECT 
    toYYYYMM(fecha) as mes,
    count(*) as registros
FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora
GROUP BY mes
ORDER BY mes DESC;
```

#### Agrupar por año
```sql
SELECT 
    toYear(fecha) as anio,
    count(*) as registros
FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora
GROUP BY anio
ORDER BY anio DESC;
```

#### Filtrar por rango de fechas
```sql
SELECT count(*) as total
FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora
WHERE fecha >= '2024-01-01' 
  AND fecha < '2025-01-01';
```

### 4. JOINs Entre Tablas

#### JOIN simple
```sql
SELECT 
    b.id,
    b.fecha,
    b.expedientes_id,
    ai.registro,
    ai.denunciante
FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora b
LEFT JOIN fiscalizacion.src__fiscalizacion__fiscalizacion__altoimpacto ai 
    ON b.expedientes_id = ai.expedientes_id
LIMIT 100;
```

#### JOIN con agregación
```sql
SELECT 
    ai.registro,
    count(b.id) as num_bitacoras
FROM fiscalizacion.src__fiscalizacion__fiscalizacion__altoimpacto ai
LEFT JOIN fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora b 
    ON ai.expedientes_id = b.expedientes_id
GROUP BY ai.registro
ORDER BY num_bitacoras DESC
LIMIT 20;
```

#### JOIN entre múltiples schemas
```sql
SELECT 
    f.expedientes_id,
    f.fecha as fecha_bitacora,
    a.nombre as archivo_relacionado
FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora f
LEFT JOIN archivos.src__archivos__archivos__archivosexpedientes a
    ON f.expedientes_id = a.expediente_id
WHERE f.fecha >= today() - INTERVAL 7 DAY
LIMIT 50;
```

### 5. Queries Avanzadas

#### Window functions
```sql
SELECT 
    fecha,
    count(*) as registros_dia,
    sum(count(*)) OVER (ORDER BY fecha) as acumulado
FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora
GROUP BY fecha
ORDER BY fecha DESC
LIMIT 30;
```

#### Subconsultas
```sql
SELECT 
    fecha,
    total
FROM (
    SELECT 
        toStartOfDay(fecha) as fecha,
        count(*) as total
    FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora
    GROUP BY fecha
)
WHERE total > 100
ORDER BY total DESC;
```

## 🔧 Funciones ClickHouse Útiles para Fechas

| Función | Descripción | Ejemplo |
|---------|-------------|---------|
| `toStartOfDay(fecha)` | Redondea a inicio del día | `2024-01-15 00:00:00` |
| `toStartOfWeek(fecha)` | Redondea a inicio de semana | `2024-01-14 (lunes)` |
| `toStartOfMonth(fecha)` | Redondea a inicio de mes | `2024-01-01` |
| `toStartOfQuarter(fecha)` | Redondea a inicio de trimestre | `2024-01-01` |
| `toStartOfYear(fecha)` | Redondea a inicio de año | `2024-01-01` |
| `toYYYYMM(fecha)` | Formato año-mes | `202401` |
| `toYYYYMMDD(fecha)` | Formato año-mes-día | `20240115` |
| `formatDateTime(fecha, '%Y-%m-%d')` | Formato personalizado | `2024-01-15` |
| `dateDiff('day', fecha1, fecha2)` | Diferencia en días | `10` |
| `today()` | Fecha actual | `2025-10-23` |
| `now()` | Fecha y hora actual | `2025-10-23 15:30:00` |

## ⚠️ Problemas Conocidos y Soluciones

### Problema 1: "Failed to start remote query on a worker"
**Causa**: Bug temporal o query muy larga  
**Solución**: Reintenta la query, el worker está funcionando

### Problema 2: Error GROUP BY con fechas en Explore
**Causa**: Superset aplica `toDateTime()` innecesariamente cuando usas Time Grain  
**Solución**: En SQL Lab, NO uses Time Grain, escribe SQL directamente

### Problema 3: Timeout en queries largas
**Causa**: Query compleja que toma mucho tiempo  
**Solución**: 
- Añade `LIMIT` a tus queries
- Filtra por fechas recientes primero
- Usa `SAMPLE 0.1` para queries exploratorias (10% de datos)

### Problema 4: "Column is not under aggregate function"
**Causa**: Falta columna en GROUP BY  
**Solución correcta**:
```sql
-- ❌ Incorrecto
SELECT fecha, count(*) FROM tabla;

-- ✅ Correcto
SELECT fecha, count(*) FROM tabla GROUP BY fecha;
```

## 📊 Creando Visualizaciones desde SQL Lab

Una vez que tu query funciona en SQL Lab:

1. **Guarda la query** con un nombre descriptivo
2. Ve a **Charts** → **+ New Chart**
3. Selecciona el **dataset** correspondiente (NO uses SQL custom en Explore)
4. **NO selecciones Time Grain** - deja en "None"
5. Selecciona métrica COUNT(*) y dimensión (sin transformaciones)

## 🎓 Ejemplos Prácticos de Casos de Uso

### Caso 1: Evolución temporal de registros
```sql
SELECT 
    toStartOfMonth(fecha) as mes,
    count(*) as total_registros
FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora
WHERE fecha >= today() - INTERVAL 12 MONTH
GROUP BY mes
ORDER BY mes;
```

### Caso 2: Análisis por usuario
```sql
SELECT 
    usuario_id,
    count(*) as num_acciones,
    min(fecha) as primera_accion,
    max(fecha) as ultima_accion
FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora
GROUP BY usuario_id
HAVING num_acciones > 10
ORDER BY num_acciones DESC
LIMIT 50;
```

### Caso 3: Cross-database analysis
```sql
WITH bitacoras AS (
    SELECT 
        expedientes_id,
        count(*) as num_bitacoras
    FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora
    GROUP BY expedientes_id
),
archivos AS (
    SELECT 
        expediente_id,
        count(*) as num_archivos
    FROM archivos.src__archivos__archivos__archivosexpedientes
    GROUP BY expediente_id
)
SELECT 
    b.expedientes_id,
    b.num_bitacoras,
    COALESCE(a.num_archivos, 0) as num_archivos
FROM bitacoras b
LEFT JOIN archivos a ON b.expedientes_id = a.expediente_id
ORDER BY b.num_bitacoras DESC
LIMIT 100;
```

## ✅ Verificación: ¿Está funcionando mi SQL Lab?

Ejecuta este script de prueba:

```bash
cd /mnt/c/proyectos/etl_prod
python3 superset_bootstrap/verify_sql_lab.py
```

Deberías ver queries con filas retornadas (rows > 0).

## 📚 Recursos Adicionales

- **ClickHouse Functions**: https://clickhouse.com/docs/en/sql-reference/functions/
- **Superset SQL Lab Docs**: https://superset.apache.org/docs/using-superset/sql-lab/
- **Esquemas disponibles**: `fiscalizacion`, `archivos`, `fiscalizacion_analytics`, `archivos_analytics`

---

**Última actualización**: 2025-10-23  
**Estado del sistema**: ✅ Operativo  
**Stack async**: ✅ Redis + Celery funcionando
