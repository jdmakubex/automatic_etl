# Progress Notes

## 2025-10-22

- Fixed superset-datasets container command so the configurator actually runs (converted to array-form bash -c with multi-line script).
- Implemented automatic marking of DateTime/Timestamp columns in Superset datasets (sets is_dttm=True) to prevent GROUP BY errors in charts.
- Added dataset discovery fallback when create returns 422 (find_dataset) and ensured column marking executes for existing datasets.
- Added function to set default Explore Time Grain to None by updating dataset.extra.default_form_data.time_grain_sqla = null.
- Inserted temporary debug logs around dataset creation/lookup flow to validate execution (to be removed after verification).
- Observed 404 on /api/v1/database/{id}/refresh in current Superset image; harmless, schemas still list successfully.

Status snapshot (evening):
- Dataset configurator ran end-to-end; logs confirm “columns DateTime ya configuradas” and “Time Grain por defecto = None aplicado” for 19 datasets across archivos and fiscalizacion.
- Admin user verified as [Admin] via CLI. Attempted to auto-assign admin as owner for all charts; current build’s user lookup API varies, so owner assignment will be re-validated using /api/v1/me/JWT on next pass.
- Added docs/SUPERSET_UI_TIPS.md with quick tips (All charts tab, dataset vs table message, Time Grain None default, ownership).

Next steps (carry-over):
- Verify in UI that Explore loads with Time Grain=None by default; if any dataset differs, persist fix via dataset.extra.
- Finalize chart ownership assignment for admin using robust user-id resolution; then remove noisy 🐛 DEBUG logs.
- Optionally harden schema refresh to be version-tolerant or skip when 404 is benign.

Next session:
- Verify logs show find_dataset and DateTime marking executing; confirm Explore defaults to Time Grain=None.
- Remove noisy debug logs.
- Optional: improve schema refresh call to be version-tolerant.
# 📋 NOTAS DE PROGRESO - ETL AUTOMATIZADO

**Fecha:** 13 de Octubre, 2025  
**Estado:** Pipeline principal FUNCIONAL, pendiente optimización ingesta de datos

---

## 🎯 AVANCES COMPLETADOS

### ✅ **Automatización Dinámica Implementada**
- **Ingesta multi-database:** Sistema lee automáticamente todas las DB desde `DB_CONNECTIONS` en `.env`
- **Detección automática de tablas:** Incluye TODAS las tablas de cada base sin configuración manual
- **Configuración centralizada:** Todo controlado desde archivo `.env` único

### ✅ **Problemas Técnicos Resueltos**
1. **Error Regex:** Corregido "nothing to repeat at position 0" en `coerce_datetime_columns()`
2. **ClickHouse Startup:** Eliminado conflicto XML/SQL usuarios, solo configuración XML
3. **Container Exit 239:** Resuelto problema de inicialización ClickHouse
4. **Volumes Cleanup:** Limpieza completa de volúmenes conflictivos

### ✅ **Servicios Operacionales**
- **ClickHouse:** ✅ Healthy (8123 HTTP, 9000 native)  
- **Superset:** ✅ Healthy (http://localhost:8088, admin/admin)
- **Kafka+Connect:** ✅ Healthy (CDC en tiempo real)
- **Pipeline Automático:** ✅ Integrado en docker-compose

### ✅ **Configuración Automática**
- Usuario admin Superset creado automáticamente
- Base de datos ClickHouse configurada en Superset
- Variables de entorno manejadas dinámicamente
- Dependencias y validaciones integradas

---

## 🔍 DIAGNÓSTICO ACTUAL

### **Estado de Servicios** (Verificado 13/Oct/2025)
```bash
# ClickHouse: Healthy ✅
curl http://localhost:8123/ → "Ok."

# Superset: Healthy ✅  
curl http://localhost:8088/login/ → HTTP 200

# Kafka Connect: Healthy ✅
curl http://localhost:8083/connectors → HTTP 200
```

### **Configuración DB_CONNECTIONS**
```json
[
  {"name":"demo2","type":"mysql","host":"172.21.61.53","port":3306,"user":"juan.marcos","pass":"123456","db":"demo2"},
  {"name":"demo1","type":"mysql","host":"172.21.61.53","port":3306,"user":"juan.marcos","pass":"123456","db":"demo1"}
]
```

---

## ⚠️ PENDIENTES CRÍTICOS

### 🔴 **Problema Principal: Ingesta de Datos**
**Síntoma:** Las tablas se crean en ClickHouse pero los datos no se insertan completamente

**Evidencia observada:**
- Tablas `demo1_analytics` y `demo2_analytics` creadas ✅
- Esquemas ClickHouse corregidos ✅
- Logs muestran "17 registros" en una tabla, "0 registros" en otras ❌
- Auditoría MySQL vs ClickHouse: diferencias en conteos ❌

### 🔴 **Posibles Causas (Investigar mañana)**
1. **Problema de conectividad MySQL:** Verificar acceso desde contenedor Docker
2. **Error en coerción de tipos:** Algunos tipos de datos pueden fallar silenciosamente
3. **Timeout en ingesta:** Procesos largos pueden estar siendo terminados
4. **Chunking issues:** Problemas con el procesamiento por lotes de pandas

### 🔴 **Verificación Final Automatización**
- Script `verify_automation.py` falla dentro del contenedor por timing/sincronización
- Funciona correctamente cuando se ejecuta manualmente
- No afecta funcionalidad principal pero impide completar pipeline automático

---

## 🧪 PRUEBAS PARA DEPURACIÓN (MAÑANA)

### **1. Diagnóstico Conectividad MySQL**
```bash
# Desde contenedor ETL verificar conectividad
docker exec -it etl_prod-etl-orchestrator-1 mysql -h 172.21.61.53 -u juan.marcos -p123456 -e "SELECT COUNT(*) FROM demo2.empleados;"

# Verificar resolución DNS desde contenedor
docker exec -it etl_prod-etl-orchestrator-1 nslookup 172.21.61.53
```

### **2. Verificación Manual de Ingesta**
```bash
# Ejecutar ingesta paso a paso con logs detallados
docker exec -it pipeline-gen python3 tools/multi_database_ingest.py

# Verificar datos específicos
docker exec clickhouse clickhouse-client --query "SELECT COUNT(*) FROM demo2_analytics.demo2_demo2__empleados"
```

### **3. Debug Tipos de Datos**
```bash
# Probar procesamiento de tipos individualmente
python3 test_date_processing.py

# Verificar logs detallados de coerción
grep "coerce_datetime" /app/logs/etl_full.log
```

### **4. Validar Configuración Simplificada**
- **Investigar:** Si los archivos SQL internos son necesarios
- **Objetivo:** Volver a configuración solo XML como funcionaba originalmente
- **Método:** Eliminar `clickhouse_init.sql` y `create_users.sql`, usar solo `users.xml`

---

## 🤖 AGENTE DE VERIFICACIÓN PROGRAMADO

### **Comando para Verificación Automática**
```bash
# Ejecutar cada hora para verificar estado
/mnt/c/proyectos/etl_prod/verify_system_status.sh
```

### **Métricas a Monitorear**
- Estado de contenedores (healthy/unhealthy)
- Conectividad a servicios (8088, 8123, 8083)
- Conteo de registros en ClickHouse vs MySQL
- Logs de error en contenedores

---

## 🎯 OBJETIVOS PARA MAÑANA

### **Prioridad Alta:**
1. **Resolver ingesta de datos:** Identificar por qué los datos no se transfieren completamente
2. **Simplificar configuración ClickHouse:** Eliminar SQL internos, volver a XML puro
3. **Validar pipeline end-to-end:** Desde MySQL hasta visualización en Superset

### **Prioridad Media:**
1. Optimizar verificación final de automatización
2. Agregar métricas de monitoreo automático
3. Documentar proceso de troubleshooting

### **Prioridad Baja:**
1. Optimización de performance
2. Configuración de alertas
3. Tests automatizados

---

## 📞 CONTACTO PARA RETOMAR

**Contexto completo disponible en:**
- Commit: `23d7ec3` - "Pipeline ETL dinámico completamente funcional"
- Logs: `/mnt/c/proyectos/etl_prod/logs/`
- Configuración: `.env` y `docker-compose.yml`

**Comando rápido para continuar:**
```bash
cd /mnt/c/proyectos/etl_prod
git pull
docker compose up -d
```

---

**Último estado:** Servicios principales funcionando, falta resolver transferencia completa de datos MySQL → ClickHouse.

---

## 🧭 Bitácora – 16 de Octubre, 2025

Contexto rápido:
- Hay dos “fuentes” visibles en Superset porque ClickHouse expone bases diferentes como “esquemas”:
  - `fiscalizacion` (tablas crudas de ingesta)
  - `fiscalizacion_analytics` (vistas para BI). Ej.: `fiscalizacion_bitacora_v` con `fecha_date` (Date) derivada.
- Ingesta verificada: tablas con datos (p.ej. bitácora ~513k filas). Superset operativo (admin/admin).
- Error típico de fechas en Superset: ocurre si se mezcla columna temporal con time grain y otras columnas no agregadas/agrupadas.

Pendiente por hacer (NO ejecutar ahora):
1) Restringir esquemas visibles en Superset (dejar solo analytics)
  - Opción A (recomendada): crear usuario ClickHouse de solo lectura con permisos en `fiscalizacion_analytics` y reconfigurar la conexión en Superset para usar ese usuario.
  - Opción B: conservar conexión actual, pero registrar datasets únicamente del esquema `fiscalizacion_analytics` y eliminar los del esquema base `fiscalizacion` para evitar duplicados.

2) Limpiar datasets del esquema base en Superset
  - Identificar datasets asociados al esquema `fiscalizacion` y eliminarlos desde Superset (UI o API) dejando únicamente los de `fiscalizacion_analytics`.

3) Asegurar columna temporal por defecto
  - En los datasets de `fiscalizacion_analytics`, confirmar que `fecha_date` sea la columna temporal por defecto (main_dttm_col), de forma que los charts no fallen al aplicar time grain.

4) Guía de uso para evitar errores de fecha en Explore
  - Para series temporales: usar dataset `fiscalizacion_analytics.*`, seleccionar `fecha_date` como Time column y elegir un Time grain (Day/Month/etc.), añadir métricas (Count o agregaciones) y evitar colocar columnas no agregadas sin agrupar.
  - Para registros detallados: cambiar Query mode a Raw Records (sin time grain) o usar filtros de rango por `fecha_date`.

Notas técnicas para implementación posterior:
- Script de datasets (`superset_bootstrap/configure_datasets.py`) se puede parametrizar con:
  - SUPERSET_SCHEMA=fiscalizacion_analytics
  - SUPERSET_TIME_COLUMN=fecha_date
- Vista creada: `fiscalizacion_analytics.fiscalizacion_bitacora_v` con `fecha_date=toDate(fecha)`.

Checklist de próxima sesión:
- [ ] Crear usuario `superset_ro` en ClickHouse con SELECT solo en `fiscalizacion_analytics` y probar conexión.
- [ ] Reconfigurar conexión de Superset a ese usuario para ocultar esquema base.
- [ ] Eliminar datasets del esquema `fiscalizacion` (si se decide mantener una sola fuente visible).
- [ ] Validar que los charts con `fecha_date` no generen errores de agregación.
