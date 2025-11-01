# CONFIGURACIÓN AUTOMÁTICA DE PERMISOS ETL

## Descripción

Este sistema configura automáticamente los permisos del usuario ETL en ClickHouse sin intervención manual. Es esencial para que el pipeline ETL funcione correctamente en cualquier ambiente.

## Archivos Principales

### `tools/etl_permissions_setup.py`
- **Propósito**: Configuración completa y automática de permisos ETL
- **Ejecución**: Automática durante el inicio del pipeline
- **Logs**: Guarda detalles en `logs/etl_permissions_setup.log`

### `tools/auto_init_etl.py` 
- **Propósito**: Inicialización automática del ambiente ETL
- **Ejecución**: Al iniciar el contenedor etl-tools
- **Función**: Orquesta la configuración sin intervención manual

### `tools/auto_setup_etl_permissions.sh`
- **Propósito**: Script de integración para llamadas manuales
- **Uso**: `bash tools/auto_setup_etl_permissions.sh`

## Integración en el Pipeline

### 1. Pipeline Automatizado (`start_automated_pipeline.sh`)
```bash
# Se ejecuta automáticamente durante:
docker compose up -d
# Luego el script llama a la configuración ETL
```

### 2. Docker Compose
```yaml
# El contenedor etl-tools ejecuta auto_init_etl.py al iniciarse
# Esto garantiza permisos correctos sin intervención manual
```

## Permisos Configurados Automáticamente

El usuario ETL recibe los siguientes permisos:

### Permisos Globales
- `CREATE DATABASE ON *.*`
- `CREATE TABLE ON *.*` 
- `CREATE VIEW ON *.*`
- `DROP TABLE ON *.*`
- `DROP DATABASE ON *.*`
- `INSERT ON *.*`
- `SELECT ON *.*`
- `ALTER ON *.*`
- `SYSTEM ON *.*`

### Permisos por Base de Datos
- `ALL ON fgeo_analytics.*`
- `ALL ON archivos.*`
- `ALL ON fiscalizacion.*`
- `ALL ON ext.*`

## Validación Automática

El sistema verifica automáticamente:
1. ✅ Conectividad con ClickHouse
2. ✅ Creación del usuario ETL 
3. ✅ Aplicación de permisos
4. ✅ Capacidad de conexión del usuario ETL
5. ✅ Creación de bases de datos necesarias

## Logs y Debugging

### Archivo de Log
```bash
# Revisar logs detallados:
tail -f logs/etl_permissions_setup.log
```

### Ejecución Manual
```bash
# Si necesitas ejecutar manualmente:
docker compose exec etl-tools python3 tools/etl_permissions_setup.py

# O usando el script de integración:
bash tools/auto_setup_etl_permissions.sh
```

### Verificación Manual
```bash
# Verificar permisos del usuario ETL:
docker compose exec clickhouse clickhouse-client --user=etl --password=Et1Ingest! --query="SHOW GRANTS FOR etl"
```

## Solución de Problemas

### Error: "Not enough privileges"
- **Causa**: Usuario default sin permisos suficientes
- **Solución**: El script maneja esto automáticamente aplicando permisos específicos

### Error: "Connection refused" 
- **Causa**: ClickHouse no está listo
- **Solución**: El script espera automáticamente hasta 15 intentos

### Error: "User already exists"
- **Causa**: Normal en re-ejecuciones
- **Solución**: Se ignora automáticamente, no es un error

## Para Ambientes de Producción

### Replicación Sin IA
1. Clona el repositorio
2. Ejecuta `docker compose up -d`
3. Los permisos ETL se configuran automáticamente
4. No requiere intervención manual

### Variables de Ambiente Necesarias
```env
CLICKHOUSE_DEFAULT_USER=default
CLICKHOUSE_DEFAULT_PASSWORD=ClickHouse123!
CLICKHOUSE_ETL_USER=etl
CLICKHOUSE_ETL_PASSWORD=Et1Ingest!
```

## Beneficios del Sistema

✅ **Automatización completa**: Sin pasos manuales  
✅ **Robustez**: Maneja errores comunes automáticamente  
✅ **Logs detallados**: Facilita debugging  
✅ **Integración transparente**: Parte del pipeline principal  
✅ **Replicabilidad**: Funciona sin asistencia de IA  

## Estado Actual

- ✅ Configuración automática implementada
- ✅ Integración en pipeline principal
- ✅ Logs y debugging configurados
- ✅ Validación automática
- ✅ Documentación completa

El sistema está **listo para producción** y **no requiere configuración manual**.