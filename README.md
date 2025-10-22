# ETL Pipeline Automático - MySQL → ClickHouse → Superset

Pipeline ETL completamente automatizado con soporte para múltiples bases de datos, CDC en tiempo real y visualización integrada.

---

## 🚀 Inicio Rápido

### Requisitos Previos
- Docker y Docker Compose instalados
- Puertos disponibles: 8088 (Superset), 8123 (ClickHouse), 9092 (Kafka), 8083 (Kafka Connect)
- Acceso a las bases de datos MySQL origen

### Configuración Inicial

1. **Copiar archivo de configuración:**
   ```bash
   cp .env.example .env
   ```

2. **Editar `.env` y configurar tus conexiones:**
   ```bash
   nano .env
   ```
   
   Ejemplo de configuración:
   ```ini
   DB_CONNECTIONS=[{"name":"db1","type":"mysql","host":"host.docker.internal","port":3306,"user":"usuario","pass":"password","db":"nombre_db"}]
   ```

3. **Ejecutar el pipeline completo:**
   ```bash
   ./start_etl_pipeline.sh
   ```

   El pipeline ejecutará automáticamente:
   - ✅ Verificación de servicios
   - ✅ Ingesta de datos desde MySQL
   - ✅ Configuración CDC (Debezium → Kafka → ClickHouse)
   - ✅ Configuración de Superset
   - ✅ Validación completa del sistema

---

## 📋 Comandos Principales

### Pipeline Automático

**Ejecución completa con limpieza (RECOMENDADO):**
```bash
# Limpieza completa del sistema
bash tools/clean_all.sh

# Reiniciar servicios con profile CDC
docker compose --profile cdc up -d

# El pipeline se ejecutará automáticamente y configurará todo
```

**Inicio normal:**
```bash
./start_etl_pipeline.sh
```

**Inicio con limpieza completa:**
```bash
./start_etl_pipeline.sh --clean
```

**Modo manual (solo servicios):**
```bash
./start_etl_pipeline.sh --manual
```

### Verificación del Estado

**Ver estado de servicios:**
```bash
docker compose ps
```

**Ver logs del orquestador:**
```bash
docker compose logs -f etl-orchestrator
```

**Verificar datos en ClickHouse:**
```bash
docker compose exec clickhouse clickhouse-client --query "SHOW TABLES FROM fgeo_analytics"
```

---

## 🔧 Configuración Detallada

### Archivo `.env`

Variables principales que debes configurar:

```ini
# Conexiones a bases de datos (formato JSON)
DB_CONNECTIONS=[...]

# Configuración ClickHouse
CLICKHOUSE_DATABASE=fgeo_analytics
CLICKHOUSE_ETL_USER=etl
CLICKHOUSE_ETL_PASSWORD=Et1Ingest!

# Configuración Superset
SUPERSET_ADMIN=admin
SUPERSET_PASSWORD=Admin123!

# Configuración Kafka
KAFKA_CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk
```

### Formato de Conexiones

Cada conexión en `DB_CONNECTIONS` debe tener:
- `name`: Nombre identificador de la conexión
- `type`: Tipo de base de datos (`mysql`)
- `host`: Host de la base de datos
- `port`: Puerto (típicamente 3306 para MySQL)
- `user`: Usuario con permisos de lectura
- `pass`: Contraseña del usuario
- `db`: Nombre de la base de datos

---

## 📊 Acceso a los Servicios

Una vez iniciado el pipeline, puedes acceder a:

**Superset (Visualización):**
- URL: http://localhost:8088
- Usuario: `admin`
- Contraseña: `Admin123!` (o la configurada en `.env`)

**ClickHouse (Base de Datos Analítica):**
- HTTP: http://localhost:8123
- TCP: localhost:9000
- Usuario: `etl`
- Base de datos: `fgeo_analytics`

**Kafka Connect (CDC):**
- API REST: http://localhost:8083
- Ver conectores: `curl http://localhost:8083/connectors`

---

## 🔍 Validación y Troubleshooting

### Verificar Estado del Pipeline

```bash
# Ver logs completos
tail -f logs/auto_pipeline_detailed.log

# Ver estado JSON
cat logs/auto_pipeline_status.json

# Ver reporte de ingesta
cat logs/multi_database_ingest_report.json
```

### Comandos de Diagnóstico

**Verificar servicios:**
```bash
docker compose ps
```

**Verificar conectores Debezium:**
```bash
curl http://localhost:8083/connectors
curl http://localhost:8083/connectors/<nombre-conector>/status
```

**Verificar datos en ClickHouse:**
```bash
docker compose exec clickhouse clickhouse-client --query "
SELECT 
    database,
    name as table,
    total_rows
FROM system.tables 
WHERE database = 'fgeo_analytics'
ORDER BY name"
```

### Reiniciar el Pipeline

Si necesitas reiniciar completamente:

```bash
# Detener servicios
docker compose down

# Limpiar volúmenes (ADVERTENCIA: Elimina datos)
docker compose down -v

# Reiniciar
./start_etl_pipeline.sh --clean
```

---

## 📚 Documentación Adicional

- **Configuración avanzada:** Ver `docs/ENVIRONMENT_VARIABLES.md`
- **Recuperación de errores:** Ver `docs/ERROR_RECOVERY.md`
- **Orden de dependencias:** Ver `docs/DEPENDENCIES.md`
- **Guía de testing:** Ver `docs/TESTING_GUIDE.md`

---

## 🛠️ Arquitectura del Sistema

```
MySQL Fuente(s)
    ↓
Debezium CDC → Kafka → ClickHouse (Kafka Engines + MVs)
    ↓                        ↓
Kafka Topics         Tablas Raw + Analytics
                            ↓
                       Superset
```

**Componentes principales:**
- **MySQL:** Bases de datos fuente con binary log habilitado
- **Debezium:** Captura cambios (CDC) de MySQL
- **Kafka:** Sistema de mensajería para streaming
- **ClickHouse:** Base de datos analítica columnar
- **Superset:** Plataforma de visualización y BI

---

## 🔐 Configuración MySQL Requerida

### Para CDC con Debezium (tiempo real):
```ini
[mysqld]
log-bin=mysql-bin
binlog_format=ROW
binlog_row_image=FULL
server_id=1
```

### Permisos del usuario:
```sql
CREATE USER 'etl_user'@'%' IDENTIFIED BY 'tu_password';
GRANT SELECT ON tu_base_datos.* TO 'etl_user'@'%';
GRANT REPLICATION SLAVE ON *.* TO 'etl_user'@'%';
GRANT REPLICATION CLIENT ON *.* TO 'etl_user'@'%';
FLUSH PRIVILEGES;
```

---

## ⚠️ Notas Importantes

1. **Seguridad:** Nunca subas el archivo `.env` al repositorio
2. **Permisos MySQL:** El usuario necesita permisos de `SELECT` y para CDC: `REPLICATION SLAVE` y `REPLICATION CLIENT`
3. **Binary Log:** Para CDC, MySQL debe tener `log-bin` habilitado y `binlog_format=ROW`
4. **Recursos:** Asegúrate de tener suficiente memoria RAM (mínimo 4GB recomendados)

---

## 🤝 Soporte

Para problemas o dudas:
1. Revisa los logs en `logs/auto_pipeline_detailed.log`
2. Consulta la documentación en `docs/`
3. Verifica el estado de los servicios con `docker compose ps`
