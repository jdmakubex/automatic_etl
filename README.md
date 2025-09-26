# etl_prod (FULL)
Producción: MySQL 5.5 → Debezium (Connect HA) → Kafka (KRaft RF=3) → ClickHouse → Superset.
Sigue el README del chat: pasos 1–7. Este paquete incluye el docker-compose COMPLETO y scripts listos.

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
- Acceso a tus BDs origen (VPN si aplican).

---

## 1) .env
Edita `etl_prod/.env` (o copia desde `.env.example`) y define **DB_CONNECTIONS**. Ejemplo mínimo:
```json
[{"type":"mysql","name":"fiscalizacion","host":"172.21.61.53","port":3306,"user":"user","pass":"***","db":"fiscalizacion"},
 {"type":"mysql","name":"sipoa","host":"172.21.61.54","port":3306,"user":"user","pass":"***","db":"sipoa"}]
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
- Ejecuta: `docker compose run --rm provision-superset` para registrar la DB ClickHouse en Superset.
- Verifica que aparezca `fgeo_analytics` y datasets.

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

## 5) Orden recomendado (full)
```bash
docker compose up -d clickhouse superset superset-venv-setup superset-init
docker compose run --rm provision-superset
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
```

---

## 8) Checklist de “Definición de listo”
- [ ] `.env` completo y válido (DB_CONNECTIONS probado)
- [ ] `clickhouse` y `superset` funcionando
- [ ] `provision-superset` registra la DB CH
- [ ] **Modo 1**: `ingestor` generó tablas y `count()>0`
- [ ] **Modo 2**: topics Debezium presentes y MVs `*_raw` insertando
- [ ] Dashboards básicos en Superset
