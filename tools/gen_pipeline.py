#!/usr/bin/env python
"""
gen_pipeline.py
Genera configuraciones y esquemas para conectores Debezium/Kafka a partir de las bases de datos origen.
Ejecución recomendada: Docker, como parte del pipeline.
"""
# -*- coding: utf-8 -*-

import os, json, re
from pathlib import Path
import pathlib
from dotenv import load_dotenv  # type: ignore
import pymysql  # type: ignore

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")



ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT = ROOT / "generated"

def env_json(name, default=None):
    raw = os.getenv(name)
    if not raw:
        return default
    return json.loads(raw)

def get_conns():
    c = env_json("DB_CONNECTIONS")
    if c:
        return c
    raise SystemExit("Define DB_CONNECTIONS en .env")

def fetch_tables(conn):
    cx = pymysql.connect(host=conn["host"], port=int(conn["port"]), user=conn["user"],
                         password=conn["pass"], database=conn["db"], charset="utf8mb4")
    try:
        with cx.cursor() as cur:
            cur.execute("""
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema=%s AND table_type='BASE TABLE'
                ORDER BY table_name
            """, (conn["db"],))
            return [{"schema": r[0], "table": r[1]} for r in cur.fetchall()]
    finally:
        cx.close()

def fetch_columns(conn, schema, table):
    cx = pymysql.connect(host=conn["host"], port=int(conn["port"]), user=conn["user"],
                         password=conn["pass"], database=conn["db"], charset="utf8mb4")
    try:
        with cx.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type, is_nullable, column_default, extra
                FROM information_schema.columns
                WHERE table_schema=%s AND table_name=%s
                ORDER BY ordinal_position
            """, (schema, table))
            rows = cur.fetchall()
            cols = []
            for name, dtype, nullable, default, extra in rows:
                cols.append({
                    "name": name,
                    "dtype": dtype,
                    "nullable": (str(nullable).upper()=="YES"),
                    "default": None if default is None else str(default),
                    "extra": extra or ""
                })
            return cols
    finally:
        cx.close()

def json_type(dtype):
    t = (dtype or "").lower()
    if any(k in t for k in ["tinyint","smallint","mediumint","int","bigint"]):
        return {"type":"integer"}
    if any(k in t for k in ["decimal","numeric","float","double","real"]):
        return {"type":"number"}
    if "date" in t or "time" in t:
        return {"type":"string"}
    if any(k in t for k in ["char","text","enum","set","blob","binary","varbinary"]):
        return {"type":"string"}
    if "json" in t:
        return {"type":"object"}
    return {"type":"string"}

def write_json_schema(schema, table, cols, dir_schemas):
    props, required = {}, []
    for c in cols:
        props[c["name"]] = json_type(c["dtype"])
        if (not c["nullable"]) and c["default"] is None and ("auto_increment" not in c["extra"]):
            required.append(c["name"])
    obj = {
        "$schema":"http://json-schema.org/draft-07/schema#",
        "title": f"{schema}.{table}",
        "type": "object",
        "properties": props,
        "required": required
    }
    p = dir_schemas / f"{schema}.{table}.schema.json"
    p.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

def connector_payload(conn_name, conn, include_list):
    # Usar variables de entorno del .env para configuración de Debezium
    prefix = os.getenv("DBZ_SERVER_NAME_PREFIX","dbserver")
    server_name = f"{prefix}_{conn_name}"
    history_suffix = os.getenv("DBZ_HISTORY_TOPIC", "schema-changes")
    history_topic = f"{history_suffix}.{conn_name}"
    snapshot_mode = os.getenv("DBZ_SNAPSHOT_MODE","initial")
    decimal_mode = os.getenv("DBZ_DECIMAL_MODE","string")
    binary_mode = os.getenv("DBZ_BINARY_MODE","base64")
    time_precision = os.getenv("DBZ_TIME_PRECISION","connect")

    # Credenciales de MySQL SIEMPRE desde DB_CONNECTIONS (deprecado usar DBZ_DATABASE_*)
    # Nota: Si existen variables legacy, informar que serán ignoradas para evitar confusiones
    legacy_dbz_vars = [
        "DBZ_DATABASE_HOSTNAME", "DBZ_DATABASE_PORT",
        "DBZ_DATABASE_USER", "DBZ_DATABASE_PASSWORD"
    ]
    if any(os.getenv(v) for v in legacy_dbz_vars):
        print("[WARN] Variables DBZ_DATABASE_* detectadas pero ignoradas. Se usan credenciales de DB_CONNECTIONS.")
    db_hostname = conn["host"]
    db_port = str(conn["port"])
    db_user = conn["user"]
    db_password = conn["pass"]
    kafka_brokers = os.getenv("KAFKA_BROKERS", "kafka:9092")
    schema_history_topic = os.getenv("SCHEMA_HISTORY_INTERNAL_KAFKA_TOPIC", history_topic)
    schema_history_brokers = os.getenv("SCHEMA_HISTORY_INTERNAL_KAFKA_BOOTSTRAP_SERVERS", kafka_brokers)

    payload = {
        "name": server_name,
        "config": {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "database.hostname": db_hostname,
            "database.port": db_port,
            "database.user": db_user,
            "database.password": db_password,
            "database.server.id": str(5400 + abs(hash(conn_name)) % 1000),
            "database.server.name": server_name,
            "topic.prefix": server_name,
            "table.include.list": include_list,
            "include.schema.changes": "false",
            "decimal.handling.mode": decimal_mode,
            "binary.handling.mode": binary_mode,
            "time.precision.mode": time_precision,
            "snapshot.mode": snapshot_mode,
            "database.history.kafka.bootstrap.servers": kafka_brokers,
            "database.history.kafka.topic": schema_history_topic,
            "schema.history.internal.kafka.bootstrap.servers": schema_history_brokers,
            "schema.history.internal.kafka.topic": schema_history_topic,
            "database.history.skip.unparseable.ddl": "true"
        }
    }
    return payload

def write_ch_raw_script(conn_name, tables, out_dir):
    ch_db = os.getenv("CLICKHOUSE_DATABASE","fgeo_analytics")
    brokers = os.getenv("KAFKA_BROKERS","kafka:9092")
    prefix = os.getenv("DBZ_SERVER_NAME_PREFIX","dbserver")
    sh = ["#!/usr/bin/env bash", "set -euo pipefail"]
    sh.append(f'DB="{ch_db}"')
    sh.append(f"TOPIC_PREFIX=\"{prefix}_{conn_name}\"")
    sh.append(f"BROKERS=\"{brokers}\"")
    sh.append('# Limpieza previa: borra tablas y vistas antiguas')
    for t in tables:
        schema, table = t["schema"], t["table"]
        kafka_tbl = f"ext.kafka_{schema}_{table}"
        raw_tbl = f"{ch_db}.{schema}_{table}_raw"
        mv_tbl = f"ext.mv_{schema}_{table}"
        src_tbl = f"{ch_db}.src__{conn_name}__{schema}__{table}"
        sh.append(f'docker exec -i clickhouse bash -lc "clickhouse-client -q \"DROP VIEW IF EXISTS {mv_tbl}; DROP TABLE IF EXISTS {raw_tbl}; DROP TABLE IF EXISTS {kafka_tbl}; DROP TABLE IF EXISTS {src_tbl};\""')
    sh.append('docker exec -i clickhouse bash -lc "clickhouse-client -q \"CREATE DATABASE IF NOT EXISTS ext; CREATE DATABASE IF NOT EXISTS ' + ch_db + ';\""')
    sh.append("")
    for t in tables:
        schema, table = t["schema"], t["table"]
        kafka_tbl = f"ext.kafka_{schema}_{table}"
        raw_tbl = f"{ch_db}.{schema}_{table}_raw"
        mv_tbl = f"ext.mv_{schema}_{table}"
        src_tbl = f"{ch_db}.src__{conn_name}__{schema}__{table}"
        topic = f"${{TOPIC_PREFIX}}.{schema}.{table}"
        # Obtener columnas reales del modelo
        cols = fetch_columns(get_conns()[0], schema, table)
        print(f"[DEBUG] Columnas obtenidas para {schema}.{table}: {cols}")
        ch_types = {
            "int": "Int32", "bigint": "Int64", "tinyint": "Int8", "smallint": "Int16", "mediumint": "Int32",
            "varchar": "String", "char": "String", "text": "String", "enum": "String", "set": "String",
            "datetime": "DateTime", "date": "Date", "timestamp": "DateTime",
            "decimal": "Decimal(18,0)", "float": "Float32", "double": "Float64", "real": "Float64",
            "blob": "String", "binary": "String", "varbinary": "String", "json": "String"
        }
        def map_type(dtype):
            t = dtype.lower()
            for k, v in ch_types.items():
                if k in t:
                    return v
            return "String"
        col_defs = ", ".join([f'{c["name"]} {map_type(c["dtype"])}' for c in cols])
        col_names = ", ".join([c["name"] for c in cols])
        print(f"[DEBUG] SQL para tabla fuente {src_tbl}: CREATE TABLE IF NOT EXISTS {src_tbl} ({col_defs}) ENGINE = MergeTree ORDER BY {cols[0]['name']};")
        # Crear tablas Kafka, raw y vistas materializadas primero
        sh += [
            f'docker exec -i clickhouse bash -lc "\\',
            'clickhouse-client -q "',
            f"  CREATE TABLE IF NOT EXISTS {kafka_tbl} (value String) ENGINE = Kafka",
            "  SETTINGS kafka_broker_list='${BROKERS}', kafka_topic_list='" + topic + "',",
            f"           kafka_group_name='ch_consumer_{schema}_{table}',",
            "           kafka_format='JSONAsString', kafka_num_consumers=1;",
            "",
            f"  CREATE TABLE IF NOT EXISTS {raw_tbl} (ingested_at DateTime DEFAULT now(), value String)",
            "  ENGINE = MergeTree ORDER BY ingested_at;",
            "",
            f"  CREATE MATERIALIZED VIEW IF NOT EXISTS {mv_tbl} TO {raw_tbl}",
            f"    AS SELECT now() AS ingested_at, value FROM {kafka_tbl};",
            '"',
            '"'
        ]
        # Crear la tabla fuente SIEMPRE con definición explícita, nunca con AS SELECT
        sh.append(f'echo "[INFO] Creando tabla fuente {src_tbl} ..."')
        sh.append(f'docker exec -i clickhouse bash -lc "clickhouse-client -q \"CREATE TABLE IF NOT EXISTS {src_tbl} ({col_defs}) ENGINE = MergeTree ORDER BY {cols[0]["name"]};\"" || echo \"[ERROR] Falló la creación de la tabla fuente {src_tbl}\"')
        # Validar existencia de la tabla fuente
        sh.append(f'docker exec -i clickhouse bash -lc "clickhouse-client -q \"EXISTS TABLE {src_tbl}\"" && echo "[OK] Tabla fuente {src_tbl} creada correctamente." || echo "[ERROR] Tabla fuente {src_tbl} no existe!"')
        # Poblar la tabla fuente con datos de ejemplo
        example_values = []
        for c in cols:
            t = map_type(c["dtype"])
            if t == "String":
                example_values.append("'demo'")
            elif t.startswith("Date"):
                example_values.append("now()")
            elif t.startswith("Int"):
                example_values.append("1")
            elif t.startswith("Float") or t.startswith("Decimal"):
                example_values.append("1.0")
            else:
                example_values.append("NULL")
        sh.append(f'echo "[INFO] Poblando tabla fuente {src_tbl} ..."')
        insert_values = ', '.join(example_values)
        sh.append(f'docker exec -i clickhouse bash -lc "clickhouse-client -q \"INSERT INTO {src_tbl} ({col_names}) SELECT {insert_values} FROM system.numbers LIMIT 1;\"" || echo \"[ERROR] Falló el INSERT en la tabla fuente {src_tbl}\"')
        sh.append(f'docker exec -i clickhouse bash -lc "clickhouse-client -q \"SELECT count() FROM {src_tbl}\""')
    sh.append('echo "Listo. Todo se ha limpiado y re-ingestado automáticamente en ${DB}."')
    (out_dir / "ch_create_raw_pipeline.sh").write_text("\n".join(sh) + "\n", encoding="utf-8")
    os.chmod(out_dir / "ch_create_raw_pipeline.sh", 0o755)

def write_ch_raw_sql(conn_name, tables, out_dir):
    """Generate a single idempotent SQL file to create Kafka engines, raw tables and MVs.
    Avoids docker/quoting issues by allowing HTTP application directly.
    """
    ch_db = os.getenv("CLICKHOUSE_DATABASE","fgeo_analytics")
    brokers = os.getenv("KAFKA_BROKERS","kafka:9092")
    prefix = os.getenv("DBZ_SERVER_NAME_PREFIX","dbserver")
    lines: list[str] = []
    lines.append("CREATE DATABASE IF NOT EXISTS ext;")
    lines.append(f"CREATE DATABASE IF NOT EXISTS {ch_db};")
    lines.append("")

    # Pre-drop objects for idempotency
    for t in tables:
        schema, table = t["schema"], t["table"]
        kafka_tbl = f"ext.kafka_{schema}_{table}"
        raw_tbl = f"{ch_db}.{schema}_{table}_raw"
        mv_tbl = f"ext.mv_{schema}_{table}"
        src_tbl = f"{ch_db}.src__{conn_name}__{schema}__{table}"
        lines.append(f"DROP VIEW IF EXISTS {mv_tbl};")
        lines.append(f"DROP TABLE IF EXISTS {raw_tbl};")
        lines.append(f"DROP TABLE IF EXISTS {kafka_tbl};")
        lines.append(f"DROP TABLE IF EXISTS {src_tbl};")
    lines.append("")

    # Create Kafka/raw/MV and source table
    for t in tables:
        schema, table = t["schema"], t["table"]
        kafka_tbl = f"ext.kafka_{schema}_{table}"
        raw_tbl = f"{ch_db}.{schema}_{table}_raw"
        mv_tbl = f"ext.mv_{schema}_{table}"
        src_tbl = f"{ch_db}.src__{conn_name}__{schema}__{table}"
        topic = f"{prefix}_{conn_name}.{schema}.{table}"

        # Kafka and RAW
        lines += [
            f"CREATE TABLE IF NOT EXISTS {kafka_tbl} (value String) ENGINE = Kafka",
            f"SETTINGS kafka_broker_list='{brokers}', kafka_topic_list='{topic}',",
            f"         kafka_group_name='ch_consumer_{schema}_{table}',",
            f"         kafka_format='JSONAsString', kafka_num_consumers=1;",
            "",
            f"CREATE TABLE IF NOT EXISTS {raw_tbl} (ingested_at DateTime DEFAULT now(), value String)",
            "ENGINE = MergeTree ORDER BY ingested_at;",
            "",
            f"CREATE MATERIALIZED VIEW IF NOT EXISTS {mv_tbl} TO {raw_tbl}",
            f"AS SELECT now() AS ingested_at, value FROM {kafka_tbl};",
            "",
        ]

        # Source table definition from real columns
        cols = fetch_columns(get_conns()[0], schema, table)
        def map_type(dtype: str) -> str:
            t = (dtype or "").lower()
            mapping = {
                "int": "Int32", "bigint": "Int64", "tinyint": "Int8", "smallint": "Int16", "mediumint": "Int32",
                "varchar": "String", "char": "String", "text": "String", "enum": "String", "set": "String",
                "datetime": "DateTime", "date": "Date", "timestamp": "DateTime",
                "decimal": "Decimal(18,0)", "float": "Float32", "double": "Float64", "real": "Float64",
                "blob": "String", "binary": "String", "varbinary": "String", "json": "String"
            }
            for k, v in mapping.items():
                if k in t:
                    return v
            return "String"
        col_defs = ", ".join([f"{c['name']} {map_type(c['dtype'])}" for c in cols])
        order_by = cols[0]['name'] if cols else 'tuple()'
        lines += [
            f"CREATE TABLE IF NOT EXISTS {src_tbl} ({col_defs}) ENGINE = MergeTree ORDER BY {order_by};",
            "",
        ]

    sql_path = out_dir / "ch_create_raw_pipeline.sql"
    sql_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

def main():
    print("[ETL][START] Proceso principal iniciado.")
    OUT.mkdir(parents=True, exist_ok=True)
    conns = get_conns()
    print(f"[ETL] Conexiones detectadas: {len(conns)}")
    for conn in conns:
        conn_name = conn["name"]
        print(f"[ETL][{conn_name}] Iniciando procesamiento de conexión...")
        out_dir = OUT / conn_name
        schemas_dir = out_dir / "schemas"
        out_dir.mkdir(parents=True, exist_ok=True)
        schemas_dir.mkdir(parents=True, exist_ok=True)

        print(f"[ETL][{conn_name}] Extrayendo tablas...")
        tables = fetch_tables(conn)
        print(f"[ETL][{conn_name}] Tablas encontradas: {len(tables)}")
        include = ",".join([f"{t['schema']}.{t['table']}" for t in tables])
        (out_dir / "tables.include.env").write_text(
            f"DBZ_MYSQL_TABLE_INCLUDE_LIST={include}\n", encoding="utf-8"
        )
        print(f"[ETL][{conn_name}] Generando payload de conector...")
        payload = connector_payload(conn_name, conn, include)
        (out_dir / "connector.json").write_text(
            json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        print(f"[ETL][{conn_name}] Generando script de creación de tablas en ClickHouse...")
        write_ch_raw_script(conn_name, tables, out_dir)
        # También generamos un SQL idempotente para aplicar vía HTTP
        write_ch_raw_sql(conn_name, tables, out_dir)

        for t in tables:
            print(f"[ETL][{conn_name}] Generando schema JSON para {t['schema']}.{t['table']}...")
            cols = fetch_columns(conn, t["schema"], t["table"])
            write_json_schema(t["schema"], t["table"], cols, schemas_dir)

        print(f"[ETL][{conn_name}] Proceso de conexión finalizado. {len(tables)} tablas -> {out_dir}")
    print("[ETL][END] Proceso principal finalizado.")

if __name__ == "__main__":
    print("[ETL][MAIN] Script ejecutándose como principal.")
    main()