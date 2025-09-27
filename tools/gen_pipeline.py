#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os, json, re
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
try:
    from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
except Exception:
    pass
import pymysql



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
    prefix = os.getenv("DBZ_SERVER_NAME_PREFIX","dbserver")
    server_name = f"{prefix}_{conn_name}"
    history_suffix = os.getenv("DBZ_HISTORY_TOPIC", "schema-changes")
    history_topic = f"{history_suffix}.{conn_name}"
    snapshot_mode = os.getenv("DBZ_SNAPSHOT_MODE","initial")
    decimal_mode = os.getenv("DBZ_DECIMAL_MODE","string")
    binary_mode = os.getenv("DBZ_BINARY_MODE","base64")
    time_precision = os.getenv("DBZ_TIME_PRECISION","connect")

    payload = {
        "name": server_name,
        "config": {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "database.hostname": conn["host"],
            "database.port": str(conn["port"]),
            "database.user": conn["user"],
            "database.password": conn["pass"],
            "database.server.id": str(5400 + abs(hash(conn_name)) % 1000),
            "database.server.name": server_name,
            "table.include.list": include_list,
            "include.schema.changes": "false",
            "decimal.handling.mode": decimal_mode,
            "binary.handling.mode": binary_mode,
            "time.precision.mode": time_precision,
            "snapshot.mode": snapshot_mode,
            "database.history.kafka.bootstrap.servers": os.getenv("KAFKA_BROKERS","kafka-1:9092,kafka-2:9092,kafka-3:9092"),
            "database.history.kafka.topic": history_topic,
            "database.history.skip.unparseable.ddl": "true"
        }
    }
    return payload

def write_ch_raw_script(conn_name, tables, out_dir):
    ch_db = os.getenv("CLICKHOUSE_DATABASE","fgeo_analytics")
    brokers = os.getenv("KAFKA_BROKERS","kafka-1:9092,kafka-2:9092,kafka-3:9092")
    prefix = os.getenv("DBZ_SERVER_NAME_PREFIX","dbserver")
    sh = ["#!/usr/bin/env bash", "set -euo pipefail"]
    sh.append(f'DB="{ch_db}"')
    sh.append(f"TOPIC_PREFIX=\"{prefix}_{conn_name}\"")
    sh.append(f"BROKERS=\"{brokers}\"")
    sh.append('docker exec -i clickhouse bash -lc "clickhouse-client -q \\"CREATE DATABASE IF NOT EXISTS ext; CREATE DATABASE IF NOT EXISTS ' + ch_db + ';\\""')
    sh.append("")
    for t in tables:
        schema, table = t["schema"], t["table"]
        kafka_tbl = f"ext.kafka_{schema}_{table}"
        raw_tbl = f"{ch_db}.{schema}_{table}_raw"
        mv_tbl = f"ext.mv_{schema}_{table}"
        topic = f"${{TOPIC_PREFIX}}.{schema}.{table}"
        sh += [
            f'docker exec -i clickhouse bash -lc "\\',
            'clickhouse-client -q \\"',
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
            '\\"',
            '"'
        ]
    sh.append('echo "Listo. Verifica conteos > 0 en ${DB}.*_raw."')
    (out_dir / "ch_create_raw_pipeline.sh").write_text("\n".join(sh) + "\n", encoding="utf-8")
    os.chmod(out_dir / "ch_create_raw_pipeline.sh", 0o755)

def main():
    load_dotenv(ROOT / ".env")
    OUT.mkdir(parents=True, exist_ok=True)
    conns = get_conns()
    for conn in conns:
        conn_name = conn["name"]
        out_dir = OUT / conn_name
        schemas_dir = out_dir / "schemas"
        out_dir.mkdir(parents=True, exist_ok=True)
        schemas_dir.mkdir(parents=True, exist_ok=True)

        tables = fetch_tables(conn)
        include = ",".join([f"{t['schema']}.{t['table']}" for t in tables])
        (out_dir / "tables.include.env").write_text(
            f"DBZ_MYSQL_TABLE_INCLUDE_LIST={include}\n", encoding="utf-8"
        )
        payload = connector_payload(conn_name, conn, include)
        (out_dir / "connector.json").write_text(
            json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        write_ch_raw_script(conn_name, tables, out_dir)

        for t in tables:
            cols = fetch_columns(conn, t["schema"], t["table"])
            write_json_schema(t["schema"], t["table"], cols, schemas_dir)

        print(f"✔ [{conn_name}] {len(tables)} tablas -> {out_dir}")

if __name__ == "__main__":
    main()
