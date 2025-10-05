#!/usr/bin/env python3
import os, json, time, sys, zlib
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parent.parent
# Carga .env si existe (opcional)
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except Exception:
    pass

CONNECT_URL = os.getenv("CONNECT_URL", "http://connect:8083")

def stable_server_id(host: str, db: str, base=5400) -> int:
    return base + (zlib.crc32(f"{host}:{db}".encode()) % 400)

def make_mysql_cfg(d: dict):
    host, port = d["host"], int(d.get("port", 3306))
    user, pwd, db  = d["user"], d["pass"], d["db"]
    prefix = d.get("topic_prefix") or d.get("name") or f"mysql_{db}"
    name = f"mysql-cdc-{d.get('name', db)}"
    cfg = {
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "tasks.max": "1",

        "database.hostname": host,
        "database.port": str(port),
        "database.user": user,
        "database.password": pwd,

        # requerido y estable
        "database.server.id": str(stable_server_id(host, db)),

        # Debezium 2.6+: usar topic.prefix (no database.server.name)
        "topic.prefix": prefix,

        # alcance
        "database.include.list": db,
        # si quieres limitar tablas: d["tables"] = "db.t1,db.t2"
        # "table.include.list": d.get("tables",""),
        **({"table.include.list": d["tables"]} if d.get("tables") else {}),

        "include.schema.changes": "false",
        "snapshot.mode": os.getenv("DBZ_SNAPSHOT_MODE", "initial"),

        # *** historia de esquema (2.6) ***
        "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
        "schema.history.internal.kafka.topic": f"schema-changes.{prefix}",

        # convertidores simples (sin schemas)
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false",

        # dev: 1 broker
        "topic.creation.default.partitions": "3",
        "topic.creation.default.replication.factor": "1",
    }
    return name, cfg

def validate_config(class_name: str, config: dict):
    url = f"{CONNECT_URL}/connector-plugins/{class_name}/config/validate"
    r = requests.put(url, json=config)
    if r.status_code != 200:
        print(f"[VALIDATE][HTTP {r.status_code}] {r.text}")
        return False
    body = r.json()
    errs = []
    for field in body.get("configs", []):
        if field.get("errors"):
            errs.append({field["name"]: field["errors"]})
    if errs:
        print("[VALIDATE][ERRORS]:")
        print(json.dumps(errs, indent=2, ensure_ascii=False))
        return False
    print("[VALIDATE] OK (0 errores)")
    return True

def ensure_connector(name: str, config: dict):
    # crea o actualiza idempotente
    get = requests.get(f"{CONNECT_URL}/connectors/{name}")
    if get.status_code == 200:
        put = requests.put(f"{CONNECT_URL}/connectors/{name}/config", json=config)
        if put.status_code >= 400:
            print(f"[CONNECT][PUT {name}] HTTP {put.status_code}: {put.text}")
            sys.exit(2)
        print(f"[CONNECT] Updated: {name}")
        return
    if get.status_code != 404:
        print(f"[CONNECT][GET {name}] HTTP {get.status_code}: {get.text}")
        sys.exit(2)

    post = requests.post(f"{CONNECT_URL}/connectors", json={"name": name, "config": config})
    if post.status_code >= 400:
        print(f"[CONNECT][POST {name}] HTTP {post.status_code}: {post.text}\nPayload:\n{json.dumps(config,indent=2)}")
        sys.exit(2)
    print(f"[CONNECT] Created: {name}")

def wait_running(name: str, timeout=120):
    t0 = time.time()
    while time.time() - t0 < timeout:
        s = requests.get(f"{CONNECT_URL}/connectors/{name}/status").json()
        states = [t.get("state") for t in s.get("tasks", [])]
        if states and all(st == "RUNNING" for st in states):
            print(f"[READY] {name} RUNNING")
            return True
        if any(st == "FAILED" for st in states):
            print(f"[FAIL] {name} {json.dumps(s, ensure_ascii=False)}")
            return False
        time.sleep(2)
    print(f"[WARN] {name} no RUNNING en {timeout}s")
    return False

def main():
    raw = os.getenv("DB_CONNECTIONS", "[]")
    try:
        conns = json.loads(raw)
    except Exception as e:
        print(f"[ERROR] DB_CONNECTIONS inválido: {e}\n<<{raw}>>")
        sys.exit(1)

    mysql_conns = [c for c in conns if c.get("type") == "mysql"]
    if not mysql_conns:
        print("[WARN] No hay conexiones MySQL en DB_CONNECTIONS; nada que hacer.")
        return

    for d in mysql_conns:
        for req in ("host","user","pass","db"):
            if req not in d:
                print(f"[WARN] Conexión incompleta, se omite: {d}")
                break
        else:
            name, cfg = make_mysql_cfg(d)
            if not validate_config("io.debezium.connector.mysql.MySqlConnector", cfg):
                print(f"[ABORT] Config inválida para {name}")
                sys.exit(2)
            ensure_connector(name, cfg)
            wait_running(name)

if __name__ == "__main__":
    try:
        main()
        print("[OK] Conectores Debezium aplicados")
        sys.exit(0)
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)
