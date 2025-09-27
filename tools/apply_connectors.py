# tools/apply_connectors.py  (versión limpia)
import os, json, time, sys
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parent.parent  # /app
# .env opcional
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except Exception:
    pass

CONNECT_URL = os.getenv("CONNECT_URL", "http://connect:8083")

def ensure_connector(name: str, config: dict):
    r = requests.get(f"{CONNECT_URL}/connectors/{name}")
    if r.status_code == 200:
        r = requests.put(f"{CONNECT_URL}/connectors/{name}/config", json=config)
        r.raise_for_status()
        print(f"[CONNECT] Updated: {name}")
    elif r.status_code == 404:
        r = requests.post(f"{CONNECT_URL}/connectors", json={"name": name, "config": config})
        r.raise_for_status()
        print(f"[CONNECT] Created: {name}")
    else:
        print(f"[CONNECT][ERROR] GET {name}: {r.status_code} {r.text}")
        r.raise_for_status()

def make_mysql_cfg(db: dict):
    dbname = db["db"]
    prefix = f"dbserver_{dbname}"
    name = f"debezium-mysql-{dbname}"
    return name, {
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "tasks.max": "1",
        "database.hostname": db["host"],
        "database.port": str(db.get("port", 3306)),
        "database.user": db["user"],
        "database.password": db["pass"],
        "database.server.id": str(5400 + int(time.time()) % 500),
        "database.server.name": prefix,
        "database.include.list": dbname,
        "include.schema.changes": "false",
        "database.history.kafka.bootstrap.servers": "kafka-1:9092",
        "database.history.kafka.topic": f"{prefix}.history",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false",
        "snapshot.mode": "initial",
        "topic.creation.default.replication.factor": "1",
        "topic.creation.default.partitions": "3",
    }

def main():
    raw = os.getenv("DB_CONNECTIONS", "[]")
    try:
        conns = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"[ERROR] DB_CONNECTIONS no es JSON válido: {e}")
        sys.exit(1)

    mysql_conns = [c for c in conns if c.get("type") == "mysql"]
    if not mysql_conns:
        print("[WARN] No hay conexiones MySQL en DB_CONNECTIONS; nada que hacer.")
        return

    for db in mysql_conns:
        required = ("host","user","pass","db")
        if any(k not in db for k in required):
            print(f"[WARN] Conexión incompleta, se omite: {db}")
            continue
        name, cfg = make_mysql_cfg(db)
        ensure_connector(name, cfg)

if __name__ == "__main__":
    try:
        main()
        print("[OK] Conectores Debezium aplicados")
        sys.exit(0)
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)
