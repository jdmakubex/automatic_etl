# tools/apply_connectors.py
<<<<<<< HEAD
import os, json, time, sys
from pathlib import Path
import requests

# Carga .env si existe y si python-dotenv está disponible; si no, seguimos con las env del contenedor
ROOT = Path(__file__).resolve().parent.parent  # /app
try:
    from dotenv import load_dotenv  # opcional
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
        # Historial en Kafka
        "database.history.kafka.bootstrap.servers": "kafka-1:9092",
        "database.history.kafka.topic": f"{prefix}.history",
        # JSON sin schema
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false",
        # Snapshot inicial (ajusta si ya hiciste BULK de esa tabla)
        "snapshot.mode": "initial",
        # Tópicos por defecto
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
=======
import os, json, time
import requests

CONNECT_URL = os.getenv("CONNECT_URL", "http://connect:8083")
DB_CONNS = os.getenv("DB_CONNECTIONS", "[]")

def ensure_connector(name: str, config: dict):
    r = requests.get(f"{CONNECT_URL}/connectors/{name}")
    if r.status_code == 200:
        # update (PUT /config)
        resp = requests.put(f"{CONNECT_URL}/connectors/{name}/config", json=config)
        resp.raise_for_status()
        print(f"[CONNECT] Updated: {name}")
    elif r.status_code == 404:
        # create (POST)
        payload = {"name": name, "config": config}
        resp = requests.post(f"{CONNECT_URL}/connectors", json=payload)
        resp.raise_for_status()
        print(f"[CONNECT] Created: {name}")
    else:
        print(r.text)
        r.raise_for_status()

def make_mysql_config(db):
    host = db["host"]; port = db.get("port", 3306)
    user = db["user"]; password = db["pass"]; database = db["db"]
    # topic prefix por DB (evita colisiones)
    topic_prefix = f"dbserver_{database}"
    # nombre del conector
    name = f"debezium-mysql-{database}"

    cfg = {
      "connector.class": "io.debezium.connector.mysql.MySqlConnector",
      "tasks.max": "1",
      "database.hostname": host,
      "database.port": str(port),
      "database.user": user,
      "database.password": password,
      "database.server.id": str(5400 + int(time.time()) % 500),
      "database.server.name": topic_prefix,
      "database.include.list": database,
      "include.schema.changes": "false",

      # MySQL 5.5 compat
      "snapshot.mode": "initial",  # o "schema_only" si carga inicial la haces en Bulk
      "database.history.kafka.bootstrap.servers": "kafka-1:9092",
      "database.history.kafka.topic": f"{topic_prefix}.history",

      # Sanitización
      "topic.creation.default.replication.factor": "1",
      "topic.creation.default.partitions": "3",
      "topic.creation.default.cleanup.policy": "delete",
      "decimal.handling.mode": "string",

      # Converters JSON sin schema
      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "key.converter.schemas.enable": "false",
      "value.converter.schemas.enable": "false",
    }
    return name, cfg

def main():
    conns = json.loads(DB_CONNS)
    mysql_conns = [c for c in conns if c.get("type") == "mysql"]
    if not mysql_conns:
        print("[WARN] No hay conexiones MySQL en DB_CONNECTIONS")
        return
    for db in mysql_conns:
        name, cfg = make_mysql_config(db)
>>>>>>> 3a5104d (Ajustes .env y docker-compose (perfiles/healthchecks))
        ensure_connector(name, cfg)

if __name__ == "__main__":
    try:
        main()
        print("[OK] Conectores Debezium aplicados")
        sys.exit(0)
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)
