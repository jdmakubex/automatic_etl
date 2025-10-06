#!/usr/bin/env python3
import os, json, time, sys, zlib
from pathlib import Path
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# Clases de error
class RecoverableError(Exception):
    """Error recuperable - se puede reintentar"""
    pass

class FatalError(Exception):
    """Error fatal - requiere intervención manual"""
    pass

ROOT = Path(__file__).resolve().parent.parent
# Carga .env si existe (opcional)
try:
    from dotenv import load_dotenv  # type: ignore
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
    try:
        import requests
    except ImportError:
        raise FatalError("requests no instalado. Instalar con: pip install requests")
    
    url = f"{CONNECT_URL}/connector-plugins/{class_name}/config/validate"
    try:
        r = requests.put(url, json=config, timeout=30)
        if r.status_code != 200:
            log.error(f"[VALIDATE][HTTP {r.status_code}] {r.text}")
            return False
        body = r.json()
        errs = []
        for field in body.get("configs", []):
            if field.get("errors"):
                errs.append({field["name"]: field["errors"]})
        if errs:
            log.error("[VALIDATE][ERRORS]:")
            log.error(json.dumps(errs, indent=2, ensure_ascii=False))
            return False
        log.info("[VALIDATE] OK (0 errores)")
        return True
    except requests.exceptions.RequestException as e:
        raise RecoverableError(f"Error validando configuración: {e}. Verifica que Kafka Connect esté disponible.")

def ensure_connector(name: str, config: dict):
    try:
        import requests
    except ImportError:
        raise FatalError("requests no instalado. Instalar con: pip install requests")
    
    # crea o actualiza idempotente
    try:
        get = requests.get(f"{CONNECT_URL}/connectors/{name}", timeout=10)
        if get.status_code == 200:
            put = requests.put(f"{CONNECT_URL}/connectors/{name}/config", json=config, timeout=30)
            if put.status_code >= 400:
                log.error(f"[CONNECT][PUT {name}] HTTP {put.status_code}: {put.text}")
                raise RecoverableError(f"No se pudo actualizar conector {name}")
            log.info(f"[CONNECT] Updated: {name}")
            return
        if get.status_code != 404:
            log.error(f"[CONNECT][GET {name}] HTTP {get.status_code}: {get.text}")
            raise RecoverableError(f"Error verificando conector {name}")

        post = requests.post(f"{CONNECT_URL}/connectors", json={"name": name, "config": config}, timeout=30)
        if post.status_code >= 400:
            log.error(f"[CONNECT][POST {name}] HTTP {post.status_code}: {post.text}\nPayload:\n{json.dumps(config,indent=2)}")
            raise RecoverableError(f"No se pudo crear conector {name}")
        log.info(f"[CONNECT] Created: {name}")
    except requests.exceptions.RequestException as e:
        raise RecoverableError(f"Error de conexión al crear/actualizar conector {name}: {e}")

def wait_running(name: str, timeout=120):
    try:
        import requests
    except ImportError:
        raise FatalError("requests no instalado. Instalar con: pip install requests")
    
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            s = requests.get(f"{CONNECT_URL}/connectors/{name}/status", timeout=10).json()
            states = [t.get("state") for t in s.get("tasks", [])]
            if states and all(st == "RUNNING" for st in states):
                log.info(f"[READY] {name} RUNNING")
                return True
            if any(st == "FAILED" for st in states):
                log.error(f"[FAIL] {name} {json.dumps(s, ensure_ascii=False)}")
                return False
            time.sleep(2)
        except requests.exceptions.RequestException as e:
            log.warning(f"[WAIT] Error verificando estado de {name}: {e}")
            time.sleep(2)
    
    log.warning(f"[WARN] {name} no RUNNING en {timeout}s")
    return False

def main():
    log.info("=== Aplicando conectores Debezium ===")
    
    raw = os.getenv("DB_CONNECTIONS", "[]")
    try:
        conns = json.loads(raw)
    except Exception as e:
        log.error(f"DB_CONNECTIONS inválido: {e}\n<<{raw}>>")
        log.error("Consulta docs/ERROR_RECOVERY.md para formato correcto")
        raise FatalError("DB_CONNECTIONS inválido")

    mysql_conns = [c for c in conns if c.get("type") == "mysql"]
    if not mysql_conns:
        log.warning("No hay conexiones MySQL en DB_CONNECTIONS; nada que hacer.")
        return

    failed_connectors = []
    
    for d in mysql_conns:
        for req in ("host","user","pass","db"):
            if req not in d:
                log.warning(f"Conexión incompleta (falta '{req}'), se omite: {d}")
                failed_connectors.append({"name": d.get("name", "unknown"), "error": f"Campo '{req}' faltante"})
                break
        else:
            name, cfg = make_mysql_cfg(d)
            try:
                if not validate_config("io.debezium.connector.mysql.MySqlConnector", cfg):
                    log.error(f"Config inválida para {name}")
                    failed_connectors.append({"name": name, "error": "Configuración inválida"})
                    continue
                
                ensure_connector(name, cfg)
                wait_running(name)
            except RecoverableError as e:
                log.error(f"Error recuperable aplicando {name}: {e}")
                failed_connectors.append({"name": name, "error": str(e)})
            except FatalError as e:
                log.error(f"Error fatal aplicando {name}: {e}")
                raise
    
    if failed_connectors:
        log.warning(f"⚠ {len(failed_connectors)} conector(es) fallaron:")
        for fc in failed_connectors:
            log.warning(f"  - {fc['name']}: {fc['error']}")
        log.warning("Consulta docs/ERROR_RECOVERY.md para soluciones.")


if __name__ == "__main__":
    try:
        main()
        log.info("[OK] Conectores Debezium aplicados")
        sys.exit(0)
    except FatalError as e:
        log.error(f"[FATAL] {e}")
        sys.exit(2)
    except Exception as e:
        log.error(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
