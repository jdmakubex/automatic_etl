import os, time, json
import requests
from tenacity import retry, stop_after_delay, wait_fixed

SUP_URL   = os.getenv("SUPERSET_URL", "http://superset:8088").rstrip("/")
ADMIN     = os.getenv("SUPERSET_ADMIN", "admin")
PASSWORD  = os.getenv("SUPERSET_PASSWORD", "Admin123!")
CH_DB     = os.getenv("CLICKHOUSE_DATABASE", "fgeo_analytics")
CH_USER   = os.getenv("CLICKHOUSE_USER", "default")
CH_PASS   = os.getenv("CLICKHOUSE_PASSWORD", "")
CH_HOST   = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_PORT   = int(os.getenv("CLICKHOUSE_PORT", "8123"))
MAKE_DEMO = os.getenv("PROVISION_SAMPLE", "true").lower() in ("1","true","yes","y")

SESSION = requests.Session()
SESSION.headers.update({"Content-Type": "application/json"})

def uri():
    pwd = f":{CH_PASS}" if CH_PASS else ""
    return f"clickhousedb+connect://{CH_USER}{pwd}@{CH_HOST}:{CH_PORT}/{CH_DB}"

@retry(stop=stop_after_delay(120), wait=wait_fixed(3))
def wait_superset_ready():
    r = SESSION.get(f"{SUP_URL}/health")
    r.raise_for_status()
    data = r.json()
    if not data.get("status") == "OK":
        raise RuntimeError(f"Health not OK: {data}")

def login():
    # /api/v1/security/login
    payload = {
        "username": ADMIN,
        "password": PASSWORD,
        "provider": "db",
        "refresh": True
    }
    r = SESSION.post(f"{SUP_URL}/api/v1/security/login", json=payload)
    r.raise_for_status()
    access = r.json()["access_token"]
    SESSION.headers.update({"Authorization": f"Bearer {access}"})

def get_database_id_by_name(name):
    r = SESSION.get(f"{SUP_URL}/api/v1/database/?q=(filters:!((col:name,opr:ct,value:'{name}')))")
    r.raise_for_status()
    res = r.json()
    for item in res.get("result", []):
        if item.get("database_name") == name:
            return item["id"]
    return None

def create_or_update_database(name, sqlalchemy_uri):
    db_id = get_database_id_by_name(name)
    payload = {
        "database_name": name,
        "sqlalchemy_uri": sqlalchemy_uri,
        "expose_in_sqllab": True,
        "allow_ctas": True,
        "allow_cvas": True
    }
    if db_id:
        r = SESSION.put(f"{SUP_URL}/api/v1/database/{db_id}", json=payload)
        r.raise_for_status()
        return db_id
    r = SESSION.post(f"{SUP_URL}/api/v1/database/", json=payload)
    r.raise_for_status()
    return r.json()["id"]

def get_dataset_id(db_id, schema, table):
    q = f"(filters:!((col:database,opr:eq,value:{db_id}),(col:schema,opr:eq,value:'{schema}'),(col:table_name,opr:eq,value:'{table}')))"
    r = SESSION.get(f"{SUP_URL}/api/v1/dataset/?q={q}")
    r.raise_for_status()
    res = r.json().get("result", [])
    return res[0]["id"] if res else None

def create_dataset(db_id, schema, table):
    payload = {
        "database": db_id,
        "schema": schema,
        "table_name": table
    }
    r = SESSION.post(f"{SUP_URL}/api/v1/dataset/", json=payload)
    r.raise_for_status()
    return r.json()["id"]

def main():
    print("[PROVISION] Esperando Superset...")
    wait_superset_ready()
    print("[PROVISION] Login...")
    login()

    db_name = f"ClickHouse {CH_HOST}:{CH_PORT}/{CH_DB}"
    u = uri()
    print(f"[PROVISION] Creando/actualizando DB '{db_name}' -> {u}")
    db_id = create_or_update_database(db_name, u)
    print(f"[PROVISION] Database ID: {db_id}")

    if MAKE_DEMO:
        schema = CH_DB
        table  = "eventos"
        print(f"[PROVISION] Intentando dataset demo: {schema}.{table}")
        ds_id = get_dataset_id(db_id, schema, table)
        if ds_id:
            print(f"[PROVISION] Dataset ya existe (ID {ds_id}), ok.")
        else:
            try:
                ds_id = create_dataset(db_id, schema, table)
                print(f"[PROVISION] Dataset creado (ID {ds_id})")
            except requests.HTTPError as e:
                print(f"[PROVISION] No se creó dataset (quizá tabla no existe). Detalle: {e}")

    print("[PROVISION] Listo.")

if __name__ == "__main__":
    main()
