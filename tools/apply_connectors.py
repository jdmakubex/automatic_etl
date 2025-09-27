#!/usr/bin/env python
# -*- coding: utf-8 -*-
# tools/apply_connectors.py  (encabezado robusto)
import pathlib
import os, json, time
from pathlib import Path  
import requests

ROOT = Path(__file__).resolve().parent.parent  # /app

# Carga .env si existe y si python-dotenv está instalado; si no, sigue con env del contenedor
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except Exception:
    pass

CONNECT_URL = os.getenv("CONNECT_URL", "http://connect:8083")
DB_CONNS = json.loads(os.getenv("DB_CONNECTIONS", "[]"))



ROOT = pathlib.Path(__file__).resolve().parents[1]
GEN = ROOT / "generated"

def wait(url, timeout=180):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            r = requests.get(url + "/connectors", timeout=5)
            if r.ok: return True
        except Exception:
            pass
        time.sleep(2)
    return False

def main():
    load_dotenv(ROOT / ".env")
    url = os.getenv("DEBEZIUM_CONNECT_URL", "http://connect-1:8083").rstrip("/")
    wait(url)

    ok = 0
    for conn_dir in GEN.glob("*"):
        cj = conn_dir / "connector.json"
        if not cj.exists():
            continue
        payload = json.loads(cj.read_text(encoding="utf-8"))
        name = payload.get("name") or conn_dir.name
        print(f"Registrando conector: {name} ...")
        r = requests.post(f"{url}/connectors", json=payload, timeout=30)
        if r.status_code in (200, 201): 
            ok += 1
            print(f"  ✔ {name} -> {r.status_code}")
        elif r.status_code == 409:
            print(f"  ≈ {name} ya existe (409). PUT /connectors/{name}/config ...")
            r2 = requests.put(f"{url}/connectors/{name}/config", json=payload["config"], timeout=30)
            r2.raise_for_status()
            ok += 1
            print(f"  ✔ {name} actualizado")
        else:
            print(f"  ✖ {name} -> {r.status_code}: {r.text}")
    print(f"Conectores aplicados: {ok}")

if __name__ == "__main__":
    try:
        main()
        print("[OK] Conectores Debezium aplicados")
        raise SystemExit(0)
    except Exception as e:
        print(f"[ERROR] {e}")
        raise SystemExit(1)
