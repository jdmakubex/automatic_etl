#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bootstrap CDC:
- (opcional) instala deps de Python
- aplica conectores Debezium
- genera objetos ClickHouse (Kafka Engines + MVs)
- aplica SQLs generados vía HTTP
- si no hay .sql, ejecuta wrappers .sh interceptando clickhouse-client -> HTTP
"""
import os
import sys
import glob
import shutil
import logging
import argparse
import subprocess
from pathlib import Path

# -----------------------------------
# Utils
# -----------------------------------
def sh(cmd: list[str] | str, **kwargs) -> subprocess.CompletedProcess:
    """Run shell command with check=True by default."""
    if "check" not in kwargs:
        kwargs["check"] = True
    if isinstance(cmd, list):
        return subprocess.run(cmd, **kwargs)
    return subprocess.run(cmd, shell=True, **kwargs)

def ensure_deps(skip_install: bool):
    """Instala dependencias mínimas si faltan (o salta si se indica)."""
    if skip_install:
        return
    pkgs = ["python-dotenv", "requests", "clickhouse-connect", "SQLAlchemy", "PyMySQL"]
    try:
        import requests  # type: ignore # noqa: F401
    except Exception:
        logging.info("[BOOT] Instalando dependencias de Python…")
        sh([sys.executable, "-m", "pip", "install", "--no-cache-dir", "-q", *pkgs])

def env_get(name: str, default: str) -> str:
    return os.environ.get(name, default)

def apply_connectors():
    logging.info("[CDC] Aplicando conectores Debezium…")
    sh([sys.executable, "tools/apply_connectors.py"])

def generate_pipeline():
    logging.info("[CDC] Generando objetos ClickHouse (Kafka Engine + MVs)…")
    gen_dir = Path("generated")
    if gen_dir.exists():
        # Borra SOLO el contenido de generated/, no la carpeta
        for p in gen_dir.glob("*"):
            if p.is_dir():
                shutil.rmtree(p, ignore_errors=True)
            else:
                p.unlink(missing_ok=True)
    else:
        gen_dir.mkdir(parents=True, exist_ok=True)
    sh([sys.executable, "tools/gen_pipeline.py"])

def apply_sql_via_http(sql_path: Path, ch_host: str, ch_port: str, ch_db: str, ch_user: str, ch_pass: str):
    import requests  # type: ignore
    with open(sql_path, "rb") as fh:
        r = requests.post(
            f"http://{ch_host}:{ch_port}/",
            params={"database": ch_db, "max_execution_time": "60"},
            data=fh.read(),
            auth=(ch_user, ch_pass),
        )
        r.raise_for_status()

def apply_sql_files(ch_host: str, ch_port: str, ch_db: str, ch_user: str, ch_pass: str) -> int:
    patterns = ["generated/**/*.sql", "generated/*/*.sql"]
    files = set()
    for pat in patterns:
        files.update(glob.glob(pat, recursive=True))
    files = sorted(files)
    applied = 0
    for f in files:
        logging.info("[CH] < %s", f)
        try:
            apply_sql_via_http(Path(f), ch_host, ch_port, ch_db, ch_user, ch_pass)
            applied += 1
        except Exception as e:
            logging.error("[CH] Error aplicando %s: %s", f, e)
            raise
    return applied

def run_wrappers_with_proxy(ch_host: str, ch_port: str, ch_db: str, ch_user: str, ch_pass: str) -> int:
    """
    Ejecuta los .sh generados, pero con funciones bash que:
    - reemplazan `docker exec … clickhouse-client` y `clickhouse-client` por llamadas HTTP.
    Retorna el conteo de scripts .sh ejecutados exitosamente.
    """
    bash_script = r'''
set -euo pipefail

click_http_py='import sys,requests,os
h=os.environ["CH_HOST"]; p=os.environ["CH_PORT"]
d=os.environ["CH_DB"];   u=os.environ["CH_USER"]; w=os.environ["CH_PASS"]
data=sys.stdin.read().encode("utf-8")
r=requests.post(f"http://{h}:{p}/", params={"database": d, "max_execution_time": "60"}, data=data, auth=(u, w))
r.raise_for_status()'

click_http() { python -c "$click_http_py"; }

clickhouse-client() {
  local SQL=""; local use_stdin=1
  while [ "$#" -gt 0 ]; do
    case "$1" in
      -q) shift; SQL="$1"; use_stdin=0 ;;
      -n|-multiquery|-m|-qf) ;;  # ignorar flags comunes
      *) ;;
    esac
    shift || true
  done
  if [ "$use_stdin" -eq 0 ]; then
    printf "%s" "$SQL" | click_http
  else
    click_http
  fi
}
export -f click_http clickhouse-client

docker() {
  # docker exec -i clickhouse [bash -lc] "clickhouse-client …"
  if [ "$1" = "exec" ] && [ "$2" = "-i" ] && [ "$3" = "clickhouse" ]; then
    shift 3
    if [ "$1" = "bash" ] && [ "$2" = "-lc" ]; then
      shift 2; bash -lc "$*"; return $?
    fi
    if [ "$1" = "clickhouse-client" ]; then
      shift; clickhouse-client "$@"; return $?
    fi
  fi
  echo "[WRAP] comando docker no soportado en bootstrap: $*" >&2
  return 1
}
export -f docker

APPLIED=0
shopt -s nullglob
for shf in generated/*/ch_create_raw_pipeline.sh generated/**/ch_create_raw_pipeline.sh; do
  echo "[CDC] bash $shf"
  if bash "$shf"; then
    APPLIED=$((APPLIED+1))
  else
    true
  fi
done

echo "APPLIED=$APPLIED"
'''
    env = os.environ.copy()
    env.update({
        "CH_HOST": ch_host,
        "CH_PORT": ch_port,
        "CH_DB": ch_db,
        "CH_USER": ch_user,
        "CH_PASS": ch_pass,
    })
    res = sh(["bash", "-lc", bash_script], env=env, capture_output=True, text=True)
    # parsea la última línea "APPLIED=N"
    last = res.stdout.strip().splitlines()[-1] if res.stdout else ""
    if last.startswith("APPLIED="):
        try:
            return int(last.split("=", 1)[1])
        except Exception:
            pass
    logging.warning("[CDC] No se pudo parsear conteo de wrappers; revisa logs.")
    return 0

# -----------------------------------
# Main
# -----------------------------------
def main():
    parser = argparse.ArgumentParser(description="Bootstrap CDC end-to-end.")
    parser.add_argument("--skip-install", action="store_true", help="No instalar dependencias de Python.")
    parser.add_argument("--only-sql", action="store_true", help="Saltar conectores/gen_pipeline; solo aplicar SQL/wrappers.")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    ensure_deps(args.skip_install)

    # Variables ClickHouse (HTTP)
    ch_host = env_get("CLICKHOUSE_HTTP_HOST", "clickhouse")
    ch_port = env_get("CLICKHOUSE_HTTP_PORT", "8123")
    ch_db   = env_get("CLICKHOUSE_DATABASE", "fgeo_analytics")
    ch_user = os.environ.get("CH_USER", "etl")
    ch_pass = os.environ.get("CH_PASSWORD", "Et1Ingest!")

    if not args.only_sql:
        apply_connectors()
        generate_pipeline()

    logging.info("[CDC] Aplicando SQL a ClickHouse vía HTTP…")
    applied = apply_sql_files(ch_host, ch_port, ch_db, ch_user, ch_pass)

    if applied == 0:
        logging.info("[CDC] No hay .sql directos; ejecutando wrappers .sh con proxy HTTP…")
        applied = run_wrappers_with_proxy(ch_host, ch_port, ch_db, ch_user, ch_pass)

    logging.info("[CDC] SQL aplicados (contando .sql o .sh): %d", applied)
    logging.info("[CDC] Bootstrap OK")
    return 0

if __name__ == "__main__":
    sys.exit(main())
