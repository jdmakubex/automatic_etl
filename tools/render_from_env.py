#!/usr/bin/env python3
import os, json, pathlib, re, sys

OUT_DIR = pathlib.Path(".generated"); OUT_DIR.mkdir(exist_ok=True)

def slug(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "db"

def env(name, default=""):
    v = os.environ.get(name, default)
    return v.strip() if isinstance(v, str) else v

def main():
    # Lee JSON desde DB_CONNECTIONS (.env)
    raw = env("DB_CONNECTIONS", "[]")
    try:
        conns = json.loads(raw)
        assert isinstance(conns, list)
    except Exception as e:
        print(f"[FATAL] DB_CONNECTIONS inválido: {e}", file=sys.stderr)
        sys.exit(2)

    # Parámetros de ClickHouse
    ch_user = env("CH_USER", "superset")
    ch_pass = env("CH_PASSWORD", "Sup3rS3cret!")
    ch_http_host = env("CLICKHOUSE_HTTP_HOST", "clickhouse")
    ch_http_port = env("CLICKHOUSE_HTTP_PORT", "8123")

    # Usuario de Superset (DB de metadatos NO se toca aquí)
    # Vamos a crear una Database en Superset por cada conexión MySQL,
    # apuntando a un esquema (db) distinto en ClickHouse.

    # 1) Genera SQL para ClickHouse (create db + grants por cada fuente)
    sql_lines = []
    sql_lines.append(f"CREATE USER IF NOT EXISTS {ch_user} IDENTIFIED WITH plaintext_password BY '{ch_pass}';")
    seen_dbs = set()

    for c in conns:
        # nombre/tipo/origen
        name = str(c.get("name") or c.get("db") or "src").strip()
        src_db = str(c.get("db") or name).strip()
        # CH db derivada: fgeo_{src_db}
        ch_db = slug(f"fgeo_{src_db}")
        if ch_db in seen_dbs:
            continue
        seen_dbs.add(ch_db)
        sql_lines.append(f"CREATE DATABASE IF NOT EXISTS {ch_db};")
        sql_lines.append(f"GRANT SELECT, INSERT, CREATE, ALTER ON {ch_db}.* TO {ch_user};")

    (OUT_DIR / "clickhouse_init.sql").write_text("\n".join(sql_lines) + "\n", encoding="utf-8")

    # 2) Script de superset para crear/actualizar conexiones
    sh = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        'export PATH="/app/.venv/bin:${PATH}"',
        "",
    ]
    for c in conns:
        name = str(c.get("name") or c.get("db") or "src").strip()
        src_db = str(c.get("db") or name).strip()
        ch_db = slug(f"fgeo_{src_db}")
        superset_name = f"CH {ch_db}"  # nombre visible en Superset
        uri = f"clickhousedb+connect://{ch_user}:{ch_pass}@{ch_http_host}:{ch_http_port}/{ch_db}"
        sh.append(f'echo ">> Configurando {superset_name}"')
        sh.append(
            f'superset set-database-uri --database-name "{superset_name}" --uri "{uri}" --extra "{{}}" --allow-csv-upload false || true'
        )
    (OUT_DIR / "superset_create_dbs.sh").write_text("\n".join(sh) + "\n", encoding="utf-8")
    os.chmod(OUT_DIR / "superset_create_dbs.sh", 0o755)

    print("Generated: .generated/clickhouse_init.sql, .generated/superset_create_dbs.sh")

if __name__ == "__main__":
    main()
