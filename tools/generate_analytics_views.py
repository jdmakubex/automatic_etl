#!/usr/bin/env python3
"""
Dynamic analytics view generator for ClickHouse.

Goal
-----
Without modifying base tables or combining date+time fields, generate per-table
views under <db>_analytics that:
  - Keep all original columns as-is
  - Add safe date-only helper columns for date-like fields to avoid GROUP BY
    alias conflicts in BI tools (e.g., Superset) and to support DD-MM-YYYY
    legacy formats.

Rules
-----
1) Do NOT change source schema or data.
2) For each column:
   - If type is DateTime -> add `<col>_date` = toDate(col)
   - If type is Date     -> add `<col>_date` = col
   - If type is String and looks date-like by name (e.g., 'fecha', 'fecha_*', '*_fecha'):
       Try parseDateTimeBestEffortOrNull(col) -> toDate(), else parse DD-MM-YYYY by slicing
       as fallback: concat(substr(col,7,4), '-', substr(col,4,2), '-', substr(col,1,2))
       Final alias: `<col>_date` (Nullable(Date))
3) Do not derive any time-of-day columns; hours are left untouched.

Usage
-----
  python3 tools/generate_analytics_views.py \
    --databases fiscalizacion catalogosgral catlegajos \
    [--include-tables table1 table2 ...]

This script shells out to `clickhouse-client` via `docker exec clickhouse` to remain
environment-agnostic.
"""

import argparse
import json
import re
import shlex
import subprocess
import sys
from typing import List, Dict


CLICKHOUSE_CONTAINER = "clickhouse"


def run_ch(query: str) -> str:
    cmd = [
        "docker", "exec", CLICKHOUSE_CONTAINER, "clickhouse-client",
        "--query", query,
    ]
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        return out.decode("utf-8").strip()
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"ClickHouse query failed: {e.output.decode('utf-8', 'ignore')}")


def list_tables(database: str) -> List[str]:
    q = (
        "SELECT name FROM system.tables "
        f"WHERE database = '{database}' AND name NOT LIKE '%\\_v' ORDER BY name"
    )
    out = run_ch(q)
    return [line.strip() for line in out.splitlines() if line.strip()]


def get_columns(database: str, table: str) -> List[Dict[str, str]]:
    q = (
        "SELECT name, type FROM system.columns "
        f"WHERE database = '{database}' AND table = '{table}' ORDER BY position"
    )
    out = run_ch(q)
    cols = []
    for line in out.splitlines():
        parts = [p.strip() for p in line.split("\t")]
        if len(parts) >= 2:
            cols.append({"name": parts[0], "type": parts[1]})
    return cols


DATE_NAME_RE = re.compile(r"(^fecha$)|(^fecha_.*)|(.*_fecha$)", re.IGNORECASE)


def build_extra_date_expr(col_name: str, col_type: str) -> str:
    col_escaped = f"`{col_name}`"
    alias = f"`{col_name}_date`"
    t = col_type.lower()

    if t.startswith("datetime"):
        return f"toDate({col_escaped}) AS {alias}"
    if t == "date" or t.startswith("date32"):
        return f"{col_escaped} AS {alias}"
    if t.startswith("nullable(datetime"):
        return f"toDate({col_escaped}) AS {alias}"
    if t.startswith("nullable(date"):
        return f"{col_escaped} AS {alias}"

    # String-like: try best effort, fallback to DD-MM-YYYY slicing
    if t.startswith("string") or t.startswith("nullable(string"):
        # parseDateTimeBestEffortOrNull may or may not parse DD-MM-YYYY.
        # If null, fallback to manual rearrangement DD-MM-YYYY -> YYYY-MM-DD
        return (
            "multiIf(\n"
            f"  parseDateTimeBestEffortOrNull({col_escaped}) IS NOT NULL, toDate(parseDateTimeBestEffortOrNull({col_escaped})),\n"
            f"  length({col_escaped}) >= 10 AND substring({col_escaped}, 3, 1) = '-' AND substring({col_escaped}, 6, 1) = '-',\n"
            f"    toDateOrNull(concat(substring({col_escaped}, 7, 4), '-', substring({col_escaped}, 4, 2), '-', substring({col_escaped}, 1, 2))),\n"
            "  NULL\n"
            f") AS {alias}"
        )

    # Non-date/time-like types: no extra column
    return ""


def generate_view(database: str, table: str, include_all_date_strings: bool = False) -> str:
    cols = get_columns(database, table)

    base_cols = ", ".join(f"`{c['name']}`" for c in cols)

    extra_exprs: List[str] = []
    for c in cols:
        name = c["name"]
        ctype = c["type"]
        # Generate helper only if name suggests 'fecha' OR column is a (Date|DateTime)
        if DATE_NAME_RE.match(name) or ctype.lower().startswith(("date", "datetime", "nullable(date", "nullable(datetime")):
            expr = build_extra_date_expr(name, ctype)
            if expr:
                # Avoid duplicate alias if already exists in base
                if f"{name}_date" not in [x["name"] for x in cols]:
                    extra_exprs.append(expr)

    # If no extra expressions, just create a passthrough view
    select_list = base_cols if not extra_exprs else base_cols + ", " + ", ".join(extra_exprs)
    analytics_db = f"{database}_analytics"
    view_name = f"{table}_v"
    create_db = f"CREATE DATABASE IF NOT EXISTS `{analytics_db}`"
    create_view = (
        f"CREATE OR REPLACE VIEW `{analytics_db}`.`{view_name}` AS "
        f"SELECT {select_list} FROM `{database}`.`{table}`"
    )
    return create_db + "\n" + create_view


def main():
    p = argparse.ArgumentParser(description="Generate analytics views with safe date helpers")
    p.add_argument("--databases", nargs="+", required=True, help="Databases to process (exclude *_analytics automatically)")
    p.add_argument("--include-tables", nargs="*", default=None, help="Optional subset of tables to process")
    args = p.parse_args()

    dbs = [d for d in args.databases if not d.endswith("_analytics")]

    for db in dbs:
        try:
            tables = list_tables(db)
        except Exception as e:
            print(f"[WARN] Cannot list tables for {db}: {e}")
            continue

        if args.include_tables:
            inc = set(args.include_tables)
            tables = [t for t in tables if t in inc]

        for t in tables:
            try:
                sql = generate_view(db, t)
                # Run split due to ClickHouse single-statement restriction
                parts = [s.strip() for s in sql.split("\n") if s.strip()]
                for part in parts:
                    run_ch(part)
                print(f"[OK] View {db}_analytics.{t}_v created/updated")
            except Exception as e:
                print(f"[WARN] Skipped {db}.{t}: {e}")

    print("[DONE] Analytics views generation complete")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[FATAL] {e}")
        sys.exit(1)
