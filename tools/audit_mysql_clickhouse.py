#!/usr/bin/env python3
"""
Auditoría de tablas y registros entre MySQL y ClickHouse
"""
import pandas as pd
from sqlalchemy import create_engine
import clickhouse_connect
import pymysql
import os
from urllib.parse import urlparse

MYSQL_URL = os.getenv('MYSQL_URL', 'mysql+pymysql://juan.marcos:123456@172.21.61.53:3306/archivos')
CH_HOST = os.getenv('CH_HOST', 'clickhouse')
CH_PORT = int(os.getenv('CH_PORT', '8123'))
CH_USER = os.getenv('CH_USER', 'etl')
CH_PASS = os.getenv('CH_PASSWORD', 'Et1Ingest!')
CH_DB = os.getenv('CH_DB', 'fgeo_analytics')

# 1. Conectar a MySQL usando PyMySQL directamente
parsed = urlparse(MYSQL_URL)
mysql_conn = pymysql.connect(
    host=parsed.hostname,
    port=parsed.port or 3306,
    user=parsed.username,
    password=parsed.password,
    database=parsed.path.lstrip('/'),
    charset='utf8mb4'
)

# Listar tablas en MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("SHOW TABLES")
    mysql_tables = [row[0] for row in cursor.fetchall()]

# 2. Conectar a ClickHouse
ch_client = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS)

print(f"\n=== AUDITORÍA MySQL → ClickHouse ===")
print(f"{'Tabla':40} | {'MySQL':>10} | {'ClickHouse':>12} | {'Diferencia':>12}")
print("-"*80)

total_mysql = 0
total_ch = 0
errores = []
for table in mysql_tables:
    # Contar registros en MySQL
    try:
        with mysql_conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
            mysql_count = cursor.fetchone()[0]
    except Exception as e:
        mysql_count = 0
        errores.append(f"MySQL {table}: {e}")
    # Contar registros en ClickHouse
    ch_table = f"{CH_DB}.src__default__archivos__{table}"
    try:
        ch_count = ch_client.query(f"SELECT COUNT(*) FROM {ch_table}").result_rows[0][0]
    except Exception as e:
        ch_count = 0
        errores.append(f"ClickHouse {table}: {e}")
    
    diff = ch_count - mysql_count if isinstance(mysql_count, int) and isinstance(ch_count, int) else "N/A"
    print(f"{table:40} | {mysql_count:>10} | {ch_count:>12} | {diff:>12}")
    
    if isinstance(mysql_count, int):
        total_mysql += mysql_count
    if isinstance(ch_count, int):
        total_ch += ch_count

print("-"*80)
total_diff = total_ch - total_mysql
print(f"{'TOTAL':40} | {total_mysql:>10} | {total_ch:>12} | {total_diff:>12}")

if errores:
    print(f"\n⚠️  Errores encontrados:")
    for error in errores:
        print(f"  - {error}")

if total_diff == 0:
    print(f"\n✅ Auditoría exitosa: {total_mysql} registros migrados correctamente")
else:
    print(f"\n⚠️  Diferencia: {total_diff} registros (ClickHouse tiene {abs(total_diff)} {'más' if total_diff > 0 else 'menos'})")

mysql_conn.close()
