#!/usr/bin/env python3
"""
Script de diagnóstico: consulta directa a ClickHouse usando SQLAlchemy y clickhouse-sqlalchemy
"""
from sqlalchemy import create_engine

# URI igual que en Superset
uri = "clickhousedb+connect://default:ClickHouse123!@clickhouse:8123/fgeo_analytics?protocol=http"
engine = create_engine(uri)

with engine.connect() as conn:
    result = conn.execute("SHOW TABLES FROM fgeo_analytics")
    print("Tablas en fgeo_analytics:")
    for row in result:
        print(row)
    result = conn.execute("SELECT * FROM fgeo_analytics.test_table LIMIT 5")
    print("\nDatos en test_table:")
    for row in result:
        print(row)
