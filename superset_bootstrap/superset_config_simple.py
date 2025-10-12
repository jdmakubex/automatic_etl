#!/usr/bin/env python3
"""
Superset Configuration - SIMPLIFIED
==================================
Configuración mínima para evitar errores de logging
"""

import os

# Basic Configuration
SECRET_KEY = "etl_prod_superset_secret_key_2024"
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

# ClickHouse Configuration
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_DATABASE = "fgeo_analytics"
CLICKHOUSE_USER = "superset"
CLICKHOUSE_PASSWORD = "Sup3rS3cret!"

# Basic Feature Flags
FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
}

# Disable problematic features
TALISMAN_ENABLED = False
WTF_CSRF_ENABLED = False

# Simple Cache
CACHE_CONFIG = {
    'CACHE_TYPE': 'SimpleCache',
    'CACHE_DEFAULT_TIMEOUT': 300
}

print("🔧 Superset: Configuración simplificada cargada")