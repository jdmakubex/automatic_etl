#!/usr/bin/env python3
"""
Superset Configuration for ETL Pipeline
=====================================
Configuración de Superset optimizada para ClickHouse y funcionalidad completa
"""

import os
from datetime import timedelta

# ----------------------------------------------------------------------
# Basic Configuration
# ----------------------------------------------------------------------
SUPERSET_WEBSERVER_PORT = 8088
SUPERSET_WEBSERVER_TIMEOUT = 300

# Security
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "etl_prod_superset_secret_key_2024")
WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = timedelta(hours=1)

# Database Configuration
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SUPERSET_DATABASE_URI", 
    "sqlite:////app/superset_home/superset.db"
)

# Redis for caching (if available)
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
CACHE_CONFIG = {
    'CACHE_TYPE': 'NullCache',  # Start with simple config
}

# ----------------------------------------------------------------------
# ClickHouse Integration
# ----------------------------------------------------------------------
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "fgeo_analytics")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "superset")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "Sup3rS3cret!")

# ClickHouse connection string
CLICKHOUSE_CONNECTION_STRING = (
    f"clickhouse+http://{CLICKHOUSE_USER}:{CLICKHOUSE_PASSWORD}@"
    f"{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DATABASE}"
)

# ----------------------------------------------------------------------
# Feature Flags & Capabilities
# ----------------------------------------------------------------------
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "GLOBAL_ASYNC_QUERIES": True,
    "VERSIONED_EXPORT": True,
    "ALERTS_ATTACH_REPORTS": True,
    "ALERT_REPORTS": True,
    "THUMBNAILS": True,
    "DASHBOARD_FILTERS_EXPERIMENTAL": True,
    "DRILL_TO_DETAIL": True,
    "DRILL_BY": True,
    "HORIZONTAL_FILTER_BAR": True,
}

# ----------------------------------------------------------------------
# Data Security & Permissions
# ----------------------------------------------------------------------
# Row Level Security
ENABLE_ROW_LEVEL_SECURITY = True

# Public role permissions
PUBLIC_ROLE_LIKE_GAMMA = True

# Admin permissions
SUPERSET_ROLES = {
    "Admin": [
        "can_read", "can_write", "can_delete",
        "all_datasource_access", "all_database_access"
    ]
}

# ----------------------------------------------------------------------
# Upload & File Handling
# ----------------------------------------------------------------------
UPLOAD_FOLDER = "/app/superset_home/uploads/"
IMG_UPLOAD_FOLDER = "/app/superset_home/static/uploads/"
IMG_UPLOAD_URL = "/static/uploads/"
UPLOAD_CHUNK_SIZE = 4096

# ----------------------------------------------------------------------
# Query & Performance
# ----------------------------------------------------------------------
# Query timeout settings
SUPERSET_WEBSERVER_TIMEOUT = 300
SQL_MAX_ROW = 100000
SQL_LAB_DEFAULT_SCHEMA = CLICKHOUSE_DATABASE

# Query optimizations for ClickHouse
SQLLAB_TIMEOUT = 300
SUPERSET_WEBSERVER_TIMEOUT = 300

# ----------------------------------------------------------------------
# Email & Alerts (Optional)
# ----------------------------------------------------------------------
SMTP_HOST = os.environ.get("SMTP_HOST", "localhost")
SMTP_STARTTLS = True
SMTP_SSL = False
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
SMTP_MAIL_FROM = os.environ.get("SMTP_MAIL_FROM", "superset@etl-prod")

EMAIL_NOTIFICATIONS = bool(os.environ.get("EMAIL_NOTIFICATIONS", False))

# ----------------------------------------------------------------------
# UI/UX Customization
# ----------------------------------------------------------------------
APP_NAME = "ETL Analytics Dashboard"
APP_ICON = "/static/assets/images/superset-logo-horiz.png"

# Branding
FAVICONS = [
    {"href": "/static/assets/images/favicon.png"},
]

# ----------------------------------------------------------------------
# Advanced Configuration
# ----------------------------------------------------------------------
# Async queries
GLOBAL_ASYNC_QUERIES_TRANSPORT = "polling"
GLOBAL_ASYNC_QUERIES_POLLING_DELAY = int(os.environ.get("POLLING_DELAY", 500))

# CORS settings
ENABLE_CORS = True
CORS_OPTIONS = {
    "supports_credentials": True,
    "allow_headers": ["*"],
    "resources": ["*"],
    "origins": ["*"]
}

# Webpack dev server
WEBPACK_DEV_SERVER_HOST = "0.0.0.0"

# ----------------------------------------------------------------------
# Database Specific Settings
# ----------------------------------------------------------------------
# For ClickHouse optimizations
DATABASE_EXECUTORS = {
    CLICKHOUSE_CONNECTION_STRING: {
        "engine": "clickhouse",
        "params": {
            "pool_size": 20,
            "max_overflow": 0,
            "pool_pre_ping": True,
            "connect_args": {
                "connect_timeout": 30,
                "send_receive_timeout": 300,
            }
        }
    }
}

# Custom SQL Lab tab names
SQL_LAB_DEFAULT_TAB_NAME = "ETL Query"

# ----------------------------------------------------------------------
# Logging - Simplified
# ----------------------------------------------------------------------
# Use default Superset logging configuration
ENABLE_PROXY_FIX = True

# ----------------------------------------------------------------------
# Security Headers
# ----------------------------------------------------------------------
TALISMAN_ENABLED = True
TALISMAN_CONFIG = {
    "force_https": False,  # Set to True in production with HTTPS
    "content_security_policy": None,
}

# ----------------------------------------------------------------------
# Development/Debug (disable in production)
# ----------------------------------------------------------------------
DEBUG = os.environ.get("SUPERSET_DEBUG", "False").lower() == "true"
TESTING = False

# Flask-WTF flag
WTF_CSRF_EXEMPT_LIST = ["superset.connectors.base.views.BaseConnectionView"]

print("🔧 Superset configurado para ETL Production Pipeline")
print(f"📊 ClickHouse: {CLICKHOUSE_CONNECTION_STRING}")
print(f"🔍 Debug Mode: {DEBUG}")
print(f"🚀 Ready for analytics!")