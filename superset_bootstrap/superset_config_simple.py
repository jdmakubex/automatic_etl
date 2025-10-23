#!/usr/bin/env python3
"""
Superset Configuration - Production with Async Queries
==================================
Configuración completa con soporte para consultas asíncronas vía Redis + Celery
"""

import os
from celery.schedules import crontab

# Basic Configuration
SECRET_KEY = "etl_prod_superset_secret_key_2024"
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

# ClickHouse Configuration
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_DATABASE = "fgeo_analytics"
CLICKHOUSE_USER = "superset"
CLICKHOUSE_PASSWORD = "Sup3rS3cret!"

# Redis Configuration
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

# Celery Configuration para Async Queries
class CeleryConfig:
    broker_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/0"
    imports = ("superset.sql_lab", "superset.tasks", "superset.tasks.scheduler")
    result_backend = f"redis://{REDIS_HOST}:{REDIS_PORT}/0"
    worker_prefetch_multiplier = 1
    task_acks_late = False
    task_annotations = {
        "sql_lab.get_sql_results": {
            "rate_limit": "100/s",
        },
    }
    beat_schedule = {
        "reports.scheduler": {
            "task": "reports.scheduler",
            "schedule": crontab(minute="*", hour="*"),
        },
        "reports.prune_log": {
            "task": "reports.prune_log",
            "schedule": crontab(minute=0, hour=0),
        },
    }

CELERY_CONFIG = CeleryConfig

# Results Backend para almacenar resultados de queries
RESULTS_BACKEND = {
    "backend": "redis",
    "redis_host": REDIS_HOST,
    "redis_port": REDIS_PORT,
    "redis_db": 1,
    "redis_connect_timeout": 5,
    "redis_read_timeout": 5,
}

# Redis Cache Configuration
CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_HOST': REDIS_HOST,
    'CACHE_REDIS_PORT': REDIS_PORT,
    'CACHE_REDIS_DB': 2,
}

DATA_CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 86400,  # 1 día
    'CACHE_KEY_PREFIX': 'superset_data_',
    'CACHE_REDIS_HOST': REDIS_HOST,
    'CACHE_REDIS_PORT': REDIS_PORT,
    'CACHE_REDIS_DB': 3,
}

# Feature Flags - Habilitar Async Queries
FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
    "GLOBAL_ASYNC_QUERIES": True,  # Habilitar consultas asíncronas globalmente
    "SQLLAB_BACKEND_PERSISTENCE": True,
}

# SQL Lab Configuration
SQLLAB_ASYNC_TIME_LIMIT_SEC = 300  # 5 minutos timeout para queries async
SQLLAB_TIMEOUT = 300  # 5 minutos timeout general
SUPERSET_WEBSERVER_TIMEOUT = 300

# Global Async Queries Configuration
GLOBAL_ASYNC_QUERIES_REDIS_CONFIG = {
    "port": REDIS_PORT,
    "host": REDIS_HOST,
    "db": 4,
    "ssl": False,
}
GLOBAL_ASYNC_QUERIES_REDIS_STREAM_PREFIX = "async-events-"
GLOBAL_ASYNC_QUERIES_REDIS_STREAM_LIMIT = 1000
GLOBAL_ASYNC_QUERIES_REDIS_STREAM_LIMIT_FIREHOSE = 1000000
GLOBAL_ASYNC_QUERIES_JWT_COOKIE_NAME = "async-token"
GLOBAL_ASYNC_QUERIES_JWT_COOKIE_SECURE = False
GLOBAL_ASYNC_QUERIES_JWT_SECRET = SECRET_KEY
GLOBAL_ASYNC_QUERIES_TRANSPORT = "polling"
GLOBAL_ASYNC_QUERIES_POLLING_DELAY = 500  # ms

# Disable problematic features
TALISMAN_ENABLED = False
WTF_CSRF_ENABLED = False

print("🔧 Superset: Configuración ASYNC habilitada (Redis + Celery)")
print(f"   - Redis: {REDIS_HOST}:{REDIS_PORT}")
print(f"   - Celery Broker: redis://{REDIS_HOST}:{REDIS_PORT}/0")
print(f"   - Results Backend: Redis DB 1")
print(f"   - GLOBAL_ASYNC_QUERIES: Enabled")