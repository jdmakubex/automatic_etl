

import os
import json
import sys
from clickhouse_driver import Client

def log_write(msg, log_file=None):
    print(msg)
    if log_file:
        try:
            log_file.write(msg + '\n')
            log_file.flush()
        except Exception:
            pass

def main():
    db_connections = os.environ.get('DB_CONNECTIONS')
    clickhouse_host = os.environ.get('CLICKHOUSE_HTTP_HOST', 'clickhouse')
    clickhouse_port = int(os.environ.get('CLICKHOUSE_NATIVE_PORT', '9000'))
    clickhouse_user = os.environ.get('CH_USER', 'etl')
    clickhouse_password = os.environ.get('CH_PASSWORD', 'Et1Ingest!')
    clickhouse_db = os.environ.get('CH_DB', 'fgeo_analytics')

    log_file = None
    log_path = '/app/logs/clickhouse_setup.log'
    try:
        os.makedirs('/app/logs', exist_ok=True)
        log_file = open(log_path, 'w')
    except Exception:
        log_file = None

    if not db_connections:
        log_write('❌ DB_CONNECTIONS no está definida', log_file)
        sys.exit(1)
    try:
        # Limpiar comillas simples externas si existen
        if db_connections.startswith("'") and db_connections.endswith("'"):
            db_connections = db_connections[1:-1]
        connections = json.loads(db_connections)
    except Exception as e:
        log_write(f'❌ Error parseando DB_CONNECTIONS: {e}', log_file)
        sys.exit(2)
    log_write(f'✅ Conexiones parseadas: {len(connections)}', log_file)
    for conn in connections:
        log_write(f"  📌 {conn['name']}: {conn['type']}://{conn['host']}:{conn['port']}/{conn['db']}", log_file)

    # Conectar a ClickHouse
    client = Client(host=clickhouse_host, port=clickhouse_port, user=clickhouse_user, password=clickhouse_password, database=clickhouse_db)

    # Verificar base de datos
    try:
        client.execute(f"CREATE DATABASE IF NOT EXISTS {clickhouse_db}")
        log_write(f"✅ Base de datos {clickhouse_db} verificada/creada", log_file)
    except Exception as e:
        log_write(f"❌ Error creando/verificando base de datos: {e}", log_file)
        sys.exit(3)

    # Crear esquemas de metadatos para cada conexión
    for conn in connections:
        metadata_table = f"connection_{conn['name']}_metadata"
        try:
            client.execute(f"""
                CREATE TABLE IF NOT EXISTS {clickhouse_db}.{metadata_table} (
                    connection_name String,
                    source_type String,
                    source_host String,
                    source_database String,
                    created_at DateTime DEFAULT now(),
                    last_updated DateTime DEFAULT now()
                ) ENGINE = MergeTree()
                ORDER BY (connection_name, created_at)
            """)
            client.execute(f"""
                INSERT INTO {clickhouse_db}.{metadata_table} (connection_name, source_type, source_host, source_database)
                VALUES (%(name)s, %(type)s, %(host)s, %(db)s)
            """, conn)
            log_write(f"  ✅ Esquema y metadatos creados para conexión: {conn['name']}", log_file)
        except Exception as e:
            log_write(f"❌ Error creando esquema/metadatos para {conn['name']}: {e}", log_file)
            sys.exit(4)

    log_write("🎉 CONFIGURACIÓN DE CLICKHOUSE COMPLETADA", log_file)
    if log_file:
        log_file.close()

if __name__ == '__main__':
    main()
