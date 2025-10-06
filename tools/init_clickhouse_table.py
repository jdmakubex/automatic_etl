import os
import requests

CLICKHOUSE_HTTP = os.getenv("CLICKHOUSE_HTTP", "http://clickhouse:8123")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "fgeo_analytics")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "etl")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "Et1Ingest!")

TABLE_NAME = "test_table"

def verify_table_exists():
    url = f"{CLICKHOUSE_HTTP}/?database={CLICKHOUSE_DATABASE}"
    query = f"EXISTS TABLE {CLICKHOUSE_DATABASE}.{TABLE_NAME}"
    auth = (CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)
    response = requests.post(url, data=query, auth=auth)
    if response.status_code == 200:
        exists = response.text.strip() == '1'
        if exists:
            print(f"La tabla '{TABLE_NAME}' existe en la base de datos '{CLICKHOUSE_DATABASE}'.")
        else:
            print(f"La tabla '{TABLE_NAME}' NO existe en la base de datos '{CLICKHOUSE_DATABASE}'.")
            exit(1)
    else:
        print(f"Error verificando la existencia de la tabla: {response.text}")
        response.raise_for_status()

if __name__ == "__main__":
    verify_table_exists()
