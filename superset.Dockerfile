FROM apache/superset:3.1.0

USER root
# Instala herramientas de red y utilidades para robustez y comunicación
RUN apt-get update && apt-get install -y iputils-ping curl netcat-openbsd
RUN pip install --no-cache-dir \
    clickhouse-connect==0.7.19 \
    clickhouse-sqlalchemy==0.2.6

# Copia el archivo ZIP de configuración de ClickHouse
COPY superset_bootstrap/clickhouse_db.zip /bootstrap/clickhouse_db.zip
COPY superset_bootstrap/clickhouse_db.zip /app/superset_home/clickhouse_db.zip

USER superset