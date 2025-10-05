FROM apache/superset:3.1.0

USER root
RUN pip install --no-cache-dir \
    clickhouse-connect==0.7.19 \
    clickhouse-sqlalchemy==0.2.6

USER superset