FROM apache/superset:latest

# Instala paquetes como root en el Python global de la imagen
USER root
RUN python -m pip install --no-cache-dir \
    clickhouse-connect==0.7.19 \
    clickhouse-sqlalchemy==0.2.6
# Regresa al usuario por defecto de la imagen
USER superset
