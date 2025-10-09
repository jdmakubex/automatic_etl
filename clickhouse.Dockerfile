FROM clickhouse/clickhouse-server:24.3

# Configurar usuario default con todos los permisos para inicialización
COPY bootstrap/users.xml /etc/clickhouse-server/users.d/init_users.xml

# Scripts de inicialización
COPY bootstrap/clickhouse_init.sql /docker-entrypoint-initdb.d/00_init.sql
COPY bootstrap/create_users.sql /docker-entrypoint-initdb.d/01_create_users.sql

# Configurar permisos
RUN chmod 644 /etc/clickhouse-server/users.d/init_users.xml
RUN chmod 644 /docker-entrypoint-initdb.d/*.sql