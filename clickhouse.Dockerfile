
FROM clickhouse/clickhouse-server:24.3
# Instala curl para permitir la configuración robusta
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# NOTA: No copiamos un users.xml estático para evitar desincronización de credenciales.
# Las credenciales se renderizan dinámicamente desde variables de entorno en tiempo de arranque
# mediante /docker-entrypoint-initdb.d/02_setup_users.sh (montado por docker-compose).

# Scripts de inicialización (solo objetos; usuarios vía XML dinámico)
COPY bootstrap/clickhouse_init.sql /docker-entrypoint-initdb.d/00_init.sql
COPY bootstrap/create_users.sql /docker-entrypoint-initdb.d/01_create_users.sql
COPY bootstrap/setup_users_automatically.sh /docker-entrypoint-initdb.d/02_setup_users.sh

# Permisos de los scripts de inicialización
RUN chmod 644 /docker-entrypoint-initdb.d/*.sql && chmod +x /docker-entrypoint-initdb.d/*.sh