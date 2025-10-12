FROM apache/superset:3.1.0

USER root

# Actualizar sistema y instalar herramientas esenciales
RUN apt-get update && apt-get install -y \
    # Herramientas de red y comunicación
    iputils-ping \
    curl \
    wget \
    netcat-openbsd \
    telnet \
    dnsutils \
    # Herramientas HTTPS y certificados
    ca-certificates \
    openssl \
    ssl-cert \
    # Python y desarrollo
    python3-dev \
    python3-pip \
    build-essential \
    # Herramientas de sistema
    procps \
    htop \
    nano \
    vim \
    less \
    # Herramientas de red avanzadas
    net-tools \
    iproute2 \
    tcpdump \
    # Limpieza
    && rm -rf /var/lib/apt/lists/*

# Actualizar certificados SSL/TLS
RUN update-ca-certificates

# Instalar dependencias Python mejoradas
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Instalar conectores y herramientas específicas
RUN pip install --no-cache-dir \
    # ClickHouse connectivity
    clickhouse-connect==0.7.19 \
    clickhouse-sqlalchemy==0.2.6 \
    # Base de datos adicionales
    psycopg2-binary \
    pymysql \
    # Herramientas de red Python
    requests[security] \
    urllib3[secure] \
    certifi \
    # Utilidades
    pyyaml \
    python-dotenv \
    # Seguridad y HTTPS (versión compatible con Superset)
    "cryptography>=41.0.2,<41.1.0" \
    pyOpenSSL

# Configurar variables de entorno para HTTPS y comunicación
ENV PYTHONHTTPSVERIFY=1
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
ENV SSL_CERT_DIR=/etc/ssl/certs
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

# Crear directorios necesarios
RUN mkdir -p /bootstrap /app/superset_home /tmp/superset

# Copia el archivo ZIP de configuración de ClickHouse y configuración de Superset
COPY superset_bootstrap/clickhouse_db.zip /bootstrap/clickhouse_db.zip
COPY superset_bootstrap/clickhouse_db.zip /app/superset_home/clickhouse_db.zip
COPY superset_bootstrap/superset_config.py /app/superset_home/superset_config.py
COPY superset_bootstrap/superset_config_simple.py /bootstrap/superset_config_simple.py

# Configurar permisos
RUN chown -R superset:superset /app/superset_home /bootstrap
RUN chmod +r /bootstrap/clickhouse_db.zip /app/superset_home/clickhouse_db.zip /app/superset_home/superset_config.py

USER superset

# Configurar variables de entorno para el usuario superset
ENV PYTHONPATH=/app/superset_home:$PYTHONPATH
# Usar configuración simplificada que funciona
ENV SUPERSET_CONFIG_PATH=/bootstrap/superset_config_simple.py