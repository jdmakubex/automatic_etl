# 📋 GUÍA DE CONFIGURACIÓN DEL PIPELINE ETL

## 🎯 Resumen
Este proyecto implementa un pipeline ETL completo que replica datos desde MySQL hacia ClickHouse usando Kafka/Debezium, con un dashboard en Apache Superset. **Toda la configuración está centralizada en el archivo `.env`** para garantizar consistencia y facilitar la replicación.

## ⚡ Inicio Rápido

### 1. Preparación del Entorno
```bash
# Clonar el repositorio
git clone [tu-repositorio]
cd etl_prod

# Verificar que Docker está instalado y funcionando
docker --version
docker compose version
```

### 2. Configuración
```bash
# El archivo .env ya está configurado con valores por defecto
# SOLO MODIFICA estas variables según tu entorno:

# En .env, cambiar estas líneas:
MYSQL_HOST=172.21.61.53          # ← Tu servidor MySQL
MYSQL_USER=juan.marcos           # ← Tu usuario MySQL  
MYSQL_PASSWORD=123456            # ← Tu contraseña MySQL
MYSQL_DATABASE=archivos          # ← Tu base de datos MySQL
```

### 3. Validación y Ejecución
```bash
# Validar configuración
python3 tools/validate_config.py

# Iniciar pipeline completo (automático)
./tools/start_pipeline.sh --clean

# O de forma manual:
docker compose down && docker compose up -d
```

### 4. Verificación
```bash
# Verificar estado
docker compose ps
docker compose exec etl-tools python3 tools/pipeline_status.py

# Ver logs
docker compose logs -f
```

## 🔧 Configuración Avanzada

### Variables Críticas en `.env`

#### MySQL (Base de Datos Origen)
```bash
MYSQL_HOST=172.21.61.53                    # IP del servidor MySQL
MYSQL_PORT=3306                            # Puerto MySQL
MYSQL_USER=juan.marcos                     # Usuario con permisos de lectura
MYSQL_PASSWORD=123456                      # Contraseña del usuario
MYSQL_DATABASE=archivos                    # Base de datos a replicar
```

#### ClickHouse (Base de Datos Destino)
```bash
CLICKHOUSE_HOST=clickhouse                 # Siempre "clickhouse" (servicio Docker)
CLICKHOUSE_HTTP_PORT=8123                  # Puerto HTTP (default 8123)
CLICKHOUSE_DATABASE=fgeo_analytics         # BD destino en ClickHouse
CH_USER=etl                               # Usuario ETL
CH_PASSWORD=Et1Ingest!                    # Contraseña usuario ETL
```

#### Kafka/Debezium (Streaming)
```bash
KAFKA_BROKERS=kafka:9092                  # Siempre "kafka:9092" (interno)
CONNECT_URL=http://connect:8083           # URL de Kafka Connect
DBZ_SERVER_NAME_PREFIX=dbserver           # Prefijo para tópicos
DBZ_SNAPSHOT_MODE=initial                 # Modo snapshot inicial
```

Nota importante sobre credenciales Debezium:
- Las variables DBZ_DATABASE_HOSTNAME/PORT/USER/PASSWORD están DEPRECADAS y se ignoran.
- Las credenciales y bases de datos de origen se toman exclusivamente de DB_CONNECTIONS (JSON en .env).
- Mantén una sola fuente de verdad para evitar inconsistencias.

#### Superset (Dashboard)
```bash
SUPERSET_URL=http://superset:8088         # URL de Superset
SUPERSET_ADMIN=admin                      # Usuario administrador
SUPERSET_PASSWORD=Admin123!               # Contraseña admin
```

## 🚀 Servicios Disponibles

Después del despliegue exitoso:

| Servicio | URL | Credenciales |
|----------|-----|--------------|
| **Superset** | http://localhost:8088 | admin / Admin123! |
| **ClickHouse** | http://localhost:8123 | etl / Et1Ingest! |
| **Kafka Connect** | http://localhost:8083 | - |

## 📊 Arquitectura

```
MySQL (Origen) 
    ↓ CDC
Debezium/Kafka 
    ↓ Stream
ClickHouse (Destino)
    ↓ Analytics
Superset (Dashboard)
```

## 🔍 Verificación del Pipeline

### Comandos de Diagnóstico
```bash
# Estado general
docker compose ps

# Verificar bases de datos
docker compose exec clickhouse clickhouse-client --query "SHOW DATABASES"
docker compose exec clickhouse clickhouse-client --query "SHOW TABLES FROM fgeo_analytics"

# Verificar conectores
docker compose exec connect curl -s http://connect:8083/connectors

# Verificar tópicos Kafka
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list

# Pipeline status completo
docker compose exec etl-tools python3 tools/pipeline_status.py
```

### Logs de Servicios
```bash
# Todos los servicios
docker compose logs -f

# Servicio específico
docker compose logs -f clickhouse
docker compose logs -f connect
docker compose logs -f superset
```

## 🛠️ Troubleshooting

### Problema: Servicios no inician
```bash
# Limpiar y reiniciar
docker compose down -v
docker system prune -f
./tools/start_pipeline.sh --clean
```

### Problema: Conectores fallan
```bash
# Verificar conectividad MySQL
docker compose exec etl-tools python3 -c "
import pymysql
conn = pymysql.connect(host='172.21.61.53', port=3306, user='juan.marcos', password='123456', db='archivos')
print('✅ MySQL conectado')
conn.close()
"

# Reaplicar conectores
docker compose exec etl-tools python3 tools/apply_connectors_auto.py
```

### Problema: No hay datos en ClickHouse
```bash
# Verificar que Debezium está replicando
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list | grep dbserver

# Verificar mensajes en tópicos
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic dbserver_default.archivos.archivos --from-beginning --max-messages 5
```

## � Nota sobre CDC Bootstrap y permisos

Para evitar errores de permisos durante el paso de CDC (instalación de dependencias y generación/aplicación de artefactos):

- El servicio `cdc-bootstrap` corre como `root` y define `HOME=/root` para que `pip` pueda instalar paquetes en contenedores base slim.
- Se monta el directorio `./generated` en `/app/generated` para garantizar escritura de artefactos (conectores y SQLs) desde el bootstrap.
- Variables relevantes en `docker-compose.yml` dentro de `cdc-bootstrap`:
    - `user: "0:0"`
    - `environment: [HOME=/root, PIP_ROOT_USER_ACTION=ignore]`
    - `volumes: [./generated:/app/generated]`

Con esto, el paso 5/5 (Bootstrap CDC) se ejecuta de forma idempotente y sin errores de permisos.

## �📁 Estructura del Proyecto

```
etl_prod/
├── .env                     # ← CONFIGURACIÓN PRINCIPAL
├── docker-compose.yml      # Definición de servicios
├── tools/                  # Scripts de automatización
│   ├── start_pipeline.sh   # ← Script principal de inicio
│   ├── validate_config.py  # Validador de configuración
│   ├── gen_pipeline.py     # Generador de configuraciones
│   └── ...
├── generated/              # Configuraciones generadas
└── logs/                   # Logs del sistema
```

## 🔒 Variables de Seguridad

**IMPORTANTE**: Para producción, cambiar estas variables:

```bash
# En .env
SUPERSET_SECRET_KEY=TuClaveSecretaSuperSegura123456789
CLICKHOUSE_ETL_PASSWORD=TuPasswordSeguroClickHouse123
SUPERSET_PASSWORD=TuPasswordSeguroSuperset123
```

## 🤝 Para el Equipo

### Al clonar el proyecto:
1. Copia el archivo `.env` y ajusta **solo** las variables MySQL según tu entorno
2. Ejecuta `python3 tools/validate_config.py` para verificar configuración
3. Ejecuta `./tools/start_pipeline.sh --clean` para desplegar
4. Verifica con `docker compose ps` que todos los servicios están healthy

### No modificar directamente:
- `docker-compose.yml` (usa variables de `.env`)
- Scripts en `tools/` (usan configuración centralizada)
- Archivos en `generated/` (se regeneran automáticamente)

### Si hay problemas:
1. Ejecutar `docker compose logs -f [servicio]` para ver logs
2. Ejecutar `python3 tools/validate_config.py` para verificar configuración
3. Ejecutar `./tools/start_pipeline.sh --clean` para reinicio limpio

## 📞 Soporte

Para problemas:
1. Verificar logs: `docker compose logs -f`
2. Validar configuración: `python3 tools/validate_config.py`
3. Estado del pipeline: `docker compose exec etl-tools python3 tools/pipeline_status.py`

---
**✅ Con esta configuración centralizada, el proyecto debería funcionar consistentemente en cualquier entorno con mínimos ajustes.**