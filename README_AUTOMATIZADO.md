# 🚀 PIPELINE ETL AUTOMÁTICO - MySQL → Kafka → ClickHouse

## 🎯 DESCRIPCIÓN

Pipeline ETL completamente automatizado que replica datos desde MySQL hacia ClickHouse usando Kafka/Debezium con **inicialización automática desde cero**.

### ✨ CARACTERÍSTICAS PRINCIPALES

- **🔄 Completamente Automático**: Un solo comando inicia todo el pipeline
- **🧹 Auto-limpieza**: Elimina configuraciones previas para evitar conflictos  
- **🔍 Auto-descubrimiento**: Detecta automáticamente tablas MySQL
- **👥 Auto-configuración**: Crea usuarios y permisos necesarios
- **🏗️ Auto-optimización**: Genera esquemas ClickHouse optimizados
- **📊 Monitoreo completo**: Logs detallados de cada paso
- **✅ Validación automática**: Verifica flujo completo de datos

## 🚀 INICIO RÁPIDO

### Prerequisitos

- Docker y Docker Compose
- Acceso a base de datos MySQL origen
- Puertos 8088, 8123, 8083, 19092 disponibles

### 1. Configuración Inicial

```bash
# Clonar el repositorio
git clone <repo-url>
cd etl_prod

# Configurar variables de entorno
cp .env.example .env
# Editar .env con credenciales MySQL
```

### 2. Iniciar Pipeline Automático

```bash
# Inicio completo automático (RECOMENDADO)
./start_etl_pipeline.sh

# Inicio con limpieza forzada
./start_etl_pipeline.sh --clean

# Inicio manual (solo servicios, sin orquestación)  
./start_etl_pipeline.sh --manual

# Ver ayuda completa
./start_etl_pipeline.sh --help
```

### 3. Verificar Estado

```bash
# Verificar estado del pipeline
python3 tools/pipeline_status.py

# Ver logs detallados
tail -f logs/orchestrator.log
```

## 🏗️ ARQUITECTURA

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│    MySQL    │───▶│    Kafka    │───▶│ ClickHouse  │───▶│  Superset   │
│   (Origen)  │    │ (Streaming) │    │ (Analítica) │    │(Visualiza.) │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                           │
                   ┌───────────────┐
                   │ Debezium CDC  │
                   │ (Conectores)  │
                   └───────────────┘
```

### Servicios Incluidos

- **ClickHouse**: Base de datos analítica (puerto 8123)
- **Kafka**: Streaming de datos con KRaft
- **Kafka Connect**: Conectores Debezium para CDC
- **Superset**: Visualización y dashboards (puerto 8088)
- **Orquestador**: Inicialización automática

## 🎛️ PROCESO DE AUTOMATIZACIÓN

El orquestador ejecuta automáticamente estas **6 fases**:

### 🧹 FASE 1: LIMPIEZA COMPLETA
- Elimina conectores Kafka Connect existentes
- Limpia tópicos de Kafka relacionados
- Borra tablas y datos de ClickHouse
- Remueve archivos de configuración previos

### 👥 FASE 2: CONFIGURACIÓN DE USUARIOS  
- Crea usuario MySQL con permisos de replicación
- Configura usuario ClickHouse con permisos de escritura
- Valida conectividad a ambas bases de datos
- Guarda credenciales para uso posterior

### 🔍 FASE 3: DESCUBRIMIENTO DE ESQUEMAS
- Se conecta a MySQL y detecta todas las tablas
- Analiza estructura de columnas y tipos de datos
- Genera configuraciones de conectores Debezium
- Crea metadatos para creación de esquemas

### 🏗️ FASE 4: CREACIÓN DE MODELOS
- Genera esquemas ClickHouse optimizados
- Mapea tipos MySQL → ClickHouse automáticamente
- Configura engines apropiados (MergeTree, etc.)
- Crea tablas con particionado y ordenamiento óptimo

### 🔌 FASE 5: DESPLIEGUE DE CONECTORES
- Aplica conectores Debezium automáticamente
- Configura CDC (Change Data Capture)
- Inicia flujo de datos MySQL → Kafka
- Verifica estado de conectores

### ✅ FASE 6: VALIDACIÓN COMPLETA
- Confirma tópicos de Kafka con datos
- Verifica datos en ClickHouse
- Valida flujo completo de extremo a extremo
- Genera reporte de estado final

## 📋 CONFIGURACIÓN

### Variables de Entorno Principales

```bash
# MySQL Origen
MYSQL_HOST=host.docker.internal
MYSQL_PORT=3306
MYSQL_ADMIN_USER=root
MYSQL_ADMIN_PASSWORD=admin
MYSQL_DATABASE=archivos

# ClickHouse Destino  
CLICKHOUSE_DATABASE=fgeo_analytics

# Superset
SUPERSET_ADMIN=admin
SUPERSET_PASSWORD=Admin123!
```

### Archivos de Configuración Generados

Después de la inicialización automática:

```
generated/default/
├── mysql_connector_auto.json      # Configuración conector MySQL
├── .env_auto                       # Credenciales generadas
├── tables_metadata.json           # Metadatos de tablas
├── clickhouse_validation.json     # Resultados de validación
└── schemas/
    ├── tabla1_clickhouse.sql      # Schema ClickHouse tabla 1
    ├── tabla2_clickhouse.sql      # Schema ClickHouse tabla 2
    └── ...
```

## 🔍 MONITOREO Y LOGS

### Logs Principales

```bash
logs/
├── orchestrator.log               # Log principal del orquestador
├── cleanup.log                   # Log de limpieza
├── discovery.log                 # Log de descubrimiento MySQL
├── users_setup.log               # Log de configuración usuarios
├── clickhouse_setup.log          # Log de creación modelos
└── orchestrator_execution_*.json # Logs de ejecución detallados
```

### Comandos de Monitoreo

```bash
# Estado general del pipeline
python3 tools/pipeline_status.py

# Verificar conectores
curl http://localhost:8083/connectors

# Ver tópicos de Kafka
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list

# Consultar datos en ClickHouse
docker compose exec clickhouse clickhouse-client --database fgeo_analytics
```

## 🛠️ HERRAMIENTAS ADICIONALES

### Scripts Individuales (para uso manual)

```bash
# Limpiar todo desde cero
python3 tools/cleanup_all.py

# Descubrir tablas MySQL
python3 tools/discover_mysql_tables.py

# Configurar usuarios y permisos
python3 tools/setup_database_users.py

# Crear modelos ClickHouse
python3 tools/create_clickhouse_models.py

# Aplicar conectores
python3 tools/apply_connectors_auto.py

# Validar pipeline completo
python3 tools/validate_etl_complete.py
```

### Gestión de Servicios

```bash
# Ver estado de servicios
docker compose ps

# Ver logs de un servicio específico
docker compose logs -f clickhouse
docker compose logs -f kafka
docker compose logs -f connect

# Reiniciar un servicio
docker compose restart clickhouse

# Detener todo
docker compose down

# Limpiar volúmenes (¡CUIDADO!)
docker compose down -v
```

## 🌐 ACCESO A SERVICIOS

| Servicio | URL | Credenciales |
|----------|-----|--------------|
| **Superset** | http://localhost:8088 | admin / Admin123! |
| **ClickHouse** | http://localhost:8123 | default / (sin password) |
| **Kafka Connect** | http://localhost:8083 | - |
| **Kafka** | localhost:19092 | - |

## 🚨 SOLUCIÓN DE PROBLEMAS

### Problemas Comunes

#### 1. Error de conexión MySQL
```bash
# Verificar conectividad
ping host.docker.internal
telnet host.docker.internal 3306

# Verificar credenciales
mysql -h host.docker.internal -u root -p
```

#### 2. Servicios no inician
```bash
# Verificar puertos ocupados
ss -tuln | grep -E ':8088|:8123|:8083|:19092'

# Limpiar y reiniciar
docker compose down
docker system prune -f
./start_etl_pipeline.sh --clean
```

#### 3. Orquestación falla
```bash
# Ver logs detallados
tail -f logs/orchestrator.log

# Ejecutar fases individualmente
python3 tools/cleanup_all.py
python3 tools/setup_database_users.py
# ... continuar con otras fases
```

#### 4. Datos no fluyen
```bash
# Verificar estado conectores
curl http://localhost:8083/connectors/mysql_source_auto/status

# Ver tópicos Kafka
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list

# Verificar logs de Connect
docker compose logs connect
```

### Logs de Depuración

Para debugging avanzado, habilitar logs DEBUG:

```bash
# Editar cualquier script Python y cambiar:
logging.basicConfig(level=logging.DEBUG)
```

## 📚 DOCUMENTACIÓN ADICIONAL

- [Guía de Dependencias](docs/DEPENDENCIES.md)
- [Variables de Entorno](docs/ENVIRONMENT_VARIABLES.md)  
- [Recuperación de Errores](docs/ERROR_RECOVERY.md)
- [Referencia Rápida](docs/QUICK_REFERENCE.md)
- [Guía de Testing](docs/TESTING_GUIDE.md)

## 🤝 CONTRIBUIR

1. Fork el repositorio
2. Crear rama feature (`git checkout -b feature/nueva-funcionalidad`)
3. Commit cambios (`git commit -am 'Agregar nueva funcionalidad'`)
4. Push a la rama (`git push origin feature/nueva-funcionalidad`) 
5. Crear Pull Request

## 📄 LICENCIA

Este proyecto está bajo la licencia MIT. Ver archivo `LICENSE` para detalles.

---

## 🎉 ¡LISTO!

Con un solo comando tienes un pipeline ETL completamente funcional:

```bash
./start_etl_pipeline.sh
```

**¡Tu datos fluyen automáticamente desde MySQL hasta ClickHouse con visualización en Superset!** 🚀