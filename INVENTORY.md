# 📋 ÍNDICE COMPLETO DE ARCHIVOS DEL PIPELINE ETL
## Inventario y propósito de cada archivo para evitar duplicaciones

**Fecha de creación:** 2025-10-31  
**Estado:** Consolidado después de implementación de Metabase dinámico  

---

## 🚀 **SCRIPTS DE INICIO PRINCIPALES**

### **SCRIPTS MAESTROS DE INICIO** ⭐
| Archivo | Propósito | Estado | Notas |
|---------|-----------|---------|-------|
| `start_etl_pipeline.sh` | **SCRIPT MAESTRO PRINCIPAL** - Inicio completo del pipeline | ✅ ACTIVO | **USAR ESTE** - Script oficial de inicio |
| `tools/start_pipeline.sh` | Script alternativo de inicio del pipeline | ✅ ACTIVO | Similar al anterior, verificar si es redundante |
| `start_automated_pipeline.sh` | Inicio automatizado con orquestación | ✅ ACTIVO | Para modo completamente automático |
| `bootstrap/run_etl_full.sh` | Script de bootstrap completo | ✅ ACTIVO | Para inicialización desde cero |

### **SCRIPTS DE LIMPIEZA Y MANTENIMIENTO**
| Archivo | Propósito | Estado |
|---------|-----------|---------|
| `tools/clean_and_restart_pipeline.sh` | Limpieza completa y reinicio | ✅ ACTIVO |
| `tools/clean_all.sh` | Limpieza de contenedores y volúmenes | ✅ ACTIVO |
| `verify_system_status.sh` | Verificación de estado del sistema | ✅ ACTIVO |

---

## 🔧 **CONFIGURADORES AUTOMÁTICOS** ⭐

### **CONFIGURADORES PRINCIPALES**
| Archivo | Propósito | Estado | Integración |
|---------|-----------|---------|-------------|
| `tools/superset_auto_configurator.py` | **Configurador automático de Superset** | ✅ ACTIVO | Integrado en docker-compose |
| `tools/metabase_dynamic_configurator.py` | **Configurador dinámico de Metabase** | ✅ ACTIVO | Integrado en docker-compose |
| `tools/metabase_schema_discovery.py` | Helper para descubrimiento de esquemas Metabase | ✅ ACTIVO | Usado por configurador dinámico |

### **CONFIGURADORES ESPECÍFICOS** 
| Archivo | Propósito | Estado | Notas |
|---------|-----------|---------|-------|
| `configure_superset_datasets.py` | Configurador manual de datasets Superset | ⚠️ LEGACY | Reemplazado por auto_configurator |
| `configure_superset_manual.py` | Configuración manual de Superset | ⚠️ LEGACY | Reemplazado por auto_configurator |
| `tools/metabase_create_admin.py` | Creación manual de admin Metabase | ⚠️ REDUNDANTE | Funcionalidad en dynamic_configurator |
| `tools/metabase_setup_ui.py` | Setup UI de Metabase | ⚠️ REDUNDANTE | Funcionalidad en dynamic_configurator |

---

## 📊 **SCRIPTS DE INGESTA Y ETL**

### **INGESTORES PRINCIPALES**
| Archivo | Propósito | Estado |
|---------|-----------|---------|
| `tools/multi_database_ingest.py` | **Ingestor principal multi-base de datos** | ✅ ACTIVO |
| `bootstrap/parse_db_connections.py` | Parser de conexiones desde .env | ✅ ACTIVO |
| `bootstrap/generate_multi_databases.py` | Generador de múltiples bases de datos | ✅ ACTIVO |
| `tools/init_clickhouse_table.py` | Inicializador de tablas ClickHouse | ✅ ACTIVO |

### **ORQUESTADORES**
| Archivo | Propósito | Estado |
|---------|-----------|---------|
| `tools/auto_pipeline.sh` | **Orquestador automático principal** | ✅ ACTIVO |
| `superset_bootstrap/orchestrate_superset_clickhouse.py` | Orquestador Superset-ClickHouse | ✅ ACTIVO |
| `superset_bootstrap/multi_database_configurator.py` | Configurador multi-BD para Superset | ✅ ACTIVO |

---

## 🔍 **SCRIPTS DE VALIDACIÓN Y DIAGNÓSTICO** ⭐

### **VALIDADORES RECIENTES** (creados en esta sesión)
| Archivo | Propósito | Estado | Fecha |
|---------|-----------|---------|--------|
| `tools/validate_pipeline_completeness.py` | **Validador completo del pipeline** | ✅ NUEVO | 2025-10-31 |
| `tools/test_container_connectivity.py` | Test exhaustivo de conectividad | ✅ NUEVO | 2025-10-31 |
| `tools/test_final_sql_labs.py` | Test final de SQL Labs | ✅ NUEVO | 2025-10-31 |
| `tools/debug_superset_sql_lab.py` | Debug específico de Superset SQL Lab | ✅ NUEVO | 2025-10-31 |
| `tools/test_sql_labs.py` | Test comparativo de SQL Labs | ✅ NUEVO | 2025-10-31 |

### **VALIDADORES EXISTENTES**
| Archivo | Propósito | Estado |
|---------|-----------|---------|
| `tools/health_validator.py` | Validador de salud de servicios | ✅ ACTIVO |
| `tools/verify_async_stack.sh` | Verificador de stack asíncrono | ✅ ACTIVO |
| `tools/run_verifications.sh` | Runner de verificaciones | ✅ ACTIVO |
| `tools/metabase_diagnostic.py` | Diagnóstico específico de Metabase | ✅ ACTIVO |

---

## 📈 **SCRIPTS DE MONITOREO Y AUDITORÍA**

| Archivo | Propósito | Estado |
|---------|-----------|---------|
| `tools/monitor_pipeline.py` | **Monitor principal del pipeline** | ✅ ACTIVO |
| `tools/multi_database_auditor.py` | Auditor de múltiples bases de datos | ✅ ACTIVO |
| `tools/cdc_monitor.sh` | Monitor de Change Data Capture | ✅ ACTIVO |
| `tools/run_tests.sh` | Runner de tests automatizados | ✅ ACTIVO |

---

## 🗄️ **SCRIPTS DE BASES DE DATOS**

### **ClickHouse**
| Archivo | Propósito | Estado |
|---------|-----------|---------|
| `bootstrap/setup_clickhouse_robust.sh` | **Setup robusto de ClickHouse** | ✅ ACTIVO |
| `bootstrap/setup_multi_clickhouse.sh` | Setup multi-BD ClickHouse | ✅ ACTIVO |
| `bootstrap/clickhouse_init.sql` | SQL de inicialización | ✅ ACTIVO |
| `tools/sync_kafka_clickhouse.sh` | Sincronizador Kafka-ClickHouse | ✅ ACTIVO |

### **Configuración de usuarios**
| Archivo | Propósito | Estado |
|---------|-----------|---------|
| `bootstrap/setup_users_automatically.sh` | Setup automático de usuarios | ✅ ACTIVO |
| `bootstrap/create_users.sql` | SQL de creación de usuarios | ✅ ACTIVO |

---

## 🔧 **ARCHIVOS DE CONFIGURACIÓN**

### **Docker y Compose**
| Archivo | Propósito | Estado |
|---------|-----------|---------|
| `docker-compose.yml` | **Configuración principal de servicios** | ✅ ACTIVO |
| `docker-compose.cdc.yml` | Configuración CDC específica | ✅ ACTIVO |
| `clickhouse.Dockerfile` | Dockerfile para ClickHouse | ✅ ACTIVO |
| `superset.Dockerfile` | Dockerfile para Superset | ✅ ACTIVO |

### **Variables de entorno**
| Archivo | Propósito | Estado |
|---------|-----------|---------|
| `.env` | **Archivo principal de variables** | ✅ ACTIVO |
| `requirements.txt` | Dependencias Python | ✅ ACTIVO |

---

## 📋 **ANÁLISIS DE DUPLICACIONES Y REDUNDANCIAS**

### 🚨 **POSIBLES DUPLICACIONES DETECTADAS:**

#### **Scripts de Inicio**
- ❓ `start_etl_pipeline.sh` vs `tools/start_pipeline.sh` 
  - **ACCIÓN REQUERIDA:** Verificar diferencias y consolidar

#### **Configuradores Metabase**
- ❓ `tools/metabase_create_admin.py` - Redundante con `metabase_dynamic_configurator.py`
- ❓ `tools/metabase_setup_ui.py` - Redundante con `metabase_dynamic_configurator.py`
  - **ACCIÓN REQUERIDA:** Deprecar los redundantes

#### **Configuradores Superset**
- ❓ `configure_superset_datasets.py` - Legacy, reemplazado por `superset_auto_configurator.py`
- ❓ `configure_superset_manual.py` - Legacy, reemplazado por `superset_auto_configurator.py`
  - **ACCIÓN REQUERIDA:** Marcar como deprecated

---

## 🎯 **ARCHIVOS CRÍTICOS DEL PIPELINE ACTUAL** ⭐

**Estos son los archivos ESENCIALES que debe usar tu equipo:**

### **Para iniciar el pipeline:**
1. `start_etl_pipeline.sh` - **SCRIPT MAESTRO OFICIAL** 🚀
2. `tools/start_pipeline.sh` - Symlink al script maestro (compatibilidad)
3. `.env` - Variables de configuración

### **Configuradores automáticos:**
1. `tools/superset_auto_configurator.py` - **Superset automático** ✅
2. `tools/metabase_dynamic_configurator.py` - **Metabase dinámico** ✅  
3. `tools/metabase_schema_discovery.py` - Helper para Metabase

### **Para validar el pipeline:**
1. `tools/validate_pipeline_completeness.py` - **Validador completo** ⭐
2. `tools/test_container_connectivity.py` - Test de conectividad

### **Core del ETL:**
1. `tools/multi_database_ingest.py` - Ingestor principal
2. `tools/auto_pipeline.sh` - Orquestador automático
3. `bootstrap/setup_clickhouse_robust.sh` - Setup de ClickHouse

### **📂 Archivos movidos a deprecated/:**
- ❌ `tools/metabase_create_admin.py` → **deprecated/** (redundante)
- ❌ `tools/metabase_setup_ui.py` → **deprecated/** (redundante)
- ❌ `configure_superset_datasets.py` → **deprecated/** (legacy)
- ❌ `configure_superset_manual.py` → **deprecated/** (legacy)

---

## ⚡ **PRÓXIMAS ACCIONES RECOMENDADAS**

### **CONSOLIDACIÓN INMEDIATA:**
1. 🔍 Comparar `start_etl_pipeline.sh` vs `tools/start_pipeline.sh`
2. 🗑️ Deprecar configuradores Metabase redundantes
3. 📝 Actualizar documentación con archivos oficiales

### **MANTENIMIENTO:**
1. 📋 Marcar archivos legacy como deprecated
2. 🔄 Crear aliases si hay dependencias externas
3. 📊 Validar que todos los servicios usen configuradores automáticos

---

**📌 NOTA IMPORTANTE:** Este índice debe actualizarse cada vez que se cree un nuevo archivo. Usar este documento como referencia antes de crear cualquier script nuevo.