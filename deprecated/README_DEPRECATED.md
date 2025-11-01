# 🗂️ Archivos Deprecated

Este directorio contiene archivos que han sido reemplazados por versiones mejoradas.

## 📋 Archivos movidos y sus reemplazos:

### Configuradores Metabase (redundantes):
- `metabase_create_admin.py` → Usar `tools/metabase_dynamic_configurator.py`
- `metabase_setup_ui.py` → Usar `tools/metabase_dynamic_configurator.py`

### Configuradores Superset (legacy):
- `configure_superset_datasets.py` → Usar `tools/superset_auto_configurator.py`
- `configure_superset_manual.py` → Usar `tools/superset_auto_configurator.py`

### Scripts de inicio (consolidados):
- `start_pipeline_backup.sh` → Usar `start_etl_pipeline.sh` (raíz del proyecto)

## ⚠️ IMPORTANTE:
NO eliminar estos archivos aún. Mantenerlos por si hay dependencias externas.
Después de validar que todo funciona, se pueden eliminar definitivamente.
