#!/bin/bash
"""
🧹 SCRIPT DE CONSOLIDACIÓN Y LIMPIEZA
===================================

Mueve archivos redundantes a deprecated/ y crea estructura limpia.
"""

set -euo pipefail

echo "🧹 CONSOLIDANDO ARCHIVOS REDUNDANTES"
echo "====================================="

# Crear directorio deprecated si no existe
mkdir -p deprecated

# Función para mover archivos de forma segura
move_to_deprecated() {
    local file="$1"
    local reason="$2"
    
    if [ -f "$file" ]; then
        echo "📦 Moviendo $file → deprecated/ ($reason)"
        mv "$file" "deprecated/$(basename "$file")"
        echo "   ✅ Movido"
    else
        echo "   ⚠️  $file no existe, omitiendo"
    fi
}

# Mover archivos redundantes detectados
echo -e "\n📋 MOVIENDO CONFIGURADORES REDUNDANTES:"

move_to_deprecated "tools/metabase_create_admin.py" "redundante con metabase_dynamic_configurator.py"
move_to_deprecated "tools/metabase_setup_ui.py" "redundante con metabase_dynamic_configurator.py" 
move_to_deprecated "configure_superset_datasets.py" "legacy, reemplazado por superset_auto_configurator.py"
move_to_deprecated "configure_superset_manual.py" "legacy, reemplazado por superset_auto_configurator.py"

# Crear symlink para start_pipeline.sh apuntando al oficial
echo -e "\n🔗 CREANDO SYMLINKS:"

if [ -f "tools/start_pipeline.sh" ]; then
    echo "📎 Creando backup de tools/start_pipeline.sh"
    mv "tools/start_pipeline.sh" "deprecated/start_pipeline_backup.sh"
fi

echo "🔗 Creando symlink tools/start_pipeline.sh → ../start_etl_pipeline.sh"
ln -sf "../start_etl_pipeline.sh" "tools/start_pipeline.sh"

# Crear archivo de referencia en deprecated
cat > deprecated/README_DEPRECATED.md << 'EOF'
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
EOF

echo -e "\n✅ CONSOLIDACIÓN COMPLETADA"
echo "============================="
echo "📂 Archivos redundantes movidos a: deprecated/"
echo "🔗 Symlinks creados para mantener compatibilidad"
echo "📝 Documentación actualizada en: deprecated/README_DEPRECATED.md"
echo ""
echo "🎯 PRÓXIMOS PASOS:"
echo "   1. Probar que start_etl_pipeline.sh funciona correctamente"
echo "   2. Validar que los configuradores automáticos funcionan"
echo "   3. Después de 1 semana sin problemas, eliminar deprecated/"