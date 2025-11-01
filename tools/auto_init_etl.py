#!/usr/bin/env python3
"""
Script de inicialización que se ejecuta automáticamente al iniciar el contenedor etl-tools.
Configura permisos ETL y prepara el ambiente sin intervención manual.
"""
import os
import time
import subprocess

def setup_etl_environment():
    """Configuración automática del ambiente ETL"""
    print("🚀 INICIALIZACIÓN AUTOMÁTICA DEL AMBIENTE ETL")
    print("=" * 50)
    
    # Esperar a que ClickHouse esté listo
    print("⏳ Esperando que ClickHouse esté completamente inicializado...")
    time.sleep(20)  # Espera inicial
    
    # Configurar permisos ETL
    try:
        result = subprocess.run([
            "python3", "/app/tools/etl_permissions_setup.py"
        ], capture_output=True, text=True, timeout=120)
        
        if result.returncode == 0:
            print("✅ Permisos ETL configurados automáticamente")
        else:
            print("⚠️  Configuración ETL completada con advertencias")
            print("📄 Revisa logs/etl_permissions_setup.log para detalles")
            
    except Exception as e:
        print(f"❌ Error en configuración ETL: {e}")
    
    print("🎯 Ambiente ETL listo para uso")

if __name__ == "__main__":
    # Solo ejecutar si estamos en el contenedor correcto
    if os.path.exists("/app/tools"):
        setup_etl_environment()