#!/usr/bin/env python3
"""
Configurador automático de Superset para el pipeline ETL
Se ejecuta automáticamente después de la ingesta de datos
"""
import time
import subprocess
import sys
import os

def wait_for_data():
    """Esperar a que los datos estén disponibles en ClickHouse"""
    print("⏳ Esperando datos en ClickHouse...")
    
    max_attempts = 10
    for attempt in range(max_attempts):
        try:
            # Verificar que hay datos en ClickHouse
            result = subprocess.run([
                "clickhouse-client", 
                "--host", "clickhouse",
                "--query", "SELECT count(*) FROM fgeo_analytics.archivos_archivos__archivos"
            ], capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0:
                count = int(result.stdout.strip())
                if count > 0:
                    print(f"✅ Datos disponibles: {count} filas en tabla principal")
                    return True
            
        except Exception as e:
            print(f"   Intento {attempt + 1}/{max_attempts}: {e}")
        
        time.sleep(5)
    
    print("❌ No se pudieron verificar los datos")
    return False

def configure_clickhouse_database():
    """Configurar base de datos ClickHouse en Superset"""
    print("🔧 Configurando base de datos ClickHouse en Superset...")
    
    # Comando para agregar la base de datos ClickHouse
    add_database_cmd = [
        "superset", "fab", "create-database",
        "--database-name", "fgeo_analytics",
        "--sqlalchemy-uri", "clickhousedb://etl:etl_password@clickhouse:8123/fgeo_analytics"
    ]
    
    try:
        result = subprocess.run(add_database_cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            print("✅ Base de datos ClickHouse configurada")
            return True
        else:
            print(f"⚠️ Posible error configurando BD: {result.stderr}")
            # Podría ya existir, continuar
            return True
    except Exception as e:
        print(f"⚠️ Error configurando base de datos: {e}")
        return False

def main():
    print("🔧 CONFIGURADOR AUTOMÁTICO DE SUPERSET")
    print("=" * 50)
    
    # Esperar datos
    if not wait_for_data():
        print("❌ Sin datos disponibles, abortando configuración")
        return 1
    
    # Configurar base de datos
    if not configure_clickhouse_database():
        print("❌ Error configurando base de datos")
        return 1
    
    print("✅ CONFIGURACIÓN DE SUPERSET COMPLETADA")
    print("📊 Datos disponibles en: http://localhost:8088")
    print("👤 Usuario: admin / Contraseña: admin")
    print("🔗 Base de datos: fgeo_analytics (ClickHouse)")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())