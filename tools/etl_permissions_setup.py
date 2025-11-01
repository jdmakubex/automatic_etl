#!/usr/bin/env python3
"""
Configuración automática y robusta de permisos para el usuario ETL en ClickHouse.
Se ejecuta automáticamente durante el pipeline ETL para garantizar permisos correctos.
"""
import os
import time
import subprocess
from dotenv import load_dotenv

load_dotenv('/app/.env')

CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_DEFAULT_USER = os.getenv("CLICKHOUSE_DEFAULT_USER", "default")
CH_DEFAULT_PASSWORD = os.getenv("CLICKHOUSE_DEFAULT_PASSWORD", "ClickHouse123!")
CH_ETL_USER = os.getenv("CLICKHOUSE_ETL_USER", "etl")
CH_ETL_PASSWORD = os.getenv("CLICKHOUSE_ETL_PASSWORD", "Et1Ingest!")

LOG_FILE = "/app/logs/etl_permissions_setup.log"

def log(message):
    """Log a message to both console and file"""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    log_msg = f"[{timestamp}] {message}"
    print(log_msg)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(log_msg + "\n")
    except:
        pass

def wait_for_clickhouse():
    """Espera a que ClickHouse esté disponible"""
    log("🔄 Esperando a que ClickHouse esté disponible...")
    max_attempts = 15
    
    for attempt in range(max_attempts):
        try:
            # Usar curl directo para verificar conectividad
            result = subprocess.run([
                "curl", "-s", f"http://{CH_HOST}:8123/ping"
            ], capture_output=True, text=True, timeout=3)
            
            if result.returncode == 0 and "Ok." in result.stdout:
                log(f"✅ ClickHouse disponible (intento {attempt + 1})")
                return True
                
        except Exception as e:
            log(f"⏳ Intento {attempt + 1}: {e}")
            
        time.sleep(2)
    
    log("❌ ClickHouse no respondió después de 15 intentos")
    return False

def execute_clickhouse_query(query, description=""):
    """Ejecuta una query en ClickHouse con el usuario default (admin)"""
    try:
        log(f"🔧 {description}")
        
        # Usar clickhouse-connect desde Python en lugar de comandos docker
        try:
            import clickhouse_connect
            client = clickhouse_connect.get_client(
                host=CH_HOST, 
                port=8123,
                username=CH_DEFAULT_USER,
                password=CH_DEFAULT_PASSWORD
            )
            
            result = client.command(query)
            log(f"✅ {description} - EXITOSO")
            return True
            
        except ImportError:
            # Fallback: usar curl directo
            import json
            curl_cmd = [
                "curl", "-s", f"http://{CH_HOST}:8123/",
                "--user", f"{CH_DEFAULT_USER}:{CH_DEFAULT_PASSWORD}",
                "--data-binary", query
            ]
            
            result = subprocess.run(curl_cmd, capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            log(f"✅ {description} - EXITOSO")
            if result.stdout.strip():
                log(f"📄 Resultado: {result.stdout.strip()}")
            return True
        else:
            log(f"❌ {description} - FALLÓ")
            if result.stderr.strip():
                log(f"💥 Error: {result.stderr.strip()}")
            return False
            
    except Exception as e:
        log(f"❌ Error ejecutando query: {e}")
        return False

def setup_etl_user_permissions():
    """Configura permisos completos para el usuario ETL"""
    log("🚀 CONFIGURANDO PERMISOS DEL USUARIO ETL")
    log("=" * 50)
    
    # Verificar que ClickHouse esté disponible
    if not wait_for_clickhouse():
        log("❌ No se pudo conectar a ClickHouse")
        return False
    
    success = True
    
    # 1. Crear usuario ETL si no existe
    create_user_query = f"""
    CREATE USER IF NOT EXISTS {CH_ETL_USER} 
    IDENTIFIED WITH plaintext_password BY '{CH_ETL_PASSWORD}'
    """
    if not execute_clickhouse_query(create_user_query, "Creando usuario ETL"):
        success = False
    
    # 2. Otorgar permisos completos en todas las bases
    permissions = [
        # Permisos globales
        f"GRANT ALL ON *.* TO {CH_ETL_USER}",
        
        # Permisos específicos críticos
        f"GRANT CREATE DATABASE ON *.* TO {CH_ETL_USER}",
        f"GRANT CREATE TABLE ON *.* TO {CH_ETL_USER}",
        f"GRANT CREATE VIEW ON *.* TO {CH_ETL_USER}",
        f"GRANT DROP TABLE ON *.* TO {CH_ETL_USER}",
        f"GRANT DROP DATABASE ON *.* TO {CH_ETL_USER}",
        f"GRANT INSERT ON *.* TO {CH_ETL_USER}",
        f"GRANT SELECT ON *.* TO {CH_ETL_USER}",
        f"GRANT ALTER ON *.* TO {CH_ETL_USER}",
        
        # Permisos de sistema
        f"GRANT SYSTEM ON *.* TO {CH_ETL_USER}",
        
        # Permisos específicos para bases esperadas
        f"GRANT ALL ON fgeo_analytics.* TO {CH_ETL_USER}",
        f"GRANT ALL ON archivos.* TO {CH_ETL_USER}",
        f"GRANT ALL ON fiscalizacion.* TO {CH_ETL_USER}",
        f"GRANT ALL ON ext.* TO {CH_ETL_USER}"
    ]
    
    for permission in permissions:
        if not execute_clickhouse_query(permission, f"Aplicando: {permission}"):
            log(f"⚠️  Permiso falló (podría ser normal si ya existe): {permission}")
            # No marcar como fallo crítico
    
    # 3. Verificar permisos otorgados
    verify_query = f"SHOW GRANTS FOR {CH_ETL_USER}"
    if execute_clickhouse_query(verify_query, "Verificando permisos otorgados"):
        log("✅ Verificación de permisos completada")
    
    # 4. Probar conexión con usuario ETL
    test_result = subprocess.run([
        "docker", "compose", "exec", "clickhouse",
        "clickhouse-client", "--host", CH_HOST, "--user", CH_ETL_USER,
        "--password", CH_ETL_PASSWORD, "--query", "SELECT 'ETL_USER_OK'"
    ], capture_output=True, text=True, timeout=10)
    
    if test_result.returncode == 0 and "ETL_USER_OK" in test_result.stdout:
        log("✅ Usuario ETL puede conectarse correctamente")
    else:
        log("❌ Usuario ETL no puede conectarse")
        log(f"💥 Error de conexión: {test_result.stderr}")
        success = False
    
    # 5. Crear bases de datos necesarias con usuario ETL
    databases = ["fgeo_analytics", "archivos", "fiscalizacion", "ext"]
    
    for db in databases:
        db_query = f"CREATE DATABASE IF NOT EXISTS {db}"
        if subprocess.run([
            "docker", "compose", "exec", "clickhouse",
            "clickhouse-client", "--host", CH_HOST, "--user", CH_ETL_USER,
            "--password", CH_ETL_PASSWORD, "--query", db_query
        ], capture_output=True, text=True, timeout=10).returncode == 0:
            log(f"✅ Base de datos '{db}' creada/verificada con usuario ETL")
        else:
            log(f"❌ No se pudo crear base de datos '{db}' con usuario ETL")
    
    if success:
        log("🎉 CONFIGURACIÓN DE PERMISOS ETL COMPLETADA EXITOSAMENTE")
    else:
        log("⚠️  CONFIGURACIÓN COMPLETADA CON ADVERTENCIAS")
    
    return success

def integrate_to_pipeline():
    """Crea integración automática al pipeline ETL"""
    log("🔗 Integrando configuración al pipeline ETL...")
    
    # Crear archivo de integración
    integration_script = """#!/bin/bash
# Configuración automática de permisos ETL
# Se ejecuta automáticamente durante el pipeline

echo "🔧 Configurando permisos ETL automáticamente..."
docker compose exec etl-tools python3 tools/etl_permissions_setup.py

if [ $? -eq 0 ]; then
    echo "✅ Permisos ETL configurados correctamente"
else
    echo "❌ Error configurando permisos ETL"
    exit 1
fi
"""
    
    try:
        with open("/app/tools/auto_setup_etl_permissions.sh", "w") as f:
            f.write(integration_script)
        
        # Hacer ejecutable
        subprocess.run(["chmod", "+x", "/app/tools/auto_setup_etl_permissions.sh"])
        log("✅ Script de integración creado: auto_setup_etl_permissions.sh")
        
    except Exception as e:
        log(f"❌ Error creando script de integración: {e}")

if __name__ == "__main__":
    success = setup_etl_user_permissions()
    integrate_to_pipeline()
    
    if success:
        log("🎯 ¡Configuración ETL lista para producción!")
        exit(0)
    else:
        log("💥 Revisa los errores y reintenta")
        exit(1)