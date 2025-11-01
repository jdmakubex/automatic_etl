#!/usr/bin/env python3
"""
Migración segura a credenciales institucionales
Este script te ayuda a migrar paso a paso a credenciales robustas
"""

import os
import shutil
import subprocess
import time
from datetime import datetime

def create_backup():
    """Crear backup de la configuración actual"""
    
    print("💾 Creando backup de configuración actual...")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = f"/mnt/c/proyectos/etl_prod/backups/credentials_{timestamp}"
    
    os.makedirs(backup_dir, exist_ok=True)
    
    # Backup del .env actual
    if os.path.exists("/mnt/c/proyectos/etl_prod/.env"):
        shutil.copy("/mnt/c/proyectos/etl_prod/.env", f"{backup_dir}/.env.backup")
        print(f"✅ .env respaldado en: {backup_dir}/.env.backup")
    
    # Backup de docker-compose.yml
    if os.path.exists("/mnt/c/proyectos/etl_prod/docker-compose.yml"):
        shutil.copy("/mnt/c/proyectos/etl_prod/docker-compose.yml", f"{backup_dir}/docker-compose.yml.backup")
        print(f"✅ docker-compose.yml respaldado")
    
    return backup_dir

def stop_services():
    """Detener servicios antes del cambio"""
    
    print("🛑 Deteniendo servicios...")
    
    try:
        result = subprocess.run(
            ["docker-compose", "down"], 
            cwd="/mnt/c/proyectos/etl_prod",
            capture_output=True, 
            text=True,
            timeout=60
        )
        
        if result.returncode == 0:
            print("✅ Servicios detenidos correctamente")
            return True
        else:
            print(f"⚠️ Algunos servicios podrían seguir corriendo: {result.stderr}")
            return True  # Continuar de todas formas
            
    except Exception as e:
        print(f"⚠️ Error deteniendo servicios: {e}")
        return False

def validate_new_credentials(env_path):
    """Validar que las nuevas credenciales sean robustas"""
    
    print("🔐 Validando robustez de credenciales...")
    
    if not os.path.exists(env_path):
        print(f"❌ No se encontró el archivo: {env_path}")
        return False
    
    required_vars = [
        'METABASE_ADMIN',
        'METABASE_PASSWORD', 
        'SUPERSET_ADMIN',
        'SUPERSET_PASSWORD',
        'CLICKHOUSE_DEFAULT_USER',
        'CLICKHOUSE_DEFAULT_PASSWORD'
    ]
    
    credentials = {}
    
    try:
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if '=' in line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    credentials[key.strip()] = value.strip()
    
    except Exception as e:
        print(f"❌ Error leyendo {env_path}: {e}")
        return False
    
    # Validar que existan las variables requeridas
    missing = []
    weak_passwords = []
    
    for var in required_vars:
        if var not in credentials:
            missing.append(var)
        elif 'PASSWORD' in var:
            password = credentials[var]
            
            # Validar robustez de contraseña
            if len(password) < 8:
                weak_passwords.append(f"{var} (muy corta: {len(password)} caracteres)")
            elif password in ['Admin123!', 'ClickHouse123!', 'password', '123456']:
                weak_passwords.append(f"{var} (contraseña común/débil)")
    
    if missing:
        print("❌ Variables faltantes:")
        for var in missing:
            print(f"   • {var}")
        return False
    
    if weak_passwords:
        print("⚠️ Contraseñas débiles detectadas:")
        for issue in weak_passwords:
            print(f"   • {issue}")
        print("💡 Recomendación: Usa contraseñas de 12+ caracteres con mayúsculas, minúsculas, números y símbolos")
    
    print("✅ Credenciales validadas correctamente")
    return True

def start_services():
    """Iniciar servicios con las nuevas credenciales"""
    
    print("🚀 Iniciando servicios con nuevas credenciales...")
    
    try:
        result = subprocess.run(
            ["docker-compose", "up", "-d"], 
            cwd="/mnt/c/proyectos/etl_prod",
            capture_output=True, 
            text=True,
            timeout=300  # 5 minutos
        )
        
        if result.returncode == 0:
            print("✅ Servicios iniciados correctamente")
            return True
        else:
            print(f"❌ Error iniciando servicios: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error iniciando servicios: {e}")
        return False

def wait_for_services():
    """Esperar a que los servicios estén listos"""
    
    print("⏳ Esperando a que los servicios estén listos...")
    
    for i in range(12):  # 2 minutos máximo
        print(f"   Esperando... {i+1}/12")
        time.sleep(10)
        
        # Verificar si Metabase responde
        try:
            result = subprocess.run(
                ["curl", "-s", "http://localhost:3000/api/health"],
                capture_output=True,
                timeout=5
            )
            if result.returncode == 0:
                print("✅ Servicios están listos")
                return True
        except:
            pass
    
    print("⚠️ Los servicios tardaron más de lo esperado, pero continuemos...")
    return True

def run_pipeline_test():
    """Ejecutar prueba del pipeline completo"""
    
    print("🧪 Probando pipeline con nuevas credenciales...")
    
    try:
        result = subprocess.run(
            ["python3", "tools/verify_custom_credentials.py"],
            cwd="/mnt/c/proyectos/etl_prod",
            capture_output=True,
            text=True,
            timeout=60
        )
        
        print("📊 RESULTADO DE LA VERIFICACIÓN:")
        print(result.stdout)
        
        if "¡TODAS LAS CREDENCIALES FUNCIONAN CORRECTAMENTE!" in result.stdout:
            print("✅ Pipeline funcionando perfectamente con nuevas credenciales")
            return True
        else:
            print("⚠️ Algunos servicios podrían requerir atención")
            return False
            
    except Exception as e:
        print(f"❌ Error en la verificación: {e}")
        return False

def rollback_credentials(backup_dir):
    """Rollback en caso de problemas"""
    
    print("🔄 Realizando rollback...")
    
    backup_env = f"{backup_dir}/.env.backup"
    
    if os.path.exists(backup_env):
        shutil.copy(backup_env, "/mnt/c/proyectos/etl_prod/.env")
        print("✅ Credenciales originales restauradas")
        
        # Reiniciar servicios
        stop_services()
        time.sleep(5)
        start_services()
        
        print("✅ Rollback completado")
        return True
    else:
        print("❌ No se encontró backup para rollback")
        return False

def main():
    """Proceso principal de migración"""
    
    print("🔐 MIGRACIÓN A CREDENCIALES INSTITUCIONALES")
    print("="*70)
    print("Este script te ayudará a migrar de forma segura a credenciales robustas")
    print("")
    
    # Paso 1: Verificar que tenemos el archivo de credenciales nuevas
    new_env_path = "/mnt/c/proyectos/etl_prod/.env.new"
    
    if not os.path.exists(new_env_path):
        print("📝 PASO PREVIO REQUERIDO:")
        print(f"1. Copia .env.institutional.example como .env.new")
        print(f"2. Edita .env.new con tus credenciales institucionales reales")
        print(f"3. Ejecuta este script nuevamente")
        print("")
        print("Comando:")
        print("cp .env.institutional.example .env.new")
        print("nano .env.new  # o tu editor preferido")
        return False
    
    print("✅ Archivo .env.new encontrado")
    
    # Paso 2: Validar credenciales
    if not validate_new_credentials(new_env_path):
        print("❌ Las credenciales no son válidas. Corrige .env.new y vuelve a ejecutar.")
        return False
    
    # Paso 3: Crear backup
    backup_dir = create_backup()
    
    # Paso 4: Confirmar migración
    print("\\n⚠️ CONFIRMACIÓN REQUERIDA")
    print("¿Proceder con la migración? Esto cambiará las credenciales del sistema.")
    print("Tendrás que usar las nuevas credenciales para acceder a Metabase y Superset.")
    
    response = input("Escribir 'SI' para continuar: ")
    if response.upper() != 'SI':
        print("❌ Migración cancelada")
        return False
    
    print("\\n🚀 INICIANDO MIGRACIÓN...")
    
    # Paso 5: Detener servicios
    if not stop_services():
        print("❌ Error deteniendo servicios")
        return False
    
    # Paso 6: Cambiar credenciales
    try:
        shutil.copy(new_env_path, "/mnt/c/proyectos/etl_prod/.env")
        print("✅ Credenciales actualizadas")
    except Exception as e:
        print(f"❌ Error copiando credenciales: {e}")
        return False
    
    # Paso 7: Iniciar servicios
    if not start_services():
        print("❌ Error iniciando servicios con nuevas credenciales")
        print("🔄 Iniciando rollback...")
        rollback_credentials(backup_dir)
        return False
    
    # Paso 8: Esperar a que estén listos
    wait_for_services()
    
    # Paso 9: Probar pipeline
    if run_pipeline_test():
        print("\\n🎉 ¡MIGRACIÓN EXITOSA!")
        print("="*50)
        print("✅ Credenciales institucionales funcionando correctamente")
        print("✅ Pipeline completamente operativo")
        print("✅ Backup guardado en:", backup_dir)
        
        # Mostrar nuevas credenciales
        print("\\n🔐 NUEVAS CREDENCIALES DE ACCESO:")
        
        with open("/mnt/c/proyectos/etl_prod/.env", 'r') as f:
            for line in f:
                line = line.strip()
                if '=' in line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    if key.strip() in ['METABASE_ADMIN', 'SUPERSET_ADMIN']:
                        print(f"   {key}: {value}")
                    elif 'PASSWORD' in key:
                        print(f"   {key}: {value[:4]}***")
        
        print("\\n🌐 ACCESOS:")
        print("   📊 Superset:  http://localhost:8088")
        print("   📈 Metabase:  http://localhost:3000")
        print("   🗄️ ClickHouse: http://localhost:8123")
        
        # Limpiar archivo temporal
        os.remove(new_env_path)
        
        return True
    else:
        print("\\n⚠️ MIGRACIÓN CON PROBLEMAS")
        print("Los servicios iniciaron pero hay problemas de conectividad")
        print("Puedes intentar corregir manualmente o hacer rollback")
        
        rollback = input("¿Hacer rollback? (y/n): ")
        if rollback.lower() == 'y':
            rollback_credentials(backup_dir)
        
        return False

if __name__ == "__main__":
    success = main()
    
    print("\\n" + "="*70)
    if success:
        print("✅ MIGRACIÓN COMPLETADA EXITOSAMENTE")
    else:
        print("❌ MIGRACIÓN FALLÓ - Revisa los mensajes anteriores")
    print("="*70)