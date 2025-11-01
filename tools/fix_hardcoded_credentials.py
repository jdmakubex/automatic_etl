#!/usr/bin/env python3
"""
Script para corregir automáticamente los archivos críticos del pipeline
para que usen correctamente las credenciales del .env
"""

import re
import os
from pathlib import Path

def fix_metabase_add_clickhouse():
    """Corregir tools/metabase_add_clickhouse.py para usar credenciales dinámicas"""
    
    file_path = "/mnt/c/proyectos/etl_prod/tools/metabase_add_clickhouse.py"
    
    print("🔧 Corrigiendo metabase_add_clickhouse.py...")
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Agregar función para cargar credenciales del .env al inicio
    env_loader = '''#!/usr/bin/env python3
"""
Script para configurar ClickHouse después del setup inicial
"""

import requests
import json
import time
import os

def load_env_credentials():
    """Cargar credenciales desde .env"""
    credentials = {
        'METABASE_ADMIN': 'admin@admin.com',
        'METABASE_PASSWORD': 'Admin123!',
        'CLICKHOUSE_DEFAULT_USER': 'default',
        'CLICKHOUSE_DEFAULT_PASSWORD': 'ClickHouse123!'
    }
    
    try:
        with open('/mnt/c/proyectos/etl_prod/.env', 'r') as f:
            for line in f:
                line = line.strip()
                if '=' in line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    if key.strip() in credentials:
                        credentials[key.strip()] = value.strip()
    except Exception:
        pass
    
    return credentials
'''
    
    # Reemplazar el inicio del archivo
    pattern = r'^#!/usr/bin/env python3.*?import time'
    replacement = env_loader.strip() + '\n'
    
    content = re.sub(pattern, replacement, content, flags=re.DOTALL)
    
    # Reemplazar credenciales hardcodeadas por dinámicas
    replacements = [
        (r'"username": "admin@admin\.com"', '"username": env_creds["METABASE_ADMIN"]'),
        (r'"password": "Admin123!"', '"password": env_creds["METABASE_PASSWORD"]'),
        (r'"user": "default"', '"user": env_creds["CLICKHOUSE_DEFAULT_USER"]'),
        (r'"password": "ClickHouse123!"', '"password": env_creds["CLICKHOUSE_DEFAULT_PASSWORD"]'),
        (r'print\("   👤 Usuario: admin@admin\.com"\)', 'print(f"   👤 Usuario: {env_creds[\'METABASE_ADMIN\']}")'),
        (r'print\("   🔑 Contraseña: Admin123!"\)', 'print(f"   🔑 Contraseña: {env_creds[\'METABASE_PASSWORD\']}")'),
    ]
    
    for pattern, replacement in replacements:
        content = re.sub(pattern, replacement, content)
    
    # Agregar carga de credenciales al inicio de la función principal
    content = re.sub(
        r'def configure_clickhouse_after_setup\(\):\s*"""[^"]*"""\s*',
        '''def configure_clickhouse_after_setup():
    """Configurar ClickHouse después de que Metabase ya esté inicializado"""
    
    # Cargar credenciales del .env
    env_creds = load_env_credentials()
    
    ''',
        content
    )
    
    with open(file_path, 'w') as f:
        f.write(content)
    
    print("✅ metabase_add_clickhouse.py corregido")

def fix_schema_cleaner():
    """Corregir tools/schema_cleaner.py"""
    
    file_path = "/mnt/c/proyectos/etl_prod/tools/schema_cleaner.py"
    
    print("🔧 Corrigiendo schema_cleaner.py...")
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Reemplazar credencial hardcodeada
    content = re.sub(
        r'self\.clickhouse_password = "ClickHouse123!"',
        'self.clickhouse_password = os.getenv("CLICKHOUSE_DEFAULT_PASSWORD", "ClickHouse123!")',
        content
    )
    
    # Asegurar que os está importado
    if 'import os' not in content:
        content = re.sub(
            r'(import [^\n]*\n)',
            r'\1import os\n',
            content,
            count=1
        )
    
    with open(file_path, 'w') as f:
        f.write(content)
    
    print("✅ schema_cleaner.py corregido")

def fix_robust_service_tester():
    """Corregir tools/robust_service_tester.py"""
    
    file_path = "/mnt/c/proyectos/etl_prod/tools/robust_service_tester.py"
    
    print("🔧 Corrigiendo robust_service_tester.py...")
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Reemplazar credencial hardcodeada
    content = re.sub(
        r'ch_password = "ClickHouse123!"',
        'ch_password = os.getenv("CLICKHOUSE_DEFAULT_PASSWORD", "ClickHouse123!")',
        content
    )
    
    # Asegurar que os está importado
    if 'import os' not in content:
        content = re.sub(
            r'(import [^\n]*\n)',
            r'\1import os\n',
            content,
            count=1
        )
    
    with open(file_path, 'w') as f:
        f.write(content)
    
    print("✅ robust_service_tester.py corregido")

def fix_pipeline_script():
    """Corregir start_etl_pipeline.sh para usar variables dinámicas"""
    
    file_path = "/mnt/c/proyectos/etl_prod/start_etl_pipeline.sh"
    
    print("🔧 Corrigiendo start_etl_pipeline.sh...")
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Reemplazar mensajes hardcodeados por dinámicos
    replacements = [
        (
            r'  - Superset: http://localhost:8088 \(admin/Admin123!\)',
            '  - Superset: http://localhost:8088 (${SUPERSET_ADMIN:-admin}/${SUPERSET_PASSWORD:-Admin123!})'
        ),
        (
            r'  - Metabase: http://localhost:3000 \(admin@admin\.com/Admin123!\)',
            '  - Metabase: http://localhost:3000 (${METABASE_ADMIN:-admin@admin.com}/${METABASE_PASSWORD:-Admin123!})'
        ),
        (
            r'echo "   📊 Superset:      http://localhost:8088  \(admin/Admin123!\)"',
            'echo "   📊 Superset:      http://localhost:8088  (${SUPERSET_ADMIN:-admin}/${SUPERSET_PASSWORD:-Admin123!})"'
        ),
        (
            r'echo "   📈 Metabase:      http://localhost:3000  \(admin@admin\.com/Admin123!\)"',
            'echo "   📈 Metabase:      http://localhost:3000  (${METABASE_ADMIN:-admin@admin.com}/${METABASE_PASSWORD:-Admin123!})"'
        ),
        (
            r'print_info "   👤 Usuario: admin@admin\.com \(credenciales \.env\)"',
            'print_info "   👤 Usuario: ${METABASE_ADMIN:-admin@admin.com} (credenciales .env)"'
        ),
        (
            r'print_info "   🔑 Contraseña: Admin123!"',
            'print_info "   🔑 Contraseña: ${METABASE_PASSWORD:-Admin123!}"'
        )
    ]
    
    for pattern, replacement in replacements:
        content = re.sub(pattern, replacement, content)
    
    with open(file_path, 'w') as f:
        f.write(content)
    
    print("✅ start_etl_pipeline.sh corregido")

def create_credential_verification_script():
    """Crear script para verificar que las credenciales funcionen correctamente"""
    
    script_content = '''#!/usr/bin/env python3
"""
Script para verificar que el pipeline funcione con credenciales personalizadas
"""

import os
import requests
import subprocess
import time

def load_env_credentials():
    """Cargar todas las credenciales del .env"""
    credentials = {}
    
    try:
        with open('/mnt/c/proyectos/etl_prod/.env', 'r') as f:
            for line in f:
                line = line.strip()
                if '=' in line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    credentials[key.strip()] = value.strip()
    except Exception as e:
        print(f"⚠️ Error leyendo .env: {e}")
    
    return credentials

def test_superset_credentials(creds):
    """Probar credenciales de Superset"""
    print("🧪 Probando Superset...")
    
    try:
        # Intentar login
        login_data = {
            "username": creds.get('SUPERSET_ADMIN', 'admin'),
            "password": creds.get('SUPERSET_PASSWORD', 'Admin123!'),
            "provider": "db"
        }
        
        response = requests.post(
            "http://localhost:8088/api/v1/security/login",
            json=login_data,
            timeout=10
        )
        
        if response.status_code == 200:
            print("✅ Superset: Credenciales funcionan")
            return True
        else:
            print(f"❌ Superset: Error {response.status_code}")
            return False
            
    except Exception as e:
        print(f"⚠️ Superset: No disponible o error - {e}")
        return None

def test_metabase_credentials(creds):
    """Probar credenciales de Metabase"""
    print("🧪 Probando Metabase...")
    
    try:
        # Intentar login
        login_data = {
            "username": creds.get('METABASE_ADMIN', 'admin@admin.com'),
            "password": creds.get('METABASE_PASSWORD', 'Admin123!')
        }
        
        response = requests.post(
            "http://localhost:3000/api/session",
            json=login_data,
            timeout=10
        )
        
        if response.status_code == 200:
            print("✅ Metabase: Credenciales funcionan")
            return True
        else:
            print(f"❌ Metabase: Error {response.status_code}")
            return False
            
    except Exception as e:
        print(f"⚠️ Metabase: No disponible o error - {e}")
        return None

def test_clickhouse_credentials(creds):
    """Probar credenciales de ClickHouse"""
    print("🧪 Probando ClickHouse...")
    
    try:
        user = creds.get('CLICKHOUSE_DEFAULT_USER', 'default')
        password = creds.get('CLICKHOUSE_DEFAULT_PASSWORD', 'ClickHouse123!')
        
        # Usar curl para probar HTTP interface
        cmd = [
            'curl', '-s', 
            f'http://{user}:{password}@localhost:8123',
            '-d', 'SELECT 1'
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0 and '1' in result.stdout:
            print("✅ ClickHouse: Credenciales funcionan")
            return True
        else:
            print(f"❌ ClickHouse: Error en conexión")
            return False
            
    except Exception as e:
        print(f"⚠️ ClickHouse: Error - {e}")
        return None

def main():
    """Función principal de verificación"""
    
    print("🔐 VERIFICACIÓN DE CREDENCIALES PERSONALIZADAS")
    print("="*70)
    
    # Cargar credenciales
    creds = load_env_credentials()
    
    print("📋 CREDENCIALES CARGADAS:")
    important_creds = [
        'SUPERSET_ADMIN', 'SUPERSET_PASSWORD',
        'METABASE_ADMIN', 'METABASE_PASSWORD', 
        'CLICKHOUSE_DEFAULT_USER', 'CLICKHOUSE_DEFAULT_PASSWORD'
    ]
    
    for key in important_creds:
        value = creds.get(key, 'NO DEFINIDO')
        if 'PASSWORD' in key:
            print(f"   {key}: {value[:4]}***")
        else:
            print(f"   {key}: {value}")
    
    print("\\n🧪 PROBANDO CONEXIONES:")
    
    # Probar cada servicio
    superset_ok = test_superset_credentials(creds)
    metabase_ok = test_metabase_credentials(creds)
    clickhouse_ok = test_clickhouse_credentials(creds)
    
    # Resultado final
    print("\\n🎯 RESULTADO FINAL:")
    print("="*40)
    
    total_services = 0
    working_services = 0
    
    for service, status in [("Superset", superset_ok), ("Metabase", metabase_ok), ("ClickHouse", clickhouse_ok)]:
        if status is not None:
            total_services += 1
            if status:
                working_services += 1
                print(f"✅ {service}: Funcionando con credenciales personalizadas")
            else:
                print(f"❌ {service}: Problemas con credenciales")
        else:
            print(f"⏳ {service}: Servicio no disponible para prueba")
    
    print(f"\\n📊 Servicios funcionando: {working_services}/{total_services}")
    
    if working_services == total_services and total_services > 0:
        print("\\n🎉 ¡TODAS LAS CREDENCIALES FUNCIONAN CORRECTAMENTE!")
        print("✅ Puedes cambiar las credenciales en .env sin problemas")
        return True
    else:
        print("\\n⚠️  ALGUNAS CREDENCIALES REQUIEREN ATENCIÓN")
        print("💡 Verifica que los servicios estén corriendo y configurados correctamente")
        return False

if __name__ == "__main__":
    main()
'''
    
    with open('/mnt/c/proyectos/etl_prod/tools/verify_custom_credentials.py', 'w') as f:
        f.write(script_content)
    
    print("✅ Script de verificación creado: tools/verify_custom_credentials.py")

def main():
    """Función principal de corrección"""
    
    print("🔧 CORRECCIÓN AUTOMÁTICA DE ARCHIVOS CRÍTICOS")
    print("="*70)
    
    # Corregir archivos críticos
    try:
        fix_metabase_add_clickhouse()
        fix_schema_cleaner()  
        fix_robust_service_tester()
        fix_pipeline_script()
        create_credential_verification_script()
        
        print("\\n✅ CORRECCIÓN COMPLETADA")
        print("="*40)
        print("🔧 Archivos corregidos:")
        print("   • tools/metabase_add_clickhouse.py")
        print("   • tools/schema_cleaner.py")
        print("   • tools/robust_service_tester.py") 
        print("   • start_etl_pipeline.sh")
        
        print("\\n📝 Script de verificación creado:")
        print("   • tools/verify_custom_credentials.py")
        
        print("\\n🎯 PRÓXIMOS PASOS:")
        print("1. Cambiar credenciales en .env por las institucionales")
        print("2. Ejecutar: python3 tools/verify_custom_credentials.py")
        print("3. Ejecutar pipeline: ./start_etl_pipeline.sh")
        print("4. Corregir scripts específicos que fallen")
        
        print("\\n💡 SCRIPTS DE PRUEBA RECOMENDADOS PARA CORREGIR:")
        print("   • tools/validate_*.py")
        print("   • tools/test_*.py") 
        print("   • tools/debug_*.py")
        print("   (Estos usan os.getenv() pero podrían necesitar ajustes)")
        
    except Exception as e:
        print(f"❌ Error durante la corrección: {e}")
        return False
    
    return True

if __name__ == "__main__":
    main()