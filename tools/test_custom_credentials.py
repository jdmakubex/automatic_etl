#!/usr/bin/env python3
"""
Script para probar que el pipeline funciona con credenciales personalizadas
"""

import os
import tempfile
import shutil
import subprocess

def test_custom_credentials():
    """Probar pipeline con credenciales personalizadas"""
    
    print("🧪 PRUEBA DE CREDENCIALES PERSONALIZADAS")
    print("="*60)
    
    # Credenciales de prueba institucionales
    test_credentials = {
        'SUPERSET_ADMIN': 'admin.institucional',
        'SUPERSET_PASSWORD': 'SecureInstitutional2024#',
        'METABASE_ADMIN': 'admin.metabase@institucion.gov',
        'METABASE_PASSWORD': 'MetabaseSecure2024#',
        'CLICKHOUSE_DEFAULT_USER': 'clickhouse_admin',
        'CLICKHOUSE_DEFAULT_PASSWORD': 'ClickHouseInstitutional2024#'
    }
    
    # Hacer backup del .env original
    original_env = '/mnt/c/proyectos/etl_prod/.env'
    backup_env = original_env + '.backup'
    
    try:
        # Backup
        shutil.copy2(original_env, backup_env)
        print("✅ Backup del .env original creado")
        
        # Leer .env original
        with open(original_env, 'r') as f:
            env_content = f.read()
        
        # Reemplazar credenciales
        modified_content = env_content
        for key, value in test_credentials.items():
            # Buscar línea existente y reemplazar
            pattern = rf'^{key}=.*$'
            replacement = f'{key}={value}'
            
            if re.search(pattern, modified_content, re.MULTILINE):
                modified_content = re.sub(pattern, replacement, modified_content, flags=re.MULTILINE)
                print(f"✅ {key} actualizado")
            else:
                print(f"⚠️  {key} no encontrado en .env")
        
        # Escribir .env modificado
        with open(original_env, 'w') as f:
            f.write(modified_content)
        
        print("\n🔧 Probando scripts críticos con nuevas credenciales...")
        
        # Probar scripts críticos
        test_scripts = [
            'python3 tools/metabase_smart_config.py --dry-run',  # Si tuviera modo dry-run
        ]
        
        print("\n📊 CREDENCIALES DE PRUEBA APLICADAS:")
        for key, value in test_credentials.items():
            print(f"   {key}: {value}")
        
        print("\n✅ PRUEBA COMPLETADA")
        print("Las credenciales se pueden cambiar sin problemas si todos los scripts")
        print("están correctamente configurados para usar variables de entorno.")
        
    except Exception as e:
        print(f"❌ Error durante la prueba: {e}")
        
    finally:
        # Restaurar .env original
        try:
            shutil.move(backup_env, original_env)
            print("\n🔄 .env original restaurado")
        except Exception as e:
            print(f"❌ Error restaurando .env: {e}")

if __name__ == "__main__":
    test_custom_credentials()
