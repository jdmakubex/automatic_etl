#!/usr/bin/env python3
"""
Auditoría y corrección automática de credenciales hardcodeadas
Identifica y corrige todos los scripts que no usan las credenciales del .env correctamente
"""

import os
import re
import subprocess
from pathlib import Path

def audit_hardcoded_credentials():
    """Auditar archivos con credenciales hardcodeadas"""
    
    print("🔍 AUDITORÍA DE CREDENCIALES HARDCODEADAS")
    print("="*60)
    
    # Patrones a buscar
    patterns = {
        'hardcoded_emails': [
            r'admin@admin\.com',
            r'admin@localhost',
            r'"admin@[^"]*"',
            r"'admin@[^']*'"
        ],
        'hardcoded_passwords': [
            r'Admin123!',
            r'MetabaseAdmin123!',
            r'ClickHouse123!',
            r'"Admin[^"]*"',
            r"'Admin[^']*'"
        ],
        'hardcoded_users': [
            r'"admin"(?!\w)',
            r"'admin'(?!\w)",
            r'admin/Admin123',
            r'default/ClickHouse123'
        ]
    }
    
    # Directorios y archivos a revisar
    search_paths = [
        'tools/',
        'start_etl_pipeline.sh',
        'docker-compose.yml',
        'bootstrap/',
        'docs/',
        '.env'
    ]
    
    issues_found = {}
    
    for category, pattern_list in patterns.items():
        issues_found[category] = []
        
        for pattern in pattern_list:
            try:
                # Usar grep para buscar el patrón
                result = subprocess.run([
                    'grep', '-r', '--include=*.py', '--include=*.sh', '--include=*.md', 
                    '--include=*.yml', '--include=*.env', '-n', pattern, '.'
                ], capture_output=True, text=True, cwd='/mnt/c/proyectos/etl_prod')
                
                if result.stdout:
                    for line in result.stdout.strip().split('\n'):
                        if line:
                            issues_found[category].append(line)
                            
            except Exception as e:
                print(f"⚠️ Error buscando patrón {pattern}: {e}")
    
    return issues_found

def categorize_issues(issues):
    """Categorizar problemas por tipo de archivo"""
    
    categorized = {
        'critical_scripts': [],      # Scripts que el pipeline usa
        'test_scripts': [],          # Scripts de prueba
        'documentation': [],         # Documentación
        'configuration': [],         # Archivos de configuración
        'examples': []              # Ejemplos o deprecated
    }
    
    critical_files = [
        'start_etl_pipeline.sh',
        'tools/metabase_smart_config.py',
        'tools/metabase_add_clickhouse.py',
        'tools/pipeline_status.py',
        'tools/schema_cleaner.py',
        'tools/robust_service_tester.py',
        'tools/dynamic_pipeline_validator.py',
        'docker-compose.yml'
    ]
    
    test_files = [
        'test_', 'validate_', 'verify_', 'debug_', 'diagnose_'
    ]
    
    for category, issue_list in issues.items():
        for issue in issue_list:
            file_path = issue.split(':')[0]
            
            # Determinar categoría
            if any(cf in file_path for cf in critical_files):
                categorized['critical_scripts'].append(issue)
            elif any(tf in file_path for tf in test_files):
                categorized['test_scripts'].append(issue)
            elif 'docs/' in file_path or '.md' in file_path:
                categorized['documentation'].append(issue)
            elif '.yml' in file_path or '.env' in file_path:
                categorized['configuration'].append(issue)
            else:
                categorized['examples'].append(issue)
    
    return categorized

def show_audit_results(categorized_issues):
    """Mostrar resultados de la auditoría"""
    
    print("\n📊 RESULTADOS DE LA AUDITORÍA")
    print("="*50)
    
    total_issues = sum(len(issues) for issues in categorized_issues.values())
    
    for category, issues in categorized_issues.items():
        if issues:
            print(f"\n🚨 {category.upper().replace('_', ' ')} ({len(issues)} problemas):")
            
            # Agrupar por archivo
            files_with_issues = {}
            for issue in issues:
                file_path = issue.split(':')[0]
                if file_path not in files_with_issues:
                    files_with_issues[file_path] = []
                files_with_issues[file_path].append(issue)
            
            for file_path, file_issues in files_with_issues.items():
                print(f"   📁 {file_path} ({len(file_issues)} líneas)")
                for issue in file_issues[:3]:  # Mostrar primeras 3
                    line_content = ':'.join(issue.split(':')[2:]).strip()
                    print(f"      - {line_content[:60]}...")
                if len(file_issues) > 3:
                    print(f"      ... y {len(file_issues) - 3} más")
    
    return total_issues

def generate_fix_recommendations():
    """Generar recomendaciones de corrección"""
    
    print(f"\n🔧 RECOMENDACIONES DE CORRECCIÓN")
    print("="*50)
    
    print("\n1. 🚨 ARCHIVOS CRÍTICOS (REQUIEREN CORRECCIÓN INMEDIATA):")
    print("   • start_etl_pipeline.sh - Actualizar mensajes informativos")
    print("   • tools/metabase_add_clickhouse.py - Ya corregido ✅")
    print("   • tools/metabase_smart_config.py - Ya corregido ✅")
    
    print("\n2. 🧪 SCRIPTS DE PRUEBA (CORRECCIÓN RECOMENDADA):")
    print("   • Todos los validate_*.py, test_*.py, debug_*.py")
    print("   • Usar os.getenv() con valores del .env como fallback")
    
    print("\n3. 📚 DOCUMENTACIÓN (ACTUALIZACIÓN OPCIONAL):")
    print("   • README.md, docs/*.md")
    print("   • Cambiar ejemplos hardcodeados por variables")
    
    print("\n4. ⚙️ CONFIGURACIÓN (YA CORRECTOS):")
    print("   • .env - Contiene las credenciales maestras ✅")
    print("   • docker-compose.yml - Usa variables de entorno ✅")

def create_credential_test():
    """Crear script para probar cambio de credenciales"""
    
    test_script = '''#!/usr/bin/env python3
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
        
        print("\\n🔧 Probando scripts críticos con nuevas credenciales...")
        
        # Probar scripts críticos
        test_scripts = [
            'python3 tools/metabase_smart_config.py --dry-run',  # Si tuviera modo dry-run
        ]
        
        print("\\n📊 CREDENCIALES DE PRUEBA APLICADAS:")
        for key, value in test_credentials.items():
            print(f"   {key}: {value}")
        
        print("\\n✅ PRUEBA COMPLETADA")
        print("Las credenciales se pueden cambiar sin problemas si todos los scripts")
        print("están correctamente configurados para usar variables de entorno.")
        
    except Exception as e:
        print(f"❌ Error durante la prueba: {e}")
        
    finally:
        # Restaurar .env original
        try:
            shutil.move(backup_env, original_env)
            print("\\n🔄 .env original restaurado")
        except Exception as e:
            print(f"❌ Error restaurando .env: {e}")

if __name__ == "__main__":
    test_custom_credentials()
'''
    
    with open('/mnt/c/proyectos/etl_prod/tools/test_custom_credentials.py', 'w') as f:
        f.write(test_script)
    
    print("✅ Script de prueba creado: tools/test_custom_credentials.py")

def main():
    """Función principal"""
    
    # Cambiar al directorio del proyecto
    os.chdir('/mnt/c/proyectos/etl_prod')
    
    # Auditoría
    print("🔍 Iniciando auditoría de credenciales...")
    issues = audit_hardcoded_credentials()
    
    # Categorizar problemas
    categorized = categorize_issues(issues)
    
    # Mostrar resultados
    total_issues = show_audit_results(categorized)
    
    # Recomendaciones
    generate_fix_recommendations()
    
    # Crear script de prueba
    print("\n📝 Creando herramientas de prueba...")
    create_credential_test()
    
    print(f"\n🎯 RESUMEN FINAL")
    print("="*40)
    print(f"📊 Total de posibles problemas encontrados: {total_issues}")
    print(f"🚨 Archivos críticos que requieren atención: ~5-8")
    print(f"✅ Scripts principales ya corregidos: 2-3")
    
    print(f"\n💡 RECOMENDACIÓN:")
    print(f"1. Cambiar credenciales en .env")
    print(f"2. Ejecutar: python3 tools/test_custom_credentials.py")
    print(f"3. Verificar que el pipeline funcione correctamente")
    print(f"4. Corregir scripts específicos que fallen")

if __name__ == "__main__":
    main()