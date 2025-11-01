#!/usr/bin/env python3
"""
Valida que todas las variables de entorno requeridas estén configuradas correctamente.
Evita errores por valores hardcodeados o variables faltantes.
"""
import os
from dotenv import load_dotenv

load_dotenv('/app/.env')

# Variables requeridas y sus descripciones
REQUIRED_VARS = {
    # ClickHouse
    'CLICKHOUSE_HOST': 'Hostname del servidor ClickHouse',
    'CLICKHOUSE_DEFAULT_USER': 'Usuario administrativo por defecto de ClickHouse',
    'CLICKHOUSE_DEFAULT_PASSWORD': 'Contraseña del usuario administrativo de ClickHouse',
    'CLICKHOUSE_ETL_USER': 'Usuario ETL para operaciones de ingesta',
    'CLICKHOUSE_ETL_PASSWORD': 'Contraseña del usuario ETL',
    'CLICKHOUSE_DATABASE': 'Base de datos principal para analytics',
    
    # Metabase
    'METABASE_URL': 'URL completa del servidor Metabase',
    'METABASE_ADMIN': 'Email del administrador de Metabase',
    'METABASE_PASSWORD': 'Contraseña del administrador de Metabase',
    
    # Superset
    'SUPERSET_URL': 'URL completa del servidor Superset',
    'SUPERSET_ADMIN': 'Usuario administrador de Superset',
    'SUPERSET_PASSWORD': 'Contraseña del administrador de Superset',
    
    # Kafka
    'KAFKA_BROKERS': 'Lista de brokers de Kafka',
    
    # Pipeline
    'MODE_INGEST': 'Modo de ingesta (cdc, batch, etc)',
    'DB_CONNECTIONS': 'Configuración JSON de conexiones a bases de datos fuente'
}

# Variables opcionales
OPTIONAL_VARS = {
    'COMPOSE_PROJECT_NAME': 'Nombre del proyecto Docker Compose',
    'ENABLE_VALIDATION': 'Habilitar validaciones automáticas',
    'ENABLE_LOGS': 'Habilitar logs detallados',
    'LOG_LEVEL': 'Nivel de logging'
}

def validate_environment():
    """Valida que todas las variables requeridas estén configuradas"""
    print("🔍 VALIDACIÓN DE VARIABLES DE ENTORNO")
    print("=" * 50)
    
    missing_vars = []
    configured_vars = []
    
    # Verificar variables requeridas
    print("📋 Variables requeridas:")
    for var, description in REQUIRED_VARS.items():
        value = os.getenv(var)
        if value:
            print(f"  ✅ {var}: Configurada")
            configured_vars.append(var)
        else:
            print(f"  ❌ {var}: FALTANTE - {description}")
            missing_vars.append((var, description))
    
    print(f"\n📋 Variables opcionales:")
    for var, description in OPTIONAL_VARS.items():
        value = os.getenv(var)
        if value:
            print(f"  ✅ {var}: {value}")
        else:
            print(f"  ⚪ {var}: No configurada (opcional)")
    
    # Validaciones específicas
    print(f"\n🔍 Validaciones específicas:")
    
    # Validar formato de DB_CONNECTIONS
    db_connections = os.getenv('DB_CONNECTIONS')
    if db_connections:
        try:
            import json
            connections = json.loads(db_connections)
            if isinstance(connections, list) and len(connections) > 0:
                print(f"  ✅ DB_CONNECTIONS: {len(connections)} conexiones configuradas")
            else:
                print(f"  ⚠️  DB_CONNECTIONS: Lista vacía o formato incorrecto")
        except json.JSONDecodeError:
            print(f"  ❌ DB_CONNECTIONS: JSON inválido")
            missing_vars.append(('DB_CONNECTIONS', 'Formato JSON válido requerido'))
    
    # Validar URLs
    for url_var in ['METABASE_URL', 'SUPERSET_URL']:
        url = os.getenv(url_var)
        if url and not url.startswith(('http://', 'https://')):
            print(f"  ⚠️  {url_var}: Debería comenzar con http:// o https://")
    
    # Validar emails
    metabase_admin = os.getenv('METABASE_ADMIN')
    if metabase_admin and '@' not in metabase_admin:
        print(f"  ⚠️  METABASE_ADMIN: No parece ser un email válido")
    
    # Mostrar resumen
    print(f"\n📊 RESUMEN:")
    print(f"  ✅ Variables configuradas: {len(configured_vars)}")
    print(f"  ❌ Variables faltantes: {len(missing_vars)}")
    
    if missing_vars:
        print(f"\n💥 VARIABLES FALTANTES:")
        for var, desc in missing_vars:
            print(f"  - {var}: {desc}")
        
        print(f"\n📝 Agrega estas variables al archivo .env:")
        for var, desc in missing_vars:
            print(f"  {var}=<valor_aquí>  # {desc}")
        
        return False
    else:
        print(f"\n🎉 ¡Todas las variables requeridas están configuradas!")
        return True

def check_no_hardcoded_values():
    """Verifica que no haya valores hardcodeados en scripts críticos"""
    print(f"\n🔍 VERIFICANDO VALORES HARDCODEADOS")
    print("=" * 50)
    
    # Archivos críticos a verificar
    critical_files = [
        'tools/metabase_query_test.py',
        'tools/metabase_diagnostic.py', 
        'tools/metabase_create_admin.py',
        'tools/metabase_create_dashboard.py',
        'tools/metabase_setup_ui.py',
        'tools/metabase_connect_clickhouse.py'
    ]
    
    hardcoded_patterns = [
        'admin@admin.com',
        'Admin123!',
        'http://metabase:3000',
        'http://superset:8088'
    ]
    
    issues_found = False
    
    for file_path in critical_files:
        if os.path.exists(f'/app/{file_path}'):
            with open(f'/app/{file_path}', 'r') as f:
                content = f.read()
                
            file_issues = []
            for pattern in hardcoded_patterns:
                if pattern in content and 'os.getenv' not in content.split(pattern)[0].split('\n')[-1]:
                    file_issues.append(pattern)
            
            if file_issues:
                print(f"  ❌ {file_path}: {', '.join(file_issues)}")
                issues_found = True
            else:
                print(f"  ✅ {file_path}: Sin valores hardcodeados")
    
    if not issues_found:
        print(f"  🎉 No se encontraron valores hardcodeados")
    
    return not issues_found

if __name__ == "__main__":
    print("🚀 VALIDACIÓN COMPLETA DE CONFIGURACIÓN")
    print("=" * 50)
    
    env_ok = validate_environment()
    hardcoded_ok = check_no_hardcoded_values()
    
    print(f"\n🎯 RESULTADO FINAL:")
    if env_ok and hardcoded_ok:
        print("✅ ¡Configuración válida y segura!")
        print("🚀 El pipeline está listo para ejecutarse")
        exit(0)
    else:
        print("❌ Se encontraron problemas de configuración")
        print("🔧 Revisa y corrige los errores antes de continuar")
        exit(1)