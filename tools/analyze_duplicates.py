#!/usr/bin/env python3
"""
🔍 ANÁLISIS DE SCRIPTS DUPLICADOS
===============================

Compara los dos scripts de inicio principales y recomienda consolidación.
"""

import os
from pathlib import Path

def analyze_script_differences():
    """Analiza las diferencias entre los scripts de inicio"""
    
    root = Path(__file__).resolve().parent.parent
    
    scripts = {
        'start_etl_pipeline.sh': {
            'path': root / 'start_etl_pipeline.sh',
            'size': 0,
            'lines': 0,
            'features': []
        },
        'tools/start_pipeline.sh': {
            'path': root / 'tools/start_pipeline.sh', 
            'size': 0,
            'lines': 0,
            'features': []
        }
    }
    
    print("🔍 ANÁLISIS DE SCRIPTS DE INICIO DUPLICADOS")
    print("=" * 60)
    
    for name, info in scripts.items():
        if info['path'].exists():
            with open(info['path']) as f:
                content = f.read()
                info['size'] = len(content)
                info['lines'] = len(content.splitlines())
                
                # Detectar características
                if '--clean' in content:
                    info['features'].append('Opción --clean')
                if '--validate-only' in content:
                    info['features'].append('Opción --validate-only') 
                if '--manual' in content:
                    info['features'].append('Opción --manual')
                if 'docker compose up' in content:
                    info['features'].append('Docker Compose')
                if 'health check' in content.lower():
                    info['features'].append('Health checks')
                if 'orquestación' in content.lower():
                    info['features'].append('Orquestación automática')
                    
            print(f"\n📄 {name}:")
            print(f"   📏 Tamaño: {info['size']} bytes, {info['lines']} líneas")
            print(f"   🎛️  Características:")
            for feature in info['features']:
                print(f"      • {feature}")
        else:
            print(f"\n❌ {name}: NO EXISTE")
    
    print("\n🎯 RECOMENDACIONES:")
    
    # Determinar cuál es mejor
    etl_script = scripts['start_etl_pipeline.sh']
    tools_script = scripts['tools/start_pipeline.sh']
    
    if etl_script['lines'] > tools_script['lines']:
        print("✅ USAR: start_etl_pipeline.sh (más completo)")
        print("❌ DEPRECAR: tools/start_pipeline.sh")
        print("\n📋 ACCIONES:")
        print("1. Verificar que start_etl_pipeline.sh tenga todas las características necesarias")
        print("2. Mover tools/start_pipeline.sh a deprecated/")
        print("3. Crear symlink si hay dependencias externas")
    else:
        print("✅ USAR: tools/start_pipeline.sh (más estructurado)")
        print("❌ DEPRECAR: start_etl_pipeline.sh")
        print("\n📋 ACCIONES:")
        print("1. Verificar que tools/start_pipeline.sh tenga todas las características necesarias")
        print("2. Mover start_etl_pipeline.sh a deprecated/")
        print("3. Crear symlink desde raíz a tools/start_pipeline.sh")


if __name__ == "__main__":
    analyze_script_differences()