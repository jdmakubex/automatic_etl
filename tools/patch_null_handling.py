#!/usr/bin/env python3
"""
Patch para mejorar el manejo de tipos nulos en ingest_runner.py
"""

import re

def apply_patch():
    filename = '/app/tools/ingest_runner.py'
    
    # Leer el archivo actual
    with open(filename, 'r') as f:
        content = f.read()
    
    # Patch 1: Mejorar manejo de enteros numpy
    old_pattern = r'''        if isinstance\(v, np\.integer\):
            # Verificar si es un valor nulo de numpy antes de convertir
            try:
                if pd\.isna\(v\):
                    return None
                return int\(v\)
            except \(ValueError, OverflowError\):
                return None'''
    
    new_replacement = '''        if isinstance(v, np.integer):
            # Verificar si es un valor nulo de numpy antes de convertir
            try:
                if pd.isna(v):
                    return None
                int_val = int(v)
                # Validar rango Int32 para ClickHouse (-2^31 a 2^31-1)
                if int_val < -2147483648 or int_val > 2147483647:
                    return None  # Fuera del rango, insertar como NULL
                return int_val
            except (ValueError, OverflowError):
                return None'''
    
    content = re.sub(old_pattern, new_replacement, content, flags=re.MULTILINE)
    
    # Patch 2: Mejorar manejo de tipos pandas nullable
    old_pattern2 = r'''                try:
                    result = int\(v\) if 'Int' in str\(v\.dtype\) else float\(v\)
                    # Validar que el entero esté en un rango válido para ClickHouse Int32
                    if 'Int' in str\(v\.dtype\) and \(result < -2147483648 or result > 2147483647\):
                        return None  # Fuera del rango Int32, insertar como NULL
                    return result
                except \(ValueError, OverflowError\):
                    return None'''
    
    if old_pattern2 not in content:
        # Si no existe el patch, agregarlo
        old_pattern2_simple = r'''                return int\(v\) if 'Int' in str\(v\.dtype\) else float\(v\)'''
        new_replacement2 = '''                try:
                    result = int(v) if 'Int' in str(v.dtype) else float(v)
                    # Validar que el entero esté en un rango válido para ClickHouse Int32
                    if 'Int' in str(v.dtype) and (result < -2147483648 or result > 2147483647):
                        return None  # Fuera del rango Int32, insertar como NULL
                    return result
                except (ValueError, OverflowError):
                    return None'''
        
        content = re.sub(old_pattern2_simple, new_replacement2, content)
    
    # Patch 3: Agregar validación para enteros Python regulares
    insert_point = '''        # Manejo específico para tipos de pandas que pueden ser nulos'''
    new_validation = '''        # Validación adicional para enteros Python regulares
        if isinstance(v, int):
            # Validar rango Int32 para ClickHouse
            if v < -2147483648 or v > 2147483647:
                return None  # Fuera del rango, insertar como NULL
            return v
        
        # Manejo específico para tipos de pandas que pueden ser nulos'''
    
    content = content.replace(insert_point, new_validation)
    
    # Escribir el archivo parcheado
    with open(filename, 'w') as f:
        f.write(content)
    
    print("✅ Patch aplicado exitosamente")

if __name__ == "__main__":
    apply_patch()