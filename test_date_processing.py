#!/usr/bin/env python3
"""
Test directo del procesamiento de campos DATE para verificar que funciona correctamente
"""

import pandas as pd
import numpy as np
from datetime import datetime, date
import sys
import os

# Agregar el directorio tools al path para importar la función
sys.path.append('/app/tools')

# Simular datos de prueba como los que vienen de MySQL catalogosgral.agencias
def test_date_processing():
    print("🧪 INICIANDO TEST DE PROCESAMIENTO DE FECHAS DATE")
    print("=" * 60)
    
    # Datos de prueba simulando catalogosgral.agencias con fechas problemáticas
    test_data = {
        'id': [1, 2, 3, 4, 5],
        'codagen': ['001', '002', '003', '004', '005'],
        'fechaini': [
            date(1900, 1, 1),    # Fecha inválida (≤1900) - debe convertirse a NULL
            date(2020, 5, 15),   # Fecha válida - debe mantenerse
            None,                # NULL - debe mantenerse como NULL
            date(1900, 1, 1),    # Otra fecha inválida - debe convertirse a NULL
            date(2023, 8, 10)    # Fecha válida - debe mantenerse
        ],
        'fechafin': [
            date(1900, 1, 1),    # Fecha inválida - debe convertirse a NULL
            date(1900, 1, 1),    # Fecha inválida - debe convertirse a NULL  
            date(2021, 12, 31),  # Fecha válida - debe mantenerse
            None,                # NULL - debe mantenerse como NULL
            date(1900, 1, 1)     # Fecha inválida - debe convertirse a NULL
        ]
    }
    
    # Crear DataFrame de prueba
    df = pd.DataFrame(test_data)
    print("📋 Datos de prueba (simulando MySQL catalogosgral.agencias):")
    print(df)
    print(f"\nTipos originales:\n{df.dtypes}")
    print()
    
    # Simular la metadata de MySQL para campos DATE
    mysql_metadata = {
        'id': {'type': 'integer', 'is_primary': True},
        'codagen': {'type': 'varchar(10)', 'is_primary': False},
        'fechaini': {'type': 'date', 'is_primary': False},      # Campo DATE MySQL
        'fechafin': {'type': 'date', 'is_primary': False}       # Campo DATE MySQL
    }
    
    # Importar y ejecutar la función process_mysql_date_columns
    try:
        from ingest_runner import process_mysql_date_columns
        print("✅ Función process_mysql_date_columns importada exitosamente")
        
        # Procesar campos DATE
        result_df = process_mysql_date_columns(df.copy(), mysql_metadata)
        
        print("\n📊 RESULTADOS DESPUÉS DEL PROCESAMIENTO:")
        print("=" * 50)
        print(result_df)
        print(f"\nTipos después del procesamiento:\n{result_df.dtypes}")
        
        # Verificar resultados esperados
        print("\n🔍 VERIFICACIÓN DE RESULTADOS:")
        print("-" * 40)
        
        # Contar NULLs en fechaini (debe ser 3: las dos 1900-01-01 + el NULL original)
        fechaini_nulls = result_df['fechaini'].isna().sum()
        print(f"fechaini NULLs: {fechaini_nulls} (esperado: 3)")
        
        # Contar valores válidos en fechaini (debe ser 2: 2020-05-15 y 2023-08-10)
        fechaini_valid = result_df['fechaini'].notna().sum()
        print(f"fechaini válidos: {fechaini_valid} (esperado: 2)")
        
        # Contar NULLs en fechafin (debe ser 4: tres 1900-01-01 + el NULL original)
        fechafin_nulls = result_df['fechafin'].isna().sum()
        print(f"fechafin NULLs: {fechafin_nulls} (esperado: 4)")
        
        # Contar valores válidos en fechafin (debe ser 1: solo 2021-12-31)
        fechafin_valid = result_df['fechafin'].notna().sum()
        print(f"fechafin válidos: {fechafin_valid} (esperado: 1)")
        
        # Verificación de éxito
        success = (fechaini_nulls == 3 and fechaini_valid == 2 and 
                  fechafin_nulls == 4 and fechafin_valid == 1)
        
        if success:
            print("\n🎉 ¡TEST EXITOSO! La función process_mysql_date_columns funciona correctamente")
            print("✅ Las fechas inválidas (1900-01-01) se convirtieron a NULL")
            print("✅ Las fechas válidas se mantuvieron intactas")
            print("✅ Los valores NULL originales se preservaron")
        else:
            print("\n❌ TEST FALLÓ - Los resultados no coinciden con lo esperado")
            
        return success
        
    except ImportError as e:
        print(f"❌ Error importando la función: {e}")
        return False
    except Exception as e:
        print(f"❌ Error durante el procesamiento: {e}")
        return False

if __name__ == "__main__":
    test_date_processing()