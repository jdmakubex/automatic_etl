#!/usr/bin/env python3
"""
Script para integrar el limpiador robusto con ingest_runner.py existente.
Aplica parches en tiempo de ejecución para mejorar el manejo de datos.
"""

import sys
import os
from pathlib import Path
import logging

# Asegurar que el directorio de herramientas esté en el path
sys.path.insert(0, str(Path(__file__).parent))

logger = logging.getLogger(__name__)

def patch_ingest_runner():
    """
    Aplicar parches al ingest_runner.py para usar limpieza robusta.
    """
    try:
        # Importar módulos necesarios
        import ingest_runner
        from data_cleaner_robust import DataCleanerRobust
        import pandas as pd
        import numpy as np
        import datetime
        
        # Crear instancia del limpiador
        cleaner = DataCleanerRobust()
        
        # Guardar función original para preservar funcionalidad existente
        original_preprocess = ingest_runner.preprocess_dataframe_for_clickhouse
        original_cell = getattr(ingest_runner, 'cell', None)
        
        def enhanced_preprocess_dataframe_for_clickhouse(df, primary_keys=None, column_types=None):
            """
            Versión mejorada que combina la lógica existente con limpieza robusta.
            """
            logger.info(f"🔧 Aplicando limpieza robusta a DataFrame con {len(df)} filas")
            
            # Primero aplicar la lógica original para preservar la funcionalidad de PK
            df_processed = original_preprocess(df, primary_keys, column_types)
            
            # Luego aplicar limpieza robusta adicional
            # Crear esquema de tabla para el limpiador
            table_schema = {
                'columns': []
            }
            
            if column_types:
                for col, mysql_type in column_types.items():
                    # Mapear tipos MySQL a ClickHouse
                    clickhouse_type = map_mysql_to_clickhouse_type(mysql_type)
                    is_nullable = col not in (primary_keys or set())
                    
                    table_schema['columns'].append({
                        'name': col,
                        'mysql_type': mysql_type,
                        'clickhouse_type': clickhouse_type,
                        'nullable': is_nullable,
                        'is_primary_key': col in (primary_keys or set())
                    })
            
            # Aplicar limpieza robusta adicional (especialmente para codificación)
            df_final = cleaner.clean_dataframe(df_processed, table_schema)
            
            logger.info(f"✅ Limpieza robusta completada. Estadísticas: {cleaner.stats}")
            return df_final
        
        def enhanced_cell_processor(v):
            """
            Procesador de celdas mejorado que combina la lógica existente con limpieza robusta.
            """
            # Aplicar limpieza robusta primero
            cleaned_value = cleaner.enhanced_cell_processor(v)
            
            # Si el limpiador devuelve None, aplicar la lógica original si es necesario
            if cleaned_value is None and original_cell:
                try:
                    return original_cell(v)
                except:
                    return None
            
            return cleaned_value
        
        def map_mysql_to_clickhouse_type(mysql_type):
            """Mapear tipos MySQL a tipos ClickHouse."""
            mysql_type = mysql_type.lower()
            
            if 'tinyint' in mysql_type:
                return 'Int8'
            elif 'smallint' in mysql_type:
                return 'Int16'
            elif 'mediumint' in mysql_type or 'int(' in mysql_type:
                return 'Int32'
            elif 'bigint' in mysql_type:
                return 'Int64'
            elif 'float' in mysql_type:
                return 'Float32'
            elif 'double' in mysql_type or 'decimal' in mysql_type:
                return 'Float64'
            elif any(t in mysql_type for t in ['varchar', 'text', 'char', 'blob']):
                return 'String'
            elif 'datetime' in mysql_type or 'timestamp' in mysql_type:
                return 'DateTime'
            elif 'date' in mysql_type:
                return 'Date'
            else:
                return 'String'  # Default
        
        # Aplicar parches
        ingest_runner.preprocess_dataframe_for_clickhouse = enhanced_preprocess_dataframe_for_clickhouse
        
        if original_cell:
            # Crear nueva función cell mejorada en el módulo
            def enhanced_cell(v):
                return enhanced_cell_processor(v)
            
            # Buscar todas las referencias a 'cell' en el módulo y reemplazarlas
            for attr_name in dir(ingest_runner):
                attr = getattr(ingest_runner, attr_name)
                if callable(attr) and hasattr(attr, '__code__'):
                    # Si la función usa 'cell', necesitamos reemplazarla en el contexto global
                    if 'cell' in attr.__code__.co_names:
                        # Actualizar el namespace global de la función
                        if hasattr(attr, '__globals__'):
                            attr.__globals__['cell'] = enhanced_cell
            
            # También asignar directamente
            ingest_runner.cell = enhanced_cell
        
        logger.info("✅ Parches aplicados exitosamente a ingest_runner.py")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error aplicando parches a ingest_runner: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Función principal para aplicar parches."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Integrar limpieza robusta con ingest_runner')
    parser.add_argument('--apply', action='store_true', help='Aplicar parches')
    parser.add_argument('--test', action='store_true', help='Probar integración')
    
    args = parser.parse_args()
    
    if args.apply or args.test:
        print("🔧 Aplicando parches al sistema de ingesta...")
        success = patch_ingest_runner()
        
        if success:
            print("✅ Parches aplicados exitosamente")
            
            if args.test:
                print("🧪 Ejecutando pruebas de integración...")
                try:
                    # Prueba básica
                    import pandas as pd
                    import ingest_runner
                    
                    # Crear DataFrame de prueba con problemas comunes
                    test_data = {
                        'id': [1, 2, 3, None, 'nan'],
                        'name': ['José García', 'María López', b'\x80\x9c\x80\x9d', '', None],
                        'value': [100.5, None, 'invalid', 999999999999, -999999999999],
                        'date_field': ['2023-01-01', None, 'invalid_date', '0000-00-00', '2023-12-31 23:59:59']
                    }
                    df_test = pd.DataFrame(test_data)
                    
                    print(f"📊 DataFrame de prueba: {len(df_test)} filas")
                    print("Tipos originales:", df_test.dtypes.to_dict())
                    
                    # Aplicar preprocesamiento
                    df_processed = ingest_runner.preprocess_dataframe_for_clickhouse(
                        df_test, 
                        primary_keys={'id'}, 
                        column_types={'id': 'int(11)', 'name': 'varchar(255)', 'value': 'decimal(10,2)', 'date_field': 'datetime'}
                    )
                    
                    print("✅ Preprocesamiento completado sin errores")
                    print("Tipos procesados:", df_processed.dtypes.to_dict())
                    print("\nMuestra de datos procesados:")
                    print(df_processed.head())
                    
                except Exception as e:
                    print(f"❌ Error en pruebas: {e}")
                    import traceback
                    traceback.print_exc()
        else:
            print("❌ Error aplicando parches")
            sys.exit(1)
    else:
        print("Uso: python integrate_robust_cleaning.py --apply [--test]")

if __name__ == "__main__":
    main()