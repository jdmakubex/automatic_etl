#!/usr/bin/env python3
"""
Script de debug para analizar valores que causan problemas de serialización 
en columnas Nullable(Int32) de ClickHouse.
"""

import pandas as pd
import pymysql
import numpy as np
import os
from typing import Any, List, Dict

def analyze_problematic_columns():
    """Analiza las columnas que causan errores de serialización."""
    
    # Configuración de conexión MySQL 
    connection_config = {
        'host': '172.21.61.53',
        'port': 3306,
        'user': 'juan.marcos',
        'password': '123456',
        'database': 'archivos',
        'charset': 'utf8mb4'
    }
    
    # Tablas y columnas problemáticas identificadas en los logs
    problematic_tables = {
        'archivos': ['tipo'],
        'archivosexpedientes': ['expedientestemp_id'],
        'archivosindiciados': ['indiciados_id'],
        'archivosnarcoticos': ['narcoticos_id']
    }
    
    print("🔍 Analizando valores problemáticos en columnas Nullable(Int32)...")
    print("="*70)
    
    for table, columns in problematic_tables.items():
        print(f"\n📋 Tabla: {table}")
        print("-" * 50)
        
        try:
            connection = pymysql.connect(**connection_config)
            
            for column in columns:
                print(f"\n🔧 Analizando columna: {column}")
                
                # Query para analizar tipos de valores
                query = f"""
                SELECT 
                    {column} as valor,
                    COUNT(*) as cantidad
                FROM {table} 
                GROUP BY {column}
                ORDER BY cantidad DESC
                LIMIT 20
                """
                
                df = pd.read_sql(query, connection)
                print(f"Distribución de valores en {column}:")
                for _, row in df.iterrows():
                    valor_type = type(row['valor']).__name__
                    print(f"  {repr(row['valor'])} (Python: {valor_type}) - {row['cantidad']} registros")
                
                # Analizar valores nulos y problemáticos específicos
                null_query = f"""
                SELECT COUNT(*) as total,
                       SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) as nulos,
                       SUM(CASE WHEN {column} = '' THEN 1 ELSE 0 END) as vacios,
                       SUM(CASE WHEN {column} = '0' THEN 1 ELSE 0 END) as ceros_string,
                       SUM(CASE WHEN {column} = 0 THEN 1 ELSE 0 END) as ceros_numeric
                FROM {table}
                """
                
                df_stats = pd.read_sql(null_query, connection)
                stats = df_stats.iloc[0]
                print(f"Estadísticas de {column}:")
                print(f"  Total registros: {stats['total']}")
                print(f"  Valores NULL: {stats['nulos']}")
                print(f"  Valores vacíos (''): {stats['vacios']}")
                print(f"  Ceros como string ('0'): {stats['ceros_string']}")
                print(f"  Ceros numéricos (0): {stats['ceros_numeric']}")
                
                # Buscar valores que no son enteros válidos
                non_int_query = f"""
                SELECT {column} as valor, COUNT(*) as cantidad
                FROM {table}
                WHERE {column} IS NOT NULL 
                  AND {column} != ''
                  AND ({column} REGEXP '[^0-9-]' OR {column} = '')
                GROUP BY {column}
                ORDER BY cantidad DESC
                LIMIT 10
                """
                
                df_non_int = pd.read_sql(non_int_query, connection)
                if not df_non_int.empty:
                    print(f"⚠️  Valores NO enteros en {column}:")
                    for _, row in df_non_int.iterrows():
                        print(f"  {repr(row['valor'])} - {row['cantidad']} registros")
                else:
                    print(f"✅ Todos los valores no nulos en {column} son enteros válidos")
                
                print()
            
            connection.close()
            
        except Exception as e:
            print(f"❌ Error analizando tabla {table}: {e}")
    
    print("\n" + "="*70)
    print("🧪 Análisis de serialización Python")
    print("="*70)
    
    # Simular problemas de serialización común
    test_values = [None, '', '0', 0, 'NULL', 'null', np.nan, pd.NA, float('nan')]
    
    for val in test_values:
        try:
            # Simular el procesamiento que hace nuestro código
            if val is None:
                result = None
            elif pd.isna(val):
                result = None
            elif isinstance(val, str) and val.strip() == "":
                result = None
            elif isinstance(val, str):
                try:
                    result = int(val)
                except:
                    result = None
            else:
                result = int(val) if not pd.isna(val) else None
            
            print(f"{repr(val):15} -> {repr(result):15} {'✅' if result is None or isinstance(result, int) else '❌'}")
            
        except Exception as e:
            print(f"{repr(val):15} -> ERROR: {e}")


if __name__ == "__main__":
    analyze_problematic_columns()