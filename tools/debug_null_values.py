#!/usr/bin/env python3
"""
Script de diagnóstico para valores nulos problemáticos en MySQL → ClickHouse
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine

def debug_table_values(connection_url, schema, table, limit=20):
    """Analiza los valores de una tabla para identificar problemas de tipos"""
    
    # Leer algunos datos de la tabla usando URL directamente
    sql = f"SELECT * FROM `{schema}`.`{table}` LIMIT {limit}"
    df = pd.read_sql(sql, connection_url)
    
    print(f"\n=== DEBUG: {schema}.{table} ===")
    print(f"Filas: {len(df)}")
    print("\nTipos de columnas:")
    print(df.dtypes)
    
    print("\nValores únicos por columna (primeros 10):")
    for col in df.columns:
        unique_vals = df[col].unique()[:10]
        print(f"{col}: {unique_vals}")
        
        # Verificar si hay valores problemáticos
        if df[col].dtype in ['object', 'int64', 'float64']:
            null_count = df[col].isnull().sum()
            print(f"  - Valores nulos: {null_count}")
            
            if df[col].dtype in ['int64', 'float64']:
                # Verificar rangos
                min_val = df[col].min()
                max_val = df[col].max()
                print(f"  - Rango: {min_val} a {max_val}")
                
                # Verificar valores fuera del rango Int32
                if df[col].dtype == 'int64':
                    out_of_range = ((df[col] < -2147483648) | (df[col] > 2147483647)).sum()
                    if out_of_range > 0:
                        print(f"  - ⚠️  Valores fuera de rango Int32: {out_of_range}")
    
    print("\nPrimeras 5 filas:")
    print(df.head())

if __name__ == "__main__":
    # Analizar la tabla problemática
    url = "mysql+pymysql://juan.marcos:123456@172.21.61.53:3306/archivos"
    debug_table_values(url, "archivos", "archivosexpedientes", limit=20)
    debug_table_values(url, "archivos", "archivos", limit=20)