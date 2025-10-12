#!/usr/bin/env python3
"""
Script de aplicación del limpiador robusto de datos.
Integra el limpiador con el pipeline ETL existente y proporciona funciones
para ser usadas por ingest_runner.py y otros componentes.
"""

import sys
import os
from pathlib import Path

# Agregar el directorio de herramientas al path
sys.path.insert(0, str(Path(__file__).parent))

from data_cleaner_robust import DataCleanerRobust
import logging

logger = logging.getLogger(__name__)

# Instancia global del limpiador
_cleaner = None

def get_cleaner():
    """Obtener instancia singleton del limpiador."""
    global _cleaner
    if _cleaner is None:
        _cleaner = DataCleanerRobust()
    return _cleaner

def preprocess_dataframe_for_clickhouse(df, table_schema=None):
    """
    Función de reemplazo para ingest_runner.py que usa el limpiador robusto.
    
    Args:
        df: DataFrame de pandas a limpiar
        table_schema: Esquema de la tabla (opcional)
        
    Returns:
        DataFrame limpio y procesado
    """
    cleaner = get_cleaner()
    return cleaner.clean_dataframe(df, table_schema)

def enhanced_cell_processor(value, column_info=None):
    """
    Procesador de celdas mejorado para usar en lugar del original.
    
    Args:
        value: Valor a procesar
        column_info: Información de la columna
        
    Returns:
        Valor procesado
    """
    cleaner = get_cleaner()
    return cleaner.enhanced_cell_processor(value, column_info)

def apply_robust_cleaning_patch():
    """
    Aplicar el parche de limpieza robusta al sistema existente.
    Reemplaza las funciones en ingest_runner.py con versiones mejoradas.
    """
    try:
        # Intentar importar y patchear ingest_runner
        import ingest_runner
        
        # Reemplazar funciones existentes
        ingest_runner.preprocess_dataframe_for_clickhouse = preprocess_dataframe_for_clickhouse
        
        # También patchear la función cell si existe
        if hasattr(ingest_runner, 'cell'):
            ingest_runner.cell = enhanced_cell_processor
            
        logger.info("✅ Parche de limpieza robusta aplicado exitosamente a ingest_runner")
        return True
        
    except ImportError as e:
        logger.warning(f"⚠ No se pudo importar ingest_runner: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Error aplicando parche robusto: {e}")
        return False

def analyze_database_tables(db_connections, output_dir="/app/logs", limit=1000):
    """
    Analizar todas las tablas de las conexiones de base de datos.
    
    Args:
        db_connections: Lista de conexiones de base de datos
        output_dir: Directorio para guardar reportes
        limit: Número de filas a analizar por tabla
        
    Returns:
        Diccionario con análisis de todas las tablas
    """
    cleaner = get_cleaner()
    all_analyses = {}
    
    for conn in db_connections:
        try:
            # Construir URL de conexión
            url = f"mysql+pymysql://{conn['user']}:{conn['pass']}@{conn['host']}:{conn['port']}/{conn['db']}"
            
            # Obtener lista de tablas
            from sqlalchemy import create_engine, text
            engine = create_engine(url)
            
            with engine.connect() as connection:
                result = connection.execute(text("SHOW TABLES"))
                tables = [row[0] for row in result]
            
            # Analizar cada tabla
            for table in tables:
                table_key = f"{conn['name']}.{conn['db']}.{table}"
                logger.info(f"Analizando tabla: {table_key}")
                
                analysis = cleaner.analyze_table(url, conn['db'], table, limit)
                all_analyses[table_key] = analysis
                
                # Guardar análisis individual
                table_report_path = Path(output_dir) / f"analysis_{conn['name']}_{table}.json"
                with open(table_report_path, 'w', encoding='utf-8') as f:
                    import json
                    json.dump(analysis, f, indent=2, ensure_ascii=False)
            
            engine.dispose()
            
        except Exception as e:
            logger.error(f"Error analizando conexión {conn['name']}: {e}")
            all_analyses[f"{conn['name']}_ERROR"] = {'error': str(e)}
    
    # Guardar análisis completo
    complete_report_path = Path(output_dir) / "complete_data_analysis.json"
    with open(complete_report_path, 'w', encoding='utf-8') as f:
        import json
        json.dump(all_analyses, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Análisis completo guardado en: {complete_report_path}")
    return all_analyses

def generate_cleaning_summary():
    """Generar resumen de estadísticas de limpieza."""
    cleaner = get_cleaner()
    return cleaner.generate_cleaning_report()

def main():
    """Función principal para uso directo del script."""
    import argparse
    import json
    
    parser = argparse.ArgumentParser(description='Aplicar limpieza robusta de datos')
    parser.add_argument('--patch', action='store_true', help='Aplicar parche al sistema')
    parser.add_argument('--analyze-all', action='store_true', help='Analizar todas las tablas de DB_CONNECTIONS')
    parser.add_argument('--connections-file', default='/app/.env', help='Archivo de configuración')
    parser.add_argument('--output-dir', default='/app/logs', help='Directorio de salida')
    parser.add_argument('--limit', type=int, default=1000, help='Límite de filas para análisis')
    
    args = parser.parse_args()
    
    if args.patch:
        success = apply_robust_cleaning_patch()
        if success:
            print("✅ Parche aplicado exitosamente")
        else:
            print("❌ Error aplicando parche")
            sys.exit(1)
    
    if args.analyze_all:
        # Leer configuración de DB_CONNECTIONS
        try:
            import os
            from dotenv import load_dotenv
            
            load_dotenv(args.connections_file)
            db_connections_str = os.getenv('DB_CONNECTIONS', '[]')
            db_connections = json.loads(db_connections_str)
            
            if not db_connections:
                print("❌ No se encontraron conexiones en DB_CONNECTIONS")
                sys.exit(1)
            
            print(f"📊 Analizando {len(db_connections)} conexiones...")
            analyses = analyze_database_tables(db_connections, args.output_dir, args.limit)
            
            print(f"✅ Análisis completado. Resultados en: {args.output_dir}")
            
            # Mostrar resumen
            total_tables = len([k for k in analyses.keys() if not k.endswith('_ERROR')])
            errors = len([k for k in analyses.keys() if k.endswith('_ERROR')])
            print(f"📈 Tablas analizadas: {total_tables}")
            if errors > 0:
                print(f"⚠ Errores: {errors}")
                
        except Exception as e:
            print(f"❌ Error en análisis: {e}")
            sys.exit(1)

if __name__ == "__main__":
    main()