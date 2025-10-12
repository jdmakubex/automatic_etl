#!/usr/bin/env python3
"""
Script robusto para limpieza, validación de tipado y corrección de codificación de datos.
Maneja problemas comunes en pipelines ETL MySQL → ClickHouse:

1. Valores nulos problemáticos
2. Tipos de datos incompatibles
3. Problemas de codificación (UTF-8, Latin-1, etc.)
4. Rangos de valores fuera de límites
5. Caracteres especiales y de control
6. Timestamps malformados
"""

import pandas as pd
import numpy as np
import datetime
import chardet
import re
import json
from typing import Any, Dict, List, Optional, Tuple
from sqlalchemy import create_engine, text
import logging
from pathlib import Path

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataCleanerRobust:
    """Limpiador robusto de datos para pipelines ETL"""
    
    def __init__(self, encoding_fallbacks: List[str] = None):
        """
        Inicializar el limpiador con configuraciones por defecto.
        
        Args:
            encoding_fallbacks: Lista de codificaciones a intentar en orden
        """
        self.encoding_fallbacks = encoding_fallbacks or [
            'utf-8', 'latin-1', 'cp1252', 'iso-8859-1', 'ascii'
        ]
        
        # Estadísticas de limpieza
        self.stats = {
            'null_fixes': 0,
            'encoding_fixes': 0,
            'type_conversions': 0,
            'range_fixes': 0,
            'character_cleanups': 0,
            'total_rows_processed': 0
        }
        
        # Configuración de tipos ClickHouse
        self.clickhouse_types = {
            'Int8': (-128, 127),
            'Int16': (-32768, 32767),
            'Int32': (-2147483648, 2147483647),
            'Int64': (-9223372036854775808, 9223372036854775807),
            'UInt8': (0, 255),
            'UInt16': (0, 65535),
            'UInt32': (0, 4294967295),
            'UInt64': (0, 18446744073709551615),
            'Float32': (-3.4e38, 3.4e38),
            'Float64': (-1.7e308, 1.7e308)
        }

    def detect_encoding(self, text: str) -> str:
        """
        Detectar y corregir la codificación de un texto.
        
        Args:
            text: Texto a analizar
            
        Returns:
            Texto con codificación corregida
        """
        if not isinstance(text, str):
            return text
            
        # Si ya es UTF-8 válido, devolver tal como está
        try:
            text.encode('utf-8')
            return text
        except UnicodeEncodeError:
            pass
        
        # Intentar detectar la codificación automáticamente
        if isinstance(text, bytes):
            detection = chardet.detect(text)
            detected_encoding = detection.get('encoding', 'utf-8')
            confidence = detection.get('confidence', 0)
            
            if confidence > 0.7:
                try:
                    return text.decode(detected_encoding)
                except (UnicodeDecodeError, LookupError):
                    pass
        
        # Probar codificaciones comunes en orden
        for encoding in self.encoding_fallbacks:
            try:
                if isinstance(text, bytes):
                    decoded = text.decode(encoding)
                else:
                    # Si es string, intentar re-codificar
                    decoded = text.encode('latin-1').decode(encoding)
                
                # Verificar que el resultado sea válido
                decoded.encode('utf-8')
                self.stats['encoding_fixes'] += 1
                return decoded
                
            except (UnicodeDecodeError, UnicodeEncodeError, LookupError):
                continue
        
        # Si todo falla, limpiar caracteres problemáticos
        if isinstance(text, bytes):
            text = text.decode('utf-8', errors='ignore')
        
        # Limpiar caracteres no imprimibles y de control
        cleaned = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', text)
        self.stats['character_cleanups'] += 1
        return cleaned

    def clean_string_value(self, value: Any) -> Optional[str]:
        """
        Limpiar y normalizar valores de cadena.
        
        Args:
            value: Valor a limpiar
            
        Returns:
            Cadena limpia o None si está vacía
        """
        if value is None:
            return None
            
        # Convertir a string si no lo es
        if not isinstance(value, str):
            if isinstance(value, bytes):
                value = self.detect_encoding(value)
            else:
                value = str(value)
        
        # Detectar y corregir codificación
        value = self.detect_encoding(value)
        
        # Limpiar espacios y caracteres especiales
        value = value.strip()
        
        # Si queda vacío después de limpiar, retornar None
        if not value:
            return None
            
        # Limpiar caracteres de control adicionales
        value = re.sub(r'[\r\n\t]+', ' ', value)  # Normalizar espacios en blanco
        value = re.sub(r'\s+', ' ', value)  # Múltiples espacios a uno solo
        
        return value

    def validate_numeric_range(self, value: Any, target_type: str) -> Any:
        """
        Validar que un valor numérico esté dentro del rango del tipo objetivo.
        
        Args:
            value: Valor numérico
            target_type: Tipo ClickHouse objetivo
            
        Returns:
            Valor ajustado al rango válido
        """
        if value is None or pd.isna(value):
            return None
            
        try:
            numeric_value = float(value)
            
            if np.isnan(numeric_value) or np.isinf(numeric_value):
                return None
                
            # Verificar rangos para tipos enteros
            if target_type in self.clickhouse_types:
                min_val, max_val = self.clickhouse_types[target_type]
                
                if numeric_value < min_val:
                    logger.warning(f"Valor {numeric_value} menor que {min_val}, ajustando")
                    self.stats['range_fixes'] += 1
                    return min_val
                elif numeric_value > max_val:
                    logger.warning(f"Valor {numeric_value} mayor que {max_val}, ajustando")
                    self.stats['range_fixes'] += 1
                    return max_val
            
            # Convertir a entero si es tipo entero
            if target_type.startswith(('Int', 'UInt')):
                return int(numeric_value)
            else:
                return float(numeric_value)
                
        except (ValueError, TypeError, OverflowError):
            logger.warning(f"Error convirtiendo valor {value} a {target_type}")
            return None

    def clean_datetime_value(self, value: Any) -> Optional[str]:
        """
        Limpiar y normalizar valores de fecha/hora, retornando string compatible con ClickHouse.
        
        Args:
            value: Valor de fecha/hora
            
        Returns:
            String en formato ISO (YYYY-MM-DD HH:MM:SS) o None
        """
        if value is None or pd.isna(value):
            return None
            
        try:
            # Si ya es datetime
            if isinstance(value, datetime.datetime):
                # Remover timezone si existe
                if value.tzinfo is not None:
                    value = value.replace(tzinfo=None)
                return value.strftime('%Y-%m-%d %H:%M:%S')
                
            # Si es date, convertir a datetime string
            if isinstance(value, datetime.date):
                dt = datetime.datetime(value.year, value.month, value.day)
                return dt.strftime('%Y-%m-%d %H:%M:%S')
                
            # Si es pandas Timestamp
            if isinstance(value, pd.Timestamp):
                if pd.isna(value):
                    return None
                if value.tzinfo is not None:
                    value = value.tz_localize(None)
                return value.strftime('%Y-%m-%d %H:%M:%S')
                
            # Si es numpy datetime64
            if isinstance(value, np.datetime64):
                if pd.isna(value):
                    return None
                ts = pd.Timestamp(value)
                if ts.tzinfo is not None:
                    ts = ts.tz_localize(None)
                return ts.strftime('%Y-%m-%d %H:%M:%S')
                
            # Si es string, intentar parsear
            if isinstance(value, str):
                value = value.strip()
                if not value:
                    return None
                    
                # Intentar formatos comunes
                formats = [
                    '%Y-%m-%d %H:%M:%S',
                    '%Y-%m-%d %H:%M:%S.%f',
                    '%Y-%m-%d',
                    '%d/%m/%Y',
                    '%d/%m/%Y %H:%M:%S',
                    '%Y%m%d',
                    '%Y%m%d%H%M%S'
                ]
                
                for fmt in formats:
                    try:
                        parsed_dt = datetime.datetime.strptime(value, fmt)
                        return parsed_dt.strftime('%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        continue
                        
                # Intentar pandas to_datetime como último recurso
                try:
                    parsed = pd.to_datetime(value)
                    if not pd.isna(parsed):
                        if parsed.tzinfo is not None:
                            parsed = parsed.tz_localize(None)
                        return parsed.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    pass
                    
        except Exception as e:
            logger.warning(f"Error procesando datetime {value}: {e}")
            
        return None

    def enhanced_cell_processor(self, value: Any, column_info: Dict = None) -> Any:
        """
        Procesador mejorado de celdas que maneja todos los problemas comunes.
        
        Args:
            value: Valor a procesar
            column_info: Información sobre la columna (tipo, nullable, etc.)
            
        Returns:
            Valor limpio y normalizado
        """
        # Casos de valores nulos
        if value is None:
            return None
            
        # pandas NA/NaT/NaN
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
            
        # Tipos especiales de pandas nullable
        if hasattr(value, '_isna'):
            try:
                if value._isna:
                    return None
            except:
                pass
                
        if str(type(value)).find('NAType') != -1:
            return None
        
        # Obtener información del tipo de columna si está disponible
        target_type = column_info.get('clickhouse_type', '') if column_info else ''
        is_nullable = column_info.get('nullable', True) if column_info else True
        
        # Procesamiento según tipo de datos
        
        # Strings y objetos
        if isinstance(value, (str, bytes)) or str(type(value)) == 'object':
            cleaned = self.clean_string_value(value)
            
            # Si la columna no es nullable y el valor es None, usar valor por defecto
            if cleaned is None and not is_nullable:
                if target_type.startswith(('Int', 'UInt')):
                    return 0
                elif target_type.startswith('Float'):
                    return 0.0
                elif target_type == 'String':
                    return ''
                else:
                    return ''
                    
            return cleaned
            
        # Valores numéricos
        if isinstance(value, (int, float, np.integer, np.floating)):
            # Verificar NaN e infinitos
            if isinstance(value, (float, np.floating)) and (np.isnan(value) or np.isinf(value)):
                return None if is_nullable else 0
                
            # Validar rango si tenemos información del tipo
            if target_type:
                return self.validate_numeric_range(value, target_type)
            else:
                # Conversión básica
                if isinstance(value, (int, np.integer)):
                    return int(value)
                else:
                    return float(value)
                    
        # Valores booleanos
        if isinstance(value, (bool, np.bool_)):
            return int(value)
            
        # Valores de fecha/hora
        if isinstance(value, (datetime.datetime, datetime.date, pd.Timestamp, np.datetime64)):
            return self.clean_datetime_value(value)
            
        # pandas nullable types
        if hasattr(value, 'dtype'):
            dtype_str = str(value.dtype)
            if dtype_str.startswith(('Int', 'UInt', 'Float')):
                try:
                    if pd.isna(value):
                        return None if is_nullable else 0
                    if dtype_str.startswith(('Int', 'UInt')):
                        return int(value)
                    else:
                        return float(value)
                except:
                    return None if is_nullable else 0
        
        # Default: intentar convertir a string y limpiar
        try:
            return self.clean_string_value(str(value))
        except:
            return None if is_nullable else ''

    def clean_dataframe(self, df: pd.DataFrame, table_schema: Dict = None) -> pd.DataFrame:
        """
        Limpiar un DataFrame completo aplicando todas las correcciones.
        
        Args:
            df: DataFrame a limpiar
            table_schema: Esquema de la tabla con información de columnas
            
        Returns:
            DataFrame limpio
        """
        logger.info(f"Iniciando limpieza de DataFrame con {len(df)} filas y {len(df.columns)} columnas")
        
        df_clean = df.copy()
        self.stats['total_rows_processed'] += len(df)
        
        for col in df_clean.columns:
            logger.info(f"Procesando columna: {col}")
            
            # Obtener información de la columna si está disponible
            column_info = None
            if table_schema and 'columns' in table_schema:
                column_info = next((c for c in table_schema['columns'] if c['name'] == col), None)
            
            # Aplicar limpieza celda por celda
            original_count = len(df_clean)
            df_clean[col] = df_clean[col].apply(
                lambda x: self.enhanced_cell_processor(x, column_info)
            )
            
            # Contar cambios
            null_count = df_clean[col].isnull().sum()
            if null_count > 0:
                self.stats['null_fixes'] += null_count
                
        logger.info(f"Limpieza completada. Estadísticas: {self.stats}")
        return df_clean

    def analyze_table(self, connection_url: str, database: str, table: str, limit: int = 1000) -> Dict:
        """
        Analizar una tabla para identificar problemas potenciales.
        
        Args:
            connection_url: URL de conexión a la base de datos
            database: Nombre de la base de datos
            table: Nombre de la tabla
            limit: Número de filas a analizar
            
        Returns:
            Diccionario con análisis de la tabla
        """
        logger.info(f"Analizando tabla {database}.{table}")
        
        try:
            engine = create_engine(connection_url)
            
            # Leer muestra de datos usando conexión apropiada
            sql = f"SELECT * FROM `{database}`.`{table}` LIMIT {limit}"
            df = pd.read_sql(sql, connection_url)
            
            analysis = {
                'table': f"{database}.{table}",
                'total_rows_sampled': len(df),
                'columns_analysis': {},
                'encoding_issues': [],
                'type_issues': [],
                'null_issues': [],
                'range_issues': []
            }
            
            for col in df.columns:
                col_analysis = {
                    'dtype': str(df[col].dtype),
                    'null_count': df[col].isnull().sum(),
                    'unique_values': df[col].nunique(),
                    'sample_values': df[col].dropna().head(5).tolist()
                }
                
                # Detectar problemas de codificación
                if df[col].dtype == 'object':
                    for idx, val in df[col].dropna().head(10).items():
                        if isinstance(val, str):
                            try:
                                val.encode('utf-8')
                            except UnicodeEncodeError:
                                analysis['encoding_issues'].append({
                                    'column': col,
                                    'row': idx,
                                    'value': repr(val),
                                    'issue': 'encoding_error'
                                })
                
                # Detectar problemas de rango en números
                if df[col].dtype in ['int64', 'float64']:
                    if df[col].dtype == 'int64':
                        out_of_int32 = ((df[col] < -2147483648) | (df[col] > 2147483647)).sum()
                        if out_of_int32 > 0:
                            analysis['range_issues'].append({
                                'column': col,
                                'issue': 'int32_overflow',
                                'count': out_of_int32,
                                'min': df[col].min(),
                                'max': df[col].max()
                            })
                
                analysis['columns_analysis'][col] = col_analysis
            
            engine.dispose()
            logger.info(f"Análisis completado para {database}.{table}")
            return analysis
            
        except Exception as e:
            logger.error(f"Error analizando tabla {database}.{table}: {e}")
            return {'error': str(e)}

    def generate_cleaning_report(self, output_path: str = None) -> str:
        """
        Generar reporte de limpieza de datos.
        
        Args:
            output_path: Ruta del archivo de reporte
            
        Returns:
            Contenido del reporte
        """
        report = {
            'timestamp': datetime.datetime.now().isoformat(),
            'statistics': self.stats,
            'summary': {
                'total_fixes': sum(self.stats.values()) - self.stats['total_rows_processed'],
                'success_rate': 'N/A' if self.stats['total_rows_processed'] == 0 else 
                              f"{((self.stats['total_rows_processed'] - sum(v for k, v in self.stats.items() if k != 'total_rows_processed')) / self.stats['total_rows_processed'] * 100):.2f}%"
            }
        }
        
        report_content = json.dumps(report, indent=2, ensure_ascii=False)
        
        if output_path:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(report_content)
            logger.info(f"Reporte guardado en: {output_path}")
        
        return report_content

    def reset_stats(self):
        """Reiniciar estadísticas de limpieza."""
        self.stats = {
            'null_fixes': 0,
            'encoding_fixes': 0,
            'type_conversions': 0,
            'range_fixes': 0,
            'character_cleanups': 0,
            'total_rows_processed': 0
        }


def main():
    """Función principal para pruebas y uso directo."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Limpiador robusto de datos para ETL')
    parser.add_argument('--analyze', help='Analizar tabla (formato: url,database,table)')
    parser.add_argument('--limit', type=int, default=1000, help='Límite de filas para análisis')
    parser.add_argument('--output', help='Archivo de salida para reporte')
    
    args = parser.parse_args()
    
    cleaner = DataCleanerRobust()
    
    if args.analyze:
        parts = args.analyze.split(',')
        if len(parts) != 3:
            print("Error: formato debe ser url,database,table")
            return
            
        connection_url, database, table = parts
        analysis = cleaner.analyze_table(connection_url, database, table, args.limit)
        
        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                json.dump(analysis, f, indent=2, ensure_ascii=False)
            print(f"Análisis guardado en: {args.output}")
        else:
            print(json.dumps(analysis, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()