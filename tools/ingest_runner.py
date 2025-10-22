#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from urllib.parse import quote_plus
import logging
import re
import json
import pandas as pd
import datetime
import numpy as np
import decimal
import math
import clickhouse_connect
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Engine
from typing import List, Tuple, Optional
import argparse
import sys


def load_env_file() -> dict:
    """
    Carga variables desde un archivo .env (prioridad) y si no existe, cae a os.environ.
    Orden de búsqueda dentro del contenedor:
      - /app/tools/.env (repo actual)
      - /app/.env
      - .env (cwd)
    """
    env_vars = {}
    candidates = [
        "/app/tools/.env",
        "/app/.env",
        ".env",
    ]
    for path in candidates:
        try:
            with open(path, "r", encoding="utf-8") as f:
                for raw in f:
                    line = raw.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    env_vars[k.strip()] = v.strip()
            # Si encontramos uno válido, retornamos inmediatamente
            return env_vars
        except FileNotFoundError:
            continue
    # Fallback a entorno del proceso
    return dict(os.environ)


def resolve_source_url(args, env: dict | None = None) -> str | None:
    """
    Resuelve la URL de conexión al origen usando (en orden):
      1) CLI --source-url
      2) SOURCE_URL en .env
      3) DB_CONNECTIONS (JSON) en .env
    Siempre prefiere leer del archivo .env (dinámico) si no se provee 'env'.
    """
    if env is None:
        env = load_env_file()

    # 1) CLI directo
    su = getattr(args, "source_url", None)
    if su:
        return su

    # 2) SOURCE_URL del .env
    su = env.get("SOURCE_URL")
    if su:
        return su

    # 3) DB_CONNECTIONS JSON
    dc_raw = env.get("DB_CONNECTIONS")
    if not dc_raw:
        return None
    try:
        conns = json.loads(dc_raw)
    except Exception:
        return None
    if isinstance(conns, dict):
        conns = [conns]
    if not isinstance(conns, list) or not conns:
        return None

    preferred = (
        getattr(args, "source_name", None)
        or env.get("SOURCE_NAME")
        or (conns[0].get("name") if isinstance(conns[0], dict) else None)
        or "default"
    )
    chosen = next((c for c in conns if c.get("name") == preferred), conns[0])

    # URL directa si viene
    if chosen.get("url"):
        return chosen["url"]

    # Construcción de URL tipo mysql+pymysql
    required = ("host", "user", "pass", "db")
    if not all(k in chosen for k in required):
        return None
    driver = chosen.get("driver", "mysql+pymysql")
    host = chosen["host"]
    port = chosen.get("port", 3306)
    user = chosen["user"]
    pwd = chosen["pass"]
    db = chosen["db"]
    return f"{driver}://{quote_plus(user)}:{quote_plus(pwd)}@{host}:{port}/{db}"



# ---------------------------------
# Logging con soporte JSON
# ---------------------------------
import json as json_module
from datetime import datetime as dt_module

class JSONFormatter(logging.Formatter):
    """Formateador de logs en JSON estructurado"""
    def format(self, record):
        log_data = {
            "timestamp": dt_module.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        # Agregar contexto adicional si está disponible
        if hasattr(record, 'context'):
            log_data["context"] = record.context
        
        return json_module.dumps(log_data, ensure_ascii=False)


# Configurar logging según formato especificado (con salida a consola y archivo)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FORMAT = os.getenv("LOG_FORMAT", "text").lower()
LOG_DIR = os.getenv("LOG_DIR", "/app/logs")

# Asegurar directorio de logs
try:
    os.makedirs(LOG_DIR, exist_ok=True)
except Exception:
    # Si no se puede crear, caerá a solo consola
    LOG_DIR = None

# Armar handlers
level = getattr(logging, LOG_LEVEL, logging.INFO)
root_logger = logging.getLogger()
root_logger.setLevel(level)

# Evitar duplicados si ya existían handlers
if root_logger.handlers:
    for h in list(root_logger.handlers):
        root_logger.removeHandler(h)

stream_handler = logging.StreamHandler()
if LOG_FORMAT == "json":
    formatter = JSONFormatter()
else:
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
stream_handler.setFormatter(formatter)
root_logger.addHandler(stream_handler)

# File handler opcional (si el directorio existe)
if LOG_DIR:
    log_filename = "ingest_runner.json" if LOG_FORMAT == "json" else "ingest_runner.log"
    file_path = os.path.join(LOG_DIR, log_filename)
    try:
        file_handler = logging.FileHandler(file_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    except Exception:
        # Si no se puede abrir el archivo, continuar solo con consola
        pass

log = logging.getLogger(__name__)


# ---------------------------------
# Clases de error personalizadas
# ---------------------------------
class RecoverableError(Exception):
    """Error recuperable - se puede reintentar o continuar"""
    pass


class FatalError(Exception):
    """Error fatal - requiere intervención manual"""
    pass


# ---------------------------------
# Utilidades de normalización
# ---------------------------------
def _parse_maybe_datetime_series(s: pd.Series) -> pd.Series:
    """
    Intenta convertir una serie a datetime (naive) de forma tolerante:
    - Si ya es datetime tz-aware -> quita tz (UTC) y deja naive.
    - Si es string/mixed -> pd.to_datetime(errors='coerce')
    - Otras cosas -> se devuelven tal cual.
    """
    if pd.api.types.is_datetime64_any_dtype(s):
        # tz-aware -> a UTC naive
        if getattr(s.dt, "tz", None) is not None:
            return s.dt.tz_convert("UTC").dt.tz_localize(None)
        return s
    if s.dtype == "object":
        try:
            parsed = pd.to_datetime(s, errors="coerce", utc=False)
            # Si salió tz-aware por algún formato raro
            if pd.api.types.is_datetime64tz_dtype(parsed):
                parsed = parsed.dt.tz_convert("UTC").dt.tz_localize(None)
            return parsed
        except Exception:
            return s
    return s


def process_mysql_date_columns(df: pd.DataFrame, mysql_column_info: dict) -> pd.DataFrame:
    """
    Procesa específicamente campos DATE/DATETIME de MySQL para compatibilidad con ClickHouse.
    
    Estrategia:
    1. Convertir object a datetime si es campo temporal MySQL
    2. Campos DATE inválidos (1900-01-01, 0000-00-00, NULL strings) → NULL
    3. Campos DATE válidos → mantener como datetime
    4. Logging detallado para troubleshooting
    """
    for col in df.columns:
        # Manejar tanto formato dict como string para column_types
        col_info = mysql_column_info.get(col, '')
        if isinstance(col_info, dict):
            # Aceptar 'mysql_type' (preferido) o 'type' (compatibilidad)
            mysql_type = (col_info.get('mysql_type') or col_info.get('type') or '').lower()
        else:
            mysql_type = str(col_info).lower()
        
        # Procesar campos DATE, DATETIME, TIMESTAMP de MySQL
        is_temporal = mysql_type and any(x in mysql_type for x in ['date', 'datetime', 'timestamp'])
        
        if is_temporal:
            log.info(f"🗓️ Procesando campo temporal MySQL: {col} (tipo: {mysql_type})")
            
            col_dtype = str(df[col].dtype)
            series = df[col]
            processed_dates = []
            
            for i, value in enumerate(series):
                # NULL explícito
                if pd.isna(value) or value is None:
                    processed_dates.append(None)
                    continue
                
                # String: puede ser fecha válida o inválida (0000-00-00)
                if isinstance(value, str):
                    value_clean = value.strip()
                    # Fechas inválidas comunes en MySQL
                    if value_clean in ('', '0000-00-00', '0000-00-00 00:00:00', 'NULL', 'None'):
                        processed_dates.append(None)
                        continue
                    # Intentar parsear
                    try:
                        dt = pd.to_datetime(value_clean, errors='raise')
                        if isinstance(dt, pd.Timestamp):
                            dt = dt.to_pydatetime()
                    except:
                        log.warning(f"⚠️ No se pudo parsear fecha en {col}[{i}]: '{value_clean}' → NULL")
                        processed_dates.append(None)
                        continue
                # Ya es Timestamp/datetime
                elif isinstance(value, pd.Timestamp):
                    dt = value.to_pydatetime()
                elif isinstance(value, datetime.datetime):
                    dt = value
                elif isinstance(value, datetime.date):
                    dt = datetime.datetime(value.year, value.month, value.day)
                else:
                    # Tipo inesperado
                    log.warning(f"⚠️ Tipo inesperado en {col}[{i}]: {type(value)} → NULL")
                    processed_dates.append(None)
                    continue
                
                # Validar rango de fechas (ClickHouse y Superset tienen límites)
                if dt.year == 0 or dt.year <= 1900 or dt.year >= 2100:
                    log.debug(f"🚫 Fecha inválida en {col}[{i}]: {dt} → NULL (fuera de rango útil)")
                    processed_dates.append(None)
                else:
                    # Fecha válida
                    processed_dates.append(dt)
            
            # Asignar la serie procesada
            df[col] = processed_dates
            valid_count = len([x for x in processed_dates if x is not None])
            null_count = len([x for x in processed_dates if x is None])
            log.info(f"✅ Campo {col} procesado: {valid_count} fechas válidas, {null_count} NULLs")
    
    return df


def fix_encoding_issues(text):
    """
    Corrige problemas comunes de codificación en strings.
    """
    if text is None or not isinstance(text, str):
        return text
    
    result = text
    
    # Corregir el patrón específico que vemos en los datos
    result = result.replace('Actualizaci??n', 'Actualización')
    result = result.replace('??', 'ó')
    
    # Otros patrones comunes de mal encoding
    encoding_fixes = {
        'Ã¡': 'á', 'Ã©': 'é', 'Ã­': 'í', 'Ã³': 'ó', 'Ãº': 'ú', 'Ã±': 'ñ'
    }
    
    for broken, correct in encoding_fixes.items():
        result = result.replace(broken, correct)
    
    # Intentar recodificación si aún hay caracteres problemáticos
    try:
        if any(char in result for char in ['Â', '¿', '¡']) and '??' not in result:
            # Intentar decodificar/recodificar
            result_bytes = result.encode('latin1', errors='ignore')
            decoded = result_bytes.decode('utf-8', errors='ignore')
            if len(decoded) > 0 and '??' not in decoded:
                result = decoded
    except (UnicodeDecodeError, UnicodeEncodeError):
        # Si falla la recodificación, mantener el original
        pass
    
    return result


def normalize_for_clickhouse(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza dataframe para ClickHouse preservando fidelidad de tipos MySQL:
    - Mantiene tipos originales definidos por get_mysql_column_types()
    - Solo procesa datetimes que YA son datetime
    - Limpia NaN/None sin alterar tipos de datos
    - Corrige problemas de codificación en strings
    """
    if df is None or df.empty:
        return df

    out = df.copy()

    # 1) Solo normalizar datetimes que YA son datetime (sin conversiones forzadas)
    for col in out.columns:
        if str(out[col].dtype).startswith(("datetime64", "datetime64[ns", "datetime64[us")):
            out[col] = _parse_maybe_datetime_series(out[col])

    # 2) Preservar tipos de pandas nullable (Int32, Int64, Float64, string)
    # Estos ya vienen correctos desde MySQL según get_mysql_column_types()

    # 3) Limpiar NaN y corregir codificación en object columns
    for col in out.columns:
        if out[col].dtype == 'object':
            # Limpiar NaN y corregir problemas de codificación
            out[col] = out[col].where(pd.notna(out[col]), None)
            out[col] = out[col].apply(fix_encoding_issues)
    # 4) Limpiar y convertir tipos preservando fidelidad
    for col in out.columns:
        # Para columnas datetime, convertir a python datetime objects para ClickHouse
        if str(out[col].dtype).startswith(("datetime64", "datetime64[ns", "datetime64[us")):
            out[col] = out[col].apply(lambda x: 
                None if pd.isna(x) else 
                x.to_pydatetime() if hasattr(x, 'to_pydatetime') else 
                x if isinstance(x, datetime.datetime) else None
            )
        # Para columnas object que pueden contener TIME de MySQL, limpiar formato
        elif out[col].dtype == 'object' or str(out[col].dtype) == 'string':
            def clean_time_string(x):
                if pd.isna(x) or x is None:
                    return None
                # Si es timedelta (TIME de MySQL), convertir a formato HH:MM:SS
                if isinstance(x, (pd.Timedelta, datetime.timedelta)):
                    if hasattr(x, 'total_seconds'):
                        total_seconds = int(x.total_seconds())
                    else:
                        total_seconds = int(x.seconds)
                    hours = total_seconds // 3600
                    minutes = (total_seconds % 3600) // 60
                    seconds = total_seconds % 60
                    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
                # Si es string que parece timedelta, procesarla
                if isinstance(x, str) and 'days' in x and ':' in x:
                    # Formato: "0 days 12:02:03" → "12:02:03"
                    try:
                        parts = x.split(' ')
                        if len(parts) >= 3:
                            return parts[-1]  # Tomar la parte HH:MM:SS
                    except:
                        pass
                return str(x) if x is not None else None
            out[col] = out[col].apply(clean_time_string)
        else:
            # Para otros tipos, solo limpiar NaN manteniendo el tipo
            out[col] = out[col].where(pd.notna(out[col]), None)

    return out

# --- NUEVO: normalizador de columnas datetime ---
def _parse_datetime_series(s: pd.Series) -> pd.Series:
    """
    Intenta parsear una serie a pandas datetime.
    - Convierte no parseables a NaT.
    - Quita timezone si la hubiera.
    - Devuelve dtype datetime64[ns] (naive).
    """
    # Intenta convertir a str por si vienen números/objetos mezclados
    s_str = s.astype("string", errors="ignore")
    parsed = pd.to_datetime(s_str, errors="coerce", utc=False, infer_datetime_format=True)

    # Si trae tz, la quitamos (ClickHouse DateTime es naive)
    try:
        # pandas >=2.0: comprobar con isinstance del dtype
        from pandas.core.dtypes.dtypes import DatetimeTZDtype
        if isinstance(parsed.dtype, DatetimeTZDtype):
            parsed = parsed.dt.tz_convert(None)
    except Exception:
        # fallback para pandas viejas
        if pd.api.types.is_datetime64tz_dtype(parsed):
            parsed = parsed.dt.tz_convert(None)

    return parsed

def coerce_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # Obtener tipos MySQL si están disponibles en el DataFrame
    mysql_types = getattr(df, "_mysql_types", None)
    for col in out.columns:
        s = out[col]
        # Solo convertir si el tipo MySQL es datetime/date/timestamp
        mysql_type = None
        if mysql_types and col in mysql_types:
            mysql_type = mysql_types[col].lower()
        if mysql_type and any(t in mysql_type for t in ["datetime", "timestamp", "date"]):
            # Si ya es datetime, normalizar timezone
            if pd.api.types.is_datetime64_any_dtype(s):
                try:
                    if hasattr(s.dt, "tz") and s.dt.tz is not None:
                        out[col] = s.dt.tz_localize(None)
                    else:
                        out[col] = s
                except Exception:
                    out[col] = s
            else:
                def robust_parse(val):
                    if pd.isna(val) or val is None:
                        return None
                    sval = str(val).strip()
                    if sval in ["", "0000-00-00", "0000-00-00 00:00:00"]:
                        return None
                    try:
                        dt = pd.to_datetime(sval, errors="coerce", utc=False)
                        if pd.isna(dt):
                            return None
                        if isinstance(dt, pd.Timestamp):
                            return dt.to_pydatetime()
                        if isinstance(dt, datetime.datetime):
                            return dt
                        return None
                    except Exception:
                        return None
                out[col] = s.apply(robust_parse)
        else:
            # Si el tipo MySQL es string, mantener como string aunque el nombre sugiera fecha
            out[col] = s.where(pd.notna(s), None)
    return out


    # 3) Parseo robusto
    for col in date_like_cols:
        s = df[col]
        # Si ya es datetime64, solo normalizamos tz → naive
        if pd.api.types.is_datetime64_any_dtype(s):
            parsed = s
        else:
            # Parseo sin formato fijo; cadenas inválidas → NaT
            parsed = pd.to_datetime(s.astype("string"), errors="coerce", utc=False)

        # tz-aware → naive
        try:
            if hasattr(parsed.dtype, "tz") and parsed.dtype.tz is not None:
                parsed = parsed.dt.tz_localize(None)
        except Exception:
            pass

        # NaT → None (lo hacemos más adelante en dataframe_to_clickhouse_rows)
        df[col] = parsed

    return df


# --- NUEVO: Preprocesamiento de DataFrame para ClickHouse ---
def clean_integer_column(series: pd.Series, is_primary_key: bool = False, column_name: str = "unknown") -> pd.Series:
    """
    Limpia una columna que debería ser entero, manejando casos problemáticos:
    - Strings numéricos -> int
    - 'null', 'NULL', 'nan', 'NaN', '', 'None' -> None (o 0 si es PK)
    - Valores válidos -> int
    """
    log.info(f"🧹 Limpiando columna integer: {column_name} (PK: {is_primary_key}, dtype: {series.dtype}, valores: {len(series)})")
    
    problematic_values = []
    null_count = 0
    converted_count = 0
    
    def clean_value(val):
        nonlocal null_count, converted_count
        original_val = val
        
        # Manejo específico para NaN de pandas/numpy
        if pd.isna(val):
            null_count += 1
            return 0 if is_primary_key else None
        
        # Si ya es None, mantenerlo así
        if val is None:
            null_count += 1
            return 0 if is_primary_key else None
        
        # Convertir a string para análisis
        str_val = str(val).strip()
        
        # Casos que representan NULL
        null_values = {'null', 'NULL', 'nan', 'NaN', 'None', '', 'na', 'NA'}
        if str_val.lower() in [v.lower() for v in null_values]:
            null_count += 1
            return 0 if is_primary_key else None
        
        # Intentar conversión a entero
        try:
            # Manejar decimales que son realmente enteros (ej: "123.0" -> 123)
            if '.' in str_val:
                float_val = float(str_val)
                if float_val.is_integer():
                    converted_count += 1
                    return int(float_val)
                else:
                    # Si es decimal y no PK, puede ser None
                    null_count += 1
                    return 0 if is_primary_key else None
            else:
                converted_count += 1
                return int(str_val)
        except (ValueError, TypeError):
            # Registrar valor problemático para debug
            if len(problematic_values) < 5:  # Solo primeros 5 para no saturar logs
                problematic_values.append(repr(original_val))
            # Si no se puede convertir y es PK, usar 0
            null_count += 1
            return 0 if is_primary_key else None
    
    result = series.apply(clean_value)
    
    # Log detallado de resultados
    log.info(f"🧹 Columna {column_name} procesada: {converted_count} convertidos, {null_count} nulls/NaN → {0 if is_primary_key else 'None'}")
    
    # Log de debug si hay valores problemáticos
    if problematic_values:
        log.warning(f"🧹 Columna {column_name}: {len(problematic_values)} valores problemáticos encontrados: {problematic_values}")
    
    return result


def preprocess_dataframe_for_clickhouse(df: pd.DataFrame, primary_keys: set = None, column_types: dict = None) -> pd.DataFrame:
    """
    Preprocesa el DataFrame para asegurar compatibilidad con ClickHouse,
    especialmente en el manejo de valores nulos en columnas numéricas y foreign keys.
    
    Args:
        df: DataFrame a procesar
        primary_keys: Set de nombres de columnas que son PRIMARY KEY
        column_types: Dict con tipos de columnas MySQL (ej: {'col1': 'int(10)', 'col2': 'varchar(255)'})
    
    IMPORTANTE: 
    - Los campos PRIMARY KEY nunca se convierten a None.
    - Los campos INT que no son PK se limpian robustamente para manejar foreign keys con datos sucios.
    """
    if primary_keys is None:
        primary_keys = set()
    if column_types is None:
        column_types = {}
    
    df = df.copy()
    
    for col in df.columns:
        dtype = str(df[col].dtype)
        is_pk = col in primary_keys
        # Obtener información de la columna MySQL
        col_type_info = column_types.get(col, {})
        if isinstance(col_type_info, dict):
            mysql_type = col_type_info.get('mysql_type', '').lower()
            nullable = col_type_info.get('nullable', True)
            is_mysql_pk = col_type_info.get('primary_key', False)
            log.info(f"🔍 DEBUG column_types para {col}: {col_type_info}")
        else:
            # Fallback para formato anterior (solo string)
            mysql_type = str(col_type_info).lower()
            nullable = True
            is_mysql_pk = False
            log.info(f"🔍 DEBUG column_types (fallback) para {col}: {col_type_info}")
        
        is_int_column = 'int' in mysql_type or dtype.startswith(('int', 'Int'))
        # Determinar si la columna no debe ser nullable
        is_non_nullable = is_pk or is_mysql_pk or not nullable
        
        if is_int_column:
            # Log para depuración
            log.info(f"🔧 Procesando columna INT detectada: {col} (MySQL: {mysql_type}, PK: {is_pk}, NOT NULL: {is_non_nullable})")
            df[col] = clean_integer_column(df[col], is_primary_key=is_non_nullable, column_name=col)
            continue
        
        # Para columnas object/string que pueden contener números (pero no detectadas como INT)
        if dtype == 'object' or dtype == 'string':
            series = df[col]
            
            # 🔥 PRESERVACIÓN DE TIPOS: Si MySQL dice que es VARCHAR/TEXT/CHAR, mantenerlo como string
            if mysql_type and any(str_type in mysql_type for str_type in ['varchar', 'text', 'char']):
                log.info(f"🔒 PRESERVANDO tipo MySQL: {col} como String (MySQL: {mysql_type})")
                log.info(f"🔧 DEBUG: Analizando condiciones para {col} - is_pk={is_pk}, is_non_nullable={is_non_nullable}")
                
                # Ya tenemos is_non_nullable calculado arriba
                
                # Mantener estrictamente como string según MySQL
                if is_pk:
                    # PKs nunca pueden ser NULL - usar string vacía o valor por defecto
                    df[col] = [str(x) if x is not None else f'default_{col}_pk' for x in series]
                elif is_non_nullable:
                    # Columnas NOT NULL - convertir NULL/vacío a valor por defecto
                    log.info(f"🔧 DEBUG: Procesando columna NOT NULL {col} - is_pk={is_pk}, is_mysql_pk={is_mysql_pk}, nullable={nullable}")
                    def clean_not_null_string(x):
                        if x is None or pd.isna(x):
                            return f'N/A'  # Valor por defecto para campos obligatorios
                        str_val = str(x).strip()
                        if str_val in {'nan', 'NaN', 'None', 'null', 'NULL', '', 'na', 'NA'}:
                            return f'N/A'  # Valor por defecto para campos obligatorios
                        return str_val
                    df[col] = [clean_not_null_string(x) for x in series]
                    log.info(f"🔧 Columna NOT NULL {col}: cadenas vacías/NULL → 'N/A'")
                else:
                    # Columnas nullable - convertir valores vacíos a None
                    null_strings = {'nan', 'NaN', 'None', 'null', 'NULL', '', 'na', 'NA'}
                    df[col] = [None if str(x).strip() in null_strings else str(x) for x in series]
                continue
            
            # Solo para columnas SIN tipo MySQL explícito: intentar inferencia automática
            # Primero convertir todo a string para limpiar
            series_str = series.astype(str)
            
            # Intentar conversión numérica
            numeric_conversion = pd.to_numeric(series_str, errors='coerce')
            if not numeric_conversion.isna().all():
                # Es mayormente numérico - usar limpieza de enteros si parece ser entero
                non_null_numeric = numeric_conversion.dropna()
                if len(non_null_numeric) > 0 and (non_null_numeric % 1 == 0).all():
                    # Parece ser columna de enteros
                    log.info(f"🔄 AUTO-CONVIRTIENDO: {col} de object -> integer (sin tipo MySQL)")
                    df[col] = clean_integer_column(series, is_primary_key=is_pk, column_name=col)
                else:
                    # Es flotante
                    log.info(f"🔄 AUTO-CONVIRTIENDO: {col} de object -> float (sin tipo MySQL)")
                    if is_pk:
                        df[col] = [0.0 if pd.isna(x) else float(x) for x in numeric_conversion]
                    else:
                        df[col] = [None if pd.isna(x) else float(x) for x in numeric_conversion]
            else:
                # No es numérico, mantener como string
                log.info(f"🔄 MANTENIENDO: {col} como String (sin tipo MySQL, no numérico)")
                if is_pk:
                    df[col] = [str(x) if x is not None else '' for x in series]
                else:
                    null_strings = {'nan', 'NaN', 'None', 'null', 'NULL', '', 'na', 'NA'}
                    df[col] = [None if str(x).strip() in null_strings else str(x) for x in series]
        
        # Para tipos numéricos pandas existentes
        elif dtype.startswith(('int', 'Int')):
            df[col] = clean_integer_column(df[col], is_primary_key=is_pk, column_name=col)
        elif dtype.startswith(('float', 'Float')):
            if is_pk:
                df[col] = [0.0 if pd.isna(x) else float(x) for x in df[col]]
            else:
                df[col] = [None if pd.isna(x) else float(x) for x in df[col]]
    
    return df


# --- MODIFICAR: preparación de filas a insertar ---
def dataframe_to_clickhouse_rows(df: pd.DataFrame, primary_keys: set = None, column_types: dict = None) -> tuple[list[tuple], list[str]]:
    """
    Convierte un DataFrame a (data, cols) para clickhouse_connect.client.insert,
    garantizando tipos nativos seguros para ClickHouse y limpiando valores raros.
    """
    # Preprocesar DataFrame para mejor manejo de nulos
    df = preprocess_dataframe_for_clickhouse(df, primary_keys, column_types)

    # Enforce: columnas enteras deben ser int nativo (no float) para evitar errores con Nullable(Int32)
    try:
        int_like_cols = set()
        if column_types and isinstance(column_types, dict):
            for col_name, col_info in column_types.items():
                if isinstance(col_info, dict):
                    mt_l = col_info.get('mysql_type', '').lower()
                else:
                    mt_l = str(col_info).lower()
                if ("int" in mt_l or "bigint" in mt_l) and col_name in df.columns:
                    int_like_cols.add(col_name)
        # fallback por dtype
        for col in df.columns:
            dts = str(df[col].dtype)
            if dts.startswith(("int", "Int")):
                int_like_cols.add(col)

        for col in int_like_cols:
            is_pk = bool(primary_keys and col in primary_keys)
            def _coerce_int(v):
                if v is None:
                    return 0 if is_pk else None
                try:
                    if pd.isna(v):
                        return 0 if is_pk else None
                except Exception:
                    pass
                if isinstance(v, (int, np.integer)):
                    return int(v)
                if isinstance(v, (float, np.floating)):
                    fv = float(v)
                    if math.isnan(fv) or math.isinf(fv):
                        return 0 if is_pk else None
                    return int(fv) if fv.is_integer() else (0 if is_pk else None)
                if isinstance(v, decimal.Decimal):
                    return int(v) if v.as_tuple().exponent == 0 else (0 if is_pk else None)
                if isinstance(v, str):
                    s = v.strip()
                    if s == "" or s.lower() in {"null", "none", "nan"}:
                        return 0 if is_pk else None
                    try:
                        if "." in s:
                            fv = float(s)
                            return int(fv) if fv.is_integer() else (0 if is_pk else None)
                        return int(s)
                    except Exception:
                        return 0 if is_pk else None
                try:
                    return int(v)
                except Exception:
                    return 0 if is_pk else None
            df[col] = df[col].apply(_coerce_int)
    except Exception:
        pass
    cols = list(df.columns)

    def cell(v):
        # None / NaN / NaT / string vacía
        if v is None:
            return None
        
        # Manejo robusto de float NaN PRIMERO (incluyendo numpy)
        if isinstance(v, (float, np.floating)):
            if np.isnan(v):
                return None
            return float(v)  # Convertir numpy a Python nativo
        
        # Manejo de pandas NA/NaT/NaN con múltiples verificaciones
        try:
            if pd.isna(v):
                return None
        except (TypeError, ValueError):
            pass
        
        # Verificación específica para pandas._libs.missing.NAType
        if str(type(v)).find('NAType') != -1:
            return None
            
        if isinstance(v, str) and v.strip() == "":
            return None
            
        # Manejo específico para tipos nullable de pandas con múltiples enfoques
        if hasattr(v, '_isna'):
            try:
                if v._isna:
                    return None
            except:
                pass
        
        # Tipos pandas nullable (Int64, Float64, etc.)
        if hasattr(v, 'dtype'):
            dtype_str = str(v.dtype)
            if dtype_str.startswith(('Int', 'UInt', 'Float')):
                try:
                    if pd.isna(v):
                        return None
                    return int(v) if dtype_str.startswith(('Int', 'UInt')) else float(v)
                except:
                    return None
        # pandas.Timestamp -> datetime (naive)
        if isinstance(v, pd.Timestamp):
            if v.tzinfo is not None:
                v = v.tz_localize(None)
            dt = v.to_pydatetime()
            
            # Validar rango útil - usar NULL para fechas inválidas
            min_date = datetime.datetime(1970, 1, 1, 0, 0, 0)  # Epoch Unix
            max_date = datetime.datetime(2299, 12, 31, 23, 59, 59)
            
            if dt < min_date:
                log.warning(f"🚨 Fecha fuera de rango útil (muy antigua): {dt} → NULL")
                return None
            elif dt > max_date:
                log.warning(f"🚨 Fecha fuera de rango ClickHouse (muy futura): {dt} → NULL")
                return None
                
            return dt
        # numpy.datetime64 -> datetime (naive)
        if isinstance(v, np.datetime64):
            ts = pd.Timestamp(v)
            if ts.tzinfo is not None:
                ts = ts.tz_localize(None)
            dt = ts.to_pydatetime()
            
            # Validar rango de ClickHouse
            min_date = datetime.datetime(1900, 1, 2, 0, 0, 0)
            max_date = datetime.datetime(2299, 12, 31, 23, 59, 59)
            
            if dt < min_date:
                log.warning(f"🚨 Fecha fuera de rango ClickHouse (muy antigua): {dt} -> {min_date}")
                return min_date
            elif dt > max_date:
                log.warning(f"🚨 Fecha fuera de rango ClickHouse (muy futura): {dt} -> {max_date}")
                return max_date
                
            return dt
        # date -> datetime (00:00:00)  
        if isinstance(v, datetime.date) and not isinstance(v, datetime.datetime):
            dt = datetime.datetime(v.year, v.month, v.day)
            
            # Validar rango útil - usar NULL para fechas inválidas
            min_date = datetime.datetime(1970, 1, 1, 0, 0, 0)  # Epoch Unix
            max_date = datetime.datetime(2299, 12, 31, 23, 59, 59)
            
            if dt < min_date:
                log.warning(f"🚨 Fecha fuera de rango útil (muy antigua): {dt} → NULL")
                return None
            elif dt > max_date:
                log.warning(f"🚨 Fecha fuera de rango ClickHouse (muy futura): {dt} → NULL")
                return None
                
            return dt
        # datetime (naive)
        if isinstance(v, datetime.datetime):
            if v.tzinfo is not None:
                v = v.replace(tzinfo=None)
            
            # Validar rango útil - usar NULL para fechas inválidas
            min_date = datetime.datetime(1970, 1, 1, 0, 0, 0)  # Epoch Unix
            max_date = datetime.datetime(2299, 12, 31, 23, 59, 59)
            
            if v < min_date:
                log.warning(f"🚨 Fecha fuera de rango útil (muy antigua): {v} → NULL")
                return None
            elif v > max_date:
                log.warning(f"🚨 Fecha fuera de rango ClickHouse (muy futura): {v} → NULL")
                return None
            
            return v
        # Números float/int estándar (incluyendo numpy)
        if isinstance(v, (float, int, np.integer, np.floating)):
            if isinstance(v, (float, np.floating)) and (np.isnan(v) or np.isinf(v)):
                return None
            # Preferir int si el valor es entero
            if isinstance(v, (float, np.floating)):
                fv = float(v)
                if fv.is_integer():
                    return int(fv)
                return fv
            if isinstance(v, np.integer):
                return int(v)
            return v
        
        # Cadenas "fecha cero" típicas de MySQL -> None
        if isinstance(v, str):
            vs = v.strip()
            if vs in ("0000-00-00", "0000-00-00 00:00:00"):
                return None
            return v  # otras strings se mandan tal cual
        # Bytes -> str (utf-8, tolerante)
        if isinstance(v, (bytes, bytearray)):
            try:
                return v.decode("utf-8", errors="replace")
            except Exception:
                return bytes(v)
        # Decimal → entero si es escala 0; en otro caso, a string (para no perder precisión)
        if isinstance(v, decimal.Decimal):
            if v.as_tuple().exponent == 0:
                return int(v)
            return str(v)
        # Tipos numpy -> nativos (con debug logging para problemas de serialización)
        if isinstance(v, np.bool_):
            return bool(v)
        if isinstance(v, np.integer):
            try:
                if pd.isna(v):
                    return None
                int_val = int(v)
                # Validación adicional de rango para Int32
                if int_val < -2147483648 or int_val > 2147483647:
                    log.warning(f"🚨 Valor numpy.integer fuera de rango Int32: {int_val} -> None")
                    return None
                return int_val
            except (ValueError, OverflowError, TypeError):
                log.warning(f"🚨 Error convirtiendo numpy.integer: {repr(v)} -> None")
                return None
        if isinstance(v, np.floating):
            try:
                fv = float(v)
                if math.isnan(fv) or math.isinf(fv):
                    return None
                # Si es un float que debería ser entero (ej: 123.0), convertirlo
                if fv.is_integer() and -2147483648 <= fv <= 2147483647:
                    return int(fv)
                return fv
            except (ValueError, OverflowError, TypeError):
                log.warning(f"🚨 Error convirtiendo numpy.floating: {repr(v)} -> None")
                return None
        # Manejo específico para tipos de pandas que pueden ser nulos
        if hasattr(v, 'dtype'):
            dtype_str = str(v.dtype)
            if dtype_str.startswith('Int') or dtype_str.startswith('Float') or dtype_str.startswith('int') or dtype_str.startswith('float'):
                if pd.isna(v):
                    return None
                try:
                    if 'Int' in dtype_str or 'int' in dtype_str:
                        result = int(v)
                        if result < -2147483648 or result > 2147483647:
                            log.warning(f"🚨 Valor pandas.Int fuera de rango Int32: {result} -> None")
                            return None
                        return result
                    else:
                        result = float(v)
                        # Si es un float que debería ser entero (ej: 123.0), convertirlo
                        if result.is_integer() and -2147483648 <= result <= 2147483647:
                            return int(result)
                        return result
                except (ValueError, OverflowError, TypeError):
                    log.warning(f"🚨 Error convirtiendo pandas dtype {dtype_str}: {repr(v)} -> None")
                    return None
        # Enteros Python regulares
        if isinstance(v, int):
            # Validación de rango para Int32
            if v < -2147483648 or v > 2147483647:
                log.warning(f"🚨 Valor int Python fuera de rango Int32: {v} -> None")
                return None
            return v
        
        # Flotantes Python regulares
        if isinstance(v, float):
            if math.isnan(v) or math.isinf(v):
                return None
            # Si es un float que debería ser entero (ej: 123.0), convertirlo
            if v.is_integer() and -2147483648 <= v <= 2147483647:
                return int(v)
            return v
        
        # Boolean Python
        if isinstance(v, bool):
            return int(v)
        
        # FALLBACK FINAL: Cualquier tipo problemático → string o None
        try:
            # Intentar convertir tipos problemáticos a string
            if hasattr(v, '__len__') and not isinstance(v, str):
                # Si tiene len(), probablemente es string-safe
                return str(v) if v is not None else None
            elif isinstance(v, (float, np.floating)) and (np.isnan(v) or np.isinf(v)):
                return None
            else:
                # Para todo lo demás, convertir a string como fallback seguro
                return str(v) if v is not None else None
        except Exception:
            # Si todo falla, devolver None
            return None

    data = [tuple(cell(df.at[i, c]) for c in cols) for i in df.index]

    # Post-proceso: asegurar ints en columnas tipo entero
    try:
        int_like_idx = set()
        if column_types and isinstance(column_types, dict):
            for idx, col in enumerate(cols):
                col_info = column_types.get(col, "")
                if isinstance(col_info, dict):
                    mt = col_info.get('mysql_type', '').lower()
                else:
                    mt = str(col_info).lower()
                if "bigint" in mt or ("int" in mt and "tinyint(1)" not in mt):
                    int_like_idx.add(idx)
        # Fallback: detectar por nombre/dtype si column_types no trae info suficiente
        if not int_like_idx:
            for idx, col in enumerate(cols):
                s = df[col]
                if str(s.dtype).startswith(("int", "Int")):
                    int_like_idx.add(idx)

        if int_like_idx:
            fixed = []
            for row in data:
                row_list = list(row)
                for j in int_like_idx:
                    v = row_list[j]
                    if v is None:
                        continue
                    # Convertir floats representando enteros → int; otros floats → None
                    if isinstance(v, (float, np.floating)):
                        if not (math.isnan(v) or math.isinf(v)) and float(v).is_integer():
                            row_list[j] = int(v)
                        else:
                            row_list[j] = None
                    elif isinstance(v, str):
                        vs = v.strip()
                        if vs == "" or vs.lower() in {"null", "none", "nan"}:
                            row_list[j] = None
                        else:
                            try:
                                if "." in vs:
                                    fv = float(vs)
                                    row_list[j] = int(fv) if fv.is_integer() else None
                                else:
                                    row_list[j] = int(vs)
                            except Exception:
                                row_list[j] = None
                    elif isinstance(v, np.integer):
                        row_list[j] = int(v)
                    elif isinstance(v, decimal.Decimal):
                        row_list[j] = int(v) if v.as_tuple().exponent == 0 else None
                    # otros tipos se dejan tal cual (int, None, etc.)
                fixed.append(tuple(row_list))
            data = fixed
    except Exception:
        # Fallo silencioso del postproceso no debe bloquear
        pass

    return data, cols






# ---------------------------------
# Conexiones
# ---------------------------------
def get_clickhouse_client() -> clickhouse_connect.driver.Client:
    ch_host = os.getenv("CH_HOST", "clickhouse")
    ch_port = int(os.getenv("CH_PORT", "8123"))
    ch_user = os.getenv("CH_USER", "etl")
    ch_pass = os.getenv("CH_PASSWORD", "")
    ch_secure = os.getenv("CH_SECURE", "false").lower() == "true"
    log.info(
        f"ClickHouse -> host={ch_host} port={ch_port} user={ch_user} secure={ch_secure}"
    )
    try:
        client = clickhouse_connect.get_client(
            host=ch_host, port=ch_port, username=ch_user, password=ch_pass, secure=ch_secure
        )
        # Verificar conexión con query simple
        client.query("SELECT 1")
        return client
    except Exception as e:
        raise FatalError(
            f"No se pudo conectar a ClickHouse en {ch_host}:{ch_port}: {e}. "
            f"Verifica que el servicio esté corriendo y accesible. "
            f"Consulta docs/ERROR_RECOVERY.md para soluciones."
        )


def get_source_engine(src_url: str) -> Engine:
    log.info(f"Fuente default -> {src_url.replace('//', '//***@')}")
    try:
        engine = create_engine(src_url, pool_recycle=3600, pool_pre_ping=True)
        # Verificar conexión
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except Exception as e:
        raise FatalError(
            f"No se pudo conectar a la base de datos fuente: {e}. "
            f"Verifica credenciales y conectividad. "
            f"URL (ofuscada): {src_url.replace('//', '//***@')}. "
            f"Consulta docs/ERROR_RECOVERY.md para soluciones."
        )


# ---------------------------------
# Existencia de tabla origen
# ---------------------------------
def source_table_exists(engine: Engine, schema: str, table: str) -> bool:
    try:
        ins = inspect(engine)
        return ins.has_table(table, schema=schema)
    except SQLAlchemyError as e:
        log.warning(
            f"No se pudo verificar existencia de {schema}.{table}: {e.__class__.__name__}: {e}"
        )
        return False


# ---------------------------------
# Esquema ClickHouse destino
# ---------------------------------
def ensure_ch_database(client, database: str):
    client.query(f"CREATE DATABASE IF NOT EXISTS {database}")


def _ch_ident(name: str) -> str:
    # Identificador escapado simple (evitamos comillas si ya viene calificado)
    return name


def map_sqlalchemy_to_ch(ins: inspect, engine: Engine, schema: str, table: str) -> List[Tuple[str, str]]:
    """
    Refleja columnas de la tabla origen y regresa pares (nombre_col, tipo_clickhouse).
    Hacemos un mapeo razonable -> String, Nullable(String), Int32, Float64, DateTime, Decimal(18,0) etc.
    IMPORTANTE: Los campos PRIMARY KEY se mantienen como NOT NULL.
    """
    cols = []
    try:
        cols_info = ins.get_columns(table, schema=schema)
        # Obtener información de primary keys
        pk_info = ins.get_pk_constraint(table, schema=schema)
        primary_keys = set(pk_info.get('constrained_columns', [])) if pk_info else set()
        
        # 🔧 OBTENER INFORMACIÓN REAL DE NULLABILITY desde MySQL
        nullable_info = {}
        try:
            # Usar conexión directa para obtener información precisa de DESCRIBE
            import pymysql
            url = engine.url
            mysql_conn = pymysql.connect(
                host=url.host,
                port=url.port or 3306,
                user=url.username,
                password=url.password,
                database=url.database,
                charset='utf8mb4'
            )
            
            cursor = mysql_conn.cursor()
            cursor.execute(f"DESCRIBE `{schema}`.`{table}`")
            describe_results = cursor.fetchall()
            
            for row in describe_results:
                col_name = row[0]
                is_nullable = row[2].upper() == 'YES'  # NULL column: YES/NO
                nullable_info[col_name] = is_nullable
                
            cursor.close()
            mysql_conn.close()
            
            log.info(f"📋 Nullability real de MySQL para {schema}.{table}: {nullable_info}")
            
        except Exception as e:
            log.warning(f"⚠️ No se pudo obtener nullability real de MySQL: {e}")
        
    except Exception as e:
        raise RuntimeError(f"No se pudieron leer columnas de {schema}.{table}: {e}") from e

    for c in cols_info:
        name = c["name"]
        type_ = str(c["type"]).lower()
        is_primary_key = name in primary_keys

        # heurística simple de mapeo (orden: casos específicos primero)
        if "tinyint(1)" in type_ or "bool" in type_:
            ch_type = "Int8"
        elif "tinyint" in type_:
            ch_type = "Int8"
        elif "smallint" in type_:
            ch_type = "Int16"
        elif "bigint" in type_:
            ch_type = "Int64"
        elif "int" in type_:
            ch_type = "Int32"
        elif "float" in type_ or "real" in type_:
            ch_type = "Float32"
        elif "double" in type_ or "numeric" in type_:
            ch_type = "Float64"
        elif "decimal" in type_:
            # si viene con precisión, podríamos parsearla; por simplicidad:
            ch_type = "Decimal(18, 6)"
        elif "datetime" in type_ or "timestamp" in type_ or "date" in type_:
            ch_type = "DateTime"
        else:
            ch_type = "String"

        # 🔑 IMPORTANTE: Usar información real de nullability de MySQL
        # Prioridad: 1) MySQL DESCRIBE, 2) PRIMARY KEY (nunca nullable), 3) SQLAlchemy fallback
        if name in nullable_info:
            nullable = nullable_info[name] and not is_primary_key
        else:
            nullable = c.get("nullable", True) and not is_primary_key
            
        if nullable and not ch_type.startswith("Nullable("):
            ch_type = f"Nullable({ch_type})"

        cols.append((name, ch_type))
        
        # Log para debugging
        if is_primary_key:
            log.info(f"Campo PRIMARY KEY detectado: {name} -> {ch_type} (NOT NULL)")
        elif not nullable:
            log.info(f"Campo NOT NULL detectado: {name} -> {ch_type} (NOT NULL según MySQL)")
    
    return cols


def build_create_table_sql(
    full_table: str,
    columns: List[Tuple[str, str]],
    engine: str,
    order_by: List[str],
    partition_by: Optional[str] = None,
    version_col: Optional[str] = None,
) -> str:
    # Solo usar columnas reales, nunca '_dummy_'
    if not columns or len(columns) == 0:
        raise RuntimeError(f"No hay columnas reales para crear la tabla {full_table}")
    cols_sql = ",\n  ".join(f"`{n}` {t}" for n, t in columns)
    engine_sql = engine
    if engine.upper().startswith("REPLACINGMERGETREE") and version_col:
        engine_sql = f"ReplacingMergeTree({version_col})"
    
    # 🔧 Si no hay order_by, buscar primera columna NOT NULL (evitar Nullable en sorting key)
    if not order_by:
        # Buscar primera columna que NO sea Nullable
        not_null_cols = [col_name for col_name, col_type in columns if "Nullable(" not in col_type]
        if not_null_cols:
            order_by = [not_null_cols[0]]
            log.info(f"✅ Usando columna NOT NULL '{order_by[0]}' como ORDER BY fallback para {full_table}")
        else:
            # Caso extremo: todas las columnas son nullable -> usar tuple()
            log.warning(f"⚠️ Todas las columnas son Nullable en {full_table}. Usando ORDER BY tuple()")
            order_sql = "tuple()"
            part_sql = f"\nPARTITION BY {partition_by}" if partition_by else ""
            return f"""
CREATE TABLE IF NOT EXISTS {full_table}
(
  {cols_sql}
)
ENGINE = {engine_sql}
ORDER BY {order_sql}{part_sql}
    """.strip()
    
    order_sql = ", ".join(f"`{c}`" for c in order_by)
    part_sql = f"\nPARTITION BY {partition_by}" if partition_by else ""
    return f"""
CREATE TABLE IF NOT EXISTS {full_table}
(
  {cols_sql}
)
ENGINE = {engine_sql}
ORDER BY ({order_sql}){part_sql}
    """.strip()


def ensure_ch_table(
    client,
    database: str,
    table: str,
    src_engine: Engine,
    schema: str,
    src_table: str,
    dedup_mode: str,
    unique_key: Optional[str],
    version_col: Optional[str],
):
    """
    Crea la tabla destino si no existe.
    - Si dedup_mode=replacing y se provee unique_key/version_col -> usa ReplacingMergeTree(version_col) ORDER BY unique_key
    - Si dedup_mode en otro caso -> MergeTree ORDER BY (unique_key) si está; si no, ORDER BY tuple()
    
    🔧 IMPORTANTE: Filtra automáticamente columnas Nullable() del ORDER BY para evitar error 
    "Sorting key contains nullable columns" en ClickHouse.
    """
    if not source_table_exists(src_engine, schema, src_table):
        log.warning(
            f"Tabla omitida {schema}.{src_table} → La tabla no existe o no es accesible en el origen (omitida)."
        )
        return False

    ins = inspect(src_engine)
    columns = map_sqlalchemy_to_ch(ins, src_engine, schema, src_table)

    # Construir mapeo de nombres de columna a tipos ClickHouse
    column_type_map = {col_name: col_type for col_name, col_type in columns}

    engine_name = "MergeTree"
    order_by_candidates = [unique_key] if unique_key else []
    
    # 🔧 FILTRAR columnas nullable del ORDER BY
    # ClickHouse no permite columnas Nullable() en sorting key por defecto
    order_by = []
    for col in order_by_candidates:
        if col in column_type_map:
            col_type = column_type_map[col]
            if "Nullable(" in col_type:
                log.warning(
                    f"⚠️ Columna '{col}' es Nullable y será excluida del ORDER BY "
                    f"para tabla {schema}.{src_table} (tipo: {col_type})"
                )
            else:
                order_by.append(col)
        else:
            # Columna no encontrada en schema, mantenerla si existe
            order_by.append(col)
    
    # Si unique_key era nullable y fue filtrado, buscar alternativa NOT NULL
    if unique_key and unique_key not in order_by:
        log.warning(
            f"⚠️ unique_key '{unique_key}' fue filtrado por ser Nullable. "
            f"Buscando columna alternativa NOT NULL para ORDER BY..."
        )
        # Intentar usar columnas NOT NULL como ORDER BY
        not_null_cols = [col_name for col_name, col_type in columns if "Nullable(" not in col_type]
        if not_null_cols:
            order_by = [not_null_cols[0]]  # Usar primera columna NOT NULL
            log.info(f"✅ Usando columna '{order_by[0]}' (NOT NULL) como ORDER BY")
        else:
            log.warning(f"⚠️ Todas las columnas son Nullable. ORDER BY será tuple()")
    
    if dedup_mode == "replacing" and unique_key and version_col:
        engine_name = "ReplacingMergeTree"
    
    if not order_by:
        # orden por nada: tuple() - ClickHouse lo permite para tablas sin sorting key
        order_by = []

    full_name = f"{database}.{table}"
    # construimos SQL
    create_sql = build_create_table_sql(
        full_table=full_name,
        columns=columns,
        engine=engine_name,
        order_by=order_by,
        partition_by=None,
        version_col=version_col if engine_name == "ReplacingMergeTree" else None,
    )
    client.query(f"CREATE DATABASE IF NOT EXISTS {database}")
    client.query(create_sql)
    return True


# ---------------------------------
# Insert
# ---------------------------------
def insert_df(
    client,
    database: str,
    table: str,
    df: pd.DataFrame,
    unique_key: Optional[str] = None,
    drop_dupes_in_chunk: bool = True,
    primary_keys: set = None,
    column_types: dict = None,
) -> int:
    if df is None or df.empty:
        return 0

    # 🔹 Asegurar que TODAS las columnas de fecha queden como datetime (o None)
    df = coerce_datetime_columns(df)

    # �️ Procesamiento especializado de campos DATE MySQL para Superset
    if column_types:
        df = process_mysql_date_columns(df, column_types)

    # �🔹 Normalización general para ClickHouse (bools, Decimal, NaN/NaT → None, etc.)
    df = normalize_for_clickhouse(df)
    
    # ✅ DEBUG: Ver qué tipos tenemos después de normalizar
    log.info(f"🔍 DEBUG tipos después de normalización:")
    for col in df.columns:
        sample_val = df[col].iloc[0] if not df.empty and not pd.isna(df[col].iloc[0]) else None
        log.info(f"  {col}: dtype={df[col].dtype}, sample={type(sample_val).__name__}={repr(sample_val)}")

    # 🔹 Deduplicado intra-chunk opcional
    if drop_dupes_in_chunk and unique_key and unique_key in df.columns:
        version_candidates = [c for c in ["factualizacion", "fcreacion", "updated_at", "fecha_actualizacion"] if c in df.columns]
        if version_candidates:
            vcol = version_candidates[0]
            df = df.sort_values(by=[unique_key, vcol], ascending=[True, False])
        df = df.drop_duplicates(subset=[unique_key], keep="first")

    # 🔹 Convertir el DF a filas con datetime nativo/None garantizado
    data, cols = dataframe_to_clickhouse_rows(df, primary_keys, column_types)
    
    # 🔹 DEBUG: Verificar tipos en los datos antes de enviar
    if data and len(data) > 0:
        first_row = data[0]
        for i, (col, val) in enumerate(zip(cols, first_row)):
            log.info(f"  🔍 {col}: {type(val).__name__}={repr(val)}")
    
    client.insert(f"{database}.{table}", data, column_names=cols)
    return len(data)



# ---------------------------------
# Lectura por chunks desde SQLAlchemy
# ---------------------------------
def get_mysql_column_types(connection, schema: str, table: str) -> dict:
    """
    Obtiene los tipos exactos de las columnas MySQL para preservar fidelidad,
    incluyendo información de nullability
    """
    cursor = connection.cursor()
    try:
        cursor.execute(f"DESCRIBE `{schema}`.`{table}`")
        columns_info = cursor.fetchall()
        
        type_mapping = {}
        for col_info in columns_info:
            col_name = col_info[0]
            mysql_type = col_info[1].lower()
            is_nullable = col_info[2].upper() == 'YES'  # NULL column: YES/NO
            is_primary_key = col_info[3].upper() == 'PRI'  # Key column: PRI/UNI/MUL/''
            
            # Mapear tipos MySQL a pandas dtypes preservando fidelidad
            if 'varchar' in mysql_type or 'char' in mysql_type or 'text' in mysql_type:
                pandas_dtype = 'string'  # Forzar string para campos de texto
            elif 'int' in mysql_type and 'bigint' not in mysql_type:
                pandas_dtype = 'Int32'  # Nullable integer
            elif 'bigint' in mysql_type:
                pandas_dtype = 'Int64'  # Nullable big integer  
            elif 'decimal' in mysql_type or 'numeric' in mysql_type:
                pandas_dtype = 'string'  # Preservar precisión como string
            elif 'float' in mysql_type or 'double' in mysql_type:
                pandas_dtype = 'Float64'  # Nullable float
            elif 'datetime' in mysql_type or 'timestamp' in mysql_type:
                # Leer como object para manejar valores inválidos (0000-00-00), luego convertir
                pandas_dtype = 'object'  # Evita errores con fechas inválidas en MySQL
            elif mysql_type.startswith('date'):  # DATE específico (solo fecha sin hora)
                # Leer como object para manejar 0000-00-00, se convierte después
                pandas_dtype = 'object'  # Evita errores con fechas MySQL inválidas
            elif 'time' in mysql_type:
                pandas_dtype = 'object'  # TIME como object para poder procesarlo después
            elif 'bool' in mysql_type or mysql_type.startswith('tinyint(1)'):
                pandas_dtype = 'boolean'
            else:
                pandas_dtype = 'string'  # Default fallback
                
            # Almacenar información completa de la columna
            type_mapping[col_name] = {
                'pandas_dtype': pandas_dtype,
                'mysql_type': mysql_type,
                'nullable': is_nullable,
                'primary_key': is_primary_key
            }
            
        return type_mapping
    finally:
        cursor.close()


def read_table_in_chunks(engine: Engine, schema: str, table: str, chunksize: int, limit: Optional[int]) -> pd.DataFrame:
    base_sql = f"SELECT * FROM `{schema}`.`{table}`"
    if limit and limit > 0:
        base_sql += f" LIMIT {int(limit)}"
    
    # Crear directamente PyMySQL connection desde la URL del engine
    import pymysql
    url = engine.url
    
    # Extraer parámetros de conexión de la URL con codificación robusta
    connection = pymysql.connect(
        host=url.host,
        port=url.port or 3306,
        user=url.username,
        password=url.password,
        database=url.database,
        charset='utf8mb4',
        use_unicode=True,
        init_command="SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci"
    )
    
    try:
        # Obtener tipos exactos de MySQL para preservar fidelidad
        column_types = get_mysql_column_types(connection, schema, table)
        
        # Extraer solo los tipos pandas para pd.read_sql
        pandas_dtypes = {}
        for col, info in column_types.items():
            if isinstance(info, dict):
                pandas_dtypes[col] = info['pandas_dtype']
            else:
                pandas_dtypes[col] = info  # Fallback para formato anterior
        
        log.info(f"📋 Tipos detectados para {schema}.{table}: {pandas_dtypes}")
        
        # Leer con tipos específicos para mantener fidelidad
        try:
            if chunksize:
                return pd.read_sql(base_sql, con=connection, chunksize=chunksize, dtype=pandas_dtypes)
            else:
                return pd.read_sql(base_sql, con=connection, dtype=pandas_dtypes)
        except Exception as e:
            log.warning(f"⚠️ Error usando tipos específicos, leyendo con inferencia automática: {e}")
            # Fallback: leer sin tipos específicos
            if chunksize:
                return pd.read_sql(base_sql, con=connection, chunksize=chunksize)
            else:
                return pd.read_sql(base_sql, con=connection)
    finally:
        connection.close()


# ---------------------------------
# Deduplicación en ClickHouse (staging + swap)
# ---------------------------------
def dedup_and_swap_clickhouse(
    client,
    database: str,
    final_table: str,
    unique_key: str,
    version_col: str,
):
    """
    Crea una tabla temporal deduplicada con window function y hace swap atómico.
    🔧 IMPORTANTE: Detecta automáticamente el ORDER BY de la tabla original para evitar 
    problemas con columnas nullable en sorting key.
    """
    final = f"{database}.{final_table}"
    tmp = f"{database}.tmp__{final_table}__dedup"
    old = f"{database}.{final_table}__old"

    # 🔧 Obtener el ORDER BY actual de la tabla final
    # ClickHouse permite consultar la definición de la tabla
    try:
        result = client.query(f"SHOW CREATE TABLE {final}")
        create_stmt = result.result_rows[0][0] if result.result_rows else ""
        
        # Extraer ORDER BY de la sentencia CREATE TABLE
        import re
        order_by_match = re.search(r'ORDER BY\s+\(([^)]+)\)', create_stmt, re.IGNORECASE)
        
        if order_by_match:
            order_by_clause = order_by_match.group(1)
            log.info(f"✅ ORDER BY detectado de tabla original: ({order_by_clause})")
        else:
            # Fallback: usar unique_key si existe, sino tuple()
            order_by_clause = f"`{unique_key}`" if unique_key else "tuple()"
            log.warning(
                f"⚠️ No se pudo extraer ORDER BY de {final}. "
                f"Usando fallback: ({order_by_clause})"
            )
    except Exception as e:
        log.warning(f"⚠️ Error obteniendo CREATE TABLE de {final}: {e}")
        # Fallback seguro: usar tuple() si no podemos detectar
        order_by_clause = "tuple()"

    # 1) Crear tabla tmp (estructura = final con ORDER BY detectado)
    client.query(f"DROP TABLE IF EXISTS {tmp}")
    client.query(f"CREATE TABLE {tmp} AS {final} ENGINE=MergeTree ORDER BY ({order_by_clause})")

    # 2) Insertar deduplicado
    # Usamos row_number() para quedarnos con la versión más reciente
    dedup_sql = f"""
    INSERT INTO {tmp}
    SELECT *
    FROM (
      SELECT
        *,
        row_number() OVER (PARTITION BY `{unique_key}`
                           ORDER BY `{version_col}` DESC NULLS LAST) AS rn
      FROM {final}
    )
    WHERE rn = 1
    """
    client.query(dedup_sql)

    # 3) Swap atómico
    client.query(f"RENAME TABLE {final} TO {old}, {tmp} TO {final}")
    client.query(f"DROP TABLE IF EXISTS {old}")


# ---------------------------------
# Ingesta de una tabla
# ---------------------------------
def ingest_one_table(
    client,
    src_engine: Engine,
    schema: str,
    table: str,
    ch_database: str,
    ch_table: str,
    chunksize: int,
    limit: Optional[int],
    truncate_before_load: bool,
    dedup_mode: str,
    unique_key: Optional[str],
    version_col: Optional[str],
    final_optimize: bool,
) -> int:
    log.info(f"== Ingerir {schema}.{table} -> {ch_database}.{ch_table} ==")
    print(f"\n== Ingerir {schema}.{table} -> {ch_database}.{ch_table} ==")

    # Verificar existencia
    if not source_table_exists(src_engine, schema, table):
        log.warning(
            f"Tabla omitida {schema}.{table} → La tabla no existe o no es accesible en el origen (omitida)."
        )
        print(f"Tabla omitida {schema}.{table} → No existe o no es accesible en el origen.")
        return 0

    # Mostrar estructura de la tabla y obtener primary keys
    primary_keys = set()
    try:
        ins = inspect(src_engine)
        cols_info = ins.get_columns(table, schema=schema)
        
        # Obtener primary keys
        try:
            pk_info = ins.get_pk_constraint(table, schema=schema)
            primary_keys = set(pk_info.get('constrained_columns', [])) if pk_info else set()
            if primary_keys:
                log.info(f"Primary keys detectadas en {schema}.{table}: {primary_keys}")
                print(f"Primary keys detectadas: {primary_keys}")
        except Exception as pk_e:
            log.warning(f"No se pudieron obtener primary keys de {schema}.{table}: {pk_e}")
        
        # Crear diccionario completo de tipos de columnas MySQL con nullability
        # Usar la información completa que ya tenemos de SQLAlchemy
        column_types = {}
        for col in cols_info:
            col_name = col['name']
            mysql_type_str = str(col['type']).lower()
            is_nullable = col.get('nullable', True)
            is_pk = col_name in primary_keys
            
            # Mapear a pandas dtype como hace get_mysql_column_types
            if 'varchar' in mysql_type_str or 'char' in mysql_type_str or 'text' in mysql_type_str:
                pandas_dtype = 'string'
            elif 'int' in mysql_type_str and 'bigint' not in mysql_type_str:
                pandas_dtype = 'Int32'
            elif 'bigint' in mysql_type_str:
                pandas_dtype = 'Int64'
            elif 'decimal' in mysql_type_str or 'numeric' in mysql_type_str:
                pandas_dtype = 'string'
            elif 'float' in mysql_type_str or 'double' in mysql_type_str:
                pandas_dtype = 'Float64'
            elif 'datetime' in mysql_type_str or 'timestamp' in mysql_type_str:
                pandas_dtype = 'datetime64[ns]'
            elif mysql_type_str.startswith('date'):
                pandas_dtype = 'datetime64[ns]'
            elif 'time' in mysql_type_str:
                pandas_dtype = 'object'
            elif 'bool' in mysql_type_str or mysql_type_str.startswith('tinyint(1)'):
                pandas_dtype = 'boolean'
            else:
                pandas_dtype = 'string'
            
            column_types[col_name] = {
                'pandas_dtype': pandas_dtype,
                'mysql_type': mysql_type_str,
                'nullable': is_nullable,
                'primary_key': is_pk
            }
        
        print(f"Estructura de {schema}.{table}:")
        log.info(f"Estructura de {schema}.{table}:")
        for col in cols_info:
            is_pk = col['name'] in primary_keys
            pk_indicator = " [PK]" if is_pk else ""
            col_str = f"  - {col['name']}: {col['type']} (nullable={col.get('nullable', True)}){pk_indicator}"
            print(col_str)
            log.info(col_str)
    except Exception as e:
        log.warning(f"No se pudo obtener estructura de {schema}.{table}: {e}")
        print(f"No se pudo obtener estructura de {schema}.{table}: {e}")

    # Mostrar muestra de datos
    try:
        with src_engine.connect() as conn:
            result = conn.execute(f"SELECT * FROM `{schema}`.`{table}` LIMIT 5")
            rows = result.fetchall()
            print(f"Primeros 5 datos de {schema}.{table}:")
            log.info(f"Primeros 5 datos de {schema}.{table}:")
            for row in rows:
                print(f"  {row}")
                log.info(f"  {row}")
    except Exception as e:
        log.warning(f"No se pudo obtener muestra de datos de {schema}.{table}: {e}")
        print(f"No se pudo obtener muestra de datos de {schema}.{table}: {e}")

    # Asegurar tabla destino (ajusta engine ORDER BY según dedup_mode)
    try:
        created = ensure_ch_table(
            client,
            ch_database,
            ch_table,
            src_engine,
            schema,
            table,
            dedup_mode=dedup_mode,
            unique_key=unique_key,
            version_col=version_col,
        )
        if not created:
            return 0
    except Exception as e:
        raise RecoverableError(f"No se pudo crear tabla {ch_database}.{ch_table}: {e}")

    # TRUNCATE si se pidió recarga total
    if truncate_before_load:
        try:
            client.query(f"TRUNCATE TABLE {ch_database}.{ch_table}")
        except Exception as e:
            log.warning(f"No se pudo truncar tabla {ch_database}.{ch_table}: {e}")

    # Carga
    inserted_total = 0
    try:
        log.info(f"Leyendo {schema}.{table} (chunksize={chunksize} limit={'∞' if not limit else limit})")
        for chunk_num, chunk in enumerate(read_table_in_chunks(src_engine, schema, table, chunksize, limit), start=1):
            try:
                n = insert_df(
                    client,
                    ch_database,
                    ch_table,
                    chunk,
                    unique_key=unique_key,
                    drop_dupes_in_chunk=True,
                    primary_keys=primary_keys,
                    column_types=column_types,
                )
                inserted_total += n
                log.info(
                    f"Chunk #{chunk_num}: insertados {n} filas en {ch_database}.{ch_table} (acumulado={inserted_total})"
                )
            except Exception as e:
                # Error en chunk específico - registrar pero continuar
                log.error(f"Error insertando chunk #{chunk_num}: {e}", exc_info=True)
                raise RecoverableError(f"Error en chunk #{chunk_num}, pero se continuará con siguientes")
    except RecoverableError:
        # Propagar error recuperable
        raise
    except Exception as e:
        # Error general de lectura/procesamiento
        log.error(f"Error ingiriendo {schema}.{table}: {e}", exc_info=True)
        raise RecoverableError(f"Error general ingiriendo {schema}.{table}: {e}")

    # Post-procesos según modo
    if dedup_mode == "staging":
        if not unique_key or not version_col:
            log.warning("dedup=staging requiere --unique-key y --version-col; se omitió el dedup final.")
        else:
            # En staging mode, lo normal es cargar a una tabla staging y luego swap.
            # Como aquí cargamos directo al final (para simplificar rutas), deduplicamos sobre la misma con swap.
            # Si prefieres staging real, podemos adaptar a crear {ch_table}__stg y cargar ahí primero.
            try:
                dedup_and_swap_clickhouse(
                    client,
                    ch_database,
                    ch_table,
                    unique_key=unique_key,
                    version_col=version_col,
                )
                log.info(f"Deduplicación por swap completada para {ch_database}.{ch_table}.")
            except Exception as e:
                log.error(f"Error deduplicando por staging/swap en {ch_database}.{ch_table}: {e}", exc_info=True)
                raise RecoverableError(f"Error en deduplicación: {e}")

    if dedup_mode == "replacing" and final_optimize:
        try:
            client.query(f"OPTIMIZE TABLE {ch_database}.{ch_table} FINAL")
            log.info(f"OPTIMIZE FINAL ejecutado en {ch_database}.{ch_table}.")
        except Exception as e:
            log.warning(f"No se pudo ejecutar OPTIMIZE FINAL en {ch_database}.{ch_table}: {e}")

    return inserted_total


# ---------------------------------
# Descubrimiento de tablas a ingerir
# ---------------------------------
def list_tables(engine: Engine, schemas: List[str]) -> List[Tuple[str, str]]:
    """
    Devuelve lista [(schema, table), ...] respetando los schemas pedidos.
    Omite schemas no existentes sin romper.
    """
    ins = inspect(engine)
    out = []
    for sc in schemas:
        try:
            tables = ins.get_table_names(schema=sc)
        except SQLAlchemyError as e:
            log.warning(f"No se pudo listar tablas de schema '{sc}': {e}")
            tables = []
        for t in tables:
            out.append((sc, t))
    return out


def run_audit(mysql_url: str, ch_database: str):
    """Ejecuta auditoría comparando registros entre MySQL y ClickHouse.

    NOTA: El parámetro ch_database se mantiene por compatibilidad pero las tablas
    ingeridas actualmente se crean en la base de datos del *schema* original
    (ej: schema 'archivos') con el patrón:
        {schema}.src__{source_name}__{schema}__{table}

    Esta función detecta dinámicamente source_name derivándolo de la URL MySQL
    y construye los nombres de tablas ClickHouse acorde al prefijo configurado.
    """
    try:
        import clickhouse_connect
        from sqlalchemy.engine.url import make_url

        # Detectar source_name desde la URL para mayor precisión
        try:
            parsed_url = make_url(mysql_url)
            # Usar el nombre de la base de datos como source_name si está disponible
            source_name = parsed_url.database or os.getenv("SOURCE_NAME", "default")
        except:
            source_name = os.getenv("SOURCE_NAME", "default")
        
        ch_prefix_template = os.getenv("CH_PREFIX", "src__{source}__")
        ch_prefix = ch_prefix_template.format(source=source_name)

        # Conectar a MySQL y obtener tablas
        mysql_engine = create_engine(mysql_url)
        mysql_db = mysql_engine.url.database or ""
        with mysql_engine.connect() as conn:
            mysql_tables_rows = conn.execute("SHOW TABLES").fetchall()
            mysql_tables = [row[0] for row in mysql_tables_rows]

        # Cliente ClickHouse
        ch_host = os.getenv("CH_HOST", "clickhouse")
        ch_port = int(os.getenv("CH_PORT", "8123"))
        ch_user = os.getenv("CH_USER", "etl")
        ch_pass = os.getenv("CH_PASSWORD", "")
        ch_client = clickhouse_connect.get_client(host=ch_host, port=ch_port, username=ch_user, password=ch_pass)

        log.info(f"{'Tabla':40} | {'MySQL':>10} | {'ClickHouse':>12} | {'Diferencia':>12}")
        log.info("-" * 80)

        total_mysql = 0
        total_ch = 0

        # Pre-cargar listado de tablas ClickHouse por database (usamos system.tables)
        ch_tables_map = {}
        try:
            # Traer todas las tablas de cada posible database relevante (mysql_db y ch_database por compatibilidad)
            dbs_to_scan = {mysql_db, ch_database}
            # Usar literal para evitar problemas con parámetros
            dbs_list = "','".join(dbs_to_scan)
            tbl_rows = ch_client.query(f"SELECT database,name FROM system.tables WHERE database IN ('{dbs_list}')").result_rows
            for dbname, tblname in tbl_rows:
                ch_tables_map.setdefault(dbname, set()).add(tblname)
            log.info(f"📊 Tablas detectadas en ClickHouse para auditoría: { {k: list(v) for k,v in ch_tables_map.items()} }")
        except Exception as e:
            log.info(f"⚠️ No se pudo listar system.tables, usando fallback directo: {e}")
            # Fallback: si no podemos listar, intentaremos consultas directas

        for table in mysql_tables:
            # Conteo MySQL usando ejecución directa para evitar anomalías con pandas.read_sql
            try:
                from sqlalchemy import text as _sql_text
                with mysql_engine.connect() as conn:
                    # Usar nombre calificado por si la conexión cambia de DB por pool
                    mysql_count = conn.execute(_sql_text(f"SELECT COUNT(*) FROM `{mysql_db}`.`{table}`")).scalar() or 0
            except Exception as e:
                log.debug(f"Fallo COUNT MySQL para {table}: {e}")
                mysql_count = 0

            # Resolver nombre en ClickHouse: patrón real creado por ingest_one_table:
            # ch_table = f"{ch_prefix}{schema}__{table}" con ch_database = schema
            # Aquí mysql_db actúa como schema
            # In ingest: ch_table = f"{ch_prefix}{schema}__{table}" with ch_prefix = 'src__{source}__'
            # Actual observed: src__archivos__archivos__{table}
            # So we need: ch_prefix + mysql_db + '__' + table
            candidates = [f"{ch_prefix}{mysql_db}__{table}"]
            ch_count = 0
            found = False
            log.info(f"🔍 Buscando {table} → candidato: {candidates[0]} en DB {mysql_db}")
            for cand in candidates:
                # Si no tenemos mapeo, intentar directamente (fallback)
                if not ch_tables_map or cand in ch_tables_map.get(mysql_db, set()):
                    try:
                        # No usar backticks: nombre ya es válido y backticks pueden causar error en CH client
                        ch_count = ch_client.query(f"SELECT COUNT(*) FROM {mysql_db}.{cand}").result_rows[0][0]
                        found = True
                        log.info(f"✔ Conteo ClickHouse OK {mysql_db}.{cand} = {ch_count}")
                        break
                    except Exception as e:
                        log.info(f"❌ Fallo COUNT en ClickHouse para {mysql_db}.{cand}: {e}")
                else:
                    log.info(f"⚠️ Candidato {cand} NO encontrado en mapeo de tablas para DB {mysql_db}")
            if not found:
                log.info(f"❌ Tabla ClickHouse no encontrada para {table} (candidatos: {candidates})")

            diff = ch_count - mysql_count
            log.info(f"{table:40} | {mysql_count:>10} | {ch_count:>12} | {diff:>12}")
            total_mysql += mysql_count
            total_ch += ch_count

        log.info("-" * 80)
        total_diff = total_ch - total_mysql
        log.info(f"{'TOTAL':40} | {total_mysql:>10} | {total_ch:>12} | {total_diff:>12}")

        if len(mysql_tables) == 0:
            log.warning("⚠️ No se encontraron tablas en MySQL para auditar")
        elif total_mysql == 0:
            log.warning("⚠️ Las tablas MySQL están vacías O no se pudieron contar (ver DEBUG)")
        elif total_diff == 0:
            log.info(f"✅ Auditoría exitosa: {total_mysql} registros verificados correctamente")
            log.info(f"📊 {len(mysql_tables)} tablas auditadas, todas coinciden")
        else:
            log.warning(f"⚠️ Discrepancia encontrada: {total_diff} registros de diferencia")
            if total_diff > 0:
                log.warning(f"   ClickHouse tiene {total_diff} registros MÁS que MySQL")
            else:
                log.warning(f"   ClickHouse tiene {abs(total_diff)} registros MENOS que MySQL")

    except ImportError:
        log.warning("clickhouse_connect no disponible para auditoría")
    except Exception as e:
        log.warning(f"Error en auditoría: {e}")


def validate_connections(env_vars: dict):
    """Valida conectividad a todas las conexiones definidas dinámicamente.

    Prioriza:
      1) Conexión principal resuelta (SOURCE_URL / --source-url / DB_CONNECTIONS)
      2) Todas las conexiones listadas en DB_CONNECTIONS (formato NAME=URL;...)
    """
    tested = []
    failures = []
    db_conns = env_vars.get("DB_CONNECTIONS", "").strip()
    mapping = {}
    # DB_CONNECTIONS admite 2 formatos: JSON (lista de dicts) o pares NAME=URL;NAME2=URL2
    if db_conns:
        if db_conns.startswith("["):
            # Intentar parsear como JSON
            try:
                import json
                parsed = json.loads(db_conns)
                for entry in parsed:
                    if not isinstance(entry, dict):
                        continue
                    name = entry.get("name") or entry.get("alias") or entry.get("db")
                    if not name:
                        continue
                    # Construir URL SQLAlchemy basado en 'type' (mysql) y campos
                    typ = entry.get("type", "mysql")
                    if typ.startswith("mysql"):
                        host = entry.get("host", "localhost")
                        port = entry.get("port", 3306)
                        user = entry.get("user", "root")
                        pwd = entry.get("pass", "")
                        db = entry.get("db", "")
                        url = f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}"
                        mapping[name] = url
            except Exception as e:
                log.warning(f"DB_CONNECTIONS JSON no parseable: {e}")
        else:
            # Formato legacy NAME=URL;NAME2=URL2
            for pair in db_conns.split(";"):
                pair = pair.strip()
                if not pair or "=" not in pair:
                    continue
                name, url = pair.split("=", 1)
                mapping[name.strip()] = url.strip()

    # Agregar SOURCE_URL explícita si no aparece
    if env_vars.get("SOURCE_URL"):
        primary_name = env_vars.get("SOURCE_NAME", "primary")
        mapping.setdefault(primary_name, env_vars["SOURCE_URL"])

    log.info("🔍 Validando conexiones detectadas:")
    for name, url in mapping.items():
        try:
            eng = create_engine(url, pool_pre_ping=True)
            with eng.connect() as c:
                c.execute(text("SELECT 1"))
                # Enumerar una tabla para validar permisos (si existe)
                try:
                    res = c.execute(text("SHOW TABLES"))
                    sample = next(iter(res), ("<sin_tablas>",))
                    sample_tbl = sample[0]
                except Exception:
                    sample_tbl = "<error_listado>"
            log.info(f"✅ Conexión '{name}' OK (sample tabla: {sample_tbl})")
            tested.append(name)
        except Exception as e:
            log.warning(f"❌ Conexión '{name}' FALLÓ: {e}")
            failures.append({"name": name, "error": str(e)})

    summary = {
        "tested": tested,
        "failures": failures,
        "total": len(mapping),
        "ok": len(tested),
    }
    log.info(f"📊 Resumen validación conexiones: {summary['ok']}/{summary['total']} OK")
    if failures:
        log.warning("Conexiones con error:")
        for f in failures:
            log.warning(f"  - {f['name']}: {f['error']}")
    return summary


# ---------------------------------
# CLI
# ---------------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Ingestor a ClickHouse con tolerancia a errores y deduplicación.")
    # Fuente
    p.add_argument("--source-url", default=os.getenv("SOURCE_URL"),
                   help="URL SQLAlchemy del origen (ej. mysql+pymysql://user:pass@host/db)")
    p.add_argument("--source-name", default=os.getenv("SOURCE_NAME", "default"),
                   help="Nombre lógico de la fuente (para prefijos y DB_CONNECTIONS legado)")

    # Alcance
    p.add_argument(
        "--schemas",
        nargs="+",
        default=None,
        help=(
            "Schemas a ingerir. Si omites este argumento o usas 'auto', el sistema intentará "
            "descubrir dinámicamente los schemas relevantes (por ejemplo 'archivos' y el nombre de la base). "
            "Ejemplos: --schemas archivos catalogosgral  |  --schemas auto"
        ),
    )
    p.add_argument("--include", nargs="*", default=[], help="Lista blanca de tablas (schema.table)")
    p.add_argument("--exclude", nargs="*", default=[], help="Lista negra de tablas (schema.table)")

    # ClickHouse destino
    p.add_argument("--ch-database", default=os.getenv("CH_DATABASE", "fgeo_analytics"))
    p.add_argument("--ch-prefix", default=os.getenv("CH_PREFIX", "src__{source}__"),
                   help="Prefijo para tabla destino. Usa {source} para el nombre de la conexión/alias.")

    # Rendimiento / límites
    p.add_argument("--chunksize", type=int, default=int(os.getenv("CHUNKSIZE", "50000")))
    p.add_argument("--limit", type=int, default=int(os.getenv("LIMIT", "0")),
                   help="LIMIT por tabla (0=sin límite)")

    # Modo de carga / dedup
    p.add_argument("--truncate-before-load", action="store_true",
                   help="TRUNCATE la tabla destino antes de cargar (recarga total idempotente)")
    p.add_argument("--dedup", choices=["none", "replacing", "staging"],
                   default=os.getenv("DEDUP_MODE", "none"),
                   help="Estrategia de deduplicación")
    p.add_argument("--unique-key", default=os.getenv("UNIQUE_KEY", ""),
                   help="Columna clave única para dedup (ej. id)")
    p.add_argument("--version-col", default=os.getenv("VERSION_COL", ""),
                   help="Columna de versión/fecha para elegir el registro más nuevo")
    p.add_argument("--final-optimize", action="store_true",
                   help="Si dedup=replacing, forzar OPTIMIZE FINAL al terminar cada tabla")

    # Validaciones / auditoría
    p.add_argument("--validate-connections", action="store_true",
                   help="Sólo validar conexiones (SELECT 1, listar 1 tabla) y salir")
    p.add_argument("--audit-only", action="store_true",
                   help="Sólo ejecutar auditoría (sin ingesta); requiere origen accesible")

    return p.parse_args()



def main():

    args = parse_args()
    print("🚀 INICIANDO PIPELINE ETL AUTOMÁTICO...")

    # 🔹 Resolver la URL del origen desde CLI / SOURCE_URL / DB_CONNECTIONS (dinámico .env)
    env_vars = load_env_file()
    source_url = resolve_source_url(args, env_vars)
    if not source_url:
        print("❌ ERROR: Debes especificar --source-url o una conexión válida vía SOURCE_URL o DB_CONNECTIONS (legado).")
        log.error("Debes especificar --source-url o una conexión válida vía SOURCE_URL o DB_CONNECTIONS (legado).")
        log.error("Consulta docs/ERROR_RECOVERY.md para más información.")
        sys.exit(1)

    unique_key = args.unique_key or None
    version_col = args.version_col or None

    # Conexiones
    try:
        print("⏳ Conectando a ClickHouse y MySQL...")
        client = get_clickhouse_client()
        src_engine = get_source_engine(source_url)  # ✅ usar la URL resuelta
        ensure_ch_database(client, args.ch_database)
        print("✅ Conexiones establecidas.")
    except FatalError as e:
        print(f"❌ Error fatal inicializando conexiones: {e}")
        log.error(f"Error fatal inicializando conexiones: {e}")
        sys.exit(2)
    except Exception as e:
        print(f"❌ Error inesperado inicializando conexiones: {e}")
        log.error(f"Error inesperado inicializando conexiones: {e}", exc_info=True)
        sys.exit(3)

    # Solo validar conexiones
    if args.validate_connections:
        print("🔍 Ejecutando validación de conexiones...")
        summary = validate_connections(env_vars)
        if summary["failures"]:
            print(f"❌ {len(summary['failures'])} conexión(es) fallidas. Revisa logs.")
            sys.exit(1)
        else:
            print(f"✅ Todas las conexiones ({summary['ok']}) válidas.")
            sys.exit(0)

    # Solo auditoría
    if args.audit_only:
        print("⏳ Ejecutando auditoría (modo audit-only)...")
        try:
            run_audit(source_url, args.ch_database)
            print("✅ Auditoría completada.")
            sys.exit(0)
        except Exception as e:
            print(f"❌ Auditoría falló: {e}")
            sys.exit(1)


    # Descubrir tablas
    # Autodetección de schemas si no se proporcionaron o si se pidió 'auto'
    if args.schemas is None or args.schemas == ["auto"]:
        detected = []
        try:
            insp = inspect(src_engine)
            # Candidatos: 'archivos' y el nombre de la base actual
            candidate_db = src_engine.url.database
            for cand in {"archivos", candidate_db}:
                if not cand:
                    continue
                try:
                    tables = insp.get_table_names(schema=cand)
                    if tables:
                        detected.append(cand)
                except Exception as e:
                    log.debug(f"Schema candidato '{cand}' no usable: {e}")
            # Si ninguno detectado, como último recurso intentar schema=nombre_db aunque no listó tablas (para fallback SHOW TABLES)
            if not detected and candidate_db:
                detected.append(candidate_db)
            args.schemas = sorted(set(detected)) if detected else ["archivos"]
            log.info(f"📌 Schemas auto-detectados: {args.schemas}")
        except Exception as e:
            log.warning(f"Fallo autodetección de schemas, se usará 'archivos': {e}")
            args.schemas = ["archivos"]

    all_pairs = list_tables(src_engine, args.schemas)  # [(schema, table)]
    include_set = set(args.include) if args.include else None
    exclude_set = set(args.exclude) if args.exclude else set()


    # --- Crear dinámicamente las bases de datos necesarias en ClickHouse ---
    detected_databases = set([args.ch_database] + [schema for (schema, _) in all_pairs])
    for db_name in detected_databases:
        ensure_ch_database(client, db_name)

    print("⏳ Iniciando ingesta de datos...")
    total_rows = 0
    failed_tables = []

    for (schema, table) in all_pairs:
        full = f"{schema}.{table}"
        
        # Lógica mejorada de filtrado: admite tanto "schema.table" como solo "table"
        if include_set is not None:
            # Verificar si coincide el formato completo O solo el nombre de la tabla
            if full not in include_set and table not in include_set:
                continue
        
        # Lógica de exclusión: admite tanto "schema.table" como solo "table"
        if full in exclude_set or table in exclude_set:
            continue

        ch_table = f"{args.ch_prefix.format(source=args.source_name)}{schema}__{table}".replace(".", "__")

        try:
            print(f"🔄 Ingestando tabla: {full} → ClickHouse:{schema}.{ch_table}")
            rows = ingest_one_table(
                client=client,
                src_engine=src_engine,
                schema=schema,
                table=table,
                ch_database=schema,  # Usar el esquema como base destino
                ch_table=ch_table,
                chunksize=args.chunksize,
                limit=(args.limit if args.limit > 0 else None),
                truncate_before_load=args.truncate_before_load,
                dedup_mode=args.dedup,
                unique_key=unique_key,
                version_col=version_col,
                final_optimize=args.final_optimize,
            )
            print(f"✅ Tabla {full} procesada: {rows} filas insertadas.")
            total_rows += rows
        except RecoverableError as e:
            print(f"⚠️  Error recuperable en tabla {full}: {e}")
            log.warning(f"Error recuperable en tabla {full}: {e}")
            failed_tables.append({"table": full, "error": str(e), "type": "recoverable"})
        except FatalError as e:
            print(f"❌ Error fatal en tabla {full}: {e}")
            log.error(f"Error fatal en tabla {full}: {e}")
            failed_tables.append({"table": full, "error": str(e), "type": "fatal"})
            log.error("Abortando ingesta debido a error fatal.")
            sys.exit(2)
        except Exception as e:
            print(f"❌ Error inesperado en tabla {full}: {e}")
            log.error(f"Error inesperado en tabla {full}: {e}", exc_info=True)
            failed_tables.append({"table": full, "error": str(e), "type": "unexpected"})

    print(f"== INGESTA TERMINADA == Filas insertadas totales: {total_rows}")
    if failed_tables:
        print(f"⚠️  {len(failed_tables)} tabla(s) con errores. Consulta logs para detalles.")

    # Ejecutar auditoría automática al final
    try:
        print("⏳ Ejecutando auditoría MySQL → ClickHouse...")
        run_audit(source_url, args.ch_database)
        print("✅ Auditoría completada.")
    except Exception as e:
        print(f"⚠️  Error en auditoría: {e}")
        log.warning(f"Error en auditoría: {e}")

    print("🎉 Pipeline ETL finalizado. Ya puedes consultar los datos en Superset con el usuario 'admin' o el usuario configurado.")

    log.info(f"== INGESTA TERMINADA == Filas insertadas totales: {total_rows}")
    
    # Ejecutar auditoría automática al final
    try:
        log.info("Ejecutando auditoría MySQL → ClickHouse...")
        run_audit(source_url, args.ch_database)
    except Exception as e:
        log.warning(f"Error en auditoría: {e}")
    
    # Escribir reporte de estado para orquestador externo
    status_report = {
        "success": len(failed_tables) == 0,
        "total_rows": total_rows,
        "failed_tables": failed_tables,
        "successful_tables": len(all_pairs) - len(failed_tables),
        "total_tables": len(all_pairs),
        "source_name": args.source_name,
        "ch_database": args.ch_database,
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z"
    }
    
    try:
        os.makedirs("logs", exist_ok=True)
        with open("logs/ingest_status.json", "w", encoding="utf-8") as f:
            json.dump(status_report, f, indent=2, ensure_ascii=False)
        log.info(f"📋 Reporte de estado guardado: logs/ingest_status.json")
    except Exception as e:
        log.warning(f"No se pudo escribir logs/ingest_status.json: {e}")
    
    if failed_tables:
        log.warning(f"⚠ {len(failed_tables)} tabla(s) con errores:")
        for ft in failed_tables:
            log.warning(f"  - {ft['table']} ({ft['type']}): {ft['error']}")
        log.warning("Consulta docs/ERROR_RECOVERY.md para soluciones.")
        sys.exit(1)
    elif len(all_pairs) == 0:
        log.warning("⚠️ No se encontraron tablas para procesar con los esquemas/filtros especificados")
        sys.exit(0)
    else:
        successful_count = len(all_pairs) - len(failed_tables)
        log.info(f"✅ Ingesta completada exitosamente: {successful_count}/{len(all_pairs)} tablas procesadas")
        log.info(f"📊 Total de registros insertados: {total_rows:,}")
        sys.exit(0)


if __name__ == "__main__":
    main()
