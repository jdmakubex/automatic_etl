#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import math
import decimal
import datetime
import os
import sys
import json
import argparse
import logging
from decimal import Decimal
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote_plus
import warnings
from pandas.core.dtypes.dtypes import DatetimeTZDtype
import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


# Silencia el aviso de pandas "Could not infer format..." durante to_datetime
warnings.filterwarnings(
    "ignore",
    message="Could not infer format, so each element will be parsed individually, falling back to `dateutil`.*",
    category=UserWarning
)

# Silencia deprecations de pandas que no te afectan funcionalmente
warnings.filterwarnings(
    "ignore",
    category=DeprecationWarning,
    module=r"pandas.*"
)

# Cliente ClickHouse
# clickhouse-connect es el que usamos (client.insert / client.query)
import clickhouse_connect


def resolve_source_url(args, env):
    """
    Prioridad:
      1) CLI: --source-url
      2) ENV: SOURCE_URL
      3) ENV: DB_CONNECTIONS con 'url'
      4) ENV: DB_CONNECTIONS legado (host/port/user/pass/db) -> construye URL
    Permite elegir conexión por nombre con --source-name o SOURCE_NAME (default: 'default').
    """
    # 1) CLI
    su = getattr(args, "source_url", None)
    if su:
        return su

    # 2) SOURCE_URL
    su = env.get("SOURCE_URL")
    if su:
        return su

    # 3/4) DB_CONNECTIONS
    dc_raw = env.get("DB_CONNECTIONS")
    if not dc_raw:
        return None

    import json
    try:
        conns = json.loads(dc_raw)
    except Exception:
        return None

    # Normalizar a lista
    if isinstance(conns, dict):
        conns = [conns]
    if not isinstance(conns, list) or not conns:
        return None

    # Selección por nombre (si hay)
    preferred = getattr(args, "source_name", None) or env.get("SOURCE_NAME") or "default"
    chosen = next((c for c in conns if c.get("name") == preferred), conns[0])

    # 3) url directa
    if chosen.get("url"):
        return chosen["url"]

    # 4) legado: construir URL
    required = ("host", "user", "pass", "db")
    if not all(k in chosen for k in required):
        return None

    driver = chosen.get("driver", "mysql+pymysql")
    host   = chosen["host"]
    port   = chosen.get("port", 3306)
    user   = chosen["user"]
    pwd    = chosen["pass"]
    db     = chosen["db"]

    # Escapar user/pass por si tienen caracteres especiales
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


# Configurar logging según formato especificado
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FORMAT = os.getenv("LOG_FORMAT", "text").lower()

if LOG_FORMAT == "json":
    handler = logging.StreamHandler()
    handler.setFormatter(JSONFormatter())
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        handlers=[handler]
    )
else:
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

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


def normalize_for_clickhouse(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza dataframe para ClickHouse:
    - datetimes → naive (sin tz)
    - bool → Int8
    - Decimal → str o float (mantenemos str si hay precisión alta)
    - NaN/NaT → None
    - Objetos raros → str
    """
    if df is None or df.empty:
        return df

    out = df.copy()

    # 1) Datetimes
    for col in out.columns:
        out[col] = _parse_maybe_datetime_series(out[col])

    # 2) Bools → Int8
    bool_cols = out.select_dtypes(include=["bool"]).columns
    if len(bool_cols) > 0:
        out[bool_cols] = out[bool_cols].astype("int8")

    # 3) Decimals / objetos → str si hace falta
    def _clean_cell(v):
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return None
        if isinstance(v, (np.floating,)):
            if np.isnan(v):
                return None
            return float(v)
        # ✅ pandas Timestamp → datetime nativo (naive)
        if isinstance(v, (pd.Timestamp,)):
            # ya deberían venir sin tz
            return None if pd.isna(v) else v.to_pydatetime()
        # ✅ NUEVO: datetime.datetime → devolver como datetime (sin tz)
        elif isinstance(v, datetime.datetime):
        # quitar tz si la hubiera
            if v.tzinfo is not None:
                v = v.replace(tzinfo=None)
            return v

        # ✅ NUEVO: datetime.date → elevar a datetime (00:00:00)
        elif isinstance(v, datetime.date):
            return datetime.datetime(v.year, v.month, v.day)
        if isinstance(v, Decimal):
            # Para no perder precisión grande lo mandamos como string
            return str(v)
        # Tipos básicos OK
        if isinstance(v, (int, float, str, bytes)):
            return v
        # Otros (dict/list/obj) -> str
        try:
            return json.dumps(v, ensure_ascii=False)
        except Exception:
            return str(v)

    out = out.where(pd.notna(out), None)
    for c in out.columns:
        out[c] = out[c].map(_clean_cell)

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
    for col in out.columns:
        s = out[col]

        # Detecta candidato a fecha (object/str, datetime, numpy datetime64)
        if (s.dtype == object) or str(s.dtype).startswith(("datetime64", "datetime64[ns", "datetime64[us")):
            try:
                # Convierte a datetime (sin timezone); entradas inválidas -> NaT
                parsed = pd.to_datetime(s, errors="coerce", utc=False)

                # Si viene con tz (poco común aquí), quítala
                # Nota: el dtype tz-aware en pandas moderno es pd.DatetimeTZDtype
                try:
                    # pandas >= 2
                    if isinstance(parsed.dtype, pd.DatetimeTZDtype):
                        parsed = parsed.dt.tz_localize(None)
                except Exception:
                    # fallback por si la clase no existe en la versión instalada
                    if hasattr(parsed.dt, "tz_localize"):
                        try:
                            parsed = parsed.dt.tz_localize(None)
                        except Exception:
                            pass

                # Normaliza fechas 'cero' que llegan como string
                if s.dtype == object:
                    mask_zero = s.astype(str).str.strip().isin(["0000-00-00", "0000-00-00 00:00:00"])
                    if mask_zero.any():
                        parsed = parsed.mask(mask_zero, pd.NaT)

                out[col] = parsed

            except Exception:
                # Si algo raro pasa, no rompas: deja la columna como estaba
                pass

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


# --- MODIFICAR: preparación de filas a insertar ---
def dataframe_to_clickhouse_rows(df: pd.DataFrame) -> tuple[list[tuple], list[str]]:
    """
    Convierte un DataFrame a (data, cols) para clickhouse_connect.client.insert,
    garantizando tipos nativos seguros para ClickHouse y limpiando valores raros.
    """
    cols = list(df.columns)

    def cell(v):
        # None / NaN / NaT
        if v is None:
            return None
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass

        # pandas.Timestamp -> datetime (naive)
        if isinstance(v, pd.Timestamp):
            if v.tzinfo is not None:
                v = v.tz_localize(None)
            return v.to_pydatetime()

        # numpy.datetime64 -> datetime (naive)
        if isinstance(v, np.datetime64):
            ts = pd.Timestamp(v)
            if ts.tzinfo is not None:
                ts = ts.tz_localize(None)
            return ts.to_pydatetime()

        # date -> datetime (00:00:00)
        if isinstance(v, datetime.date) and not isinstance(v, datetime.datetime):
            return datetime.datetime(v.year, v.month, v.day)

        # datetime (naive)
        if isinstance(v, datetime.datetime):
            if v.tzinfo is not None:
                v = v.replace(tzinfo=None)
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
                # Decimal(18,0) → int, perfecto para ClickHouse Decimal(18,0)
                return int(v)
            # Para decimales con parte fraccional, mejor str (ClickHouse castea sin perder precisión)
            return str(v)

        # Tipos numpy -> nativos
        if isinstance(v, np.bool_):
            return bool(v)
        if isinstance(v, np.integer):
            return int(v)
        if isinstance(v, np.floating):
            fv = float(v)
            if math.isnan(fv) or math.isinf(fv):
                return None
            return fv

        # Por defecto, devolver tal cual (dict/list se serializan aguas arriba si aplica)
        return v

    data = [tuple(cell(df.at[i, c]) for c in cols) for i in df.index]
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
    """
    cols = []
    try:
        cols_info = ins.get_columns(table, schema=schema)
    except Exception as e:
        raise RuntimeError(f"No se pudieron leer columnas de {schema}.{table}: {e}") from e

    for c in cols_info:
        name = c["name"]
        type_ = str(c["type"]).lower()

        # heurística simple de mapeo
        if "int" in type_:
            ch_type = "Int32"
        elif "bigint" in type_:
            ch_type = "Int64"
        elif "smallint" in type_:
            ch_type = "Int16"
        elif "tinyint(1)" in type_ or "bool" in type_:
            ch_type = "Int8"
        elif "tinyint" in type_:
            ch_type = "Int8"
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

        nullable = c.get("nullable", True)
        if nullable and not ch_type.startswith("Nullable("):
            ch_type = f"Nullable({ch_type})"

        cols.append((name, ch_type))
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
    # Si no hay order_by, usar la primera columna; nunca '_dummy_'
    order_sql = ", ".join(f"`{c}`" for c in (order_by if order_by else [columns[0][0]]))
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
    """
    if not source_table_exists(src_engine, schema, src_table):
        log.warning(
            f"Tabla omitida {schema}.{src_table} → La tabla no existe o no es accesible en el origen (omitida)."
        )
        return False

    ins = inspect(src_engine)
    columns = map_sqlalchemy_to_ch(ins, src_engine, schema, src_table)

    engine_name = "MergeTree"
    order_by = [unique_key] if unique_key else []
    if dedup_mode == "replacing" and unique_key and version_col:
        engine_name = "ReplacingMergeTree"
    if not order_by:
        # orden por nada: tuple()
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
) -> int:
    if df is None or df.empty:
        return 0

    # 🔹 Asegurar que TODAS las columnas de fecha queden como datetime (o None)
    df = coerce_datetime_columns(df)

    # 🔹 Normalización general para ClickHouse (bools, Decimal, NaN/NaT → None, etc.)
    df = normalize_for_clickhouse(df)

    # 🔹 Deduplicado intra-chunk opcional
    if drop_dupes_in_chunk and unique_key and unique_key in df.columns:
        version_candidates = [c for c in ["factualizacion", "fcreacion", "updated_at", "fecha_actualizacion"] if c in df.columns]
        if version_candidates:
            vcol = version_candidates[0]
            df = df.sort_values(by=[unique_key, vcol], ascending=[True, False])
        df = df.drop_duplicates(subset=[unique_key], keep="first")

    # 🔹 Convertir el DF a filas con datetime nativo/None garantizado
    data, cols = dataframe_to_clickhouse_rows(df)
    client.insert(f"{database}.{table}", data, column_names=cols)
    return len(data)



# ---------------------------------
# Lectura por chunks desde SQLAlchemy
# ---------------------------------
def read_table_in_chunks(engine: Engine, schema: str, table: str, chunksize: int, limit: Optional[int]) -> pd.DataFrame:
    base_sql = f"SELECT * FROM `{schema}`.`{table}`"
    if limit and limit > 0:
        base_sql += f" LIMIT {int(limit)}"
    return pd.read_sql(text(base_sql), con=engine, chunksize=chunksize)


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
    """
    final = f"{database}.{final_table}"
    tmp = f"{database}.tmp__{final_table}__dedup"
    old = f"{database}.{final_table}__old"

    # 1) Crear tabla tmp (estructura = final)
    client.query(f"DROP TABLE IF EXISTS {tmp}")
    client.query(f"CREATE TABLE {tmp} AS {final} ENGINE=MergeTree ORDER BY `{unique_key}`")

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

    # Mostrar estructura de la tabla
    try:
        ins = inspect(src_engine)
        cols_info = ins.get_columns(table, schema=schema)
        print(f"Estructura de {schema}.{table}:")
        log.info(f"Estructura de {schema}.{table}:")
        for col in cols_info:
            col_str = f"  - {col['name']}: {col['type']} (nullable={col.get('nullable', True)})"
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
    p.add_argument("--schemas", nargs="+", default=os.getenv("SCHEMAS", "archivos").split(),
                   help="Schemas a ingerir")
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

    return p.parse_args()



def main():
    args = parse_args()

    # 🔹 Resolver la URL del origen desde CLI / SOURCE_URL / DB_CONNECTIONS (legado)
    source_url = resolve_source_url(args, os.environ)
    if not source_url:
        log.error("Debes especificar --source-url o una conexión válida vía SOURCE_URL o DB_CONNECTIONS (legado).")
        log.error("Consulta docs/ERROR_RECOVERY.md para más información.")
        sys.exit(1)

    unique_key = args.unique_key or None
    version_col = args.version_col or None

    # Conexiones
    try:
        client = get_clickhouse_client()
        src_engine = get_source_engine(source_url)  # ✅ usar la URL resuelta
        ensure_ch_database(client, args.ch_database)
    except FatalError as e:
        log.error(f"Error fatal inicializando conexiones: {e}")
        sys.exit(2)
    except Exception as e:
        log.error(f"Error inesperado inicializando conexiones: {e}", exc_info=True)
        sys.exit(3)

    # Descubrir tablas
    all_pairs = list_tables(src_engine, args.schemas)  # [(schema, table)]
    include_set = set(args.include) if args.include else None
    exclude_set = set(args.exclude) if args.exclude else set()

    total_rows = 0
    failed_tables = []
    
    for (schema, table) in all_pairs:
        full = f"{schema}.{table}"
        if include_set is not None and full not in include_set:
            continue
        if full in exclude_set:
            continue

        ch_table = f"{args.ch_prefix.format(source=args.source_name)}{schema}__{table}".replace(".", "__")

        try:
            rows = ingest_one_table(
                client=client,
                src_engine=src_engine,
                schema=schema,
                table=table,
                ch_database=args.ch_database,
                ch_table=ch_table,
                chunksize=args.chunksize,
                limit=(args.limit if args.limit > 0 else None),
                truncate_before_load=args.truncate_before_load,
                dedup_mode=args.dedup,
                unique_key=unique_key,
                version_col=version_col,
                final_optimize=args.final_optimize,
            )
            total_rows += rows
        except RecoverableError as e:
            # Error recuperable - registrar y continuar con siguiente tabla
            log.warning(f"Error recuperable en tabla {full}: {e}")
            failed_tables.append({"table": full, "error": str(e), "type": "recoverable"})
        except FatalError as e:
            # Error fatal - abortar
            log.error(f"Error fatal en tabla {full}: {e}")
            failed_tables.append({"table": full, "error": str(e), "type": "fatal"})
            log.error("Abortando ingesta debido a error fatal.")
            sys.exit(2)
        except Exception as e:
            # Error inesperado - registrar y continuar
            log.error(f"Error inesperado en tabla {full}: {e}", exc_info=True)
            failed_tables.append({"table": full, "error": str(e), "type": "unexpected"})

    log.info(f"== INGESTA TERMINADA == Filas insertadas totales: {total_rows}")
    
    if failed_tables:
        log.warning(f"⚠ {len(failed_tables)} tabla(s) con errores:")
        for ft in failed_tables:
            log.warning(f"  - {ft['table']} ({ft['type']}): {ft['error']}")
        log.warning("Consulta docs/ERROR_RECOVERY.md para soluciones.")
        sys.exit(1)
    else:
        log.info("✓ Todas las tablas se ingirieron exitosamente")
        sys.exit(0)


if __name__ == "__main__":
    main()
