#!/usr/bin/env python3
"""
Parche inmediato para arreglar el problema de serialización de nulos en ClickHouse.
"""

import pandas as pd
import numpy as np
import datetime
from typing import Any

def enhanced_cell_processor(v: Any) -> Any:
    """Procesador mejorado de celdas para ClickHouse."""
    
    # Casos de valores nulos
    if v is None:
        return None
    
    # pandas NA/NaT/NaN
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    
    # String vacía
    if isinstance(v, str) and v.strip() == "":
        return None
    
    # Pandas nullable types (_isna method)
    if hasattr(v, '_isna'):
        try:
            if v._isna:
                return None
        except:
            pass
    
    # pandas._libs.missing.NAType
    if str(type(v)).find('NAType') != -1:
        return None
    
    # pandas nullable integers
    if hasattr(v, 'dtype') and str(v.dtype).startswith(('Int', 'UInt', 'Float')):
        try:
            if pd.isna(v):
                return None
            return int(v) if str(v.dtype).startswith(('Int', 'UInt')) else float(v)
        except:
            return None
    
    # Numeric conversion for object types that might be numeric strings
    if isinstance(v, (str, bytes)):
        try:
            # Try to convert to number if possible
            if isinstance(v, bytes):
                v = v.decode('utf-8', errors='ignore')
            v_stripped = str(v).strip()
            if v_stripped.replace('-', '').replace('.', '').isdigit():
                if '.' in v_stripped:
                    return float(v_stripped)
                else:
                    num_val = int(v_stripped)
                    # Check if it fits in Int32 range
                    if -2147483648 <= num_val <= 2147483647:
                        return num_val
                    else:
                        return num_val  # Let ClickHouse handle it
            return v_stripped if v_stripped else None
        except:
            return str(v) if v else None
    
    # Timestamps
    if isinstance(v, pd.Timestamp):
        if pd.isna(v):
            return None
        if v.tzinfo is not None:
            v = v.tz_localize(None)
        return v.to_pydatetime()
    
    # numpy datetime64
    if isinstance(v, np.datetime64):
        if pd.isna(v):
            return None
        ts = pd.Timestamp(v)
        if ts.tzinfo is not None:
            ts = ts.tz_localize(None)
        return ts.to_pydatetime()
    
    # datetime handling
    if isinstance(v, datetime.date) and not isinstance(v, datetime.datetime):
        return datetime.datetime(v.year, v.month, v.day)
    
    if isinstance(v, datetime.datetime):
        if v.tzinfo is not None:
            v = v.replace(tzinfo=None)
        return v
    
    # Numeric types
    if isinstance(v, (int, np.integer)):
        # Check for overflow values that might cause issues
        val = int(v)
        return val
    
    if isinstance(v, (float, np.floating)):
        if np.isnan(v) or np.isinf(v):
            return None
        return float(v)
    
    # Boolean
    if isinstance(v, (bool, np.bool_)):
        return int(v)
    
    # Default for other types
    try:
        return v
    except:
        return str(v) if v is not None else None


def enhanced_preprocess_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Preprocesamiento mejorado del DataFrame."""
    df = df.copy()
    
    for col in df.columns:
        dtype = str(df[col].dtype)
        
        # Handle object columns (might contain mixed types)
        if dtype == 'object':
            df[col] = df[col].apply(enhanced_cell_processor)
        
        # Handle pandas nullable integer types
        elif dtype.startswith('Int') or dtype.startswith('UInt'):
            df[col] = df[col].apply(lambda x: None if pd.isna(x) else int(x))
        
        # Handle pandas nullable float types  
        elif dtype.startswith('Float'):
            df[col] = df[col].apply(lambda x: None if pd.isna(x) else float(x))
        
        # Handle regular int/float with potential NaN
        elif dtype.startswith(('int', 'float')):
            df[col] = df[col].apply(enhanced_cell_processor)
    
    return df


# Patch the functions in ingest_runner.py
def apply_patch():
    """Apply the immediate patch to fix null handling."""
    import sys
    import os
    
    # Add current directory to path
    sys.path.insert(0, '/app/tools')
    
    # Import and patch the module
    try:
        import ingest_runner
        
        # Replace the problematic functions
        ingest_runner.preprocess_dataframe_for_clickhouse = enhanced_preprocess_dataframe
        
        # Also patch the cell function if it exists
        if hasattr(ingest_runner, 'cell'):
            ingest_runner.cell = enhanced_cell_processor
        
        print("✅ Patch aplicado exitosamente")
        return True
    except Exception as e:
        print(f"❌ Error aplicando patch: {e}")
        return False


if __name__ == "__main__":
    apply_patch()