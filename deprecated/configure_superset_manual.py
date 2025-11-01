#!/usr/bin/env python3
"""
Configurador manual de ClickHouse en Superset
"""
import os
from superset import app
from superset.models.core import Database
from superset.extensions import db

def configure_clickhouse():
    """Configura ClickHouse en Superset usando SQLAlchemy directamente"""
    print("🔧 Configurando ClickHouse en Superset...")
    
    with app.app_context():
        # Verificar si ya existe la base de datos
        existing_db = db.session.query(Database).filter_by(database_name='fgeo_analytics_clickhouse').first()
        
        if existing_db:
            print("✅ Base de datos ClickHouse ya existe")
            print(f"   - Nombre: {existing_db.database_name}")
            print(f"   - URI: {existing_db.sqlalchemy_uri}")
            return existing_db
        else:
            # Crear nueva base de datos ClickHouse
            clickhouse_db = Database(
                database_name='fgeo_analytics_clickhouse',
                sqlalchemy_uri='clickhouse+http://etl:Et1Ingest!@clickhouse:8123/fgeo_analytics',
                expose_in_sqllab=True,
                allow_ctas=True,
                allow_cvas=True,
                allow_run_async=True,
                cache_timeout=86400,  # 24 horas
                extra='''{
                    "allows_virtual_table_explore": true,
                    "disable_data_preview": false
                }'''
            )
            
            # Agregar a la sesión y confirmar
            db.session.add(clickhouse_db)
            db.session.commit()
            
            print("✅ Base de datos ClickHouse configurada exitosamente")
            print(f"   - Nombre: {clickhouse_db.database_name}")
            print(f"   - URI: {clickhouse_db.sqlalchemy_uri}")
            print(f"   - ID: {clickhouse_db.id}")
            
            return clickhouse_db

if __name__ == "__main__":
    configure_clickhouse()