#!/usr/bin/env python3
"""
Script para crear datasets usando el CLI de Superset (shell)
"""
import os

# Código Python que se ejecutará dentro del shell de Superset
superset_commands = '''
from superset import db
from superset.models.core import Database
from superset.connectors.sqla.models import SqlaTable
from superset.connectors.sqla.models import TableColumn

print("🔍 Buscando base de datos ClickHouse...")

# Buscar la base de datos ClickHouse
clickhouse_db = db.session.query(Database).filter(Database.database_name.like('%ClickHouse%')).first()

if not clickhouse_db:
    print("❌ Base de datos ClickHouse no encontrada")
    exit(1)

print(f"✅ Base de datos encontrada: {clickhouse_db.database_name} (ID: {clickhouse_db.id})")

# Obtener las tablas disponibles
from superset import security_manager
from sqlalchemy import create_engine, inspect

engine = clickhouse_db.get_sqla_engine()
inspector = inspect(engine)

print("📋 Tablas disponibles:")
table_names = inspector.get_table_names(schema='fgeo_analytics')
for table_name in table_names:
    print(f"  - {table_name}")

# Crear datasets para cada tabla
created_datasets = []
for table_name in table_names:
    print(f"🔄 Creando dataset para tabla: {table_name}")
    
    # Verificar si el dataset ya existe
    existing_dataset = db.session.query(SqlaTable).filter(
        SqlaTable.table_name == table_name,
        SqlaTable.database_id == clickhouse_db.id
    ).first()
    
    if existing_dataset:
        print(f"⚠️  Dataset '{table_name}' ya existe, actualizando...")
        dataset = existing_dataset
    else:
        # Crear nuevo dataset
        dataset = SqlaTable(
            table_name=table_name,
            database=clickhouse_db,
            schema='fgeo_analytics'
        )
        db.session.add(dataset)
        print(f"✅ Dataset '{table_name}' creado")
    
    # Obtener columnas de la tabla
    columns = inspector.get_columns(table_name, schema='fgeo_analytics')
    
    # Crear/actualizar columnas
    for col_info in columns:
        col_name = col_info['name']
        col_type = str(col_info['type'])
        
        existing_column = db.session.query(TableColumn).filter(
            TableColumn.table_id == dataset.id,
            TableColumn.column_name == col_name
        ).first()
        
        if not existing_column:
            column = TableColumn(
                column_name=col_name,
                type=col_type,
                table=dataset
            )
            db.session.add(column)
    
    created_datasets.append(table_name)

# Commit los cambios
try:
    db.session.commit()
    print(f"✅ Se crearon/actualizaron {len(created_datasets)} datasets:")
    for dataset_name in created_datasets:
        print(f"  - {dataset_name}")
except Exception as e:
    print(f"❌ Error al guardar datasets: {e}")
    db.session.rollback()

print("🎉 Configuración de datasets completada!")
'''

# Escribir el script temporal
with open('/tmp/superset_dataset_creation.py', 'w') as f:
    f.write(superset_commands)

print("✅ Script creado en /tmp/superset_dataset_creation.py")
print("🔄 Ejecutar con: docker exec superset python /tmp/superset_dataset_creation.py")