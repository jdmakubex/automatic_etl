#!/usr/bin/env python3
"""
Script para crear datasets usando el contexto de aplicación de Superset
"""

# Importar los módulos necesarios
from superset.app import create_app
from superset import db
from superset.models.core import Database
from superset.connectors.sqla.models import SqlaTable, TableColumn
from sqlalchemy import inspect
import logging

# Configurar logging básico
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Crear contexto de aplicación
app = create_app()

with app.app_context():
    try:
        print("🔍 Buscando base de datos ClickHouse...")
        
        # Buscar la base de datos ClickHouse
        clickhouse_db = db.session.query(Database).filter(
            Database.database_name.like('%ClickHouse%')
        ).first()
        
        if not clickhouse_db:
            print("❌ Base de datos ClickHouse no encontrada")
            # Listar todas las bases de datos disponibles
            all_dbs = db.session.query(Database).all()
            print("📋 Bases de datos disponibles:")
            for db_item in all_dbs:
                print(f"  - {db_item.database_name}")
            exit(1)
        
        print(f"✅ Base de datos encontrada: {clickhouse_db.database_name} (ID: {clickhouse_db.id})")
        print(f"🔗 URI: {clickhouse_db.sqlalchemy_uri}")
        
        # Obtener inspector para la base de datos
        try:
            engine = clickhouse_db.get_sqla_engine()
            inspector = inspect(engine)
            
            # Obtener tablas del esquema
            schema_name = 'fgeo_analytics'
            print(f"📋 Buscando tablas en esquema: {schema_name}")
            
            table_names = inspector.get_table_names(schema=schema_name)
            if not table_names:
                # Intentar sin esquema específico
                table_names = inspector.get_table_names()
                schema_name = None
                print("📋 Tablas encontradas (esquema por defecto):")
            else:
                print(f"📋 Tablas encontradas en {schema_name}:")
            
            for table_name in table_names:
                print(f"  - {table_name}")
            
            if not table_names:
                print("❌ No se encontraron tablas")
                exit(1)
            
            # Crear datasets para cada tabla
            created_count = 0
            updated_count = 0
            
            for table_name in table_names:
                print(f"\n🔄 Procesando tabla: {table_name}")
                
                # Verificar si el dataset ya existe
                existing_dataset = db.session.query(SqlaTable).filter(
                    SqlaTable.table_name == table_name,
                    SqlaTable.database_id == clickhouse_db.id
                ).first()
                
                if existing_dataset:
                    print(f"⚠️  Dataset '{table_name}' ya existe, actualizando...")
                    dataset = existing_dataset
                    updated_count += 1
                else:
                    # Crear nuevo dataset
                    dataset = SqlaTable(
                        table_name=table_name,
                        database=clickhouse_db,
                        schema=schema_name
                    )
                    db.session.add(dataset)
                    db.session.flush()  # Para obtener el ID
                    print(f"✅ Dataset '{table_name}' creado")
                    created_count += 1
                
                # Obtener columnas de la tabla
                try:
                    columns = inspector.get_columns(table_name, schema=schema_name)
                    print(f"  📄 Encontradas {len(columns)} columnas")
                    
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
                            print(f"    + {col_name} ({col_type})")
                        else:
                            print(f"    ✓ {col_name} ({col_type})")
                    
                except Exception as col_error:
                    print(f"  ⚠️  Error obteniendo columnas: {col_error}")
            
            # Commit los cambios
            try:
                db.session.commit()
                print(f"\n✅ Configuración completada:")
                print(f"   📊 Datasets creados: {created_count}")
                print(f"   🔄 Datasets actualizados: {updated_count}")
                print(f"   📋 Total procesados: {created_count + updated_count}")
                
            except Exception as commit_error:
                print(f"❌ Error al guardar datasets: {commit_error}")
                db.session.rollback()
                exit(1)
                
        except Exception as engine_error:
            print(f"❌ Error conectando a ClickHouse: {engine_error}")
            exit(1)
            
    except Exception as general_error:
        print(f"❌ Error general: {general_error}")
        exit(1)

print("🎉 ¡Configuración de datasets completada exitosamente!")