#!/usr/bin/env python3
"""
Script para corregir esquemas de ClickHouse haciendo columnas nullable cuando sea necesario.
Resuelve el problema "Invalid None value in non-Nullable column".
"""

import sys
import os
import json
import logging
from pathlib import Path
import clickhouse_connect
from typing import Dict, List, Tuple

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ClickHouseSchemaFixer:
    """Herramienta para corregir esquemas de ClickHouse y hacerlos más tolerantes a NULL."""
    
    def __init__(self, clickhouse_host='clickhouse', clickhouse_port=8123, 
                 clickhouse_user='etl', clickhouse_password='Et1Ingest!'):
        """
        Inicializar el corrector de esquemas.
        
        Args:
            clickhouse_host: Host de ClickHouse
            clickhouse_port: Puerto de ClickHouse
            clickhouse_user: Usuario de ClickHouse
            clickhouse_password: Contraseña de ClickHouse
        """
        self.client = clickhouse_connect.get_client(
            host=clickhouse_host,
            port=clickhouse_port,
            username=clickhouse_user,
            password=clickhouse_password
        )
        
    def get_table_schema(self, database: str, table: str) -> List[Dict]:
        """
        Obtener el esquema actual de una tabla.
        
        Args:
            database: Nombre de la base de datos
            table: Nombre de la tabla
            
        Returns:
            Lista de diccionarios con información de columnas
        """
        try:
            query = f"DESCRIBE TABLE `{database}`.`{table}`"
            result = self.client.query(query)
            
            columns = []
            for row in result.result_rows:
                name, type_def, default_type, default_expr, comment, codec_expr, ttl_expr = row
                columns.append({
                    'name': name,
                    'type': type_def,
                    'default_type': default_type,
                    'default_expression': default_expr,
                    'comment': comment,
                    'is_nullable': 'Nullable(' in type_def
                })
                
            return columns
            
        except Exception as e:
            logger.error(f"Error obteniendo esquema de {database}.{table}: {e}")
            return []
    
    def fix_column_nullability(self, database: str, table: str, column: str, 
                              current_type: str, make_nullable: bool = True) -> bool:
        """
        Modificar una columna para hacerla nullable o no nullable.
        
        Args:
            database: Nombre de la base de datos
            table: Nombre de la tabla
            column: Nombre de la columna
            current_type: Tipo actual de la columna
            make_nullable: Si hacer la columna nullable
            
        Returns:
            True si la modificación fue exitosa
        """
        try:
            # Determinar el nuevo tipo
            if make_nullable and not current_type.startswith('Nullable('):
                new_type = f"Nullable({current_type})"
            elif not make_nullable and current_type.startswith('Nullable('):
                # Extraer el tipo interno
                new_type = current_type[9:-1]  # Remover "Nullable(" y ")"
            else:
                # No hay cambio necesario
                return True
            
            # Ejecutar ALTER TABLE
            alter_query = f"ALTER TABLE `{database}`.`{table}` MODIFY COLUMN `{column}` {new_type}"
            logger.info(f"Ejecutando: {alter_query}")
            
            self.client.command(alter_query)
            logger.info(f"✅ Columna {database}.{table}.{column} modificada: {current_type} → {new_type}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error modificando columna {database}.{table}.{column}: {e}")
            return False
    
    def fix_table_schema(self, database: str, table: str, 
                        primary_keys: List[str] = None) -> Dict:
        """
        Corregir el esquema de una tabla completa.
        
        Args:
            database: Nombre de la base de datos
            table: Nombre de la tabla
            primary_keys: Lista de columnas que son primary keys (no se hacen nullable)
            
        Returns:
            Diccionario con estadísticas de la corrección
        """
        logger.info(f"🔧 Corrigiendo esquema de {database}.{table}")
        
        if primary_keys is None:
            primary_keys = []
            
        stats = {
            'table': f"{database}.{table}",
            'columns_checked': 0,
            'columns_modified': 0,
            'errors': []
        }
        
        # Obtener esquema actual
        columns = self.get_table_schema(database, table)
        if not columns:
            stats['errors'].append("No se pudo obtener esquema de la tabla")
            return stats
            
        stats['columns_checked'] = len(columns)
        
        # Procesar cada columna
        for col in columns:
            col_name = col['name']
            col_type = col['type']
            is_nullable = col['is_nullable']
            is_pk = col_name in primary_keys
            
            # Decidir si la columna debe ser nullable
            should_be_nullable = not is_pk  # Todas las columnas excepto PK deben ser nullable
            
            if should_be_nullable and not is_nullable:
                # Hacer la columna nullable
                success = self.fix_column_nullability(database, table, col_name, col_type, True)
                if success:
                    stats['columns_modified'] += 1
                else:
                    stats['errors'].append(f"Error haciendo nullable: {col_name}")
                    
            elif not should_be_nullable and is_nullable:
                # Las primary keys no deben ser nullable, pero esto es más complejo
                logger.warning(f"⚠ Columna PK {col_name} es nullable, revisar manualmente")
        
        logger.info(f"✅ Esquema corregido: {stats['columns_modified']}/{stats['columns_checked']} columnas modificadas")
        return stats
    
    def fix_database_schemas(self, database: str, 
                           table_primary_keys: Dict[str, List[str]] = None) -> Dict:
        """
        Corregir esquemas de todas las tablas en una base de datos.
        
        Args:
            database: Nombre de la base de datos
            table_primary_keys: Diccionario tabla -> lista de primary keys
            
        Returns:
            Diccionario con estadísticas completas
        """
        logger.info(f"🏭 Corrigiendo esquemas de base de datos: {database}")
        
        if table_primary_keys is None:
            table_primary_keys = {}
            
        # Obtener lista de tablas
        try:
            tables_query = f"SHOW TABLES FROM `{database}`"
            result = self.client.query(tables_query)
            tables = [row[0] for row in result.result_rows]
        except Exception as e:
            logger.error(f"Error obteniendo tablas de {database}: {e}")
            return {'error': str(e)}
        
        total_stats = {
            'database': database,
            'tables_processed': 0,
            'total_columns_checked': 0,
            'total_columns_modified': 0,
            'table_results': {},
            'errors': []
        }
        
        # Procesar cada tabla
        for table in tables:
            primary_keys = table_primary_keys.get(table, [])
            table_stats = self.fix_table_schema(database, table, primary_keys)
            
            total_stats['tables_processed'] += 1
            total_stats['total_columns_checked'] += table_stats['columns_checked']
            total_stats['total_columns_modified'] += table_stats['columns_modified']
            total_stats['table_results'][table] = table_stats
            
            if table_stats['errors']:
                total_stats['errors'].extend([f"{table}: {err}" for err in table_stats['errors']])
        
        logger.info(f"🎯 Base de datos {database} procesada:")
        logger.info(f"   📊 Tablas: {total_stats['tables_processed']}")
        logger.info(f"   📊 Columnas revisadas: {total_stats['total_columns_checked']}")
        logger.info(f"   ✅ Columnas modificadas: {total_stats['total_columns_modified']}")
        
        return total_stats

def main():
    """Función principal para uso directo del script."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Corregir esquemas de ClickHouse')
    parser.add_argument('--database', required=True, help='Base de datos a corregir')
    parser.add_argument('--table', help='Tabla específica a corregir (opcional)')
    parser.add_argument('--primary-keys', help='Primary keys en formato JSON: {"tabla": ["col1", "col2"]}')
    parser.add_argument('--host', default='clickhouse', help='Host de ClickHouse')
    parser.add_argument('--port', type=int, default=8123, help='Puerto de ClickHouse')
    parser.add_argument('--user', default='etl', help='Usuario de ClickHouse')
    parser.add_argument('--password', default='Et1Ingest!', help='Contraseña de ClickHouse')
    parser.add_argument('--output', help='Archivo para guardar reporte')
    
    args = parser.parse_args()
    
    # Parsear primary keys si se proporcionan
    table_primary_keys = {}
    if args.primary_keys:
        try:
            table_primary_keys = json.loads(args.primary_keys)
        except json.JSONDecodeError as e:
            print(f"❌ Error parseando primary keys JSON: {e}")
            sys.exit(1)
    
    # Crear fixer
    try:
        fixer = ClickHouseSchemaFixer(
            clickhouse_host=args.host,
            clickhouse_port=args.port,
            clickhouse_user=args.user,
            clickhouse_password=args.password
        )
    except Exception as e:
        print(f"❌ Error conectando a ClickHouse: {e}")
        sys.exit(1)
    
    # Ejecutar corrección
    if args.table:
        # Corregir tabla específica
        primary_keys = table_primary_keys.get(args.table, [])
        result = fixer.fix_table_schema(args.database, args.table, primary_keys)
    else:
        # Corregir toda la base de datos
        result = fixer.fix_database_schemas(args.database, table_primary_keys)
    
    # Guardar reporte si se solicita
    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        print(f"📋 Reporte guardado en: {args.output}")
    
    # Mostrar resumen
    if 'error' in result:
        print(f"❌ Error: {result['error']}")
        sys.exit(1)
    else:
        print("✅ Corrección de esquemas completada exitosamente")

if __name__ == "__main__":
    main()