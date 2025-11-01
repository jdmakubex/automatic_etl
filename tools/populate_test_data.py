#!/usr/bin/env python3
"""
Genera datos de prueba directamente en ClickHouse para validar Metabase.
No requiere el pipeline ETL completo.
"""
import os
import time
from clickhouse_connect import get_client
from dotenv import load_dotenv

load_dotenv('/app/.env')

CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_USER = os.getenv("CH_USER", "etl")
CH_PASSWORD = os.getenv("CH_PASSWORD", "Et1Ingest!")
CH_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "fgeo_analytics")

def create_sample_data():
    """Crea datos de prueba en algunas tablas de ClickHouse"""
    
    # Conectar a ClickHouse
    client = get_client(
        host=CH_HOST,
        port=8123,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE
    )
    
    print("🔗 Conectado a ClickHouse")
    
    # Datos de prueba para archivos
    print("📄 Insertando datos de prueba en archivos_archivos_raw...")
    try:
        archivos_data = [
            (time.strftime('%Y-%m-%d %H:%M:%S'), '{"id": 1, "nombre": "Expediente_001.pdf", "tipo": "PDF", "tamaño": 2048}'),
            (time.strftime('%Y-%m-%d %H:%M:%S'), '{"id": 2, "nombre": "Documento_002.docx", "tipo": "DOCX", "tamaño": 1024}'),
            (time.strftime('%Y-%m-%d %H:%M:%S'), '{"id": 3, "nombre": "Imagen_003.jpg", "tipo": "JPG", "tamaño": 512}'),
            (time.strftime('%Y-%m-%d %H:%M:%S'), '{"id": 4, "nombre": "Informe_004.xlsx", "tipo": "XLSX", "tamaño": 4096}'),
            (time.strftime('%Y-%m-%d %H:%M:%S'), '{"id": 5, "nombre": "Acta_005.txt", "tipo": "TXT", "tamaño": 256}'),
        ]
        
        client.insert('archivos_archivos_raw', archivos_data, column_names=['ingested_at', 'value'])
        print(f"✅ Insertados {len(archivos_data)} registros en archivos_archivos_raw")
    except Exception as e:
        print(f"❌ Error insertando en archivos_archivos_raw: {e}")
    
    # Datos de prueba para fiscalización
    print("⚖️ Insertando datos de prueba en fiscalizacion_altoimpacto_raw...")
    try:
        fiscalizacion_data = [
            (time.strftime('%Y-%m-%d %H:%M:%S'), '{"id": 101, "caso": "ALTO_001", "fecha": "2024-01-15", "estado": "ACTIVO", "gravedad": "ALTA"}'),
            (time.strftime('%Y-%m-%d %H:%M:%S'), '{"id": 102, "caso": "ALTO_002", "fecha": "2024-01-16", "estado": "EN_PROCESO", "gravedad": "MEDIA"}'),
            (time.strftime('%Y-%m-%d %H:%M:%S'), '{"id": 103, "caso": "ALTO_003", "fecha": "2024-01-17", "estado": "CERRADO", "gravedad": "ALTA"}'),
            (time.strftime('%Y-%m-%d %H:%M:%S'), '{"id": 104, "caso": "ALTO_004", "fecha": "2024-01-18", "estado": "ACTIVO", "gravedad": "BAJA"}'),
            (time.strftime('%Y-%m-%d %H:%M:%S'), '{"id": 105, "caso": "ALTO_005", "fecha": "2024-01-19", "estado": "EN_REVISION", "gravedad": "ALTA"}'),
        ]
        
        client.insert('fiscalizacion_altoimpacto_raw', fiscalizacion_data, column_names=['ingested_at', 'value'])
        print(f"✅ Insertados {len(fiscalizacion_data)} registros en fiscalizacion_altoimpacto_raw")
    except Exception as e:
        print(f"❌ Error insertando en fiscalizacion_altoimpacto_raw: {e}")
    
    # Datos adicionales para bitácora
    print("📋 Insertando datos de prueba en fiscalizacion_bitacora_raw...")
    try:
        bitacora_data = [
            (time.strftime('%Y-%m-%d %H:%M:%S'), '{"id": 201, "usuario": "admin", "accion": "LOGIN", "timestamp": "2024-01-15 08:00:00"}'),
            (time.strftime('%Y-%m-%d %H:%M:%S'), '{"id": 202, "usuario": "fiscal1", "accion": "CREAR_CASO", "timestamp": "2024-01-15 09:15:00"}'),
            (time.strftime('%Y-%m-%d %H:%M:%S'), '{"id": 203, "usuario": "fiscal2", "accion": "ACTUALIZAR_CASO", "timestamp": "2024-01-15 10:30:00"}'),
            (time.strftime('%Y-%m-%d %H:%M:%S'), '{"id": 204, "usuario": "supervisor", "accion": "REVISAR_CASO", "timestamp": "2024-01-15 11:45:00"}'),
            (time.strftime('%Y-%m-%d %H:%M:%S'), '{"id": 205, "usuario": "admin", "accion": "GENERAR_REPORTE", "timestamp": "2024-01-15 12:00:00"}'),
        ]
        
        client.insert('fiscalizacion_bitacora_raw', bitacora_data, column_names=['ingested_at', 'value'])
        print(f"✅ Insertados {len(bitacora_data)} registros en fiscalizacion_bitacora_raw")
    except Exception as e:
        print(f"❌ Error insertando en fiscalizacion_bitacora_raw: {e}")
    
    # Verificar conteos
    print("\n📊 Verificando datos insertados:")
    try:
        result = client.query("SELECT name, total_rows FROM system.tables WHERE database = 'fgeo_analytics' AND total_rows > 0 ORDER BY total_rows DESC")
        for row in result.result_rows:
            print(f"  📈 {row[0]}: {row[1]} filas")
    except Exception as e:
        print(f"❌ Error verificando datos: {e}")
    
    print("\n✅ ¡Datos de prueba generados exitosamente!")
    print("🎯 Ahora puedes validar la visualización en Metabase y Superset")

if __name__ == "__main__":
    print("🚀 GENERANDO DATOS DE PRUEBA PARA METABASE")
    print("=" * 50)
    create_sample_data()