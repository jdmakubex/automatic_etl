#!/usr/bin/env python3
"""
Validador final del Pipeline ETL Automatizado
Verifica que todo el pipeline esté funcionando correctamente
"""

import os
import sys
import requests
from datetime import datetime

def check_clickhouse_data():
    """Verifica que ClickHouse tenga datos"""
    print("🔍 Verificando datos en ClickHouse...")
    
    import clickhouse_connect
    try:
        client = clickhouse_connect.get_client(
            host='clickhouse',
            port=8123,
            username='etl',
            password='Et1Ingest!',
            database='fgeo_analytics'
        )
        
        result = client.query("""
            SELECT database, table, total_rows 
            FROM system.tables 
            WHERE database = 'fgeo_analytics' AND total_rows > 0
            ORDER BY total_rows DESC
        """)
        
        if result.result_rows:
            print("✅ Datos encontrados en ClickHouse:")
            total_rows = 0
            for row in result.result_rows:
                database, table, rows = row
                print(f"   📊 {table}: {rows:,} filas")
                total_rows += rows
            print(f"📈 Total de registros: {total_rows:,}")
            return True
        else:
            print("❌ No se encontraron datos en ClickHouse")
            return False
            
    except Exception as e:
        print(f"❌ Error conectando a ClickHouse: {e}")
        return False

def check_superset():
    """Verifica que Superset esté disponible"""
    print("🔍 Verificando Superset...")
    
    try:
        response = requests.get("http://superset:8088/health", timeout=10)
        if response.status_code == 200:
            print("✅ Superset está disponible en http://localhost:8088")
            print("👤 Usuario: admin / Contraseña: admin")
            return True
        else:
            print(f"❌ Superset retornó código: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Error conectando a Superset: {e}")
        return False

def save_final_status(clickhouse_ok, superset_ok):
    """Guarda el estado final del pipeline"""
    status = {
        "timestamp": datetime.now().isoformat(),
        "clickhouse": "OK" if clickhouse_ok else "ERROR",
        "superset": "OK" if superset_ok else "ERROR",
        "overall": "SUCCESS" if (clickhouse_ok and superset_ok) else "PARTIAL_SUCCESS" if (clickhouse_ok or superset_ok) else "ERROR"
    }
    
    import json
    with open('/app/logs/auto_pipeline_status.json', 'w') as f:
        json.dump(status, f, indent=2)
    
    return status["overall"]

def main():
    """Función principal de validación"""
    start_time = datetime.now()
    print("🎯 VALIDACIÓN FINAL DEL PIPELINE ETL AUTOMATIZADO")
    print("=" * 60)
    print(f"⏰ Iniciado: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Verificar ClickHouse
    print("📋 FASE 1: Verificación de datos en ClickHouse")
    print("-" * 50)
    clickhouse_ok = check_clickhouse_data()
    print()
    
    # Verificar Superset
    print("📋 FASE 2: Verificación de interfaz Superset")
    print("-" * 50)
    superset_ok = check_superset()
    print()
    
    # Estado final
    final_status = save_final_status(clickhouse_ok, superset_ok)
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("🏁 RESULTADO FINAL:")
    print("=" * 60)
    print(f"⏰ Finalizado: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⏱️  Duración: {duration.total_seconds():.1f} segundos")
    print()
    
    if final_status == "SUCCESS":
        print("🎉 ¡PIPELINE ETL AUTOMATIZADO COMPLETADO EXITOSAMENTE!")
        print("✅ ClickHouse: Datos disponibles y verificados")
        print("✅ Superset: Interfaz web disponible y accesible")
        print("📈 Acceso a dashboards: http://localhost:8088")
        print("👤 Credenciales: admin/admin")
        print("📊 Listo para crear visualizaciones")
        return 0
    elif final_status == "PARTIAL_SUCCESS":
        print("⚠️  PIPELINE PARCIALMENTE EXITOSO")
        print(f"{'✅' if clickhouse_ok else '❌'} ClickHouse: {'Datos OK' if clickhouse_ok else 'Sin datos/Error'}")
        print(f"{'✅' if superset_ok else '❌'} Superset: {'Disponible' if superset_ok else 'No disponible'}")
        print("🔧 Revisa los componentes con errores")
        return 1
    else:
        print("❌ PIPELINE FALLÓ COMPLETAMENTE")
        print("❌ ClickHouse: Error de conectividad o sin datos")
        print("❌ Superset: No disponible")
        print("🚨 Revisa logs para diagnóstico")
        return 2

if __name__ == "__main__":
    sys.exit(main())