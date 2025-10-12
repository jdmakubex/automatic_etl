#!/usr/bin/env python3
"""
Monitor en tiempo real del Pipeline ETL Automatizado
Muestra el progreso y estado de todos los componentes
"""

import os
import sys
import json
import time
import subprocess
from datetime import datetime

def get_container_status():
    """Obtiene el estado de todos los contenedores"""
    try:
        result = subprocess.run(
            ["docker", "compose", "ps", "--format", "json"],
            capture_output=True,
            text=True,
            cwd="/mnt/c/proyectos/etl_prod"
        )
        
        if result.returncode == 0:
            containers = []
            for line in result.stdout.strip().split('\n'):
                if line:
                    containers.append(json.loads(line))
            return containers
        return []
    except Exception as e:
        print(f"Error obteniendo estado de contenedores: {e}")
        return []

def get_orchestrator_logs():
    """Obtiene los últimos logs del orquestador"""
    try:
        result = subprocess.run(
            ["docker", "logs", "etl_prod-etl-orchestrator-1", "--tail", "10"],
            capture_output=True,
            text=True,
            cwd="/mnt/c/proyectos/etl_prod"
        )
        
        if result.returncode == 0:
            return result.stdout.strip().split('\n')
        return ["Orquestador no iniciado"]
    except Exception:
        return ["Error obteniendo logs"]

def get_pipeline_status():
    """Lee el estado del pipeline del archivo JSON"""
    try:
        status_file = "/mnt/c/proyectos/etl_prod/logs/auto_pipeline_status.json"
        if os.path.exists(status_file):
            with open(status_file, 'r') as f:
                return json.load(f)
        return {"status": "running", "message": "Pipeline en ejecución"}
    except Exception:
        return {"status": "unknown", "message": "Estado desconocido"}

def get_clickhouse_data_count():
    """Obtiene el conteo de datos en ClickHouse"""
    try:
        result = subprocess.run([
            "docker", "compose", "exec", "-T", "clickhouse", 
            "clickhouse-client", "--user=etl", "--password=Et1Ingest!",
            "--query=SELECT sum(total_rows) FROM system.tables WHERE database = 'fgeo_analytics' AND total_rows > 0"
        ], capture_output=True, text=True, cwd="/mnt/c/proyectos/etl_prod")
        
        if result.returncode == 0:
            return int(result.stdout.strip() or "0")
        return 0
    except Exception:
        return 0

def print_status_line(service, status, details=""):
    """Imprime una línea de estado formateada"""
    status_icon = {
        "running": "🟢",
        "healthy": "✅", 
        "unhealthy": "🔴",
        "exited": "⚪",
        "starting": "🟡"
    }.get(status.lower(), "❓")
    
    print(f"{status_icon} {service:<20} {status:<12} {details}")

def clear_screen():
    """Limpia la pantalla"""
    os.system('cls' if os.name == 'nt' else 'clear')

def monitor_pipeline():
    """Monitorea el pipeline en tiempo real"""
    start_time = datetime.now()
    
    while True:
        clear_screen()
        current_time = datetime.now()
        elapsed = current_time - start_time
        
        print("🚀 MONITOR ETL PIPELINE AUTOMATIZADO")
        print("=" * 70)
        print(f"⏰ Tiempo transcurrido: {elapsed}")
        print(f"🔄 Actualizado: {current_time.strftime('%H:%M:%S')}")
        print()
        
        # Estado de contenedores
        print("📦 ESTADO DE CONTENEDORES:")
        print("-" * 50)
        containers = get_container_status()
        
        if containers:
            for container in containers:
                service = container.get('Service', 'Unknown')
                state = container.get('State', 'Unknown')
                status = container.get('Status', '')
                print_status_line(service, state, status)
        else:
            print("⚠️  No se pudieron obtener estados de contenedores")
        
        print()
        
        # Estado del pipeline
        print("🎯 ESTADO DEL PIPELINE:")
        print("-" * 50)
        pipeline_status = get_pipeline_status()
        status_icon = {
            "success": "🎉",
            "error": "❌",
            "running": "🔄",
            "unknown": "❓"
        }.get(pipeline_status.get("status"), "❓")
        
        print(f"{status_icon} Estado: {pipeline_status.get('status', 'unknown').upper()}")
        print(f"📝 Mensaje: {pipeline_status.get('message', 'Sin mensaje')}")
        
        if pipeline_status.get("details"):
            details = pipeline_status["details"]
            print(f"📊 Registros procesados: {details.get('total_rows_ingested', 'N/A')}")
            if details.get('superset_url'):
                print(f"🌐 Superset: {details['superset_url']}")
        
        print()
        
        # Datos en ClickHouse
        print("💾 DATOS EN CLICKHOUSE:")
        print("-" * 50)
        data_count = get_clickhouse_data_count()
        if data_count > 0:
            print(f"✅ Registros totales: {data_count:,}")
        else:
            print("⏳ Sin datos o ClickHouse no disponible")
        
        print()
        
        # Últimos logs del orquestador
        print("📄 ÚLTIMOS LOGS DEL ORQUESTADOR:")
        print("-" * 50)
        logs = get_orchestrator_logs()
        for log in logs[-5:]:  # Últimos 5 logs
            if log.strip():
                print(f"  {log}")
        
        print()
        print("💡 Presiona Ctrl+C para salir")
        print("🔄 Actualizando cada 10 segundos...")
        
        # Verificar si el pipeline terminó
        if pipeline_status.get("status") in ["success", "error"]:
            print(f"\n🏁 PIPELINE FINALIZADO: {pipeline_status.get('status').upper()}")
            break
        
        try:
            time.sleep(10)
        except KeyboardInterrupt:
            print("\n\n👋 Monitor detenido por el usuario")
            break

def main():
    """Función principal"""
    print("🚀 Iniciando monitor del pipeline ETL...")
    time.sleep(2)
    monitor_pipeline()

if __name__ == "__main__":
    main()