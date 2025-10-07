#!/usr/bin/env python3
"""
Resumen final del estado del pipeline ETL
"""

import sys
import subprocess
import json

def run_docker_command(cmd):
    """Ejecutar comando docker y retornar resultado"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
        return result.returncode == 0, result.stdout.strip(), result.stderr.strip()
    except subprocess.TimeoutExpired:
        return False, "", "Timeout"
    except Exception as e:
        return False, "", str(e)

def check_kafka_topics():
    """Verificar tópicos de Kafka"""
    success, stdout, stderr = run_docker_command(
        "docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list 2>/dev/null | grep dbserver_default | wc -l"
    )
    if success:
        count = int(stdout) if stdout.isdigit() else 0
        return count > 0, f"{count} tópicos de datos encontrados"
    return False, "Error obteniendo tópicos"

def check_mysql_connector():
    """Verificar conector MySQL"""
    success, stdout, stderr = run_docker_command(
        'docker compose run --rm etl-tools curl -s http://connect:8083/connectors/dbserver_default/status 2>/dev/null'
    )
    if success and stdout:
        try:
            status = json.loads(stdout)
            connector_state = status.get('connector', {}).get('state', 'UNKNOWN')
            task_state = status.get('tasks', [{}])[0].get('state', 'UNKNOWN') if status.get('tasks') else 'NO_TASKS'
            return connector_state == 'RUNNING' and task_state == 'RUNNING', f"Connector: {connector_state}, Task: {task_state}"
        except:
            return False, "Error parseando estado"
    return False, "No hay respuesta del connector"

def check_clickhouse_data():
    """Verificar datos en ClickHouse"""
    success, stdout, stderr = run_docker_command(
        "docker compose exec clickhouse clickhouse-client --database fgeo_analytics --query 'SELECT COUNT(*) FROM archivos' 2>/dev/null"
    )
    if success and stdout.isdigit():
        count = int(stdout)
        return count > 0, f"{count} registros en tabla archivos"
    return False, "No hay datos o error de conexión"

def check_data_sample():
    """Obtener muestra de datos"""
    success, stdout, stderr = run_docker_command(
        "docker compose exec clickhouse clickhouse-client --database fgeo_analytics --query 'SELECT id, nombre, tamano FROM archivos ORDER BY id LIMIT 3' --format TSV 2>/dev/null"
    )
    if success and stdout:
        return True, stdout
    return False, "No se pudo obtener muestra"

def main():
    print("=" * 60)
    print("🎯 RESUMEN FINAL DEL PIPELINE ETL")
    print("=" * 60)
    
    all_good = True
    
    # 1. Verificar tópicos de Kafka
    print("\n📊 1. TÓPICOS DE DATOS EN KAFKA")
    success, message = check_kafka_topics()
    status = "✅" if success else "❌"
    print(f"   {status} {message}")
    if not success:
        all_good = False
    
    # 2. Verificar conector MySQL
    print("\n🔌 2. CONECTOR MYSQL → KAFKA")
    success, message = check_mysql_connector()
    status = "✅" if success else "❌"
    print(f"   {status} {message}")
    if not success:
        all_good = False
    
    # 3. Verificar datos en ClickHouse
    print("\n🏠 3. DATOS EN CLICKHOUSE")
    success, message = check_clickhouse_data()
    status = "✅" if success else "❌"
    print(f"   {status} {message}")
    if not success:
        all_good = False
    
    # 4. Muestra de datos
    print("\n📝 4. MUESTRA DE DATOS")
    success, sample = check_data_sample()
    if success:
        print("   ✅ Datos disponibles:")
        for line in sample.split('\n')[:3]:
            if line.strip():
                parts = line.split('\t')
                if len(parts) >= 3:
                    print(f"      ID: {parts[0]}, Archivo: {parts[1]}, Tamaño: {parts[2]} bytes")
    else:
        print("   ❌ No se pudo obtener muestra de datos")
        all_good = False
    
    print("\n" + "=" * 60)
    if all_good:
        print("🎉 PIPELINE ETL FUNCIONANDO CORRECTAMENTE")
        print("✅ MySQL → Kafka → ClickHouse: DATOS FLUYENDO")
        print("\n🚀 Próximos pasos recomendados:")
        print("   1. Configurar Superset para visualización")
        print("   2. Agregar más tablas según necesidades")
        print("   3. Configurar monitoreo y alertas")
    else:
        print("⚠️  PIPELINE PARCIALMENTE FUNCIONAL")
        print("💡 Revisar componentes marcados con ❌")
    
    print("=" * 60)
    return 0 if all_good else 1

if __name__ == "__main__":
    sys.exit(main())