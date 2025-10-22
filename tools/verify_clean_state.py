#!/usr/bin/env python3
"""
Script para verificar que el sistema está limpio antes de iniciar el pipeline.
Verifica que no hay datos antiguos en ClickHouse, Kafka, ni Superset.
"""
import os
import sys
import requests
import subprocess
import json
from typing import Dict, List

def run_clickhouse_query(query: str) -> str:
    """Ejecutar query en ClickHouse y retornar resultado"""
    ch_user = os.getenv("CLICKHOUSE_DEFAULT_USER", "default")
    ch_pass = os.getenv("CLICKHOUSE_DEFAULT_PASSWORD", "ClickHouse123!")
    
    cmd = [
        "clickhouse-client",
        f"--user={ch_user}",
        f"--password={ch_pass}",
        f"--query={query}"
    ]
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"❌ Error ejecutando query: {e.stderr}")
        return ""

def check_clickhouse_databases() -> Dict:
    """Verificar bases de datos en ClickHouse"""
    print("\n📊 Verificando bases de datos en ClickHouse...")
    
    query = """
    SELECT name, count() as table_count 
    FROM system.databases 
    LEFT JOIN system.tables USING (database)
    WHERE name NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
    GROUP BY name
    ORDER BY name
    """
    
    result = run_clickhouse_query(query)
    
    if not result:
        print("✅ ClickHouse limpio - solo bases del sistema")
        return {"status": "clean", "databases": []}
    
    databases = []
    for line in result.split('\n'):
        if line.strip():
            parts = line.strip().split('\t')
            if len(parts) >= 2:
                db_name = parts[0]
                table_count = int(parts[1]) if parts[1] else 0
                databases.append({"name": db_name, "tables": table_count})
    
    # Filtrar solo fgeo_analytics (debe existir) y default
    expected_dbs = {'fgeo_analytics', 'default'}
    unexpected = [db for db in databases if db['name'] not in expected_dbs]
    
    if unexpected:
        print(f"⚠️  Bases de datos inesperadas encontradas: {[db['name'] for db in unexpected]}")
        for db in unexpected:
            print(f"   - {db['name']}: {db['tables']} tablas")
        return {"status": "dirty", "databases": databases}
    
    print(f"✅ Solo bases esperadas: {[db['name'] for db in databases]}")
    return {"status": "clean", "databases": databases}

def check_clickhouse_tables() -> Dict:
    """Verificar tablas en ClickHouse (excepto test_table que es legítima)"""
    print("\n📋 Verificando tablas en ClickHouse...")
    
    query = """
    SELECT database, name, total_rows 
    FROM system.tables 
    WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
    AND name NOT LIKE '.inner%'
    ORDER BY database, name
    """
    
    result = run_clickhouse_query(query)
    
    if not result:
        print("✅ No hay tablas de usuario")
        return {"status": "clean", "tables": []}
    
    tables = []
    for line in result.split('\n'):
        if line.strip():
            parts = line.strip().split('\t')
            if len(parts) >= 3:
                database = parts[0]
                name = parts[1]
                rows = int(parts[2]) if parts[2] else 0
                tables.append({"database": database, "name": name, "rows": rows})
    
    # test_table es esperada en fgeo_analytics
    expected_tables = {'test_table'}
    unexpected = [
        t for t in tables 
        if not (t['database'] == 'fgeo_analytics' and t['name'] in expected_tables)
        and t['database'] not in ['default']  # default puede tener tablas del sistema
    ]
    
    if unexpected:
        print(f"⚠️  Tablas inesperadas encontradas ({len(unexpected)}):")
        for t in unexpected[:10]:  # Mostrar solo las primeras 10
            print(f"   - {t['database']}.{t['name']}: {t['rows']} filas")
        if len(unexpected) > 10:
            print(f"   ... y {len(unexpected) - 10} más")
        return {"status": "dirty", "tables": tables}
    
    print(f"✅ Solo tablas esperadas en fgeo_analytics")
    return {"status": "clean", "tables": tables}

def check_kafka_topics() -> Dict:
    """Verificar topics en Kafka (los antiguos deberían estar eliminados)"""
    print("\n📨 Verificando topics en Kafka...")
    
    try:
        result = subprocess.run(
            ["kafka-topics", "--bootstrap-server", "localhost:19092", "--list"],
            capture_output=True,
            text=True,
            check=True,
            timeout=10
        )
        topics = [t.strip() for t in result.stdout.split('\n') if t.strip() and not t.startswith('__')]
        
        # Topics del sistema de Kafka Connect son esperados
        system_topics = {'connect_configs', 'connect_offsets', 'connect_statuses'}
        user_topics = [t for t in topics if t not in system_topics]
        
        if user_topics:
            print(f"⚠️  Topics de usuario encontrados: {user_topics}")
            return {"status": "dirty", "topics": topics}
        
        print(f"✅ Solo topics del sistema: {list(system_topics)}")
        return {"status": "clean", "topics": topics}
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        print(f"⚠️  No se pudo verificar Kafka (puede estar apagado): {e}")
        return {"status": "unknown", "topics": []}

def check_superset_datasets() -> Dict:
    """Verificar datasets en Superset"""
    print("\n📊 Verificando datasets en Superset...")
    
    superset_url = os.getenv("SUPERSET_URL", "http://localhost:8088")
    admin_user = os.getenv("SUPERSET_ADMIN", "admin")
    admin_pass = os.getenv("SUPERSET_PASSWORD", "Admin123!")
    
    try:
        # Login
        login_resp = requests.post(
            f"{superset_url}/api/v1/security/login",
            json={"username": admin_user, "password": admin_pass, "provider": "db", "refresh": True},
            timeout=10
        )
        
        if login_resp.status_code != 200:
            print(f"⚠️  No se pudo conectar a Superset (puede estar apagado)")
            return {"status": "unknown", "datasets": []}
        
        token = login_resp.json().get("access_token")
        headers = {"Authorization": f"Bearer {token}"}
        
        # Obtener datasets
        ds_resp = requests.get(
            f"{superset_url}/api/v1/dataset/",
            headers=headers,
            timeout=10
        )
        
        if ds_resp.status_code != 200:
            print(f"⚠️  Error obteniendo datasets: {ds_resp.status_code}")
            return {"status": "unknown", "datasets": []}
        
        datasets = ds_resp.json().get("result", [])
        
        # Filtrar datasets esperados (solo test_table en fgeo_analytics)
        expected = []
        unexpected = []
        for ds in datasets:
            table_name = ds.get("table_name", "")
            schema = ds.get("schema", "")
            if schema == "fgeo_analytics" and table_name == "test_table":
                expected.append(f"{schema}.{table_name}")
            else:
                unexpected.append(f"{schema}.{table_name}")
        
        if unexpected:
            print(f"⚠️  Datasets inesperados encontrados ({len(unexpected)}):")
            for ds in unexpected[:5]:
                print(f"   - {ds}")
            if len(unexpected) > 5:
                print(f"   ... y {len(unexpected) - 5} más")
            return {"status": "dirty", "datasets": unexpected}
        
        print(f"✅ Solo datasets esperados: {expected}")
        return {"status": "clean", "datasets": expected}
        
    except requests.exceptions.RequestException as e:
        print(f"⚠️  No se pudo verificar Superset (puede estar apagado): {e}")
        return {"status": "unknown", "datasets": []}

def main():
    print("🔍 VERIFICACIÓN DE ESTADO LIMPIO DEL SISTEMA")
    print("=" * 60)
    
    results = {
        "clickhouse_databases": check_clickhouse_databases(),
        "clickhouse_tables": check_clickhouse_tables(),
        "kafka_topics": check_kafka_topics(),
        "superset_datasets": check_superset_datasets(),
    }
    
    # Determinar estado general
    all_clean = all(
        r["status"] in ["clean", "unknown"] 
        for r in results.values()
    )
    
    print("\n" + "=" * 60)
    print("🏁 RESULTADO DE VERIFICACIÓN:")
    print("=" * 60)
    
    if all_clean:
        print("✅ SISTEMA LIMPIO - Listo para ingesta")
        print("   No se encontraron datos de ejecuciones anteriores")
        
        # Guardar reporte
        report_path = "/app/logs/clean_state_verification.json"
        try:
            os.makedirs(os.path.dirname(report_path), exist_ok=True)
            with open(report_path, 'w') as f:
                json.dump({
                    "status": "clean",
                    "timestamp": subprocess.run(["date", "-Iseconds"], capture_output=True, text=True).stdout.strip(),
                    "results": results
                }, f, indent=2)
            print(f"\n📝 Reporte guardado en: {report_path}")
        except Exception as e:
            print(f"⚠️  No se pudo guardar reporte: {e}")
        
        return 0
    else:
        print("⚠️  SISTEMA CON DATOS ANTIGUOS - Se recomienda limpieza")
        print("\nComponentes con datos:")
        for component, result in results.items():
            if result["status"] == "dirty":
                print(f"   ❌ {component}")
        
        # Guardar reporte
        report_path = "/app/logs/clean_state_verification.json"
        try:
            os.makedirs(os.path.dirname(report_path), exist_ok=True)
            with open(report_path, 'w') as f:
                json.dump({
                    "status": "dirty",
                    "timestamp": subprocess.run(["date", "-Iseconds"], capture_output=True, text=True).stdout.strip(),
                    "results": results
                }, f, indent=2)
            print(f"\n📝 Reporte guardado en: {report_path}")
        except Exception as e:
            print(f"⚠️  No se pudo guardar reporte: {e}")
        
        return 1

if __name__ == "__main__":
    sys.exit(main())
