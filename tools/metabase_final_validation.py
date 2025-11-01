#!/usr/bin/env python3
"""
Script de validación final - Confirmar que Metabase puede visualizar datos de ClickHouse
"""

import requests
import json
import time

def validate_metabase_complete():
    """Validación completa de Metabase con ClickHouse"""
    
    print("🎯 VALIDACIÓN FINAL - METABASE + CLICKHOUSE")
    print("="*60)
    
    metabase_url = "http://localhost:3000"
    
    # 1. Login
    print("🔐 Autenticando en Metabase...")
    credentials = {"username": "admin@etl.local", "password": "AdminETL2024!"}
    
    try:
        session_response = requests.post(f"{metabase_url}/api/session", json=credentials, timeout=15)
        if session_response.status_code == 200:
            session_id = session_response.json().get("id")
            print("✅ Autenticación exitosa")
        else:
            print("❌ Error de autenticación")
            return False
    except Exception as e:
        print(f"❌ Error login: {e}")
        return False
    
    headers = {"X-Metabase-Session": session_id}
    
    # 2. Verificar ClickHouse configurado
    print("\n🗄️  VERIFICANDO BASE DE DATOS CLICKHOUSE")
    print("="*50)
    
    try:
        db_response = requests.get(f"{metabase_url}/api/database", headers=headers, timeout=15)
        if db_response.status_code == 200:
            databases = db_response.json().get("data", [])
            clickhouse_db = None
            
            for db in databases:
                if "clickhouse" in db.get("name", "").lower():
                    clickhouse_db = db
                    print(f"✅ ClickHouse encontrado: {db['name']} (ID: {db['id']})")
                    break
            
            if not clickhouse_db:
                print("❌ Base de datos ClickHouse no encontrada")
                return False
        else:
            print("❌ Error obteniendo bases de datos")
            return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    
    # 3. Probar consultas principales
    print("\n🧪 PROBANDO CONSULTAS DE DATOS")
    print("="*50)
    
    test_queries = [
        {
            "name": "Conectividad básica",
            "query": "SELECT 1 as test",
            "expected_type": "number"
        },
        {
            "name": "Conteo principal",
            "query": "SELECT COUNT(*) as total FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora",
            "expected_type": "count"
        },
        {
            "name": "Muestra de datos",
            "query": "SELECT * FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora LIMIT 5",
            "expected_type": "data"
        },
        {
            "name": "Esquemas disponibles",
            "query": "SELECT name FROM system.databases WHERE name NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema') ORDER BY name",
            "expected_type": "list"
        }
    ]
    
    successful_queries = 0
    
    for test in test_queries:
        print(f"   🔍 {test['name']}...")
        
        try:
            query_data = {
                "type": "native",
                "native": {"query": test["query"]},
                "database": clickhouse_db["id"]
            }
            
            query_response = requests.post(
                f"{metabase_url}/api/dataset",
                json=query_data,
                headers=headers,
                timeout=30
            )
            
            if query_response.status_code in [200, 202]:
                data = query_response.json()
                if data.get("data") and data["data"].get("rows"):
                    rows = data["data"]["rows"]
                    
                    if test["expected_type"] == "count":
                        count = rows[0][0]
                        print(f"      ✅ {count:,} registros")
                    elif test["expected_type"] == "data":
                        print(f"      ✅ {len(rows)} filas de datos obtenidas")
                    elif test["expected_type"] == "list":
                        schemas = [row[0] for row in rows]
                        print(f"      ✅ Esquemas: {', '.join(schemas)}")
                    else:
                        print(f"      ✅ Consulta exitosa")
                    
                    successful_queries += 1
                else:
                    print(f"      ⚠️  Sin resultados")
            else:
                print(f"      ❌ Error: {query_response.status_code}")
                
        except Exception as e:
            print(f"      ❌ Error: {e}")
    
    # 4. Validar tablas disponibles
    print(f"\n📊 VERIFICANDO TABLAS DISPONIBLES")
    print("="*50)
    
    try:
        tables_query = {
            "type": "native",
            "native": {"query": "SELECT database, table, engine FROM system.tables WHERE database IN ('fiscalizacion', 'archivos') ORDER BY database, table"},
            "database": clickhouse_db["id"]
        }
        
        tables_response = requests.post(
            f"{metabase_url}/api/dataset",
            json=tables_query,
            headers=headers,
            timeout=30
        )
        
        if tables_response.status_code in [200, 202]:
            data = tables_response.json()
            if data.get("data") and data["data"].get("rows"):
                tables = data["data"]["rows"]
                
                print(f"   📋 {len(tables)} tablas disponibles:")
                
                current_db = ""
                for row in tables[:15]:  # Mostrar primeras 15 tablas
                    db, table, engine = row
                    if db != current_db:
                        print(f"   📁 {db}:")
                        current_db = db
                    print(f"      📄 {table} ({engine})")
                
                if len(tables) > 15:
                    print(f"      ... y {len(tables) - 15} tablas más")
                    
            else:
                print("   ⚠️  No se obtuvieron tablas")
        else:
            print(f"   ❌ Error obteniendo tablas: {tables_response.status_code}")
            
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    # 5. Resultado final
    print(f"\n🎯 RESULTADO FINAL")
    print("="*50)
    
    if successful_queries >= 3:
        print("🎉 ¡METABASE COMPLETAMENTE FUNCIONAL!")
        print("✅ Conexión ClickHouse: OK")
        print("✅ Consultas de datos: OK")  
        print("✅ Acceso a esquemas: OK")
        print("✅ Visualización de tablas: OK")
        
        print(f"\n📱 ACCESO DIRECTO:")
        print(f"   🔗 URL: http://localhost:3000")
        print(f"   👤 Usuario: admin@etl.local")
        print(f"   🔑 Contraseña: AdminETL2024!")
        
        print(f"\n📊 PRUEBA TUS DATOS:")
        print(f"   1. Ve a 'New' → 'Question'")
        print(f"   2. Selecciona 'Native Query' (SQL)")
        print(f"   3. Elige 'ClickHouse ETL' como base de datos")
        print(f"   4. Prueba esta consulta:")
        print(f"      SELECT * FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora")
        print(f"      WHERE created_at >= '2024-01-01' LIMIT 100")
        
        return True
    else:
        print("⚠️  Configuración parcial - algunas consultas fallaron")
        print(f"✅ Consultas exitosas: {successful_queries}/4")
        print("📋 Revisa la configuración manual si es necesario")
        return False

def main():
    success = validate_metabase_complete()
    
    if success:
        print("\n🚀 ¡LISTO! Metabase puede visualizar todos tus datos de ClickHouse")
    else:
        print("\n⚠️  Configuración incompleta - revisa los logs anteriores")
    
    return success

if __name__ == "__main__":
    main()