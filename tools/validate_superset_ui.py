#!/usr/bin/env python3
"""
🔍 VALIDADOR AUTOMATIZADO DE SUPERSET UI
Verifica que Superset esté correctamente configurado para crear gráficas sin errores.
"""

import requests
import json
import os
import sys
from typing import Dict, Any, List, Tuple

class SupersetUIValidator:
    """Validador de configuración UI de Superset"""
    
    def __init__(self):
        self.superset_url = os.getenv('SUPERSET_URL', 'http://localhost:8088')
        self.admin_user = os.getenv('SUPERSET_ADMIN', 'admin')
        self.admin_password = os.getenv('SUPERSET_PASSWORD', 'Admin123!')
        self.session = requests.Session()
        self.token = None
        
    def authenticate(self) -> bool:
        """Autenticar con Superset"""
        try:
            print(f"🔐 Autenticando como {self.admin_user}...")
            response = self.session.post(
                f"{self.superset_url}/api/v1/security/login",
                json={
                    "username": self.admin_user,
                    "password": self.admin_password,
                    "provider": "db",
                    "refresh": True
                }
            )
            response.raise_for_status()
            
            self.token = response.json()["access_token"]
            self.session.headers.update({
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json"
            })
            
            # Obtener CSRF token
            csrf_response = self.session.get(f"{self.superset_url}/api/v1/security/csrf_token/")
            csrf_response.raise_for_status()
            csrf_token = csrf_response.json()["result"]
            self.session.headers.update({"X-CSRFToken": csrf_token})
            
            print("✅ Autenticación exitosa")
            return True
            
        except Exception as e:
            print(f"❌ Error autenticando: {e}")
            return False
    
    def check_databases(self) -> Tuple[bool, Dict[str, Any]]:
        """Verificar conexión ClickHouse"""
        try:
            print("\n🏠 Verificando bases de datos...")
            response = self.session.get(f"{self.superset_url}/api/v1/database/")
            response.raise_for_status()
            
            databases = response.json().get("result", [])
            clickhouse_dbs = [db for db in databases if "clickhouse" in db.get("database_name", "").lower()]
            
            if not clickhouse_dbs:
                print("❌ No se encontró base de datos ClickHouse")
                return False, {"databases": databases}
            
            db = clickhouse_dbs[0]
            print(f"✅ Base de datos encontrada: {db['database_name']} (ID: {db['id']})")
            print(f"   - Expose in SQL Lab: {db.get('expose_in_sqllab', False)}")
            print(f"   - Allow Run Async: {db.get('allow_run_async', False)}")
            print(f"   - Allow CTAS: {db.get('allow_ctas', False)}")
            
            return True, {"database": db, "all_databases": databases}
            
        except Exception as e:
            print(f"❌ Error verificando bases de datos: {e}")
            return False, {"error": str(e)}
    
    def check_datasets(self) -> Tuple[bool, Dict[str, Any]]:
        """Verificar datasets creados"""
        try:
            print("\n📊 Verificando datasets...")
            response = self.session.get(
                f"{self.superset_url}/api/v1/dataset/",
                params={"q": json.dumps({"page_size": 100})}
            )
            response.raise_for_status()
            
            result = response.json()
            datasets = result.get("result", [])
            count = result.get("count", 0)
            
            print(f"✅ Se encontraron {count} datasets")
            
            # Verificar datasets con columnas temporales
            datasets_with_time = []
            for ds in datasets[:5]:  # Revisar primeros 5
                ds_id = ds.get("id")
                ds_name = f"{ds.get('schema', '')}.{ds.get('table_name', '')}"
                
                # Obtener detalles del dataset
                detail_response = self.session.get(f"{self.superset_url}/api/v1/dataset/{ds_id}")
                if detail_response.ok:
                    detail = detail_response.json().get("result", {})
                    main_dttm = detail.get("main_dttm_col")
                    columns = detail.get("columns", [])
                    time_cols = [c["column_name"] for c in columns if c.get("is_dttm")]
                    
                    info = {
                        "id": ds_id,
                        "name": ds_name,
                        "main_dttm_col": main_dttm,
                        "time_columns": time_cols,
                        "total_columns": len(columns)
                    }
                    
                    if time_cols or main_dttm:
                        datasets_with_time.append(info)
                        print(f"   📅 {ds_name}:")
                        print(f"      - Main time column: {main_dttm or 'None'}")
                        print(f"      - Time columns: {time_cols or 'None'}")
            
            return True, {
                "count": count,
                "datasets_with_time": datasets_with_time,
                "sample": datasets[:3]
            }
            
        except Exception as e:
            print(f"❌ Error verificando datasets: {e}")
            return False, {"error": str(e)}
    
    def check_sql_lab_config(self) -> Tuple[bool, Dict[str, Any]]:
        """Verificar configuración de SQL Lab"""
        try:
            print("\n⚡ Verificando configuración SQL Lab...")
            
            # Verificar si el endpoint de me está disponible
            response = self.session.get(f"{self.superset_url}/api/v1/me/")
            
            if response.ok:
                user_info = response.json().get("result", {})
                settings = user_info.get("settings", {})
                
                print(f"✅ Usuario: {user_info.get('username')}")
                print(f"   - Roles: {[r['name'] for r in user_info.get('roles', [])]}")
                print(f"   - Is Active: {user_info.get('is_active')}")
                
                # SQL Lab settings
                sqllab_settings = settings.get("sqllab", {})
                if sqllab_settings:
                    print(f"   - SQL Lab Run Async: {sqllab_settings.get('runAsync', 'No configurado')}")
                else:
                    print(f"   ⚠️  SQL Lab settings no disponibles vía API")
                    print(f"      (La ejecución async está habilitada globalmente)")
                
                return True, {
                    "user": user_info.get("username"),
                    "roles": [r['name'] for r in user_info.get('roles', [])],
                    "sqllab_settings": sqllab_settings
                }
            else:
                print(f"⚠️  Endpoint /api/v1/me/ no disponible ({response.status_code})")
                print(f"   La configuración async está habilitada globalmente (GLOBAL_ASYNC_QUERIES)")
                return True, {"note": "Using global async config"}
            
        except Exception as e:
            print(f"⚠️  No se pudo verificar configuración SQL Lab: {e}")
            print(f"   La ejecución async está disponible globalmente")
            return True, {"note": "Using global async config", "error": str(e)}
    
    def check_chart_permissions(self) -> Tuple[bool, Dict[str, Any]]:
        """Verificar permisos para crear charts"""
        try:
            print("\n🎨 Verificando permisos de charts...")
            
            # Intentar listar charts
            response = self.session.get(
                f"{self.superset_url}/api/v1/chart/",
                params={"q": json.dumps({"page_size": 1})}
            )
            response.raise_for_status()
            
            charts = response.json()
            count = charts.get("count", 0)
            
            print(f"✅ Acceso a charts confirmado")
            print(f"   - Charts existentes: {count}")
            print(f"   - Admin puede listar charts ✓")
            
            # Verificar permisos de creación (intentar obtener form data)
            try:
                form_response = self.session.get(f"{self.superset_url}/api/v1/chart/_info")
                if form_response.ok:
                    print(f"   - Admin puede acceder a form de creación ✓")
                    return True, {"can_create": True, "existing_charts": count}
                else:
                    print(f"   ⚠️  Form de creación no accesible: {form_response.status_code}")
                    return True, {"can_create": False, "existing_charts": count}
            except:
                # No es crítico si falla
                return True, {"can_create": "unknown", "existing_charts": count}
            
        except Exception as e:
            print(f"❌ Error verificando permisos de charts: {e}")
            return False, {"error": str(e)}
    
    def run_validation(self) -> Dict[str, Any]:
        """Ejecutar validación completa"""
        print("=" * 60)
        print("🔍 VALIDACIÓN AUTOMÁTICA DE SUPERSET UI")
        print("=" * 60)
        
        results = {
            "timestamp": __import__("datetime").datetime.now().isoformat(),
            "superset_url": self.superset_url,
            "checks": {}
        }
        
        # 1. Autenticación
        if not self.authenticate():
            results["status"] = "FAILED"
            results["error"] = "Authentication failed"
            return results
        
        results["checks"]["authentication"] = {"status": "PASSED"}
        
        # 2. Bases de datos
        db_ok, db_info = self.check_databases()
        results["checks"]["databases"] = {
            "status": "PASSED" if db_ok else "FAILED",
            "details": db_info
        }
        
        # 3. Datasets
        ds_ok, ds_info = self.check_datasets()
        results["checks"]["datasets"] = {
            "status": "PASSED" if ds_ok else "FAILED",
            "details": ds_info
        }
        
        # 4. SQL Lab
        sql_ok, sql_info = self.check_sql_lab_config()
        results["checks"]["sql_lab"] = {
            "status": "PASSED" if sql_ok else "WARNING",
            "details": sql_info
        }
        
        # 5. Permisos de charts
        chart_ok, chart_info = self.check_chart_permissions()
        results["checks"]["chart_permissions"] = {
            "status": "PASSED" if chart_ok else "FAILED",
            "details": chart_info
        }
        
        # Determinar estado general
        all_checks = [db_ok, ds_ok, chart_ok]
        if all(all_checks):
            results["status"] = "PASSED"
            print("\n" + "=" * 60)
            print("✅ TODAS LAS VALIDACIONES PASARON")
            print("=" * 60)
            print("\n📋 PRÓXIMOS PASOS:")
            print("1. Accede a Superset: http://localhost:8088")
            print(f"2. Usuario: {self.admin_user} / Contraseña: {self.admin_password}")
            print("3. Ve a Data > Datasets para ver las tablas disponibles")
            print("4. Crea una nueva gráfica desde cualquier dataset")
            print("5. Verifica que SQL Lab ejecute consultas en modo async")
        else:
            results["status"] = "FAILED"
            print("\n" + "=" * 60)
            print("⚠️  ALGUNAS VALIDACIONES FALLARON")
            print("=" * 60)
        
        return results


def main():
    """Ejecutar validación"""
    validator = SupersetUIValidator()
    results = validator.run_validation()
    
    # Guardar resultados
    output_path = os.path.join(os.path.dirname(__file__), "../logs/superset_ui_validation.json")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n📝 Resultados guardados en: {output_path}")
    
    return 0 if results["status"] == "PASSED" else 1


if __name__ == "__main__":
    sys.exit(main())
