#!/usr/bin/env python3
"""
Script para completar el setup inicial de Metabase correctamente
"""

import requests
import json
import time
import sys

class MetabaseSetup:
    def __init__(self):
        self.metabase_url = "http://localhost:3000"
        
    def wait_for_metabase(self, max_attempts=10):
        """Esperar que Metabase esté disponible"""
        print("⏳ Esperando que Metabase esté disponible...")
        
        for attempt in range(max_attempts):
            try:
                response = requests.get(f"{self.metabase_url}/api/health", timeout=10)
                if response.status_code == 200:
                    print("✅ Metabase disponible")
                    return True
            except:
                pass
            
            print(f"   Intento {attempt + 1}/{max_attempts}...")
            time.sleep(5)
        
        return False
    
    def get_setup_token(self):
        """Obtener el setup token necesario para la configuración inicial"""
        print("🔑 Obteniendo setup token...")
        
        try:
            response = requests.get(
                f"{self.metabase_url}/api/session/properties",
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                setup_token = data.get("setup-token")
                
                if setup_token:
                    print("✅ Setup token obtenido")
                    return setup_token
                else:
                    print("ℹ️  Metabase ya configurado o no necesita setup")
                    return None
            else:
                print(f"⚠️  Error obteniendo properties: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ Error: {e}")
            return None
    
    def complete_initial_setup(self, setup_token):
        """Completar la configuración inicial de Metabase"""
        print("⚙️  Completando configuración inicial de Metabase...")
        
        setup_data = {
            "token": setup_token,
            "user": {
                "first_name": "Admin",
                "last_name": "ETL",
                "email": "admin@etl.local", 
                "site_name": "ETL Analytics",
                "password": "AdminETL2024!"
            },
            "database": {
                "engine": "clickhouse",
                "name": "ClickHouse ETL",
                "details": {
                    "host": "clickhouse",
                    "port": 8123,
                    "dbname": "default", 
                    "user": "default",
                    "password": "ClickHouse123!",
                    "ssl": False,
                    "additional-options": "socket_timeout=300000&connection_timeout=10000"
                }
            },
            "invite": None,
            "prefs": {
                "site_name": "ETL Analytics",
                "allow_tracking": False
            }
        }
        
        try:
            response = requests.post(
                f"{self.metabase_url}/api/setup",
                json=setup_data,
                timeout=60
            )
            
            if response.status_code == 200:
                result = response.json()
                print("✅ Setup inicial completado exitosamente")
                print(f"   👤 Usuario: admin@etl.local")
                print(f"   🔑 Contraseña: AdminETL2024!")
                print(f"   🗄️  Base de datos ClickHouse configurada")
                
                return result.get("id")  # Session ID
            else:
                print(f"❌ Error en setup: {response.status_code}")
                print(response.text)
                return None
                
        except Exception as e:
            print(f"❌ Error completando setup: {e}")
            return None
    
    def test_login(self):
        """Probar login con las credenciales configuradas"""
        print("🔐 Probando login...")
        
        credentials = {
            "username": "admin@etl.local",
            "password": "AdminETL2024!"
        }
        
        try:
            response = requests.post(
                f"{self.metabase_url}/api/session",
                json=credentials,
                timeout=15
            )
            
            if response.status_code == 200:
                session_id = response.json().get("id")
                print("✅ Login exitoso")
                return session_id
            else:
                print(f"❌ Error login: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ Error probando login: {e}")
            return None
    
    def verify_database_sync(self, session_id):
        """Verificar que la base de datos se haya sincronizado"""
        print("🔄 Verificando sincronización de base de datos...")
        
        headers = {"X-Metabase-Session": session_id}
        
        try:
            # Obtener bases de datos
            response = requests.get(
                f"{self.metabase_url}/api/database",
                headers=headers,
                timeout=15
            )
            
            if response.status_code == 200:
                databases = response.json().get("data", [])
                
                for db in databases:
                    if "clickhouse" in db.get("name", "").lower():
                        print(f"✅ Base de datos encontrada: {db['name']} (ID: {db['id']})")
                        
                        # Forzar sincronización
                        sync_response = requests.post(
                            f"{self.metabase_url}/api/database/{db['id']}/sync_schema",
                            headers=headers,
                            timeout=60
                        )
                        
                        if sync_response.status_code in [200, 202]:
                            print("✅ Sincronización iniciada")
                            time.sleep(15)  # Esperar sincronización
                            return db['id']
                
                print("⚠️  No se encontró base de datos ClickHouse")
                return None
            else:
                print(f"❌ Error obteniendo bases de datos: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ Error verificando base de datos: {e}")
            return None
    
    def test_query_execution(self, session_id, db_id):
        """Probar ejecución de consulta para verificar acceso a datos"""
        print("🧪 Probando ejecución de consulta...")
        
        headers = {"X-Metabase-Session": session_id}
        
        test_query = {
            "type": "native",
            "native": {
                "query": "SELECT COUNT(*) as total_registros FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora"
            },
            "database": db_id
        }
        
        try:
            response = requests.post(
                f"{self.metabase_url}/api/dataset",
                json=test_query,
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get("data") and data["data"].get("rows"):
                    count = data["data"]["rows"][0][0]
                    print(f"✅ Consulta exitosa: {count:,} registros encontrados")
                    return True
                else:
                    print("⚠️  Consulta ejecutada pero sin resultados")
                    return False
            else:
                print(f"❌ Error ejecutando consulta: {response.status_code}")
                print(response.text)
                return False
                
        except Exception as e:
            print(f"❌ Error probando consulta: {e}")
            return False
    
    def run_complete_setup(self):
        """Ejecutar setup completo de Metabase"""
        print("🚀 CONFIGURACIÓN COMPLETA DE METABASE")
        print("="*50)
        
        # 1. Esperar que Metabase esté disponible
        if not self.wait_for_metabase():
            print("❌ Metabase no disponible")
            return False
        
        # 2. Obtener setup token
        setup_token = self.get_setup_token()
        if not setup_token:
            print("ℹ️  Intentando login directo (posible configuración previa)...")
            session_id = self.test_login()
            
            if session_id:
                print("✅ Metabase ya configurado, probando funcionalidad...")
                # Verificar base de datos
                db_id = self.verify_database_sync(session_id)
                if db_id:
                    if self.test_query_execution(session_id, db_id):
                        print("\n🎉 METABASE COMPLETAMENTE FUNCIONAL")
                        self.show_access_info()
                        return True
                return False
            else:
                print("❌ Metabase en estado desconocido")
                return False
        
        # 3. Completar setup inicial
        session_id = self.complete_initial_setup(setup_token)
        if not session_id:
            return False
        
        # 4. Verificar login
        session_id = self.test_login()
        if not session_id:
            return False
        
        # 5. Verificar sincronización de base de datos
        print("⏳ Esperando sincronización inicial...")
        time.sleep(20)
        
        db_id = self.verify_database_sync(session_id)
        if not db_id:
            return False
        
        # 6. Probar consulta de datos
        if self.test_query_execution(session_id, db_id):
            print("\n🎉 METABASE CONFIGURADO Y FUNCIONAL")
            self.show_access_info()
            return True
        else:
            print("\n⚠️  Metabase configurado pero con problemas de datos")
            self.show_access_info()
            return False
    
    def show_access_info(self):
        """Mostrar información de acceso"""
        print("\n📊 INFORMACIÓN DE ACCESO:")
        print("="*40)
        print(f"🔗 URL: {self.metabase_url}")
        print(f"👤 Usuario: admin@etl.local")
        print(f"🔑 Contraseña: AdminETL2024!")
        print(f"📋 Para ver datos: Browse Data → ClickHouse ETL")
        print(f"🔍 Para consultas SQL: New Question → Native Query")

def main():
    setup = MetabaseSetup()
    success = setup.run_complete_setup()
    
    if success:
        print("\n✅ SETUP COMPLETADO - Metabase listo para usar")
        sys.exit(0)
    else:
        print("\n❌ SETUP INCOMPLETO - Revisa configuración manual")
        sys.exit(1)

if __name__ == "__main__":
    main()