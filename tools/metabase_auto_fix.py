#!/usr/bin/env python3
"""
Script automático para configurar correctamente ClickHouse en Metabase
Arregla problemas de visualización de datos
"""

import requests
import json
import time
import sys

class MetabaseFixer:
    def __init__(self):
        self.metabase_url = "http://localhost:3000"
        self.session_token = None
        
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
    
    def setup_metabase_admin(self):
        """Configurar admin si es necesario"""
        print("🔧 VERIFICANDO CONFIGURACIÓN INICIAL DE METABASE")
        print("="*50)
        
        try:
            # Verificar si ya está configurado
            setup_response = requests.get(
                f"{self.metabase_url}/api/session/properties",
                timeout=10
            )
            
            if setup_response.status_code == 200:
                data = setup_response.json()
                if data.get("setup-token"):
                    print("ℹ️  Metabase necesita configuración inicial")
                    # Si necesita setup, intentar configurar
                    return self.complete_setup(data["setup-token"])
                else:
                    print("✅ Metabase ya está configurado")
                    return True
            
        except Exception as e:
            print(f"⚠️  Error verificando setup: {e}")
            return True  # Continuar de todas formas
    
    def complete_setup(self, setup_token):
        """Completar setup inicial de Metabase"""
        print("⚙️  Completando setup inicial...")
        
        try:
            setup_data = {
                "token": setup_token,
                "user": {
                    "first_name": "Admin",
                    "last_name": "User",
                    "email": "admin@localhost",
                    "password": "MetabaseAdmin123!"
                },
                "database": None,
                "invite": None,
                "prefs": {
                    "site_name": "ETL Analytics",
                    "allow_tracking": False
                }
            }
            
            response = requests.post(
                f"{self.metabase_url}/api/setup",
                json=setup_data,
                timeout=30
            )
            
            if response.status_code == 200:
                print("✅ Setup inicial completado")
                return True
            else:
                print(f"⚠️  Setup parcial: {response.status_code}")
                return True
                
        except Exception as e:
            print(f"⚠️  Error en setup: {e}")
            return True
    
    def get_session_token(self):
        """Obtener token de sesión para API"""
        print("\n🔑 OBTENIENDO TOKEN DE SESIÓN")
        print("="*50)
        
        # Intentar diferentes credenciales
        credentials_to_try = [
            {"username": "admin@localhost", "password": "MetabaseAdmin123!"},
            {"username": "admin", "password": "admin"},
            {"username": "admin@localhost", "password": "admin"}
        ]
        
        for creds in credentials_to_try:
            try:
                response = requests.post(
                    f"{self.metabase_url}/api/session",
                    json=creds,
                    timeout=15
                )
                
                if response.status_code == 200:
                    self.session_token = response.json().get("id")
                    print(f"✅ Token obtenido con {creds['username']}")
                    return True
                else:
                    print(f"⚠️  Fallo con {creds['username']}: {response.status_code}")
                    
            except Exception as e:
                print(f"⚠️  Error con {creds['username']}: {e}")
        
        print("❌ No se pudo obtener token de sesión")
        return False
    
    def get_databases(self):
        """Obtener lista de bases de datos configuradas"""
        if not self.session_token:
            return []
        
        try:
            headers = {"X-Metabase-Session": self.session_token}
            response = requests.get(
                f"{self.metabase_url}/api/database",
                headers=headers,
                timeout=15
            )
            
            if response.status_code == 200:
                return response.json().get("data", [])
            else:
                print(f"⚠️  Error obteniendo bases de datos: {response.status_code}")
                return []
                
        except Exception as e:
            print(f"❌ Error: {e}")
            return []
    
    def configure_clickhouse_database(self):
        """Configurar base de datos ClickHouse correctamente"""
        print("\n🔧 CONFIGURANDO BASE DE DATOS CLICKHOUSE")
        print("="*50)
        
        if not self.session_token:
            print("❌ Sin token de sesión")
            return False
        
        headers = {"X-Metabase-Session": self.session_token}
        
        # Verificar bases de datos existentes
        databases = self.get_databases()
        clickhouse_db = None
        
        for db in databases:
            if "clickhouse" in db.get("name", "").lower() or db.get("engine") == "clickhouse":
                clickhouse_db = db
                break
        
        # Configuración correcta para ClickHouse
        db_config = {
            "name": "ClickHouse ETL",
            "engine": "clickhouse",
            "details": {
                "host": "clickhouse",
                "port": 8123,
                "dbname": "default",
                "user": "default",
                "password": "ClickHouse123!",
                "ssl": False,
                "additional-options": "socket_timeout=300000&connection_timeout=10000"
            },
            "auto_run_queries": True,
            "is_full_sync": True,
            "cache_ttl": None
        }
        
        try:
            if clickhouse_db:
                # Actualizar configuración existente
                print(f"🔄 Actualizando base de datos existente (ID: {clickhouse_db['id']})")
                
                update_config = {**db_config, "id": clickhouse_db["id"]}
                
                response = requests.put(
                    f"{self.metabase_url}/api/database/{clickhouse_db['id']}",
                    json=update_config,
                    headers=headers,
                    timeout=30
                )
                
                if response.status_code == 200:
                    print("✅ Configuración actualizada")
                    db_id = clickhouse_db["id"]
                else:
                    print(f"⚠️  Error actualizando: {response.status_code}")
                    print(response.text)
                    return False
            else:
                # Crear nueva configuración
                print("📝 Creando nueva base de datos ClickHouse")
                
                response = requests.post(
                    f"{self.metabase_url}/api/database",
                    json=db_config,
                    headers=headers,
                    timeout=30
                )
                
                if response.status_code == 200:
                    db_id = response.json().get("id")
                    print(f"✅ Nueva base de datos creada (ID: {db_id})")
                else:
                    print(f"❌ Error creando base de datos: {response.status_code}")
                    print(response.text)
                    return False
            
            # Sincronizar esquema
            print("🔄 Sincronizando esquema de base de datos...")
            
            sync_response = requests.post(
                f"{self.metabase_url}/api/database/{db_id}/sync_schema",
                headers=headers,
                timeout=60
            )
            
            if sync_response.status_code in [200, 202]:
                print("✅ Sincronización iniciada")
                
                # Esperar un poco para que se complete la sincronización
                print("⏳ Esperando sincronización...")
                time.sleep(30)
                
                return True
            else:
                print(f"⚠️  Error sincronizando: {sync_response.status_code}")
                return False
                
        except Exception as e:
            print(f"❌ Error configurando ClickHouse: {e}")
            return False
    
    def test_data_access(self):
        """Probar acceso a datos después de la configuración"""
        print("\n🧪 PROBANDO ACCESO A DATOS")
        print("="*50)
        
        if not self.session_token:
            print("❌ Sin token de sesión")
            return False
        
        headers = {"X-Metabase-Session": self.session_token}
        
        # Consulta de prueba
        test_query = {
            "type": "native",
            "native": {
                "query": "SELECT COUNT(*) as total FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora"
            },
            "database": None  # Se completará con la ID de la base de datos
        }
        
        # Obtener ID de base de datos ClickHouse
        databases = self.get_databases()
        for db in databases:
            if "clickhouse" in db.get("name", "").lower():
                test_query["database"] = db["id"]
                break
        
        if not test_query["database"]:
            print("❌ No se encontró base de datos ClickHouse")
            return False
        
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
                    print(f"✅ Consulta exitosa: {count:,} registros")
                    return True
                else:
                    print("⚠️  Consulta ejecutada pero sin datos")
                    return False
            else:
                print(f"❌ Error ejecutando consulta: {response.status_code}")
                print(response.text)
                return False
                
        except Exception as e:
            print(f"❌ Error probando datos: {e}")
            return False
    
    def run_complete_fix(self):
        """Ejecutar proceso completo de arreglo"""
        print("🔧 REPARACIÓN AUTOMÁTICA DE METABASE - VISUALIZACIÓN DE DATOS")
        print("="*70)
        
        # 1. Esperar Metabase
        if not self.wait_for_metabase():
            print("❌ Metabase no disponible")
            return False
        
        # 2. Setup inicial si es necesario
        self.setup_metabase_admin()
        
        # 3. Obtener token de sesión
        if not self.get_session_token():
            print("❌ No se pudo autenticar en Metabase")
            print("\n💡 SOLUCIÓN MANUAL:")
            print("   1. Ve a http://localhost:3000")
            print("   2. Completa el setup inicial si es necesario")
            print("   3. Configura ClickHouse manualmente usando la guía")
            return False
        
        # 4. Configurar ClickHouse
        if not self.configure_clickhouse_database():
            print("❌ Error configurando ClickHouse")
            return False
        
        # 5. Probar acceso a datos
        if self.test_data_access():
            print("\n🎉 REPARACIÓN COMPLETADA EXITOSAMENTE")
            print("✅ Metabase puede acceder a los datos de ClickHouse")
            print(f"\n📊 ACCESO:")
            print(f"   🔗 Metabase: {self.metabase_url}")
            print(f"   📋 Ve a 'Browse Data' para explorar")
            print(f"   🔍 Usa 'New Question' → 'Native Query' para consultas SQL")
            return True
        else:
            print("\n⚠️  CONFIGURACIÓN APLICADA PERO DATOS NO ACCESIBLES")
            print("📋 Revisa la configuración manual usando: logs/metabase_data_fix_guide.md")
            return False

def main():
    fixer = MetabaseFixer()
    fixer.run_complete_fix()

if __name__ == "__main__":
    main()