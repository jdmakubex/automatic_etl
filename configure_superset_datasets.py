#!/usr/bin/env python3
"""
Script para configurar datasets en Superset con las tablas de ClickHouse
"""
import os
import requests
import json
from typing import Dict, List

# Configuración de Superset
SUPERSET_HOST = "http://localhost:8088"
SUPERSET_USERNAME = "admin"
SUPERSET_PASSWORD = "admin"

# Información de la base de datos ClickHouse
CLICKHOUSE_DB_INFO = {
    "database_name": "fgeo_analytics", 
    "sqlalchemy_uri": "clickhousedb://etl:etl_password@clickhouse:8123/fgeo_analytics"
}

# Tablas disponibles
TABLES = [
    "archivos_archivos__archivos",
    "archivos_archivos__oficiosconsulta", 
    "archivos_archivos__acuerdosdeterminaciones",
    "archivos_archivos__archivosnarcoticos",
    "archivos_archivos__archivosindiciados",
    "archivos_archivos__archivosexpedientes",
    "archivos_archivos__archivosnoticias",
    "archivos_archivos__archivosofendidostemp",
    "archivos_archivos__archivosordenes",
    "archivos_archivos__archivosvehiculos",
    "archivos_archivos__puestaordenes"
]

class SupersetConfigurer:
    def __init__(self):
        self.session = requests.Session()
        self.csrf_token = None
        self.access_token = None
        
    def authenticate(self) -> bool:
        """Autenticar con Superset"""
        print("🔐 Autenticando con Superset...")
        
        # Obtener token CSRF
        csrf_response = self.session.get(f"{SUPERSET_HOST}/api/v1/security/csrf_token/")
        if csrf_response.status_code != 200:
            print(f"❌ Error obteniendo CSRF token: {csrf_response.status_code}")
            return False
            
        csrf_data = csrf_response.json()
        self.csrf_token = csrf_data.get("result")
        
        # Login
        login_data = {
            "username": SUPERSET_USERNAME,
            "password": SUPERSET_PASSWORD,
            "provider": "db",
            "refresh": True
        }
        
        headers = {
            "Content-Type": "application/json",
            "X-CSRFToken": self.csrf_token
        }
        
        login_response = self.session.post(
            f"{SUPERSET_HOST}/api/v1/security/login",
            json=login_data,
            headers=headers
        )
        
        if login_response.status_code == 200:
            login_result = login_response.json()
            self.access_token = login_result.get("access_token")
            print("✅ Autenticación exitosa")
            return True
        else:
            print(f"❌ Error en login: {login_response.status_code} - {login_response.text}")
            return False
    
    def get_database_id(self) -> int:
        """Obtener ID de la base de datos ClickHouse"""
        print("🔍 Buscando base de datos ClickHouse...")
        
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "X-CSRFToken": self.csrf_token
        }
        
        response = self.session.get(f"{SUPERSET_HOST}/api/v1/database/", headers=headers)
        
        if response.status_code == 200:
            databases = response.json()["result"]
            for db in databases:
                if db["database_name"] == CLICKHOUSE_DB_INFO["database_name"]:
                    print(f"✅ Base de datos encontrada: {db['database_name']} (ID: {db['id']})")
                    return db["id"]
        
        print("❌ Base de datos ClickHouse no encontrada")
        return None
    
    def create_dataset(self, table_name: str, database_id: int) -> bool:
        """Crear dataset para una tabla"""
        print(f"📊 Creando dataset para tabla: {table_name}")
        
        dataset_data = {
            "database": database_id,
            "table_name": table_name,
            "schema": CLICKHOUSE_DB_INFO["database_name"]
        }
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}",
            "X-CSRFToken": self.csrf_token
        }
        
        response = self.session.post(
            f"{SUPERSET_HOST}/api/v1/dataset/",
            json=dataset_data,
            headers=headers
        )
        
        if response.status_code == 201:
            print(f"✅ Dataset creado exitosamente para {table_name}")
            return True
        else:
            print(f"❌ Error creando dataset para {table_name}: {response.status_code} - {response.text}")
            return False
    
    def list_existing_datasets(self) -> List[str]:
        """Listar datasets existentes"""
        print("📋 Listando datasets existentes...")
        
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "X-CSRFToken": self.csrf_token
        }
        
        response = self.session.get(f"{SUPERSET_HOST}/api/v1/dataset/", headers=headers)
        
        if response.status_code == 200:
            datasets = response.json()["result"]
            existing_tables = [ds["table_name"] for ds in datasets]
            print(f"✅ Encontrados {len(existing_tables)} datasets existentes")
            return existing_tables
        else:
            print(f"❌ Error listando datasets: {response.status_code}")
            return []
    
    def configure_all_datasets(self):
        """Configurar todos los datasets"""
        print("🚀 Iniciando configuración de datasets...")
        
        # Autenticar
        if not self.authenticate():
            return False
        
        # Obtener ID de base de datos
        database_id = self.get_database_id()
        if not database_id:
            return False
        
        # Listar datasets existentes
        existing_datasets = self.list_existing_datasets()
        
        # Crear datasets para tablas que no existen
        success_count = 0
        for table in TABLES:
            if table in existing_datasets:
                print(f"⏭️  Dataset ya existe para {table}")
                success_count += 1
            else:
                if self.create_dataset(table, database_id):
                    success_count += 1
        
        print(f"\n🎉 Configuración completada: {success_count}/{len(TABLES)} datasets configurados")
        return success_count == len(TABLES)

def main():
    """Función principal"""
    print("=" * 60)
    print("🔧 CONFIGURADOR DE DATASETS DE SUPERSET")
    print("=" * 60)
    
    configurator = SupersetConfigurer()
    success = configurator.configure_all_datasets()
    
    if success:
        print("\n✅ ¡Todos los datasets configurados exitosamente!")
        print(f"🌐 Accede a Superset en: {SUPERSET_HOST}")
        print("📊 Los datasets estarán disponibles en la sección 'Datasets'")
    else:
        print("\n❌ Hubo errores en la configuración")
    
    return 0 if success else 1

if __name__ == "__main__":
    exit(main())