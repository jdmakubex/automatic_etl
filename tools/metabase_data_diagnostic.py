#!/usr/bin/env python3
"""
Diagnóstico y solución para problemas de visualización de datos en Metabase
Verifica conectividad, permisos y configuración de ClickHouse en Metabase
"""

import subprocess
import json
import sys
import requests
import time
from datetime import datetime

class MetabaseDiagnostic:
    def __init__(self):
        self.clickhouse_user = "default"
        self.clickhouse_password = "ClickHouse123!"
        self.metabase_url = "http://localhost:3000"
        
    def run_clickhouse_query(self, query, timeout=30):
        """Ejecutar consulta en ClickHouse"""
        try:
            result = subprocess.run([
                'docker', 'exec', 'clickhouse', 
                'clickhouse-client', 
                '--user', self.clickhouse_user, 
                '--password', self.clickhouse_password,
                '--query', query
            ], capture_output=True, text=True, timeout=timeout)
            
            if result.returncode == 0:
                return result.stdout.strip()
            else:
                print(f"❌ Error en consulta: {result.stderr}")
                return None
        except Exception as e:
            print(f"❌ Error ejecutando consulta: {e}")
            return None
    
    def verify_clickhouse_direct_access(self):
        """Verificar acceso directo a ClickHouse desde el host"""
        print("🔍 VERIFICANDO ACCESO DIRECTO A CLICKHOUSE")
        print("="*50)
        
        # Verificar conectividad básica
        basic_query = "SELECT 1 as test"
        result = self.run_clickhouse_query(basic_query)
        if result:
            print("✅ Conectividad básica: OK")
        else:
            print("❌ Conectividad básica: FALLO")
            return False
        
        # Verificar datos en tablas principales
        test_queries = [
            ("fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora", "SELECT COUNT(*) FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora"),
            ("archivos.src__archivos__archivos__archivos", "SELECT COUNT(*) FROM archivos.src__archivos__archivos__archivos")
        ]
        
        for table_name, query in test_queries:
            result = self.run_clickhouse_query(query)
            if result and int(result) > 0:
                print(f"✅ {table_name}: {int(result):,} registros")
            else:
                print(f"❌ {table_name}: Sin datos o error")
        
        # Verificar muestra de datos
        sample_query = "SELECT * FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora LIMIT 3"
        result = self.run_clickhouse_query(sample_query)
        if result and len(result.split('\n')) >= 3:
            print("✅ Muestra de datos: Disponible")
            return True
        else:
            print("❌ Muestra de datos: No disponible")
            return False
    
    def test_clickhouse_http_interface(self):
        """Probar la interfaz HTTP de ClickHouse (que usa Metabase)"""
        print("\n🌐 VERIFICANDO INTERFAZ HTTP DE CLICKHOUSE")
        print("="*50)
        
        try:
            # Probar acceso HTTP básico
            response = requests.get(
                "http://localhost:8123/",
                timeout=10
            )
            
            if response.status_code == 200:
                print("✅ Interfaz HTTP: Disponible")
            else:
                print(f"❌ Interfaz HTTP: Error {response.status_code}")
                return False
                
            # Probar consulta HTTP con autenticación
            query = "SELECT COUNT(*) FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora"
            response = requests.post(
                "http://localhost:8123/",
                data=query,
                auth=(self.clickhouse_user, self.clickhouse_password),
                timeout=15
            )
            
            if response.status_code == 200:
                count = int(response.text.strip())
                print(f"✅ Consulta HTTP autenticada: {count:,} registros")
                return True
            else:
                print(f"❌ Consulta HTTP: Error {response.status_code}")
                print(f"   Respuesta: {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Error probando HTTP: {e}")
            return False
    
    def check_metabase_health(self):
        """Verificar el estado de Metabase"""
        print("\n🏥 VERIFICANDO ESTADO DE METABASE")
        print("="*50)
        
        try:
            # Verificar que Metabase esté disponible
            health_response = requests.get(
                f"{self.metabase_url}/api/health",
                timeout=10
            )
            
            if health_response.status_code == 200:
                print("✅ Metabase: Disponible")
            else:
                print(f"❌ Metabase: Error {health_response.status_code}")
                return False
            
            # Verificar interfaz web
            web_response = requests.get(
                self.metabase_url,
                timeout=10
            )
            
            if web_response.status_code == 200:
                print("✅ Interfaz web: Disponible")
                return True
            else:
                print(f"⚠️  Interfaz web: Error {web_response.status_code}")
                return False
                
        except Exception as e:
            print(f"❌ Error verificando Metabase: {e}")
            return False
    
    def create_direct_test_queries(self):
        """Crear consultas de prueba directas para Metabase"""
        print("\n📝 GENERANDO CONSULTAS DE PRUEBA PARA METABASE")
        print("="*50)
        
        test_queries = [
            {
                "name": "Prueba básica de conectividad",
                "query": "SELECT 1 as test_connection",
                "expected": "Debe devolver: 1"
            },
            {
                "name": "Contar registros en bitácora",
                "query": "SELECT COUNT(*) as total_registros FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora",
                "expected": "Debe devolver: ~513,344"
            },
            {
                "name": "Primeros 5 registros de archivos",
                "query": "SELECT * FROM archivos.src__archivos__archivos__archivos LIMIT 5",
                "expected": "Debe mostrar 5 filas con datos de archivos"
            },
            {
                "name": "Verificar tipos de datos",
                "query": "SELECT id, nombre, fsubida FROM archivos.src__archivos__archivos__archivos LIMIT 3",
                "expected": "Debe mostrar ID numérico, nombre texto, fecha"
            }
        ]
        
        print("🎯 CONSULTAS PARA PROBAR EN METABASE:")
        print("-" * 40)
        
        for i, test in enumerate(test_queries, 1):
            print(f"\n{i}. {test['name']}")
            print(f"   Consulta: {test['query']}")
            print(f"   Esperado: {test['expected']}")
            
            # Probar la consulta directamente
            result = self.run_clickhouse_query(test['query'])
            if result:
                lines = result.split('\n')
                if len(lines) <= 3:
                    print(f"   ✅ Resultado: {result}")
                else:
                    print(f"   ✅ Resultado: {len(lines)} filas de datos")
            else:
                print(f"   ❌ Error ejecutando consulta")
        
        return test_queries
    
    def check_metabase_logs(self):
        """Revisar logs de Metabase por errores"""
        print("\n📋 REVISANDO LOGS DE METABASE")
        print("="*50)
        
        try:
            # Obtener logs recientes de Metabase
            log_result = subprocess.run([
                'docker', 'logs', 'metabase', '--tail', '50'
            ], capture_output=True, text=True, timeout=30)
            
            if log_result.returncode == 0:
                logs = log_result.stdout
                
                # Buscar errores comunes
                error_patterns = [
                    "ERROR", "Exception", "Failed", "Connection refused",
                    "timeout", "Authentication", "Permission denied"
                ]
                
                found_errors = []
                for line in logs.split('\n'):
                    for pattern in error_patterns:
                        if pattern.lower() in line.lower():
                            found_errors.append(line.strip())
                
                if found_errors:
                    print("⚠️  Errores encontrados en logs:")
                    for error in found_errors[-10:]:  # Últimos 10 errores
                        print(f"   {error}")
                else:
                    print("✅ No se encontraron errores evidentes en logs")
                
                return len(found_errors) == 0
            else:
                print("❌ No se pudieron obtener los logs de Metabase")
                return False
                
        except Exception as e:
            print(f"❌ Error revisando logs: {e}")
            return False
    
    def generate_metabase_connection_guide(self):
        """Generar guía específica para configurar la conexión en Metabase"""
        print("\n📖 GENERANDO GUÍA DE CONFIGURACIÓN PARA METABASE")
        print("="*50)
        
        guide_content = f"""# GUÍA DE SOLUCIÓN PARA METABASE - VISUALIZACIÓN DE DATOS

## 🚨 PROBLEMA IDENTIFICADO
Metabase puede ver esquemas y tablas pero no puede visualizar los datos.

## 🔧 SOLUCIONES PASO A PASO

### SOLUCIÓN 1: Verificar configuración de base de datos
1. Ve a **Metabase** → **Admin** (ícono de engrane)
2. Selecciona **Databases**
3. Haz clic en la base de datos **ClickHouse**
4. Verifica estos campos:

**Configuración recomendada:**
- **Display name:** ClickHouse
- **Host:** clickhouse (o localhost)
- **Port:** 8123
- **Database:** default
- **Username:** {self.clickhouse_user}
- **Password:** {self.clickhouse_password}

5. Haz clic en **"Save changes"**
6. Haz clic en **"Sync database schema now"**

### SOLUCIÓN 2: Probar conexión manualmente
1. En Metabase, ve a **Browse Data**
2. Selecciona **ClickHouse**
3. Si ves las bases de datos pero no datos:
   - Ve a **New** → **Question**
   - Selecciona **Native Query**
   - Escribe: `SELECT COUNT(*) FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora`
   - Haz clic en **"Get Answer"**

**Resultado esperado:** 513,344

### SOLUCIÓN 3: Reiniciar conexión de Metabase
Si sigue sin funcionar:
```bash
# Reiniciar solo Metabase
docker compose restart metabase

# Esperar 2-3 minutos y volver a intentar
```

### SOLUCIÓN 4: Configuración avanzada de ClickHouse
Si persisten problemas, en la configuración de ClickHouse en Metabase:

**Configuración adicional (Advanced options):**
- **Additional JDBC connection string options:**
  `socket_timeout=300000&connection_timeout=10000`

### SOLUCIÓN 5: Usar puerto nativo de ClickHouse
Cambiar configuración en Metabase:
- **Host:** clickhouse
- **Port:** 9000 (en lugar de 8123)
- **Usar ClickHouse Native protocol**

## 🧪 CONSULTAS DE PRUEBA

### Prueba 1: Conectividad básica
```sql
SELECT 1 as test
```

### Prueba 2: Contar datos
```sql
SELECT COUNT(*) as total FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora
```

### Prueba 3: Ver datos reales
```sql
SELECT * FROM archivos.src__archivos__archivos__archivos LIMIT 10
```

## 📊 BASES DE DATOS DISPONIBLES
- **archivos:** 34K registros
- **fiscalizacion:** 726K registros  
- **fiscalizacion_analytics:** Vistas procesadas

## ✅ VERIFICACIÓN EXITOSA
Después de aplicar las soluciones, deberías poder:
1. ✅ Ver las bases de datos en Browse Data
2. ✅ Ejecutar consultas SQL nativas
3. ✅ Ver datos en las tablas
4. ✅ Crear preguntas y dashboards

Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
        
        # Guardar guía
        with open('/mnt/c/proyectos/etl_prod/logs/metabase_data_fix_guide.md', 'w') as f:
            f.write(guide_content)
        
        print("✅ Guía guardada: logs/metabase_data_fix_guide.md")
        
        return guide_content
    
    def run_complete_diagnostic(self):
        """Ejecutar diagnóstico completo"""
        print("🔬 DIAGNÓSTICO COMPLETO DE METABASE - VISUALIZACIÓN DE DATOS")
        print("="*70)
        
        # Resultados del diagnóstico
        results = {}
        
        # 1. Verificar ClickHouse directo
        results['clickhouse_direct'] = self.verify_clickhouse_direct_access()
        
        # 2. Verificar interfaz HTTP
        results['clickhouse_http'] = self.test_clickhouse_http_interface()
        
        # 3. Verificar Metabase
        results['metabase_health'] = self.check_metabase_health()
        
        # 4. Revisar logs
        results['metabase_logs'] = self.check_metabase_logs()
        
        # 5. Crear consultas de prueba
        print(f"\n" + "="*70)
        test_queries = self.create_direct_test_queries()
        
        # 6. Generar guía
        print(f"\n" + "="*70)
        self.generate_metabase_connection_guide()
        
        # Resumen del diagnóstico
        print(f"\n🎯 RESUMEN DEL DIAGNÓSTICO:")
        print(f"   ClickHouse (Directo): {'✅' if results['clickhouse_direct'] else '❌'}")
        print(f"   ClickHouse (HTTP): {'✅' if results['clickhouse_http'] else '❌'}")
        print(f"   Metabase (Estado): {'✅' if results['metabase_health'] else '❌'}")
        print(f"   Logs limpios: {'✅' if results['metabase_logs'] else '⚠️'}")
        
        # Recomendación
        if all([results['clickhouse_direct'], results['clickhouse_http'], results['metabase_health']]):
            print(f"\n💡 RECOMENDACIÓN:")
            print(f"   Los datos están disponibles. El problema está en la configuración")
            print(f"   de Metabase. Sigue la guía: logs/metabase_data_fix_guide.md")
        else:
            print(f"\n⚠️  PROBLEMAS DETECTADOS:")
            if not results['clickhouse_direct']:
                print(f"   - ClickHouse no responde correctamente")
            if not results['clickhouse_http']:
                print(f"   - Interfaz HTTP de ClickHouse tiene problemas")
            if not results['metabase_health']:
                print(f"   - Metabase no está funcionando correctamente")
        
        return results

def main():
    diagnostic = MetabaseDiagnostic()
    diagnostic.run_complete_diagnostic()

if __name__ == "__main__":
    main()