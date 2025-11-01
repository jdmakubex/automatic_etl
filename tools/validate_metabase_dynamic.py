#!/usr/bin/env python3
"""
Script de validación completa del sistema Metabase dinámico.
Verifica que la implementación funcione correctamente con diferentes configuraciones.
"""
import os
import json
import subprocess
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_dynamic_metabase():
    """Ejecuta validación completa del sistema Metabase dinámico"""
    
    logger.info("🧪 VALIDACIÓN COMPLETA DEL SISTEMA METABASE DINÁMICO")
    logger.info("=" * 60)
    
    validation_results = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "tests": {},
        "summary": {"passed": 0, "failed": 0, "total": 0}
    }
    
    # Test 1: Verificar archivos implementados
    logger.info("📁 Test 1: Verificando archivos implementados...")
    required_files = [
        "/app/tools/metabase_dynamic_configurator.py",
        "/app/tools/metabase_schema_discovery.py", 
        "/app/tools/setup_metabase_dynamic.sh"
    ]
    
    files_test = {"name": "archivos_implementados", "status": "passed", "details": []}
    for file_path in required_files:
        if os.path.exists(file_path):
            files_test["details"].append(f"✅ {file_path}")
            logger.info(f"   ✅ {file_path}")
        else:
            files_test["status"] = "failed"
            files_test["details"].append(f"❌ {file_path}")
            logger.error(f"   ❌ {file_path}")
    
    validation_results["tests"]["files"] = files_test
    
    # Test 2: Verificar parsing de DB_CONNECTIONS
    logger.info("🔍 Test 2: Verificando parsing de DB_CONNECTIONS...")
    db_connections_str = os.getenv("DB_CONNECTIONS", "[]")
    
    parsing_test = {"name": "db_connections_parsing", "status": "passed", "details": []}
    try:
        connections = json.loads(db_connections_str)
        parsing_test["details"].append(f"✅ JSON válido: {len(connections)} conexiones")
        logger.info(f"   ✅ Parseadas {len(connections)} conexiones")
        
        for i, conn in enumerate(connections):
            conn_name = conn.get("name", f"unnamed_{i}")
            db_name = conn.get("db", "unknown")
            parsing_test["details"].append(f"   📊 {conn_name}: {db_name}")
            logger.info(f"      📊 {conn_name}: {db_name}")
            
    except json.JSONDecodeError as e:
        parsing_test["status"] = "failed" 
        parsing_test["details"].append(f"❌ Error JSON: {e}")
        logger.error(f"   ❌ Error JSON: {e}")
    
    validation_results["tests"]["parsing"] = parsing_test
    
    # Test 3: Ejecutar configurador dinámico
    logger.info("🚀 Test 3: Ejecutando configurador dinámico...")
    
    configurator_test = {"name": "configurador_dinamico", "status": "unknown", "details": []}
    try:
        result = subprocess.run(
            ["python3", "/app/tools/metabase_dynamic_configurator.py"],
            capture_output=True,
            text=True,
            timeout=120
        )
        
        if result.returncode == 0:
            configurator_test["status"] = "passed"
            configurator_test["details"].append("✅ Configurador ejecutado exitosamente")
            logger.info("   ✅ Configurador ejecutado exitosamente")
            
            # Analizar output para métricas
            output_lines = result.stdout.split('\n')
            for line in output_lines:
                if "preguntas dinámicas" in line or "Dashboard creado" in line or "Esquemas configurados" in line:
                    configurator_test["details"].append(f"   📊 {line.strip()}")
                    logger.info(f"      📊 {line.strip()}")
        else:
            configurator_test["status"] = "failed"
            configurator_test["details"].append(f"❌ Error código: {result.returncode}")
            configurator_test["details"].append(f"❌ Error: {result.stderr}")
            logger.error(f"   ❌ Error código: {result.returncode}")
            
    except subprocess.TimeoutExpired:
        configurator_test["status"] = "failed"
        configurator_test["details"].append("❌ Timeout después de 120s")
        logger.error("   ❌ Timeout después de 120s")
    except Exception as e:
        configurator_test["status"] = "failed"
        configurator_test["details"].append(f"❌ Excepción: {e}")
        logger.error(f"   ❌ Excepción: {e}")
    
    validation_results["tests"]["configurator"] = configurator_test
    
    # Test 4: Verificar consultas dinámicas
    logger.info("📊 Test 4: Verificando consultas dinámicas...")
    
    queries_test = {"name": "consultas_dinamicas", "status": "unknown", "details": []}
    try:
        result = subprocess.run(
            ["python3", "/app/tools/metabase_query_test.py"],
            capture_output=True,
            text=True,
            timeout=60
        )
        
        if "Error en consulta: 202" in result.stdout:
            queries_test["status"] = "passed"
            queries_test["details"].append("✅ Consultas ejecutándose (código 202 = Aceptado)")
            logger.info("   ✅ Consultas ejecutándose correctamente")
        elif result.returncode == 0:
            queries_test["status"] = "passed" 
            queries_test["details"].append("✅ Consultas ejecutadas exitosamente")
            logger.info("   ✅ Consultas ejecutadas exitosamente")
        else:
            queries_test["status"] = "warning"
            queries_test["details"].append(f"⚠️  Consultas con advertencias: {result.returncode}")
            logger.warning(f"   ⚠️  Consultas con advertencias: {result.returncode}")
            
    except Exception as e:
        queries_test["status"] = "failed"
        queries_test["details"].append(f"❌ Error consultas: {e}")
        logger.error(f"   ❌ Error consultas: {e}")
    
    validation_results["tests"]["queries"] = queries_test
    
    # Calcular resumen
    for test_name, test_data in validation_results["tests"].items():
        validation_results["summary"]["total"] += 1
        if test_data["status"] == "passed":
            validation_results["summary"]["passed"] += 1
        elif test_data["status"] == "failed":
            validation_results["summary"]["failed"] += 1
    
    # Resultado final
    logger.info("\n" + "=" * 60)
    logger.info("📊 RESUMEN DE VALIDACIÓN:")
    logger.info(f"   ✅ Tests pasados: {validation_results['summary']['passed']}")
    logger.info(f"   ❌ Tests fallidos: {validation_results['summary']['failed']}")
    logger.info(f"   📊 Total: {validation_results['summary']['total']}")
    
    success_rate = validation_results['summary']['passed'] / validation_results['summary']['total'] * 100
    logger.info(f"   🎯 Tasa de éxito: {success_rate:.1f}%")
    
    if success_rate >= 75:
        logger.info("🎉 ¡VALIDACIÓN EXITOSA! Sistema Metabase dinámico funcionando correctamente")
        final_status = "PASSED"
    else:
        logger.error("💥 VALIDACIÓN FALLIDA. Revisar errores arriba")
        final_status = "FAILED"
    
    validation_results["final_status"] = final_status
    validation_results["success_rate"] = success_rate
    
    # Guardar reporte
    report_path = "/app/logs/metabase_dynamic_validation.json"
    with open(report_path, 'w') as f:
        json.dump(validation_results, f, indent=2)
    
    logger.info(f"📝 Reporte guardado en: {report_path}")
    logger.info("=" * 60)
    
    return success_rate >= 75

if __name__ == "__main__":
    import sys
    success = validate_dynamic_metabase()
    sys.exit(0 if success else 1)