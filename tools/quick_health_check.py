#!/usr/bin/env python3
"""
⚡ QUICK HEALTH CHECK
====================

Verificación rápida del estado del sistema con output limpio.
Ideal para scripts y automatización.
"""

import sys
import os
import json
from pathlib import Path

# Agregar el directorio actual al path para importaciones
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from robust_service_tester import RobustServiceTester
    
    def quick_health_check():
        """Verificación rápida con output mínimo"""
        tester = RobustServiceTester()
        
        # Configurar para ser más rápido
        tester.retry_config.max_attempts = 2
        tester.retry_config.initial_delay = 0.5
        tester.retry_config.timeout_per_attempt = 8.0
        
        results = tester.run_all_tests()
        
        # Output limpio para scripts
        print(f"SCORE: {results['average_score']:.1f}%")
        print(f"STATUS: {results['overall_status']}")
        print(f"SERVICES: {results['services_successful']}/{results['services_tested']}")
        
        # Guardar resultados rápidos
        logs_dir = "/app/logs" if os.path.exists("/app/logs") else "./logs"
        os.makedirs(logs_dir, exist_ok=True)
        
        quick_results = {
            "score": results['average_score'],
            "status": results['overall_status'],
            "services_ok": results['services_successful'],
            "total_services": results['services_tested'],
            "timestamp": results['timestamp']
        }
        
        with open(f"{logs_dir}/quick_health.json", 'w') as f:
            json.dump(quick_results, f)
        
        return results['average_score'] >= 90
        
except ImportError:
    def quick_health_check():
        print("SCORE: 0%")
        print("STATUS: SISTEMA ROBUSTO NO DISPONIBLE")
        print("SERVICES: 0/5")
        return False

if __name__ == "__main__":
    success = quick_health_check()
    sys.exit(0 if success else 1)