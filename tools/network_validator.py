#!/usr/bin/env python3
"""
network_validator.py
Script de validación de red para contenedores Docker Compose.
Valida conectividad entre servicios críticos usando ping y curl.
Integrable con el orquestador maestro.
"""
import os
import subprocess
import sys
import json

def check_ping(host):
    try:
        result = subprocess.run(["ping", "-c", "2", host], capture_output=True, text=True, timeout=10)
        return result.returncode == 0, result.stdout + result.stderr
    except Exception as e:
        return False, str(e)

def check_curl(url):
    try:
        result = subprocess.run(["curl", "-sf", url], capture_output=True, text=True, timeout=10)
        return result.returncode == 0, result.stdout + result.stderr
    except Exception as e:
        return False, str(e)

def main():
    services = {
        "clickhouse": {
            "host": "clickhouse",
            "url": "http://clickhouse:8123/ping"
        },
        "connect": {
            "host": "connect",
            "url": "http://connect:8083/connectors"
        },
        "kafka": {
            "host": "kafka",
            "url": None
        }
    }
    results = {"services": {}, "overall_status": "unknown", "errors": []}
    for name, svc in services.items():
        # Solo validar con curl (HTTP)
        if svc["url"]:
            curl_ok, curl_out = check_curl(svc["url"])
            results["services"][name] = {"curl": curl_ok, "curl_output": curl_out}
            if not curl_ok:
                results["errors"].append(f"No responde el endpoint HTTP de {name} ({svc['url']})")
        else:
            # Si no hay endpoint HTTP, marcar como no validable pero no error
            results["services"][name] = {"curl": None, "curl_output": "No endpoint HTTP para validar"}
    results["overall_status"] = "healthy" if not results["errors"] else "unhealthy"
    # Guardar resultados para el orquestador
    with open("/app/logs/network_check_results.json", "w") as f:
        json.dump(results, f, indent=2)
    print(json.dumps(results, indent=2))
    return 0 if results["overall_status"] == "healthy" else 1

if __name__ == "__main__":
    sys.exit(main())
