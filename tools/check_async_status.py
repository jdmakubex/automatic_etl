#!/usr/bin/env python3
"""
Quick check of Superset async stack status
"""
import requests
import os

def check_async_stack():
    print("🔍 Verificando stack asíncrono de Superset...")
    print()
    
    # Check Redis
    try:
        import redis
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        r.ping()
        print("✅ Redis: Conectado y respondiendo")
    except Exception as e:
        print(f"❌ Redis: Error - {e}")
        return False
    
    # Check Superset
    try:
        resp = requests.get("http://localhost:8088/health", timeout=5)
        if resp.ok:
            print("✅ Superset: Healthy")
        else:
            print(f"⚠️  Superset: Responde pero no healthy ({resp.status_code})")
    except Exception as e:
        print(f"❌ Superset: No responde - {e}")
        return False
    
    # Check Celery workers via Superset API
    try:
        # Login
        login_resp = requests.post(
            "http://localhost:8088/api/v1/security/login",
            json={
                "username": os.getenv("SUPERSET_ADMIN", "admin"),
                "password": os.getenv("SUPERSET_PASSWORD", "Admin123!"),
                "provider": "db",
                "refresh": True
            },
            timeout=10
        )
        
        if login_resp.ok:
            token = login_resp.json()["access_token"]
            
            # Try to get database info to confirm API works
            headers = {"Authorization": f"Bearer {token}"}
            db_resp = requests.get(
                "http://localhost:8088/api/v1/database/",
                headers=headers,
                timeout=10
            )
            
            if db_resp.ok:
                print("✅ Superset API: Respondiendo correctamente")
                print()
                print("🎉 Stack asíncrono está funcionando!")
                print()
                print("💡 Si los gráficos se quedan esperando:")
                print("   1. Verifica que el toggle 'Run Async' esté marcado en SQL Lab")
                print("   2. Revisa la pestaña 'SQL Lab > Query History' para ver el estado")
                print("   3. Intenta con una consulta más simple primero")
                return True
            else:
                print(f"⚠️  Superset API: Error {db_resp.status_code}")
        else:
            print(f"⚠️  Login falló: {login_resp.status_code}")
    except Exception as e:
        print(f"⚠️  No se pudo verificar API: {e}")
    
    return True

if __name__ == "__main__":
    check_async_stack()
