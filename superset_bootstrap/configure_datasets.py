#!/usr/bin/env python3
"""
Script para configurar automáticamente los datasets de ClickHouse en Superset
"""
import requests
import time
import json
import os
import sys

def wait_for_superset(url, timeout=300):
    """Esperar a que Superset esté disponible y completamente inicializado"""
    print(f"🔄 Esperando a que Superset esté disponible en {url}...")
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            response = requests.get(f"{url}/health", timeout=5)
            if response.status_code == 200:
                print("✅ Superset está disponible")
                # Espera adicional para asegurar que la BD y el admin estén listos
                print("⏳ Esperando inicialización completa de Superset (10s)...")
                time.sleep(10)
                return True
        except requests.exceptions.RequestException:
            pass
        
        time.sleep(5)
    
    print("❌ Timeout esperando a Superset")
    return False

def wait_for_admin_ready(url, username, max_wait=180):
    """Esperar a que el admin user esté realmente creado y disponible.
    
    Intenta login periódicamente hasta que funcione o se agote el timeout.
    Esto asegura que superset-init haya completado la creación del admin.
    """
    print(f"🔐 Esperando a que el usuario admin esté listo en {url}...")
    start_time = time.time()
    attempt = 0
    
    while time.time() - start_time < max_wait:
        attempt += 1
        # Intentar un login simple para verificar si el admin existe
        try:
            resp = requests.post(
                f"{url}/api/v1/security/login",
                json={"username": username, "password": "Admin123!", "provider": "db", "refresh": True},
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            if resp.status_code == 200:
                print(f"✅ Usuario admin listo después de {attempt} intentos")
                return True
            elif resp.status_code == 401:
                # Admin existe pero contraseña incorrecta - probar alternativas
                for pwd in ["admin", os.getenv("SUPERSET_PASSWORD"), "Admin123!"]:
                    if not pwd:
                        continue
                    r = requests.post(
                        f"{url}/api/v1/security/login",
                        json={"username": username, "password": pwd, "provider": "db", "refresh": True},
                        timeout=10
                    )
                    if r.status_code == 200:
                        print(f"✅ Usuario admin listo (contraseña validada: {pwd[:4]}***)")
                        return True
        except requests.exceptions.RequestException:
            pass
        
        if attempt % 5 == 0:
            print(f"   Esperando admin... intento {attempt} ({int(time.time() - start_time)}s transcurridos)")
        
        time.sleep(3)
    
    print(f"⚠️  Admin no estuvo listo después de {max_wait}s - continuando de todas formas")
    return False

def get_auth_token(url, username, password, timeout_sec: int = 300, initial_backoff: int = 3, max_backoff: int = 30):
    """Obtener token de autenticación de Superset con reintentos robustos y backoff exponencial.

    Devuelve una tupla (token, used_password). Si falla, devuelve (None, None).
    Intenta con la contraseña proporcionada y, si hay 401, prueba con alternativas comunes.
    Usa backoff exponencial para no saturar el servidor.
    """
    print("🔑 Obteniendo token de autenticación...")

    # Candidatas de contraseña por si hay desalineación entre servicios/env
    pwd_candidates = []
    seen = set()
    for p in [password, os.getenv("SUPERSET_PASSWORD"), "Admin123!", "admin"]:
        if p and p not in seen:
            seen.add(p)
            pwd_candidates.append(p)

    start = time.time()
    last_err = None
    attempt = 0
    backoff = initial_backoff
    
    while time.time() - start < timeout_sec:
        attempt += 1
        
        for pwd in pwd_candidates:
            login_data = {
                "username": username,
                "password": pwd,
                "provider": "db",
                "refresh": True,
            }
            try:
                response = requests.post(
                    f"{url}/api/v1/security/login",
                    json=login_data,
                    headers={"Content-Type": "application/json"},
                    timeout=10,
                )
                if response.status_code == 200:
                    token = response.json().get("access_token")
                    if token:
                        print(f"✅ Token obtenido exitosamente (intento {attempt})")
                        return token, pwd
                    else:
                        last_err = "respuesta sin token"
                elif response.status_code == 401:
                    last_err = f"401 - credenciales incorrectas para password {pwd[:4]}***"
                    # Continuar con siguiente candidato
                    continue
                else:
                    last_err = f"{response.status_code} - {response.text[:100]}"
            except requests.exceptions.RequestException as e:
                last_err = f"Error de conexión: {str(e)[:100]}"
        
        # Log periódico de progreso
        if attempt % 5 == 0:
            elapsed = int(time.time() - start)
            print(f"   Intento {attempt} después de {elapsed}s - Último error: {last_err}")
        
        # Backoff exponencial con límite
        time.sleep(backoff)
        backoff = min(backoff * 1.5, max_backoff)

    print(f"❌ No se pudo obtener token tras {attempt} intentos en {int(time.time()-start)}s")
    print(f"   Último error: {last_err}")
    return None, None

def authed_request(base_url, method, path, token_ref, username, password, headers=None, max_reauth=1, **kwargs):
    """Realiza una petición autenticada con Bearer token y reintenta tras 401 reautenticando.

    - base_url: p.ej. http://superset:8088
    - method: 'GET'|'POST'|'PUT'...
    - path: p.ej. '/api/v1/dataset/' (con o sin barra inicial)
    - token_ref: dict con clave 'token' y 'password' para actualizar tras re-auth
    """
    if not path.startswith('/'):
        path = '/' + path
    url = f"{base_url}{path}"
    headers = dict(headers or {})
    if token_ref.get('token'):
        headers["Authorization"] = f"Bearer {token_ref['token']}"
    # Primer intento
    resp = requests.request(method, url, headers=headers, **kwargs)
    if resp.status_code != 401 or max_reauth <= 0:
        return resp
    # Intentar reautenticación
    print("🔄 Token inválido/expirado (401). Re-autenticando...")
    new_token, used_pwd = get_auth_token(base_url, username, token_ref.get('password') or password)
    if not new_token:
        return resp  # devolver el 401 original
    token_ref['token'] = new_token
    token_ref['password'] = used_pwd
    headers["Authorization"] = f"Bearer {new_token}"
    return requests.request(method, url, headers=headers, **kwargs)

def get_databases(url, token_ref, username, password):
    """Obtener lista de bases de datos configuradas"""
    print("📊 Obteniendo bases de datos configuradas...")
    
    headers = {"Content-Type": "application/json"}
    
    try:
        response = authed_request(url, "GET", "/api/v1/database/", token_ref, username, password, headers=headers)
        if response.status_code == 200:
            databases = response.json().get("result", [])
            print(f"✅ Se encontraron {len(databases)} bases de datos")
            return databases
        else:
            print(f"❌ Error al obtener bases de datos: {response.status_code}")
            return []
    except requests.exceptions.RequestException as e:
        print(f"❌ Error de conexión: {e}")
        return []

def sync_database_schemas(url, token_ref, username, password, database_id):
    """Sincronizar esquemas de una base de datos"""
    print(f"🔄 Sincronizando esquemas para base de datos ID: {database_id}...")
    
    headers = {"Content-Type": "application/json"}
    
    try:
        # Forzar la actualización de metadatos usando el endpoint refresh (puede ser 404 en algunas versiones)
        response = authed_request(url, "POST", f"/api/v1/database/{database_id}/refresh", token_ref, username, password, headers=headers)
        if response.status_code in [200, 202]:
            print("✅ Metadatos de la base de datos actualizados correctamente")
            return True
        else:
            print(f"⚠️  No se pudo actualizar metadatos: {response.status_code} - {response.text}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"❌ Error al actualizar metadatos: {e}")
        return False

def get_database_tables(url, token_ref, username, password, database_id, schema_name="fgeo_analytics"):
    """Obtener tablas de una base de datos"""
    print(f"🔄 Obteniendo tablas para base de datos ID: {database_id}, esquema: {schema_name}...")
    
    headers = {"Content-Type": "application/json"}
    
    try:
        # El endpoint espera un parámetro 'q' en formato rison (objeto)
        # Ejemplo: q=(schema_name:'fgeo_analytics')
        rison_query = "(schema_name:'{}')".format(schema_name)
        response = authed_request(url, "GET", f"/api/v1/database/{database_id}/tables?q={rison_query}", token_ref, username, password, headers=headers)
        if response.status_code == 200:
            tables = response.json().get("result", [])
            print(f"✅ Se encontraron {len(tables)} tablas: {[t.get('value', t) for t in tables[:5]]}...")
            return tables
        else:
            print(f"⚠️  No se pudieron obtener tablas: {response.status_code} - {response.text}")
            return []
    except requests.exceptions.RequestException as e:
        print(f"❌ Error al obtener tablas: {e}")
        return []

def create_dataset(url, token_ref, username, password, database_id, table_name, schema_name="fgeo_analytics"):
    """Crear un dataset en Superset"""
    print(f"📊 Creando dataset para tabla: {table_name}...")
    
    headers = {"Content-Type": "application/json"}
    
    dataset_data = {
        "database": database_id,
        "schema": schema_name,
        "table_name": table_name,
        "sql": ""
    }
    
    try:
        response = authed_request(url, "POST", "/api/v1/dataset/", token_ref, username, password, json=dataset_data, headers=headers)
        
        if response.status_code in [200, 201]:
            print(f"✅ Dataset creado para {table_name}")
            return response.json().get("id") or True
        else:
            print(f"⚠️  No se pudo crear dataset para {table_name}: {response.status_code}")
            try:
                print("  Response body:", response.text)
            except Exception:
                pass
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Error al crear dataset para {table_name}: {e}")
        return False

def find_dataset(url, token_ref, username, password, database_id, table_name, schema_name):
    """Buscar dataset por DB/tabla/esquema y devolver su ID"""
    headers = {"Content-Type": "application/json"}
    # Filtro rison por nombre de tabla y DB
    rison_query = f"(filters:!((col:table_name,opr:eq,value:'{table_name}'),(col:database_id,opr:eq,value:{database_id}),(col:schema,opr:eq,value:'{schema_name}')),columns:!(id,table_name,schema,database_id),page:0,page_size:100)"
    resp = authed_request(url, "GET", f"/api/v1/dataset/?q={rison_query}", token_ref, username, password, headers=headers)
    if resp.status_code == 200:
        results = resp.json().get("result", [])
        if results:
            return results[0].get("id")
    return None

def update_dataset_time_column(url, token_ref, username, password, dataset_id, time_col):
    """Establecer la columna temporal por defecto (main_dttm_col).
    Si time_col es vacío o None, no realiza cambios para evitar configuraciones erróneas.
    """
    if not time_col:
        print("ℹ️  SUPERSET_TIME_COLUMN vacío: no se configurará columna temporal por defecto")
        return True
    headers = {"Content-Type": "application/json"}
    payload = {"main_dttm_col": time_col}
    resp = authed_request(url, "PUT", f"/api/v1/dataset/{dataset_id}", token_ref, username, password, json=payload, headers=headers)
    if resp.status_code in [200, 201]:
        print(f"⏱️  Columna de tiempo por defecto configurada: {time_col}")
        return True
    else:
        print(f"⚠️  No se pudo configurar columna de tiempo ({resp.status_code}): {resp.text}")
        return False

def parse_schemas_from_env() -> list:
    """Descubre esquemas de ClickHouse a mapear como datasets en Superset.
    Prioriza SUPERSET_SCHEMAS (coma-separado). Si no está definido, deriva de DB_CONNECTIONS
    excluyendo schemas de sistema (information_schema, mysql, performance_schema, sys) y variantes
    técnicas (_analytics, ext).
    Fallback: ['fgeo_analytics']
    """
    # 1) SUPERSET_SCHEMAS explicit (comma-separated)
    raw = os.getenv("SUPERSET_SCHEMAS", "").strip()
    if raw:
        items = [s.strip() for s in raw.split(",") if s.strip()]
        if items:
            return sorted(set(items))

    # 2) Derive from DB_CONNECTIONS JSON, filtering out system and technical schemas
    system_schemas = {"information_schema", "mysql", "performance_schema", "sys"}
    excluded_patterns = ["_analytics", "ext", "default", "system", "INFORMATION_SCHEMA"]
    
    try:
        db_conns = os.getenv("DB_CONNECTIONS", "").strip()
        schemas = []
        if db_conns.startswith("[") and "]" in db_conns:
            import json as _json
            conns = _json.loads(db_conns)
            if isinstance(conns, dict):
                conns = [conns]
            for c in conns or []:
                dbn = (c or {}).get("db")
                if dbn and dbn not in system_schemas:
                    # Excluir variantes técnicas
                    if not any(pattern in dbn for pattern in excluded_patterns):
                        schemas.append(dbn)
        # Fallback default
        schemas = [s for s in schemas if s]
        if schemas:
            return sorted(set(schemas))
    except Exception:
        pass

    # 3) Default fallback
    return [os.getenv("SUPERSET_SCHEMA", "fgeo_analytics")]

def cleanup_duplicate_databases(url, token_ref, username, password):
    """Elimina bases de datos ClickHouse duplicadas o legacy, dejando solo 'ClickHouse ETL Database'"""
    print("🧹 Limpiando bases de datos ClickHouse duplicadas...")
    
    headers = {"Content-Type": "application/json"}
    
    try:
        response = authed_request(url, "GET", "/api/v1/database/", token_ref, username, password, headers=headers)
        if response.status_code == 200:
            databases = response.json().get("result", [])
            official_db_name = "ClickHouse ETL Database"
            official_db_id = None
            
            for db in databases:
                db_name = db.get('database_name', '')
                db_id = db.get('id')
                
                if db_name == official_db_name:
                    official_db_id = db_id
                    print(f"✅ BD oficial encontrada: {db_name} (ID {db_id})")
                elif 'clickhouse' in db_name.lower():
                    print(f"🗑️  Eliminando BD duplicada: {db_name} (ID {db_id})")
                    del_resp = authed_request(url, "DELETE", f"/api/v1/database/{db_id}", token_ref, username, password, headers=headers)
                    if del_resp.ok:
                        print(f"✅ BD eliminada: {db_name}")
                    else:
                        print(f"⚠️  No se pudo eliminar {db_name}: {del_resp.status_code}")
            
            return official_db_id
        return None
    except Exception as e:
        print(f"❌ Error limpiando bases de datos duplicadas: {e}")
        return None

def main():
    # Configuración
    SUPERSET_URL = os.getenv("SUPERSET_URL", "http://localhost:8088")
    SUPERSET_ADMIN = os.getenv("SUPERSET_ADMIN", "admin")
    SUPERSET_PASSWORD = os.getenv("SUPERSET_PASSWORD", "Admin123!")
    # Esquemas a procesar (dinámico)
    SCHEMAS = parse_schemas_from_env()
    # No forzar columna temporal por defecto a menos que se indique explícitamente
    TIME_COLUMN = os.getenv("SUPERSET_TIME_COLUMN", "")
    
    print("🚀 Iniciando configuración automática de datasets en Superset")
    print(f"📍 URL: {SUPERSET_URL}")
    print(f"👤 Usuario: {SUPERSET_ADMIN}")
    
    # Esperar a que Superset esté disponible
    if not wait_for_superset(SUPERSET_URL):
        print("❌ Superset no está disponible")
        sys.exit(1)
    
    # Esperar a que el admin esté creado y listo
    if not wait_for_admin_ready(SUPERSET_URL, SUPERSET_ADMIN, max_wait=180):
        print("⚠️  Admin no confirmado - intentando de todas formas...")
    
    # Obtener token de autenticación con reintentos robustos
    token, SUPERSET_PASSWORD = get_auth_token(SUPERSET_URL, SUPERSET_ADMIN, SUPERSET_PASSWORD, timeout_sec=300)
    if not token:
        print("❌ No se pudo obtener token de autenticación")
        sys.exit(1)
    token_ref = {"token": token, "password": SUPERSET_PASSWORD}
    
    # Limpiar bases de datos duplicadas y obtener la oficial
    official_db_id = cleanup_duplicate_databases(SUPERSET_URL, token_ref, SUPERSET_ADMIN, SUPERSET_PASSWORD)
    
    # Obtener bases de datos (después de limpieza)
    databases = get_databases(SUPERSET_URL, token_ref, SUPERSET_ADMIN, SUPERSET_PASSWORD)
    
    clickhouse_db = None
    for db in databases:
        if "clickhouse" in db.get("database_name", "").lower() or "fgeo_analytics" in db.get("database_name", "").lower():
            clickhouse_db = db
            break
    
    if not clickhouse_db:
        print("❌ No se encontró la base de datos de ClickHouse")
        sys.exit(1)
    
    print(f"✅ Base de datos ClickHouse encontrada: {clickhouse_db['database_name']} (ID: {clickhouse_db['id']})")
    
    # Sincronizar esquemas
    sync_database_schemas(SUPERSET_URL, token_ref, SUPERSET_ADMIN, SUPERSET_PASSWORD, clickhouse_db['id'])
    time.sleep(2)  # Esperar un poco
    
    # Obtener tablas por esquema y crear datasets
    total_tables = 0
    created_count = 0
    time_col_set = 0
    candidates_report = []
    for SCHEMA_NAME in SCHEMAS:
        tables = get_database_tables(SUPERSET_URL, token_ref, SUPERSET_ADMIN, SUPERSET_PASSWORD, clickhouse_db['id'], SCHEMA_NAME)
        if not tables:
            print(f"ℹ️  No hay tablas visibles en esquema '{SCHEMA_NAME}' (puede ser normal si aún no ingesta)")
            continue
        print(f"🔨 Creando datasets para {len(tables)} tablas en esquema '{SCHEMA_NAME}'...")
        total_tables += len(tables)
        for table in tables:
            table_name = table.get('value', table) if isinstance(table, dict) else table
            # Try to create dataset; if it already exists, find its ID and continue
            dataset_id = create_dataset(SUPERSET_URL, token_ref, SUPERSET_ADMIN, SUPERSET_PASSWORD, clickhouse_db['id'], table_name, SCHEMA_NAME)
            ds_id = None
            if isinstance(dataset_id, int):
                ds_id = dataset_id
                created_count += 1
            else:
                # dataset_id could be True (created without id) or False (not created)
                found = find_dataset(SUPERSET_URL, token_ref, SUPERSET_ADMIN, SUPERSET_PASSWORD, clickhouse_db['id'], table_name, SCHEMA_NAME)
                ds_id = found
                # Determine a safe time column to set as main_dttm_col.
                set_time_col = False
                chosen_time_col = None
                if ds_id:
                    if TIME_COLUMN:
                        chosen_time_col = TIME_COLUMN
                        set_time_col = True
                    else:
                        # Try to fetch dataset detail and inspect columns
                        try:
                            resp = authed_request(SUPERSET_URL, "GET", f"/api/v1/dataset/{ds_id}", token_ref, SUPERSET_ADMIN, SUPERSET_PASSWORD, headers={"Content-Type": "application/json"}, timeout=10)
                            if resp.status_code == 200:
                                result = resp.json().get('result', {})
                                cols = result.get('columns') or []
                                # Collect candidate names by heuristic
                                candidates = []
                                for c in cols:
                                    col_name = c.get('column_name') or c.get('name') or c.get('column_name')
                                    if not col_name:
                                        continue
                                    ln = col_name.lower()
                                    if any(k in ln for k in ['date', 'fecha', 'time', 'timestamp']) or ln.endswith('_at'):
                                        candidates.append(col_name)
                                # Save candidates to report (even if 0 or >1)
                                candidates_report.append({"schema": SCHEMA_NAME, "table": table_name, "candidates": candidates})
                                # If exactly one candidate, choose it; otherwise skip to avoid wrong auto-setting
                                if len(candidates) == 1:
                                    chosen_time_col = candidates[0]
                                    set_time_col = True
                                else:
                                    print(f"ℹ️  No se establecerá columna temporal automática para {SCHEMA_NAME}.{table_name}: candidatos={candidates}")
                            else:
                                print(f"⚠️  No se pudo leer metadata del dataset {ds_id}: {resp.status_code}")
                                candidates_report.append({"schema": SCHEMA_NAME, "table": table_name, "candidates": [], "error": f"metadata_status_{resp.status_code}"})
                        except requests.exceptions.RequestException as e:
                            print(f"⚠️  Error buscando columnas del dataset {ds_id}: {e}")
                            candidates_report.append({"schema": SCHEMA_NAME, "table": table_name, "candidates": [], "error": str(e)})

                if set_time_col and ds_id and chosen_time_col:
                    if update_dataset_time_column(SUPERSET_URL, token_ref, SUPERSET_ADMIN, SUPERSET_PASSWORD, ds_id, chosen_time_col):
                        time_col_set += 1
                time.sleep(1)  # Pequeña pausa entre creaciones

    if total_tables:
        print(f"✅ Datasets verificados/creados para {total_tables} tablas en {len(SCHEMAS)} esquema(s)")
        if TIME_COLUMN:
            print(f"⏱️  Columna temporal '{TIME_COLUMN}' establecida en {time_col_set} datasets")
        # Persist candidates report to a sensible path: prefer /app/logs if available, else /tmp
        out_paths = ['/app/logs/dataset_candidates.json', '/tmp/dataset_candidates.json']
        wrote = False
        for out_path in out_paths:
            try:
                dirp = os.path.dirname(out_path)
                if not os.path.exists(dirp):
                    continue
                with open(out_path, 'w') as fh:
                    json.dump(candidates_report, fh, indent=2, ensure_ascii=False)
                print(f"📝 Candidates report written to {out_path}")
                wrote = True
                break
            except Exception as e:
                # try next
                continue
        if not wrote:
            print("⚠️  No se pudo escribir report de candidatos en rutas conocidas; imprimiendo en stdout")
            print(json.dumps(candidates_report, indent=2, ensure_ascii=False))
        # Also persist chosen time column mapping for reproducibility
        mapping = {it['table']: it.get('chosen') for it in []}
        # Build mapping from candidates_report where chosen_time_col was set
        # Note: we don't track chosen per item earlier, so reproduce by querying datasets for main_dttm_col
        try:
            mapping_path = '/app/logs/dataset_time_mapping.json'
            if os.path.exists('/app/logs'):
                # query each dataset to see main_dttm_col
                mapping = {}
                for item in candidates_report:
                    tname = item['table']
                    schema_for_t = item.get('schema') or os.getenv("SUPERSET_SCHEMA", "fgeo_analytics")
                    q = f"(filters:!((col:table_name,opr:eq,value:'{tname}'),(col:database_id,opr:eq,value:{clickhouse_db['id']}),(col:schema,opr:eq,value:'{schema_for_t}')),columns:!(id),page:0,page_size:1)"
                    resp = authed_request(SUPERSET_URL, "GET", f"/api/v1/dataset/?q={q}", token_ref, SUPERSET_ADMIN, SUPERSET_PASSWORD, headers={"Content-Type": "application/json"})
                    res = resp.json().get('result') or []
                    if res:
                        did = res[0].get('id')
                        r2 = authed_request(SUPERSET_URL, "GET", f"/api/v1/dataset/{did}", token_ref, SUPERSET_ADMIN, SUPERSET_PASSWORD, headers={"Content-Type": "application/json"})
                        if r2.status_code==200:
                            main = r2.json().get('result',{}).get('main_dttm_col')
                            mapping[f"{schema_for_t}.{tname}"] = main
                with open(mapping_path,'w') as mf:
                    json.dump(mapping, mf, indent=2, ensure_ascii=False)
                print(f"📝 Time column mapping written to {mapping_path}")
        except Exception:
            pass
        # Print short summary
        print("📋 Resumen de candidatos detectados:")
        for item in candidates_report:
            prefix = f"{item.get('schema')}." if item.get('schema') else ""
            print(f" - {prefix}{item['table']}: {item.get('candidates')}")
    else:
        print("⚠️  No se encontraron tablas para crear datasets en los esquemas configurados")
    
    print("🎉 Configuración de datasets completada!")
    print("📋 Ve a Superset > Data > Datasets para ver las tablas disponibles")

if __name__ == "__main__":
    main()