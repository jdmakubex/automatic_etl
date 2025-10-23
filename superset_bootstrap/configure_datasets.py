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
            dataset_id = response.json().get("id")
            if dataset_id:
                print(f"✅ Dataset creado para {table_name} (ID: {dataset_id})")
                return dataset_id
            else:
                print(f"✅ Dataset creado para {table_name} (sin ID en respuesta) - se buscará su ID")
                return None
        elif response.status_code == 422:
            print(f"ℹ️  Dataset {table_name} ya existe (422) - se buscará su ID")
            return False  # Señal para buscar el dataset existente
        else:
            print(f"⚠️  No se pudo crear dataset para {table_name}: {response.status_code}")
            try:
                print(f"  Response body: {response.text[:200]}")
            except Exception:
                pass
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Error al crear dataset para {table_name}: {e}")
        return False

def find_dataset(url, token_ref, username, password, database_id, table_name, schema_name):
    """Buscar dataset por DB/tabla/esquema y devolver su ID (robusto con fallback a listado completo)."""
    headers = {"Content-Type": "application/json"}
    # Intento 1: Filtro rison (puede fallar según versión/API)
    try:
        strict_q = (
            f"(filters:!((col:table_name,opr:eq,value:'{table_name}'),"
            f"(col:database_id,opr:eq,value:{database_id}),"
            f"(col:schema,opr:eq,value:'{schema_name}')),"
            "columns:!(id,table_name,schema,database_id),page:0,page_size:100)"
        )
        resp = authed_request(url, "GET", f"/api/v1/dataset/?q={strict_q}", token_ref, username, password, headers=headers)
        if resp.status_code == 200:
            results = resp.json().get("result", [])
            if results:
                ds = results[0]
                dataset_id = ds.get("id")
                print(f"✅ Dataset encontrado: {schema_name}.{table_name} (ID: {dataset_id})")
                return dataset_id
            else:
                print(f"ℹ️  Estricto sin resultados para {schema_name}.{table_name}, probando búsqueda laxa…")
        else:
            print(f"⚠️  Error en búsqueda estricta {table_name}: {resp.status_code}")
    except Exception as e:
        print(f"ℹ️  Búsqueda estricta lanzó excepción, se hará listado total: {e}")

    # Intento 2: Listado completo (o por nombre) y filtrado en cliente
    try:
        list_q = "(page:0,page_size:2000)"  # sin filtros para maximizar compatibilidad
        resp2 = authed_request(url, "GET", f"/api/v1/dataset/?q={list_q}", token_ref, username, password, headers=headers)
        if resp2.status_code == 200:
            items = resp2.json().get("result", [])
            for ds in items:
                tn = ds.get("table_name")
                sc = ds.get("schema")
                # database puede venir como id directo o como objeto anidado
                dbid = ds.get("database_id")
                if dbid is None and isinstance(ds.get("database"), dict):
                    dbid = ds.get("database", {}).get("id")
                if tn == table_name and sc == schema_name and int(dbid or -1) == int(database_id):
                    print(f"✅ Dataset encontrado (listado): {schema_name}.{table_name} (ID: {ds.get('id')})")
                    return ds.get("id")
            print(f"⚠️  No se encontró dataset {schema_name}.{table_name} tras listado completo")
        else:
            print(f"⚠️  Error listando datasets: {resp2.status_code}")
    except Exception as e:
        print(f"⚠️  Error en listado completo de datasets: {e}")
    return None

def mark_datetime_columns(url, token_ref, username, password, dataset_id):
    """Marca automáticamente todas las columnas DateTime/Timestamp como is_dttm=True.
    Esto previene errores de GROUP BY en Superset cuando se usan fechas en visualizaciones.
    """
    try:
        headers = {"Content-Type": "application/json"}
        # Obtener información del dataset
        resp = authed_request(url, "GET", f"/api/v1/dataset/{dataset_id}", token_ref, username, password, headers=headers, timeout=10)
        if resp.status_code != 200:
            print(f"⚠️  No se pudo obtener dataset {dataset_id}: {resp.status_code}")
            return False
        
        result = resp.json().get('result', {})
        cols = result.get('columns') or []
        dataset_name = result.get('table_name', 'unknown')
        
        # Marcar todas las columnas DateTime/Timestamp como is_dttm
        updates_needed = []
        datetime_cols = []
        for col in cols:
            col_name = col.get('column_name') or col.get('name')
            col_type = (col.get('type') or col.get('type_generic') or '').upper()
            col_id = col.get('id')
            is_dttm_current = col.get('is_dttm', False)
            
            # Detectar columnas de fecha/tiempo
            is_datetime = any(dt in col_type for dt in ['DATE', 'TIME', 'TIMESTAMP'])
            
            if is_datetime and not is_dttm_current and col_id:
                updates_needed.append({
                    "is_dttm": True,
                    "id": col_id,
                    "name": col_name
                })
                datetime_cols.append(col_name)
        
        if updates_needed:
            print(f"⏱️  Marcando {len(updates_needed)} columna(s) DateTime en {dataset_name}: {datetime_cols}")
            # Actualizar columnas en batch
            success_count = 0
            for upd in updates_needed:
                resp = authed_request(url, "PUT", f"/api/v1/dataset/{dataset_id}/column/{upd['id']}", 
                                    token_ref, username, password, json={"is_dttm": True, "id": upd['id']}, headers=headers, timeout=10)
                if resp.status_code in [200, 201]:
                    success_count += 1
                else:
                    print(f"⚠️  Fallo marcando {upd['name']}: {resp.status_code}")
            
            if success_count == len(updates_needed):
                print(f"✅ {success_count} columna(s) DateTime configuradas correctamente en {dataset_name}")
            else:
                print(f"⚠️  Solo {success_count}/{len(updates_needed)} columnas configuradas en {dataset_name}")
        else:
            print(f"ℹ️  {dataset_name}: columnas DateTime ya configuradas")
        
        return True
    except Exception as e:
        print(f"⚠️  Error marcando columnas DateTime: {e}")
        return False

def update_dataset_time_column(url, token_ref, username, password, dataset_id, time_col):
    """Establecer la columna temporal por defecto (main_dttm_col) y marcarla como is_dttm.
    Si time_col es vacío o None, no realiza cambios para evitar configuraciones erróneas.
    """
    if not time_col:
        print("ℹ️  SUPERSET_TIME_COLUMN vacío: no se configurará columna temporal por defecto")
        return True
    headers = {"Content-Type": "application/json"}
    
    # Primero, obtener el dataset completo para encontrar el column_id correcto
    try:
        resp = authed_request(url, "GET", f"/api/v1/dataset/{dataset_id}", token_ref, username, password, headers=headers, timeout=10)
        if resp.status_code == 200:
            dataset = resp.json().get('result', {})
            columns = dataset.get('columns', [])
            
            # Buscar la columna de tiempo y marcarla como is_dttm=True
            time_col_found = False
            for col in columns:
                col_name = col.get('column_name', '')
                col_id = col.get('id')
                if col_name == time_col and col_id:
                    # Actualizar la columna para marcarla como temporal
                    col_payload = {
                        "is_dttm": True,
                        "python_date_format": None  # Dejar que Superset detecte automáticamente
                    }
                    col_resp = authed_request(url, "PUT", f"/api/v1/dataset/{dataset_id}/column/{col_id}", token_ref, username, password, json=col_payload, headers=headers)
                    if col_resp.status_code in [200, 201]:
                        print(f"⏱️  Columna {time_col} marcada como temporal (is_dttm=True)")
                        time_col_found = True
                    else:
                        print(f"⚠️  No se pudo marcar columna {time_col} como temporal: {col_resp.status_code}")
                    break
            
            if not time_col_found:
                print(f"⚠️  Columna {time_col} no encontrada en el dataset")
        
        # Finalmente, establecer main_dttm_col en el dataset
        payload = {"main_dttm_col": time_col}
        resp = authed_request(url, "PUT", f"/api/v1/dataset/{dataset_id}", token_ref, username, password, json=payload, headers=headers)
        if resp.status_code in [200, 201]:
            print(f"⏱️  Columna de tiempo por defecto configurada: {time_col}")
            return True
        else:
            print(f"⚠️  No se pudo configurar columna de tiempo ({resp.status_code}): {resp.text}")
            return False
    except Exception as e:
        print(f"⚠️  Error configurando columna de tiempo: {e}")
        return False

def set_dataset_default_time_grain_none(url, token_ref, username, password, dataset_id):
    """Ajusta el Explore default para que 'Time Grain' venga en None por defecto.

    Implementación: actualiza el campo 'extra' del dataset para incluir
    extra.default_form_data.time_grain_sqla = None.
    """
    headers = {"Content-Type": "application/json"}
    try:
        # Leer dataset para obtener y fusionar 'extra'
        resp = authed_request(url, "GET", f"/api/v1/dataset/{dataset_id}", token_ref, username, password, headers=headers, timeout=10)
        if resp.status_code != 200:
            print(f"⚠️  No se pudo leer dataset {dataset_id} para setear Time Grain None: {resp.status_code}")
            return False
        result = resp.json().get('result', {})
        dataset_name = result.get('table_name', 'unknown')
        extra = result.get('extra')

        # 'extra' puede venir como dict o como string JSON
        if isinstance(extra, str):
            try:
                extra_obj = json.loads(extra) if extra.strip() else {}
            except Exception:
                extra_obj = {}
        elif isinstance(extra, dict):
            extra_obj = dict(extra)
        else:
            extra_obj = {}

        default_form = extra_obj.get('default_form_data') or {}
        # Setear None explícitamente; el backend acepta null
        default_form['time_grain_sqla'] = None
        extra_obj['default_form_data'] = default_form

        payload = {
            # La API de datasets espera 'extra' como string JSON
            'extra': json.dumps(extra_obj, ensure_ascii=False)
        }
        put = authed_request(url, "PUT", f"/api/v1/dataset/{dataset_id}", token_ref, username, password, json=payload, headers=headers, timeout=10)
        if put.status_code in [200, 201]:
            print(f"🕒 Time Grain por defecto = None aplicado a dataset {dataset_name} (ID {dataset_id})")
            return True
        else:
            print(f"⚠️  Falló setear Time Grain None en dataset {dataset_id}: {put.status_code} - {put.text[:180]}")
            return False
    except Exception as e:
        print(f"⚠️  Error configurando default Time Grain None en dataset {dataset_id}: {e}")
        return False

def ensure_count_metric(url, token_ref, username, password, dataset_id):
    """Asegura que el dataset tenga una métrica COUNT(*) predeterminada.
    
    Esto previene errores de GROUP BY al crear visualizaciones porque proporciona
    una métrica válida que el usuario puede seleccionar fácilmente.
    """
    headers = {"Content-Type": "application/json"}
    try:
        # Leer metrics actuales del dataset
        resp = authed_request(url, "GET", f"/api/v1/dataset/{dataset_id}", token_ref, username, password, headers=headers, timeout=10)
        if resp.status_code != 200:
            print(f"⚠️  No se pudo leer dataset {dataset_id} para añadir métrica COUNT: {resp.status_code}")
            return False
        
        result = resp.json().get('result', {})
        dataset_name = result.get('table_name', 'unknown')
        metrics = result.get('metrics') or []
        
        # Verificar si ya existe una métrica de conteo
        has_count = any(
            m.get('metric_name') == 'count' or 
            m.get('expression') == 'count(*)' or
            'count(*)' in str(m.get('expression', '')).lower()
            for m in metrics
        )
        
        if has_count:
            print(f"ℹ️  Dataset {dataset_name} ya tiene métrica de conteo")
            return True
        
        # Crear métrica COUNT(*)
        metric_payload = {
            "expression": "count(*)",
            "metric_name": "count",
            "metric_type": "count",
            "verbose_name": "COUNT(*)",
            "description": "Conteo total de registros"
        }
        
        create_resp = authed_request(
            url, "POST", f"/api/v1/dataset/{dataset_id}/metric", 
            token_ref, username, password, json=metric_payload, headers=headers, timeout=10
        )
        
        if create_resp.status_code in [200, 201]:
            print(f"📊 Métrica COUNT(*) creada para dataset {dataset_name}")
            return True
        else:
            print(f"⚠️  No se pudo crear métrica COUNT en {dataset_name}: {create_resp.status_code} - {create_resp.text[:200]}")
            return False
            
    except Exception as e:
        print(f"⚠️  Error creando métrica COUNT para dataset {dataset_id}: {e}")
        return False

def get_user_id_by_username(url, token_ref, username, password, target_username):
    """Obtiene el ID de usuario por username, probando rutas compatibles según versión.
    Si el username destino coincide con el usuario autenticado actual, usa /api/v1/me para resolver el ID.
    """
    headers = {"Content-Type": "application/json"}
    # 0) Si target es el usuario actual, usar /api/v1/me (siempre presente)
    try:
        me = authed_request(url, "GET", "/api/v1/me", token_ref, username, password, headers=headers, timeout=10)
        if me.status_code == 200:
            me_json = me.json() or {}
            if me_json.get('username') == target_username and me_json.get('id'):
                return me_json.get('id')
    except Exception:
        pass
    # 1) Try security/users endpoint with rison filter
    try:
        q = f"(filters:!((col:username,opr:eq,value:'{target_username}')),columns:!(id,username),page:0,page_size:50)"
        resp = authed_request(url, "GET", f"/api/v1/security/users/?q={q}", token_ref, username, password, headers=headers, timeout=10)
        if resp.status_code == 200:
            items = resp.json().get('result', [])
            if items:
                return items[0].get('id')
    except Exception:
        pass
    # 2) Fallback: /api/v1/users
    try:
        q2 = "(page:0,page_size:1000)"
        resp2 = authed_request(url, "GET", f"/api/v1/users/?q={q2}", token_ref, username, password, headers=headers, timeout=10)
        if resp2.status_code == 200:
            for u in resp2.json().get('result', []):
                if u.get('username') == target_username:
                    return u.get('id')
    except Exception:
        pass
    return None

def ensure_dataset_owner(url, token_ref, username, password, dataset_id, owner_username):
    """Asegura que el dataset tenga al usuario indicado como owner."""
    headers = {"Content-Type": "application/json"}
    try:
        owner_id = get_user_id_by_username(url, token_ref, username, password, owner_username)
        if not owner_id:
            print(f"⚠️  No se pudo resolver user id para '{owner_username}'")
            return False
        # Leer dataset actual para conservar otros owners
        resp = authed_request(url, "GET", f"/api/v1/dataset/{dataset_id}", token_ref, username, password, headers=headers, timeout=10)
        if resp.status_code != 200:
            print(f"⚠️  No se pudo leer dataset {dataset_id} para owners: {resp.status_code}")
            return False
        result = resp.json().get('result', {})
        current_owners = result.get('owners') or []
        current_ids = [o.get('id') for o in current_owners if isinstance(o, dict) and 'id' in o]
        if owner_id in current_ids:
            # ya está asignado
            return True
        new_owner_ids = list({owner_id, *[oid for oid in current_ids if oid]})
        payload = {"owners": new_owner_ids}
        put = authed_request(url, "PUT", f"/api/v1/dataset/{dataset_id}", token_ref, username, password, json=payload, headers=headers, timeout=10)
        if put.status_code in [200, 201]:
            print(f"👤 Owner '{owner_username}' asignado al dataset {dataset_id}")
            return True
        else:
            print(f"⚠️  Falló asignar owner en dataset {dataset_id}: {put.status_code}")
            return False
    except Exception as e:
        print(f"⚠️  Error asignando owner en dataset {dataset_id}: {e}")
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

def ensure_admin_is_superuser(url, token_ref, username, password, admin_username):
    """Asegura que el usuario admin tenga el rol 'Admin' (superusuario) con todos los permisos"""
    print(f"👑 Verificando que {admin_username} sea superusuario...")
    
    headers = {"Content-Type": "application/json"}
    
    try:
        # Obtener lista de usuarios
        resp = authed_request(url, "GET", "/api/v1/security/users/?q=(filters:!((col:username,opr:eq,value:'{}')))".format(admin_username), 
                             token_ref, username, password, headers=headers, timeout=10)
        
        if resp.status_code != 200:
            print(f"⚠️  No se pudo verificar usuario {admin_username}: {resp.status_code}")
            return False
        
        users = resp.json().get("result", [])
        if not users:
            print(f"⚠️  Usuario {admin_username} no encontrado")
            return False
        
        user = users[0]
        user_id = user.get("id")
        current_roles = user.get("roles", [])
        role_names = [r.get("name") for r in current_roles]
        
        print(f"ℹ️  Usuario {admin_username} tiene roles: {role_names}")
        
        # Verificar si ya tiene el rol Admin
        if "Admin" in role_names:
            print(f"✅ Usuario {admin_username} ya es superusuario (rol Admin)")
            return True
        
        # Obtener ID del rol Admin
        roles_resp = authed_request(url, "GET", "/api/v1/security/roles/", token_ref, username, password, headers=headers, timeout=10)
        if roles_resp.status_code != 200:
            print(f"⚠️  No se pudieron obtener roles: {roles_resp.status_code}")
            return False
        
        all_roles = roles_resp.json().get("result", [])
        admin_role = next((r for r in all_roles if r.get("name") == "Admin"), None)
        
        if not admin_role:
            print("⚠️  Rol 'Admin' no encontrado en Superset")
            return False
        
        admin_role_id = admin_role.get("id")
        
        # Asignar rol Admin al usuario
        print(f"🔧 Asignando rol 'Admin' a {admin_username}...")
        update_payload = {
            "roles": [admin_role_id] + [r.get("id") for r in current_roles if r.get("id") != admin_role_id]
        }
        
        update_resp = authed_request(url, "PUT", f"/api/v1/security/users/{user_id}", 
                                    token_ref, username, password, json=update_payload, headers=headers, timeout=10)
        
        if update_resp.status_code in [200, 201]:
            print(f"✅ Rol 'Admin' asignado exitosamente a {admin_username}")
            return True
        else:
            print(f"⚠️  No se pudo asignar rol Admin: {update_resp.status_code} - {update_resp.text[:200]}")
            return False
            
    except Exception as e:
        print(f"⚠️  Error verificando/asignando rol Admin: {e}")
        return False

import base64

def _decode_jwt_user_id(token: str):
    """Intenta decodificar el JWT sin verificar firma y extraer el user id (user_id|sub)."""
    try:
        parts = token.split('.')
        if len(parts) < 2:
            return None
        payload = parts[1]
        # padding
        rem = len(payload) % 4
        if rem:
            payload += '=' * (4 - rem)
        data = base64.urlsafe_b64decode(payload.encode('utf-8'))
        obj = json.loads(data.decode('utf-8'))
        return obj.get('user_id') or obj.get('sub') or obj.get('user')
    except Exception:
        return None

def get_user_id_by_name_cached(url, token_ref, username, password, cache: dict, target_username: str):
    """Pequeño helper con caché en memoria para resolver user_id por username.
    Intenta por /api/v1/me y, si coincide el usuario actual, decodifica el JWT como fallback.
    """
    if 'users' not in cache:
        cache['users'] = {}
    if target_username in cache['users']:
        return cache['users'][target_username]
    # Si el target es el usuario con el que nos autenticamos, intentar decodificar el token
    if target_username == username and token_ref.get('token'):
        uid = _decode_jwt_user_id(token_ref['token'])
        if uid:
            cache['users'][target_username] = uid
            return uid
    uid = get_user_id_by_username(url, token_ref, username, password, target_username)
    cache['users'][target_username] = uid
    return uid

def assign_admin_owner_to_all_charts(url, token_ref, username, password, admin_username):
        """Asigna al usuario admin como owner de todos los charts existentes.

        Esto ayuda a evitar escenarios en los que el selector de tipo de visualización aparezca bloqueado
        por permisos/propiedad cuando se abre un chart guardado en modo vista.
        """
        print("🖼️  Asegurando que 'admin' sea owner de todos los charts…")
        headers = {"Content-Type": "application/json"}
        cache = {}
        try:
            admin_id = get_user_id_by_name_cached(url, token_ref, username, password, cache, admin_username)
            if not admin_id:
                print("⚠️  No se pudo resolver ID de admin; se omite asignación de owners de charts")
                return False

            page = 0
            page_size = 500
            total_updated = 0
            while True:
                q = f"(page:{page},page_size:{page_size},columns:!(id,owners,slice_name))"
                resp = authed_request(url, "GET", f"/api/v1/chart/?q={q}", token_ref, username, password, headers=headers, timeout=15)
                if resp.status_code != 200:
                    print(f"⚠️  No se pudo listar charts (status {resp.status_code})")
                    break
                data = resp.json() or {}
                results = data.get('result') or []
                if not results:
                    break
                for ch in results:
                    cid = ch.get('id')
                    owners = ch.get('owners') or []
                    owner_ids = [o.get('id') for o in owners if isinstance(o, dict) and 'id' in o]
                    if admin_id in owner_ids:
                        continue
                    new_owner_ids = list({admin_id, *[oid for oid in owner_ids if oid]})
                    payload = {"owners": new_owner_ids}
                    put = authed_request(url, "PUT", f"/api/v1/chart/{cid}", token_ref, username, password, json=payload, headers=headers, timeout=15)
                    if put.status_code in [200, 201]:
                        total_updated += 1
                    else:
                        # Algunos builds no permiten PUT directo; continuar
                        print(f"⚠️  Falló asignar owner en chart {cid}: {put.status_code}")
                # Continuar a siguiente página si hay resultados llenos
                if len(results) < page_size:
                    break
                page += 1
            print(f"✅ Owners de charts asegurados; charts actualizados: {total_updated}")
            return True
        except Exception as e:
            print(f"⚠️  Error asignando owners a charts: {e}")
            return False

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
    
    # Asegurar que el usuario admin sea superusuario (rol Admin)
    ensure_admin_is_superuser(SUPERSET_URL, token_ref, SUPERSET_ADMIN, SUPERSET_PASSWORD, SUPERSET_ADMIN)
    
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
            print(f"🐛 DEBUG: create_dataset returned type={type(dataset_id)} value={dataset_id}")
            # Nota: bool es subclass de int en Python; evitar tratar False/True como IDs válidos
            if isinstance(dataset_id, int) and dataset_id > 0:
                ds_id = dataset_id
                created_count += 1
            else:
                print(f"🐛 DEBUG: Entrando al else, buscando dataset existente para {table_name}")
                # dataset_id could be True (created without id) or False (not created)
                # Siempre buscar el ID del dataset, incluso si ya existe (422)
                found = find_dataset(SUPERSET_URL, token_ref, SUPERSET_ADMIN, SUPERSET_PASSWORD, clickhouse_db['id'], table_name, SCHEMA_NAME)
                print(f"🐛 DEBUG: find_dataset returned type={type(found)} value={found}")
                ds_id = found
            print(f"🐛 DEBUG: Antes del if ds_id, ds_id={ds_id}, type={type(ds_id)}, bool={bool(ds_id)}")
            # IMPORTANTE: Marcar automáticamente todas las columnas DateTime como is_dttm
            # Esto previene errores de GROUP BY cuando se usan estas columnas en charts
            # Se ejecuta SIEMPRE, incluso para datasets existentes, para asegurar configuración correcta
            if ds_id:
                print(f"🔧 Configurando columnas DateTime para dataset {table_name} (ID: {ds_id})...")
                mark_datetime_columns(SUPERSET_URL, token_ref, SUPERSET_ADMIN, SUPERSET_PASSWORD, ds_id)
                # Asegurar métrica COUNT(*) para evitar errores de GROUP BY
                ensure_count_metric(SUPERSET_URL, token_ref, SUPERSET_ADMIN, SUPERSET_PASSWORD, ds_id)
                # Asegurar que el admin sea owner del dataset para permitir edición completa
                ensure_dataset_owner(SUPERSET_URL, token_ref, SUPERSET_ADMIN, SUPERSET_PASSWORD, ds_id, SUPERSET_ADMIN)
                
                # Determine a safe time column to set as main_dttm_col.
                set_time_col = False
                chosen_time_col = None
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
                # Asegurar que el Time Grain por defecto sea None en Explore
                set_dataset_default_time_grain_none(SUPERSET_URL, token_ref, SUPERSET_ADMIN, SUPERSET_PASSWORD, ds_id)
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
    
    # Como paso final, asegurar que el usuario admin sea owner de todos los charts,
    # para evitar restricciones de edición en Explore por propiedad.
    try:
        assign_admin_owner_to_all_charts(SUPERSET_URL, token_ref, SUPERSET_ADMIN, SUPERSET_PASSWORD, SUPERSET_ADMIN)
    except Exception:
        # No detener el proceso por esto
        pass

    print("🎉 Configuración de datasets completada!")
    print("📋 Ve a Superset > Data > Datasets para ver las tablas disponibles")

if __name__ == "__main__":
    main()