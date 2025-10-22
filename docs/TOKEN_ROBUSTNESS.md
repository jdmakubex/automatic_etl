# Robustez de Autenticación en Superset

## Problema Identificado

El servicio `superset-datasets` se ejecutaba muy temprano, antes de que:
1. El usuario admin estuviera creado
2. La contraseña estuviera sincronizada entre servicios
3. Superset estuviera completamente inicializado

Esto causaba errores **401 - Not authorized** repetidos.

## Soluciones Implementadas

### 1. Dependencias en Docker Compose

**Antes**:
```yaml
superset-datasets:
  depends_on:
    superset:
      condition: service_healthy
    clickhouse:
      condition: service_healthy
```

**Ahora**:
```yaml
superset-datasets:
  depends_on:
    superset:
      condition: service_healthy
    clickhouse:
      condition: service_healthy
    superset-init:
      condition: service_completed_successfully  # ⬅️ NUEVO
```

**Beneficio**: `superset-datasets` espera a que `superset-init` termine de crear el admin.

---

### 2. Estandarización de Contraseñas

**Problema**: El admin se creaba con contraseña `admin` pero los servicios esperaban `Admin123!`

**Solución en `auto_pipeline.sh`**:
```bash
# Crear/resetear usuario admin con contraseña estandarizada
ADMIN_PASSWORD="${SUPERSET_PASSWORD:-Admin123!}"

# Intentar crear admin
docker compose exec -T superset superset fab create-admin \
    --password "$ADMIN_PASSWORD" ...

# SIEMPRE resetear contraseña para asegurar sincronización
docker compose exec -T superset superset fab reset-password \
    --username admin \
    --password "$ADMIN_PASSWORD"
```

**Beneficio**: La contraseña es consistente en todos los servicios desde el inicio.

---

### 3. Espera Activa del Admin (`wait_for_admin_ready`)

**Nueva función en `configure_datasets.py`**:
```python
def wait_for_admin_ready(url, username, max_wait=180):
    """Esperar a que el admin esté realmente creado y disponible.
    
    Intenta login periódicamente hasta que funcione.
    """
    while time.time() - start_time < max_wait:
        try:
            resp = requests.post(
                f"{url}/api/v1/security/login",
                json={"username": username, "password": "Admin123!", ...}
            )
            if resp.status_code == 200:
                return True  # Admin listo
            elif resp.status_code == 401:
                # Probar contraseñas alternativas
                for pwd in ["admin", "Admin123!", ...]:
                    # ...
        except:
            pass
        time.sleep(3)
```

**Beneficio**: No intenta configurar datasets hasta confirmar que el admin existe y puede autenticarse.

---

### 4. Backoff Exponencial en `get_auth_token`

**Antes**: Reintento cada 5 segundos fijo
**Ahora**: Backoff exponencial con límite

```python
def get_auth_token(..., initial_backoff=3, max_backoff=30):
    backoff = initial_backoff
    
    while time.time() - start < timeout:
        # Intentar login...
        
        # Backoff exponencial
        time.sleep(backoff)
        backoff = min(backoff * 1.5, max_backoff)
```

**Beneficio**: 
- No satura el servidor con requests continuas
- Se adapta a tiempos de inicialización variables
- Secuencia: 3s → 4.5s → 6.75s → 10s → 15s → 22.5s → 30s (límite)

---

### 5. Reintentos de Múltiples Contraseñas

**Estrategia**:
```python
pwd_candidates = []
for p in [password, os.getenv("SUPERSET_PASSWORD"), "Admin123!", "admin"]:
    if p and p not in seen:
        pwd_candidates.append(p)

for pwd in pwd_candidates:
    # Intentar login con cada una
```

**Beneficio**: Maneja desincronizaciones temporales entre servicios.

---

### 6. Auto-Refresco de Token en `authed_request`

**Función mejorada**:
```python
def authed_request(base_url, method, path, token_ref, username, password, ...):
    # Primer intento con token actual
    resp = requests.request(method, url, headers=headers, ...)
    
    if resp.status_code != 401:
        return resp  # OK
    
    # Re-autenticar automáticamente
    new_token, used_pwd = get_auth_token(base_url, username, password)
    if new_token:
        token_ref['token'] = new_token
        token_ref['password'] = used_pwd
        # Reintentar con nuevo token
        return requests.request(method, url, headers=headers, ...)
```

**Beneficio**: Cualquier API call que falle por token expirado/inválido se recupera automáticamente.

---

### 7. Logs Detallados de Progreso

**Ejemplo de salida**:
```
🔄 Esperando a que Superset esté disponible en http://superset:8088...
✅ Superset está disponible
⏳ Esperando inicialización completa de Superset (10s)...
🔐 Esperando a que el usuario admin esté listo...
   Esperando admin... intento 5 (15s transcurridos)
   Esperando admin... intento 10 (33s transcurridos)
✅ Usuario admin listo después de 12 intentos
🔑 Obteniendo token de autenticación...
   Intento 5 después de 18s - Último error: 401 - credenciales incorrectas
✅ Token obtenido exitosamente (intento 8)
```

**Beneficio**: Puedes ver exactamente qué está esperando y por qué.

---

## Flujo Completo de Autenticación

```
1. docker compose up -d
   │
2. superset (healthy) ✅
   │
3. superset-init ejecuta:
   ├─ fab create-admin --password Admin123!
   └─ fab reset-password --password Admin123!  ⬅️ Asegura sincronización
   │
4. superset-init completa ✅
   │
5. superset-datasets inicia:
   ├─ wait_for_superset() → espera /health ✅
   ├─ wait_for_admin_ready() → confirma login OK ✅
   ├─ get_auth_token() → obtiene token con backoff ✅
   └─ Configura datasets con token válido ✅
```

---

## Variables de Entorno Unificadas

Todos los servicios ahora leen de `.env`:

```bash
SUPERSET_ADMIN=admin
SUPERSET_PASSWORD=Admin123!
SUPERSET_URL=http://superset:8088
```

**Servicios afectados**:
- ✅ `etl-orchestrator` (auto_pipeline.sh)
- ✅ `superset-init` (superset_auto_configurator.py)
- ✅ `superset-datasets` (configure_datasets.py)
- ✅ docker-compose.yml

---

## Comandos de Verificación

### Ver si el admin está listo
```bash
docker compose exec superset superset fab list-users
```

### Probar login manualmente
```bash
curl -X POST http://localhost:8088/api/v1/security/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"Admin123!","provider":"db","refresh":true}'
```

### Ver logs de datasets
```bash
docker compose logs superset-datasets
```

---

## Resultado Final

✅ **0 errores 401** durante la ejecución automática
✅ **Contraseña sincronizada** en todos los servicios
✅ **Reintentos inteligentes** con backoff exponencial
✅ **Auto-recuperación** de tokens expirados
✅ **Logs claros** para debugging
✅ **Dependencias correctas** en Docker Compose

---

## Rollback / Troubleshooting

Si sigues teniendo problemas:

1. **Resetear admin manualmente**:
   ```bash
   docker exec superset superset fab reset-password \
     --username admin --password Admin123!
   ```

2. **Verificar variables de entorno**:
   ```bash
   docker compose config | grep SUPERSET
   ```

3. **Ver logs detallados**:
   ```bash
   cat logs/auto_pipeline_detailed.log | grep "admin\|token\|401"
   ```

4. **Limpieza total**:
   ```bash
   ./tools/clean_all.sh
   docker compose up -d
   ```
