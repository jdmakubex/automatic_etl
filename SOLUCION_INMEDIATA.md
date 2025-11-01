# 🚑 SOLUCIÓN RÁPIDA PARA SUPERSET Y METABASE

## 🎯 **PROBLEMA IDENTIFICADO:**
- La depuración eliminó conexiones pero los servicios necesitan reconfiguración manual
- Los datos SÍ están disponibles en ClickHouse (1.4M+ registros)

---

## 🚀 **SOLUCIÓN INMEDIATA PARA SUPERSET**

### **Paso 1: Acceder a Superset**
1. Ve a: **http://localhost:8088**
2. Login: `admin` / `admin`

### **Paso 2: Configurar Conexión ClickHouse**
1. Ve a **Settings** → **Database Connections**
2. Clic en **"+ DATABASE"**
3. Completa los campos:
   - **Display Name:** `ClickHouse_Clean`
   - **SQLAlchemy URI:** `clickhouse://default:ClickHouse123!@clickhouse:9000/default`
4. Clic en **"TEST CONNECTION"** (debe decir ✅ "Connection looks good!")
5. Clic en **"CONNECT"**

### **Paso 3: Usar SQL Lab**
1. Ve a **SQL** → **SQL Lab**
2. Selecciona la base de datos **"ClickHouse_Clean"**
3. Selecciona schema **"archivos"** o **"fiscalizacion"**
4. Prueba esta consulta:
```sql
SELECT COUNT(*) FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora;
```
**Resultado esperado: 513,344**

### **Paso 4: Crear Datasets (Opcional)**
1. Ve a **Data** → **Datasets**
2. Clic **"+ DATASET"**
3. Selecciona:
   - **Database:** ClickHouse_Clean
   - **Schema:** fiscalizacion
   - **Table:** src__fiscalizacion__fiscalizacion__bitacora
4. Clic **"CREATE DATASET AND CREATE CHART"**

---

## 🚀 **SOLUCIÓN INMEDIATA PARA METABASE**

### **Paso 1: Acceder a Metabase**
1. Ve a: **http://localhost:3000**
2. Login con tus credenciales

### **Paso 2: Verificar Conexión**
1. Ve a **Admin** (ícono de engrane) → **Databases**
2. Haz clic en la base de datos **ClickHouse**
3. Clic en **"Sync database schema now"**
4. Espera a que termine la sincronización

### **Paso 3: Navegar Datos**
1. Ve a **Browse Data**
2. Selecciona **ClickHouse**
3. Deberías ver solo estas bases de datos:
   - **archivos** (9 tablas)
   - **fiscalizacion** (8 tablas) 
   - **fiscalizacion_analytics** (8 vistas)

### **Paso 4: Crear Pregunta de Prueba**
1. Clic **"Ask a question"**
2. Selecciona **Native query**
3. Escribe:
```sql
SELECT COUNT(*) as total_registros 
FROM fiscalizacion.src__fiscalizacion__fiscalizacion__bitacora;
```
4. Clic **"Get Answer"**
**Resultado esperado: 513,344**

---

## 📊 **TABLAS PRINCIPALES DISPONIBLES**

### **Base `archivos` (34K registros total):**
- `src__archivos__archivos__archivos` - **17,026 registros**
- `src__archivos__archivos__oficiosconsulta` - **7,354 registros**
- `src__archivos__archivos__acuerdosdeterminaciones` - **3,492 registros**

### **Base `fiscalizacion` (726K registros total):**
- `src__fiscalizacion__fiscalizacion__bitacora` - **513,344 registros** ⭐
- `src__fiscalizacion__fiscalizacion__ofeindisdup` - **209,171 registros** ⭐
- `src__fiscalizacion__fiscalizacion__session_access` - **3,208 registros**

### **Base `fiscalizacion_analytics` (vistas procesadas):**
- Mismas tablas que `fiscalizacion` pero con sufijo `_v`
- Son vistas optimizadas para análisis

---

## 🔧 **SI AÚN HAY PROBLEMAS**

### **Para Superset:**
```bash
# Reiniciar solo Superset
docker compose restart superset
```

### **Para Metabase:**
```bash
# Reiniciar solo Metabase  
docker compose restart metabase
```

### **Verificar datos están ahí:**
```bash
# Comando directo en ClickHouse
docker exec clickhouse clickhouse-client --user default --password ClickHouse123! --query "SELECT database, table, sum(rows) FROM system.parts WHERE active=1 GROUP BY database, table ORDER BY sum(rows) DESC"
```

---

## ✅ **RESULTADO ESPERADO**
- **Superset:** Solo bases de datos con datos reales
- **Metabase:** Solo 3 bases de datos limpias  
- **ClickHouse:** 1.4M+ registros organizados
- **Sin esquemas vacíos** que causen confusión

¡Los datos están ahí y listos para usar! 🎯