# 🔄 DEMOSTRACIÓN: CAMBIO AUTOMÁTICO DE BASES DE DATOS

## CONFIGURACIÓN ACTUAL (archivos + fiscalizacion)
```bash
# Archivo .env actual
SOURCE_URL=mysql+pymysql://juan.marcos:123456@172.21.61.53:3306/archivos
DB_CONNECTIONS=[
  {"name": "archivos", "type": "mysql", "host": "172.21.61.53", "port": 3306, 
   "user": "juan.marcos", "pass": "123456", "db": "archivos"},
  {"name": "fiscalizacion", "type": "mysql", "host": "172.21.61.53", "port": 3306, 
   "user": "juan.marcos", "pass": "123456", "db": "fiscalizacion"},
  {"name": "information_schema", "type": "mysql", "host": "172.21.61.53", "port": 3306, 
   "user": "juan.marcos", "pass": "123456", "db": "information_schema"}
]
```

## NUEVA CONFIGURACIÓN (ventas + inventario + clientes)
```bash
# Simplemente cambia el .env
SOURCE_URL=mysql+pymysql://user:pass@nuevo-server:3306/ventas
DB_CONNECTIONS=[
  {"name": "ventas", "type": "mysql", "host": "nuevo-server", "port": 3306, 
   "user": "user", "pass": "pass", "db": "ventas"},
  {"name": "inventario", "type": "mysql", "host": "nuevo-server", "port": 3306, 
   "user": "user", "pass": "pass", "db": "inventario"},
  {"name": "clientes", "type": "mysql", "host": "otro-server", "port": 3306, 
   "user": "admin", "pass": "secret", "db": "clientes"},
  {"name": "contabilidad", "type": "mysql", "host": "contab-server", "port": 3306, 
   "user": "contador", "pass": "12345", "db": "contabilidad"}
]
```

## COMANDOS IDÉNTICOS - BASES DIFERENTES
```bash
# Validar NUEVAS conexiones (sin modificar código)
python3 /app/tools/ingest_runner.py --validate-connections

# Ingestar NUEVA base ventas
python3 /app/tools/ingest_runner.py --source-name ventas --truncate-before-load

# Ingestar NUEVA base inventario  
python3 /app/tools/ingest_runner.py --source-name inventario --truncate-before-load

# Auditoría de NUEVAS bases
python3 /app/tools/ingest_runner.py --audit-only
```

## FUNCIONES AUTOMÁTICAS
✅ **Detección automática de tablas** (SHOW TABLES)
✅ **Análisis automático de esquemas** (DESCRIBE TABLE) 
✅ **Resolución automática de tipos** (INT, VARCHAR, DATETIME, etc.)
✅ **Detección automática de Primary Keys**
✅ **Mapeo automático de nombres** ClickHouse
✅ **Auditoría cross-platform** automática

## EJEMPLO CON BASE E-COMMERCE
```bash
# Nueva empresa e-commerce
SOURCE_URL=mysql+pymysql://ecom:pass@ecommerce-db:3306/productos
DB_CONNECTIONS=[
  {"name": "productos", "type": "mysql", "host": "ecommerce-db", "db": "productos"},
  {"name": "ordenes", "type": "mysql", "host": "ecommerce-db", "db": "ordenes"},
  {"name": "usuarios", "type": "mysql", "host": "user-db", "db": "usuarios"},
  {"name": "pagos", "type": "mysql", "host": "payment-db", "db": "pagos"}
]

# ¡MISMO PIPELINE, CERO MODIFICACIONES DE CÓDIGO!
python3 ingest_runner.py --source-name productos
python3 ingest_runner.py --source-name ordenes  
python3 ingest_runner.py --source-name usuarios
python3 ingest_runner.py --audit-only  # Audita TODO automáticamente
```

## CAPACIDADES AUTOMÁTICAS POR ESQUEMA

### 📊 RETAIL/VENTAS
- Tablas: productos, ventas, clientes, inventario, categorias
- Tipos: precios (DECIMAL), fechas (DATE/DATETIME), textos (VARCHAR), IDs (INT)
- ✅ **Todo manejado automáticamente**

### 🏥 SALUD/HOSPITALES  
- Tablas: pacientes, consultas, medicamentos, doctores, historiales
- Tipos: fechas_nacimiento (DATE), diagnosticos (TEXT), montos (DECIMAL)
- ✅ **Todo manejado automáticamente**

### 🏫 EDUCACIÓN
- Tablas: estudiantes, cursos, calificaciones, profesores, materias
- Tipos: notas (DECIMAL), fechas_inscripcion (DATE), nombres (VARCHAR)
- ✅ **Todo manejado automáticamente**

### 🏭 MANUFACTURA
- Tablas: productos, ordenes_trabajo, inventario, empleados, maquinas
- Tipos: cantidades (INT), costos (DECIMAL), timestamps (DATETIME)
- ✅ **Todo manejado automáticamente**