# 🚀 Guía de Inicio Rápido: Agente de Ciclo Automático ETL

## Instalación

No se requiere instalación adicional. El agente está listo para usar.

## Pre-requisitos

1. **Python 3.8+** instalado
2. **Git** configurado
3. **Docker y Docker Compose** (opcional, pero recomendado)
4. Repositorio clonado localmente

## Uso Básico

### 1. Ejecución Simple

```bash
# Navegar al directorio del proyecto
cd /ruta/a/automatic_etl

# Ejecutar el agente
python tools/auto_etl_cycle_agent.py
```

**Salida esperada:**
```
🤖 === AGENTE DE CICLO AUTOMÁTICO ETL ACTIVADO ===
🆔 ID de Ejecución: 20250109_123456
🔄 Máximo de iteraciones: 5
📁 Directorio de trabajo: /ruta/a/automatic_etl

================================================================================
🔄 === INICIANDO CICLO 1/5 ===
================================================================================

📝 === PASO 1: COMMIT Y PUSH (Iteración 1) ===
...
```

### 2. Con Opciones

```bash
# Aumentar iteraciones máximas
python tools/auto_etl_cycle_agent.py --max-iterations 10

# Modo debug (logs detallados)
python tools/auto_etl_cycle_agent.py --debug

# Combinación
python tools/auto_etl_cycle_agent.py --max-iterations 10 --debug
```

## Verificar Resultados

### 1. Ver Logs en Tiempo Real

```bash
# En otra terminal, seguir el log principal
tail -f logs/auto_cycle_*.log
```

### 2. Revisar Reporte Final

```bash
# Ver último reporte generado
cat logs/auto_cycle_report_*.json | jq .

# O usar Python para formato legible
python3 -c "
import json
import glob
files = glob.glob('logs/auto_cycle_report_*.json')
if files:
    with open(max(files), 'r') as f:
        data = json.load(f)
        print(f\"Ejecución: {data['execution_id']}\")
        print(f\"Éxito: {data['overall_success']}\")
        print(f\"Iteraciones: {data['iterations_executed']}\")
"
```

### 3. Verificar Estado del Pipeline

```bash
# Si el agente fue exitoso, verificar servicios
python tools/pipeline_status.py

# Verificar ClickHouse
python tools/validate_clickhouse.py

# Verificar Superset
python tools/validate_superset.py
```

## Interpretación de Resultados

### ✅ Éxito Completo

```
📊 REPORTE FINAL - AGENTE DE CICLO AUTOMÁTICO ETL
================================================================================
🆔 Ejecución: 20250109_123456
⏰ Duración total: 634.5s (10.6 min)
🔄 Iteraciones ejecutadas: 2

🎉 RESULTADO: ÉXITO COMPLETO
✅ Pipeline ETL completamente operativo
🏆 Éxito alcanzado en iteración: 2

📋 RESUMEN POR ITERACIÓN:
   ❌ Iteración 1: failed - 245.3s - 2 errores - 1 ajustes
   ✅ Iteración 2: success - 389.2s - 0 errores - 0 ajustes
```

**Acción:** ✅ Sistema listo para uso

### ⚠️ Éxito Parcial

```
📊 REPORTE FINAL - AGENTE DE CICLO AUTOMÁTICO ETL
================================================================================
❌ RESULTADO: NO SE ALCANZÓ ÉXITO
⚠️  Se ejecutaron 5 iteraciones sin éxito

🔍 ÚLTIMOS ERRORES DETECTADOS:
   1. ERROR: Conexión a base de datos falló
   2. Timeout waiting for service
```

**Acción:** 
1. Revisar logs detallados: `logs/auto_cycle_*.log`
2. Verificar servicios: `docker-compose ps`
3. Corregir problemas y re-ejecutar

## Casos de Uso Comunes

### Caso 1: Primera Inicialización

```bash
# Inicializar entorno desde cero
python tools/auto_etl_cycle_agent.py --max-iterations 5
```

**Qué hace:**
1. Limpia todo el estado previo
2. Ejecuta orquestador completo
3. Configura usuarios y permisos
4. Crea tablas y esquemas
5. Valida pipeline completo

### Caso 2: Después de Cambios

```bash
# Validar cambios en código
python tools/auto_etl_cycle_agent.py --max-iterations 3
```

**Qué hace:**
1. Commitea cambios actuales
2. Re-despliega pipeline
3. Valida que todo funciona

### Caso 3: Recuperación de Errores

```bash
# Recuperar sistema después de fallo
python tools/auto_etl_cycle_agent.py --max-iterations 10 --debug
```

**Qué hace:**
1. Intenta múltiples veces
2. Registra logs detallados
3. Aplica ajustes automáticos

## Monitoreo Durante Ejecución

### Ver Progreso

```bash
# Terminal 1: Ejecutar agente
python tools/auto_etl_cycle_agent.py

# Terminal 2: Monitorear logs
tail -f logs/auto_cycle_*.log

# Terminal 3: Ver recursos
watch -n 5 'docker stats --no-stream'
```

### Detener Ejecución

Si necesitas detener el agente:

```bash
# Presionar Ctrl+C en la terminal del agente
# El agente guardará el estado actual antes de salir
```

## Troubleshooting Rápido

### Problema: "No se encontró script orquestador"

**Solución:**
```bash
# Verificar que los scripts existen
ls -la tools/orchestrate_etl_init.py
ls -la tools/master_etl_agent.py

# Si no existen, verificar instalación
git status
```

### Problema: "Permission denied en git"

**Solución:**
```bash
# Configurar git
git config user.name "Tu Nombre"
git config user.email "tu@email.com"

# O ejecutar sin git push
# (ya está deshabilitado por defecto en CI/CD)
```

### Problema: "Timeouts constantes"

**Solución:**
```bash
# Aumentar iteraciones para dar más tiempo
python tools/auto_etl_cycle_agent.py --max-iterations 10

# Verificar recursos del sistema
docker stats
free -h
```

### Problema: "Limpieza no funciona"

**Solución:**
```bash
# Limpieza manual antes de ejecutar agente
docker-compose down -v
rm -rf generated/default/*
rm -rf logs/*

# Luego ejecutar agente
python tools/auto_etl_cycle_agent.py
```

## Integración con CI/CD

### GitHub Actions (ejemplo mínimo)

Crear `.github/workflows/auto-etl-cycle.yml`:

```yaml
name: Auto ETL Cycle

on:
  push:
    branches: [ main ]

jobs:
  auto-cycle:
    runs-on: ubuntu-latest
    timeout-minutes: 60
    
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: pip install -r requirements.txt
      
      - name: Run Auto ETL Cycle
        run: python tools/auto_etl_cycle_agent.py --max-iterations 5
      
      - name: Upload logs
        if: always()
        uses: actions/upload-artifact@v3
        with:
          name: etl-logs
          path: logs/
```

## Siguientes Pasos

Después de una ejecución exitosa:

1. **Verificar Servicios**
   ```bash
   python tools/pipeline_status.py
   ```

2. **Acceder a Superset**
   - URL: http://localhost:8088
   - Usuario: admin
   - Password: Admin123!

3. **Verificar Datos**
   ```bash
   docker exec -it clickhouse clickhouse-client
   SELECT * FROM fgeo_analytics.<tu_tabla> LIMIT 10;
   ```

4. **Configurar Monitoreo**
   - Configurar alertas basadas en logs
   - Programar ejecuciones periódicas
   - Revisar reportes JSON históricos

## Recursos Adicionales

- **Documentación Completa:** [docs/AUTO_ETL_CYCLE_AGENT.md](./AUTO_ETL_CYCLE_AGENT.md)
- **Variables de Entorno:** [docs/ENVIRONMENT_VARIABLES.md](./ENVIRONMENT_VARIABLES.md)
- **Guía de Tests:** [docs/TESTING_GUIDE.md](./TESTING_GUIDE.md)
- **Recuperación de Errores:** [docs/ERROR_RECOVERY.md](./ERROR_RECOVERY.md)

## Ayuda

Para ayuda sobre opciones:

```bash
python tools/auto_etl_cycle_agent.py --help
```

Para reportar issues:
- GitHub Issues: https://github.com/jdmakubex/automatic_etl/issues
- Incluir logs relevantes de `logs/`
- Incluir reporte JSON de la ejecución
