# 🤖 Agente ETL: Ciclo Automático de Validación, Ajuste y Ejecución

## Descripción

El **Agente de Ciclo Automático ETL** es un sistema completamente autónomo que ejecuta ciclos de validación, ajuste y ejecución del pipeline ETL hasta lograr una ejecución exitosa sin errores.

**NO REQUIERE INTERACCIÓN NI AUTORIZACIÓN DEL USUARIO.**

## Características Principales

### ✅ Automatización Completa

El agente ejecuta los siguientes pasos en cada ciclo:

1. **Commit y Push del Estado Actual**
   - Detecta cambios en el repositorio
   - Crea commits automáticos con contexto
   - Registra el estado antes de cada ciclo

2. **Limpieza Completa**
   - Elimina contenedores y datos previos
   - Limpia archivos generados
   - Resetea el estado del sistema

3. **Ejecución del Orquestador**
   - Construye infraestructura completa
   - Copia scripts necesarios
   - Crea usuarios y permisos
   - Lee y crea tablas
   - Ingiere datos

4. **Verificación Automática**
   - Analiza resultados en consola
   - Revisa logs de ejecución
   - Detecta errores y problemas
   - Valida servicios adicionales

5. **Ajustes Automáticos**
   - Identifica patrones de error
   - Aplica correcciones automáticas
   - Prepara siguiente iteración

6. **Iteración hasta Éxito**
   - Repite el ciclo si hay errores
   - Aplica backoff exponencial
   - Continúa hasta máximo de iteraciones

### 🔄 Ciclo de Vida

```
┌─────────────────────────────────────────────────┐
│  INICIO: Agente de Ciclo Automático ETL        │
└──────────────────┬──────────────────────────────┘
                   │
                   ▼
        ┌──────────────────────┐
        │  1. COMMIT & PUSH    │◄─────────┐
        └──────────┬───────────┘          │
                   │                       │
                   ▼                       │
        ┌──────────────────────┐          │
        │  2. LIMPIEZA         │          │
        │     COMPLETA         │          │
        └──────────┬───────────┘          │
                   │                       │
                   ▼                       │
        ┌──────────────────────┐          │
        │  3. EJECUTAR         │          │
        │     ORQUESTADOR      │          │
        └──────────┬───────────┘          │
                   │                       │
                   ▼                       │
        ┌──────────────────────┐          │
        │  4. VALIDAR          │          │
        │     RESULTADOS       │          │
        └──────────┬───────────┘          │
                   │                       │
                   ▼                       │
             ¿Éxito?                      │
            /        \                    │
           /          \                   │
         SÍ           NO                  │
         │            │                   │
         │            ▼                   │
         │   ┌──────────────────┐        │
         │   │  5. AJUSTES      │        │
         │   │     AUTOMÁTICOS  │        │
         │   └────────┬─────────┘        │
         │            │                   │
         │            ▼                   │
         │   ¿Hay más iteraciones?       │
         │        /        \              │
         │      SÍ         NO             │
         │       │          │             │
         │       └──────────┤             │
         │                  │             │
         ▼                  ▼             │
    ┌────────┐        ┌─────────┐        │
    │ ÉXITO  │        │ FALLÓ   │        │
    └────────┘        └─────────┘        │
                                          │
         └────────────────────────────────┘
              (Siguiente iteración)
```

## Uso

### Ejecución Básica

```bash
python tools/auto_etl_cycle_agent.py
```

### Opciones Avanzadas

```bash
# Especificar número máximo de iteraciones
python tools/auto_etl_cycle_agent.py --max-iterations 10

# Activar modo debug con logs detallados
python tools/auto_etl_cycle_agent.py --debug

# Combinación de opciones
python tools/auto_etl_cycle_agent.py --max-iterations 10 --debug
```

### Parámetros

| Parámetro | Default | Descripción |
|-----------|---------|-------------|
| `--max-iterations` | 5 | Número máximo de ciclos a ejecutar |
| `--debug` | False | Activar logs detallados y trazas |

## Salidas y Logs

### Archivos Generados

1. **Log Principal**
   - `logs/auto_cycle_YYYYMMDD_HHMMSS.log`
   - Contiene todos los eventos del agente

2. **Logs de Orquestador**
   - `logs/orchestrator_iteration_N_YYYYMMDD_HHMMSS.log`
   - Output completo de cada iteración del orquestador

3. **Reporte JSON**
   - `logs/auto_cycle_report_YYYYMMDD_HHMMSS.json`
   - Resumen estructurado de la ejecución completa

### Formato del Reporte JSON

```json
{
  "execution_id": "20250109_123456",
  "start_time": "2025-01-09T12:34:56",
  "end_time": "2025-01-09T12:45:30",
  "total_duration_seconds": 634.5,
  "max_iterations": 5,
  "iterations_executed": 3,
  "overall_success": true,
  "cycles": [
    {
      "iteration": 1,
      "status": "failed",
      "success": false,
      "duration_seconds": 245.3,
      "errors_count": 2,
      "errors": ["Error 1", "Error 2"],
      "adjustments_count": 1,
      "adjustments": ["Ajuste aplicado"]
    },
    {
      "iteration": 2,
      "status": "success",
      "success": true,
      "duration_seconds": 389.2,
      "errors_count": 0,
      "errors": [],
      "adjustments_count": 0,
      "adjustments": []
    }
  ]
}
```

## Detección y Ajuste de Errores

### Errores Detectados Automáticamente

El agente detecta y analiza los siguientes patrones de error:

- ✅ **Timeouts**: Conexiones o procesos que exceden tiempo límite
- ✅ **Conexiones Rechazadas**: Servicios no disponibles
- ✅ **Permisos Denegados**: Problemas de autorización
- ✅ **Archivos No Encontrados**: Paths incorrectos
- ✅ **Tablas No Existen**: Problemas de esquema
- ✅ **Recursos Ya Existen**: Conflictos de estado
- ✅ **Conectores Duplicados**: Problemas de configuración

### Ajustes Automáticos

Para cada tipo de error, el agente puede aplicar:

| Error | Ajuste Automático |
|-------|-------------------|
| Timeout | Incrementar tiempo de espera en siguiente iteración |
| Connection Refused | Esperar más tiempo para que servicios se inicialicen |
| Permission Denied | Registrar para revisión de configuración de permisos |
| File Not Found | Registrar para verificación de paths |
| No Such Table | Re-crear esquema en siguiente iteración |
| Already Exists | Limpieza más profunda en siguiente ciclo |

## Validaciones Ejecutadas

### Validación Principal

1. **Análisis de Output del Orquestador**
   - Cuenta indicadores de éxito (✅, SUCCESS, COMPLETADA)
   - Cuenta indicadores de error (❌, FAILED, ERROR)
   - Compara balance de éxito vs errores

2. **Código de Retorno**
   - Verifica que el orquestador terminó sin errores

### Validaciones Adicionales

3. **Archivos de Log**
   - Verifica que se crearon logs de ejecución
   - Confirma que el sistema registró actividad

4. **Archivos Generados**
   - Verifica que se generaron configuraciones
   - Confirma que el pipeline produjo salidas

5. **Validador ETL**
   - Ejecuta `validate_etl_simple.py` si existe
   - Verifica flujo de datos

## Integración con CI/CD

### GitHub Actions

```yaml
name: Auto ETL Cycle

on:
  push:
    branches: [ main ]
  workflow_dispatch:

jobs:
  auto-etl-cycle:
    runs-on: ubuntu-latest
    
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
      
      - name: Run Auto ETL Cycle Agent
        run: |
          python tools/auto_etl_cycle_agent.py --max-iterations 5
        timeout-minutes: 60
      
      - name: Upload logs
        if: always()
        uses: actions/upload-artifact@v3
        with:
          name: etl-cycle-logs
          path: logs/auto_cycle_*.log
      
      - name: Upload report
        if: always()
        uses: actions/upload-artifact@v3
        with:
          name: etl-cycle-report
          path: logs/auto_cycle_report_*.json
```

### GitLab CI

```yaml
auto_etl_cycle:
  stage: deploy
  image: python:3.11
  
  script:
    - pip install -r requirements.txt
    - python tools/auto_etl_cycle_agent.py --max-iterations 5
  
  artifacts:
    when: always
    paths:
      - logs/auto_cycle_*.log
      - logs/auto_cycle_report_*.json
    expire_in: 1 week
  
  timeout: 1h
  
  only:
    - main
```

## Casos de Uso

### 1. Desarrollo Local

Validar cambios antes de commit:

```bash
# Ejecutar ciclo de validación
python tools/auto_etl_cycle_agent.py

# Si es exitoso, los cambios son seguros
git push
```

### 2. CI/CD Automatizado

Ejecutar como parte del pipeline de despliegue:

```bash
# En pipeline de CI/CD
python tools/auto_etl_cycle_agent.py --max-iterations 3
```

### 3. Recuperación Automática

Recuperar sistema después de fallas:

```bash
# Ejecutar con más iteraciones para problemas complejos
python tools/auto_etl_cycle_agent.py --max-iterations 10 --debug
```

### 4. Inicialización de Entorno

Configurar entorno nuevo desde cero:

```bash
# Primera ejecución en nuevo entorno
python tools/auto_etl_cycle_agent.py
```

## Troubleshooting

### El agente no alcanza éxito después de todas las iteraciones

**Causas comunes:**
- Configuración incorrecta de variables de entorno
- Servicios Docker no disponibles
- Problemas de red o conectividad
- Recursos insuficientes (memoria, CPU)

**Solución:**
1. Revisar logs detallados: `logs/auto_cycle_*.log`
2. Verificar último reporte JSON para errores específicos
3. Ejecutar validaciones manuales: `python tools/validators.py`
4. Verificar servicios: `docker-compose ps`

### Timeouts frecuentes

**Causas:**
- Servicios lentos en iniciar
- Recursos de sistema limitados

**Solución:**
```bash
# Ejecutar con más iteraciones para dar más tiempo
python tools/auto_etl_cycle_agent.py --max-iterations 10
```

### Errores de permisos

**Causas:**
- Usuario sin permisos adecuados
- Configuración incorrecta de credenciales

**Solución:**
1. Verificar `.env` con credenciales correctas
2. Ejecutar `python tools/test_permissions.py` manualmente
3. Revisar configuración de usuarios en MySQL/ClickHouse

### Limpieza no completa

**Causas:**
- Servicios no respondiendo
- Recursos bloqueados

**Solución:**
```bash
# Limpieza manual antes de ejecutar agente
python tools/cleanup_all.py
docker-compose down -v
docker-compose up -d
```

## Mejores Prácticas

### 1. Primera Ejecución

Siempre ejecutar con máximo de iteraciones moderado:

```bash
python tools/auto_etl_cycle_agent.py --max-iterations 5
```

### 2. Debugging

Activar modo debug solo cuando sea necesario:

```bash
python tools/auto_etl_cycle_agent.py --debug
```

### 3. Monitoreo

Revisar logs periódicamente:

```bash
# Ver últimos logs
tail -f logs/auto_cycle_*.log

# Analizar reporte JSON
cat logs/auto_cycle_report_*.json | jq .
```

### 4. Backup

Guardar reportes exitosos como referencia:

```bash
# Copiar reporte exitoso
cp logs/auto_cycle_report_*.json backups/success_baseline.json
```

## Limitaciones Conocidas

1. **Git Push Deshabilitado**: Por seguridad, el push automático está deshabilitado en entornos CI/CD
2. **Máximo de Iteraciones**: Limitado a evitar loops infinitos
3. **Ajustes Automáticos**: Limitados a patrones conocidos de error
4. **Dependencia de Servicios**: Requiere Docker y servicios configurados

## Comparación con Otros Agentes

| Característica | auto_etl_cycle_agent | master_etl_agent | orchestrate_etl_init |
|----------------|---------------------|------------------|---------------------|
| Ciclos Iterativos | ✅ | ❌ | ❌ |
| Ajustes Automáticos | ✅ | ❌ | ❌ |
| Limpieza Automática | ✅ | ❌ | ❌ |
| Commits Automáticos | ✅ | ❌ | ❌ |
| Validación Integrada | ✅ | ✅ | ✅ |
| Sin Intervención Usuario | ✅ | ✅ | ✅ |
| Reportes JSON | ✅ | ✅ | ✅ |

## Referencias

- [Documentación de Validaciones](./TESTING_GUIDE.md)
- [Variables de Entorno](./ENVIRONMENT_VARIABLES.md)
- [Guía de Dependencias](./DEPENDENCIES.md)
- [Recuperación de Errores](./ERROR_RECOVERY.md)

## Contribuir

Para mejorar el agente:

1. Agregar nuevos patrones de error en `_analyze_and_adjust_error()`
2. Implementar nuevos ajustes automáticos
3. Mejorar validaciones en `_run_additional_validations()`
4. Documentar casos de uso específicos

## Changelog

### v1.0.0 (2025-01-09)

- ✅ Implementación inicial
- ✅ Ciclos iterativos de validación y ajuste
- ✅ Detección automática de errores
- ✅ Ajustes automáticos básicos
- ✅ Commits automáticos de estado
- ✅ Limpieza completa integrada
- ✅ Validaciones múltiples
- ✅ Reportes JSON estructurados
- ✅ Integración con CI/CD
- ✅ Documentación completa
