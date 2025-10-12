# Documentación de scripts en `tools/`

Este archivo describe brevemente cada script presente en la carpeta `tools/`, su propósito y desde dónde se recomienda ejecutarlo (servicio Docker, contenedor, shell local, etc).

---

| Script                          | Descripción breve                                                                 | Ejecución recomendada           |
|----------------------------------|----------------------------------------------------------------------------------|---------------------------------|
| orchestrate_etl_init.py          | Orquestador maestro del pipeline ETL. Ejecuta todas las fases y maneja reintentos.| Servicio Docker (`etl-orchestrator`) |
| gen_pipeline.py                  | Genera configuraciones y esquemas para conectores Debezium/Kafka.                 | Docker, como parte del pipeline |
| apply_connectors_auto.py         | Aplica conectores Debezium automáticamente en Kafka Connect.                      | Docker, como parte del pipeline |
| apply_connectors.py              | Aplica conectores manualmente en Kafka Connect.                                   | Docker, manual                  |
| pipeline_status.py               | Valida el estado final del pipeline (Kafka, conectores, ClickHouse).              | Docker o local                  |
| validate_config.py               | Valida la configuración global y variables de entorno.                            | Docker o local                  |
| validate_clickhouse.py           | Valida bases, tablas y datos en ClickHouse.                                       | Docker                          |
| validate_etl_complete.py         | Validación completa del pipeline ETL.                                             | Docker                          |
| validate_etl_simple.py           | Validación simple del pipeline ETL.                                               | Docker                          |
| verify_dependencies.py           | Verifica dependencias y conectividad entre servicios.                             | Docker o local                  |
| health_validator.py              | Valida la salud de los servicios principales.                                     | Docker                          |
| pre_validation.py                | Validaciones previas al pipeline ETL.                                             | Docker                          |
| run_etl_validations.py           | Ejecuta validaciones ETL.                                                         | Docker                          |
| setup_database_users.py          | Crea y configura usuarios en MySQL y ClickHouse.                                  | Docker                          |
| automatic_user_manager.py        | Automatiza gestión de usuarios y permisos.                                        | Docker                          |
| superset_auto_configurator.py    | Configura Superset automáticamente (usuarios, conexiones, dashboards).            | Docker                          |
| provision_superset.py            | Inicializa y provisiona Superset con datasets y dashboards.                       | Docker                          |
| setup_superset_venv.py           | Prepara entorno virtual para Superset.                                            | Docker                          |
| cleanup_all.py                   | Limpia conectores, tópicos y tablas para reiniciar el pipeline.                   | Docker                          |
| generate_test_data.py            | Genera datos de prueba para las tablas destino.                                   | Docker                          |
| fix_clickhouse_config.py         | Corrige configuración de ClickHouse.                                              | Docker                          |
| fix_mysql_config.py              | Corrige configuración de MySQL.                                                   | Docker                          |
| fix_null_immediate.py            | Corrige problemas de valores nulos en datos.                                      | Docker                          |
| patch_null_handling.py           | Aplica parches para manejo de nulos.                                              | Docker                          |
| run_configurator.sh              | Script shell para configuración rápida.                                           | Shell/Docker                    |
| run_tests.sh                     | Ejecuta pruebas automáticas del pipeline.                                         | Shell/Docker                    |
| audit_mysql_clickhouse.py        | Audita la sincronización entre MySQL y ClickHouse.                                | Docker                          |
| multi_database_auditor.py        | Auditoría de múltiples bases de datos.                                            | Docker                          |
| diagnose_clickhouse_sqlalchemy.py| Diagnóstico de conexión SQLAlchemy a ClickHouse.                                  | Docker                          |
| discover_mysql_tables.py         | Descubre automáticamente tablas en MySQL.                                         | Docker                          |
| create_clickhouse_models.py      | Crea modelos y tablas en ClickHouse.                                              | Docker                          |
| init_clickhouse_table.py         | Inicializa tablas específicas en ClickHouse.                                      | Docker                          |
| validators.py                    | Módulo de validaciones reutilizables.                                             | Docker/Local                    |
| requirements.txt                 | Dependencias Python para los scripts.                                             | Docker/Local                    |
| start_pipeline.sh                | Script shell para iniciar el pipeline completo.                                   | Shell/Docker                    |
| sync_kafka_clickhouse.sh         | Sincroniza datos entre Kafka y ClickHouse.                                        | Shell/Docker                    |
| test_permissions.py              | Prueba de permisos en los servicios.                                              | Docker                          |
| ingest_runner.py                 | Ejecuta procesos de ingesta de datos.                                             | Docker                          |
| master_etl_agent.py              | Agente para flujos ETL avanzados.                                                 | Docker                          |
| intelligent_init_agent.py        | Inicialización adaptativa del pipeline.                                           | Docker                          |
| debug_null_values.py             | Diagnóstico de valores nulos en datos.                                            | Docker                          |
| render_from_env.py               | Renderiza configuraciones desde variables de entorno.                             | Docker                          |
| setup_clickhouse_complete.py     | Inicializa y configura ClickHouse completamente.                                  | Docker                          |
| kafka_to_clickhouse.py           | Transfiere datos de Kafka a ClickHouse.                                           | Docker                          |
| __pycache__/                     | Archivos temporales de Python.                                                    | No ejecutar                     |
| Dockerfile.pipeline-gen          | Dockerfile para construir imagen de pipeline-gen.                                 | Docker                          |
| tools/                           | Subcarpeta, puede contener utilidades extra.                                      | -                               |

---

> Cada script incluye en su cabecera una breve descripción y puede consultarse para detalles específicos.
> Para ejecutar desde Docker, usar `docker compose exec <servicio> python3 tools/<script>.py` o el servicio correspondiente.
