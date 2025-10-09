-- Script robusto para crear/recrear usuarios en ClickHouse
-- Se ejecuta con permisos de administrador

-- Eliminar usuarios existentes para recrearlos (forzar recreación)
DROP USER IF EXISTS etl;
DROP USER IF EXISTS superset;

-- Crear usuarios ETL y Superset con permisos apropiados
CREATE USER etl IDENTIFIED BY 'Et1Ingest!';
CREATE USER superset IDENTIFIED BY 'Sup3rS3cret!';

-- Crear base de datos si no existe
CREATE DATABASE IF NOT EXISTS fgeo_analytics;

-- Otorgar permisos al usuario ETL (completos para ingesta)
GRANT SELECT, INSERT, CREATE, ALTER, DROP ON fgeo_analytics.* TO etl WITH GRANT OPTION;

-- Otorgar permisos al usuario Superset (solo lectura para visualización)
GRANT SELECT ON fgeo_analytics.* TO superset;
GRANT SELECT ON system.* TO superset;

-- Verificar que los usuarios fueron creados
SELECT 'Usuario etl creado correctamente' as status WHERE EXISTS(SELECT 1 FROM system.users WHERE name = 'etl');
SELECT 'Usuario superset creado correctamente' as status WHERE EXISTS(SELECT 1 FROM system.users WHERE name = 'superset');