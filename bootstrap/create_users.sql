-- Crear usuarios ETL y Superset con permisos apropiados
CREATE USER IF NOT EXISTS etl IDENTIFIED BY 'Et1Ingest!';
CREATE USER IF NOT EXISTS superset IDENTIFIED BY 'Sup3rS3cret!';

-- Otorgar permisos al usuario ETL (completos)
GRANT ALL ON *.* TO etl WITH GRANT OPTION;

-- Otorgar permisos al usuario Superset (solo lectura)
GRANT SELECT ON *.* TO superset;

-- Crear base de datos si no existe
CREATE DATABASE IF NOT EXISTS fgeo_analytics;

-- Otorgar permisos específicos en la base de datos
GRANT ALL ON fgeo_analytics.* TO etl WITH GRANT OPTION;
GRANT SELECT ON fgeo_analytics.* TO superset;