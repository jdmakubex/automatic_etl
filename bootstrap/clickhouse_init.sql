CREATE DATABASE IF NOT EXISTS fgeo_analytics;

CREATE USER IF NOT EXISTS superset IDENTIFIED BY 'Sup3rS3cret!';
CREATE USER IF NOT EXISTS etl IDENTIFIED BY 'Et1Ingest!';

GRANT SELECT ON fgeo_analytics.* TO superset;
GRANT SELECT, INSERT, CREATE, ALTER, DROP ON fgeo_analytics.* TO etl;

-- Crear tabla de prueba automática
CREATE TABLE IF NOT EXISTS fgeo_analytics.test_table (
	id UInt32,
	name String
) ENGINE = MergeTree() ORDER BY id;

