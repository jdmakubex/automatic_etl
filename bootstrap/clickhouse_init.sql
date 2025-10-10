-- Inicialización robusta de ClickHouse: base, usuarios, permisos, tabla y datos
CREATE DATABASE IF NOT EXISTS fgeo_analytics;

-- Eliminar usuarios para recreación idempotente
DROP USER IF EXISTS etl;
DROP USER IF EXISTS superset;

-- Crear usuarios
CREATE USER etl IDENTIFIED BY 'Et1Ingest!';
CREATE USER superset IDENTIFIED BY 'Sup3rS3cret!';

-- Asignar permisos
GRANT SELECT, INSERT, CREATE, ALTER, DROP ON fgeo_analytics.* TO etl WITH GRANT OPTION;
GRANT SELECT ON fgeo_analytics.* TO superset;
GRANT SELECT ON system.* TO superset;

-- Crear tabla de prueba
CREATE TABLE IF NOT EXISTS fgeo_analytics.test_table (
    id UInt32,
    name String
) ENGINE = MergeTree() ORDER BY id;

-- Insertar datos de prueba
INSERT INTO fgeo_analytics.test_table (id, name) VALUES (1, 'dato1'), (2, 'dato2'), (3, 'dato3');

