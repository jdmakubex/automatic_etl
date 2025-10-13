
CREATE DATABASE IF NOT EXISTS fgeo_analytics;
-- Solo inicialización de bases de datos y tablas, sin tocar usuarios (gestionados por XML)

CREATE TABLE IF NOT EXISTS fgeo_analytics.test_table (
    id UInt32,
    name String
) ENGINE = MergeTree() ORDER BY id;

INSERT INTO fgeo_analytics.test_table (id, name) VALUES (1, 'dato1'), (2, 'dato2'), (3, 'dato3');

