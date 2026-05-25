-- =====================================================================
-- Crea la BD que Metabase usa para sus metadatos (dashboards, users, etc.)
-- DEBE ejecutarse ANTES de init_schema.sql, por eso el prefijo "00_".
-- Docker Postgres ejecuta los .sql de /docker-entrypoint-initdb.d/ en
-- orden alfabético.
-- =====================================================================
CREATE DATABASE metabase;
