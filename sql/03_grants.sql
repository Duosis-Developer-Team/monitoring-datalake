-- ============================================================================
-- Grants for the pipeline's PostgreSQL user
-- Replace <user> with the login configured in the Airflow Postgres connection.
-- Run as a database superuser / owner.
-- ============================================================================

-- Create the user if it does not exist (replace the password placeholder).
-- DO $$
-- BEGIN
--     IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '<user>') THEN
--         CREATE USER <user> WITH PASSWORD '<password>';
--     END IF;
-- END $$;

-- Connect privilege on the database
GRANT CONNECT ON DATABASE <database> TO <user>;

-- Schema usage + ability to create tables (the writer auto-creates tables and
-- temporary UNLOGGED staging tables for the COPY pattern)
GRANT USAGE  ON SCHEMA public TO <user>;
GRANT CREATE ON SCHEMA public TO <user>;

-- DML on existing tables
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO <user>;

-- DML on future tables (so newly auto-created tables are usable without
-- re-granting each time)
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO <user>;

-- Sequences (if any tables use them)
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO <user>;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO <user>;
