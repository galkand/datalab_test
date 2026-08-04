SET password_encryption = 'md5';

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'hive') THEN
    CREATE ROLE hive LOGIN;
  END IF;
END $$;

ALTER ROLE hive WITH PASSWORD 'hive';

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'metastore') THEN
    CREATE DATABASE metastore OWNER hive;
  END IF;
END $$;
