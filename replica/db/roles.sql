-- The roles the endpoint files (endpoints/*.sql) run as, in a plain Postgres:
-- no HAF role groups to join, unlike db/builtin_roles.sql.
DO $$
BEGIN
  CREATE ROLE proxy_whitelist_owner WITH NOLOGIN;
EXCEPTION WHEN duplicate_object THEN RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
END
$$;

DO $$
BEGIN
  CREATE ROLE proxy_whitelist_user WITH LOGIN;
EXCEPTION WHEN duplicate_object THEN RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
END
$$;

GRANT proxy_whitelist_user TO proxy_whitelist_owner;

-- endpoints/endpoint_schema.sql creates its schema as proxy_whitelist_owner.
DO $$
BEGIN
  EXECUTE format('GRANT CREATE ON DATABASE %I TO proxy_whitelist_owner', current_database());
END
$$;
