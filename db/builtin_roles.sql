DO $$
BEGIN
CREATE ROLE proxy_whitelist_owner WITH LOGIN INHERIT IN ROLE hive_applications_owner_group, hivemind;
EXCEPTION WHEN duplicate_object THEN RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
END
$$;

DO $$
BEGIN
  CREATE ROLE proxy_whitelist_user WITH LOGIN INHERIT IN ROLE hive_applications_group, hivemind;
EXCEPTION WHEN duplicate_object THEN RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
END
$$;

--- Allow to create schemas
GRANT proxy_whitelist_owner TO haf_admin;
GRANT proxy_whitelist_user TO proxy_whitelist_owner;
