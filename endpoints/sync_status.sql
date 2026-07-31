SET ROLE proxy_whitelist_owner;

/*
 * sync_status() — the last processed block as {last_block_num, last_block_time},
 * for GET /sync-status (the HAF-wide uniform health/freshness API). The
 * timestamp lets consumers compute staleness with a single call
 * (age = now() - last_block_time) instead of needing a second head-block
 * reference.
 *
 * The HAF context name equals the install schema (db/database_schema.sql
 * creates the context from SHOW SEARCH_PATH), so it is baked in here via the
 * same SHOW SEARCH_PATH + format() ceremony. LEFT JOIN so the pre-sync case
 * (no processed block yet) still yields an object with a null time rather
 * than an error.
 */
DO $$
DECLARE
  __schema_name VARCHAR;
BEGIN
  SHOW SEARCH_PATH INTO __schema_name;
  EXECUTE format(
  $BODY$
    CREATE OR REPLACE FUNCTION proxy_whitelist_endpoints.sync_status()
    RETURNS JSON
    LANGUAGE 'plpgsql' STABLE
    AS
    $pb$
    BEGIN
      -- No cache - sync status needs real-time accuracy
      PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=0"}]', true);

      -- Fail fast during HAF massive sync: hafd.blocks' PK is dropped for the
      -- duration (hive.disable_indexes_of_irreversible), so the join below
      -- would seq-scan the largest table in the database. Health-check agents
      -- gate on is_instance_ready() before calling APIs; this guard protects
      -- any caller that does not (e.g. a raw haproxy httpchk) by erroring in
      -- milliseconds instead of stalling.
      IF NOT hive.is_instance_ready() THEN
        RAISE EXCEPTION 'HAF instance is not ready (massive sync in progress)'
          USING ERRCODE = '55000';
      END IF;

      RETURN (
        SELECT json_build_object(
          'last_block_num', c.current_block_num,
          'last_block_time', to_char(b.created_at, 'YYYY-MM-DD"T"HH24:MI:SS')
        )
        FROM hafd.contexts c
        LEFT JOIN hafd.blocks b ON b.num = c.current_block_num
        WHERE c.name = '%s'
      );
    END
    $pb$;
  $BODY$, __schema_name);
END
$$;

GRANT EXECUTE ON FUNCTION proxy_whitelist_endpoints.sync_status()
    TO proxy_whitelist_user;

RESET ROLE;
