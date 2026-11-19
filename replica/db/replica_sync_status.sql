SET ROLE proxy_whitelist_owner;

/*
 * sync_status() of a replica: the primary's fields (endpoints/sync_status.sql)
 * with the values of the source the replica has fully replicated, plus the
 * replica's own state. last_block_num/last_block_time are the source's head
 * as of the last poll whose every stream was applied, so they stop advancing
 * when the source stalls or is unreachable, the syncer dies or the source
 * identity no longer matches: a health check that judges the primary by the
 * age of last_block_time judges a replica the same way. Both are null until
 * the first full catch-up (bootstrap_complete). sync_uuid is the replica's own
 * identity (re-minted when it truncates and bootstraps again), not the
 * source's: the replica serves no streams under the source's identity.
 */
CREATE OR REPLACE FUNCTION proxy_whitelist_replica.sync_status()
RETURNS JSON
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
  PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=0"}]', true);

  RETURN (
    SELECT json_build_object(
      'last_block_num', s.source_block_num,
      'last_block_time', to_char(s.source_block_time AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS'),
      'sync_uuid', s.replica_sync_uuid,
      'stream_schema_version', s.stream_schema_version,
      'stream_sentinel_block', s.stream_sentinel_block,
      'source_sync_uuid', s.source_sync_uuid,
      'source_url', s.source_url,
      'polled_at', to_char(s.polled_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS'),
      'bootstrap_complete', s.bootstrap_complete,
      'sync_error', s.sync_error
    )
    FROM proxy_whitelist_replica.state s
  );
END
$$;

-- GET /sync-status of the replica's endpoint schema.
CREATE OR REPLACE FUNCTION proxy_whitelist_endpoints.sync_status()
RETURNS JSON
LANGUAGE sql
STABLE
AS $$
  SELECT proxy_whitelist_replica.sync_status();
$$;

GRANT USAGE ON SCHEMA proxy_whitelist_replica TO proxy_whitelist_user;
GRANT SELECT ON proxy_whitelist_replica.state TO proxy_whitelist_user;
GRANT EXECUTE ON FUNCTION proxy_whitelist_replica.sync_status() TO proxy_whitelist_user;
GRANT EXECUTE ON FUNCTION proxy_whitelist_endpoints.sync_status() TO proxy_whitelist_user;

RESET ROLE;
