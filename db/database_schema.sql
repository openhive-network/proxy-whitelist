SET ROLE proxy_whitelist_owner;

DO $BODY$
DECLARE
  __schema_name VARCHAR;
  __parallel_workers INT := current_setting('PG_TEMP.PARALLEL_WORKERS', TRUE)::INT;
  synchronization_stages hive.application_stages;
BEGIN
  SHOW SEARCH_PATH INTO __schema_name;

  ASSERT __parallel_workers IS NOT NULL, 'No parallel_workers';
  ASSERT __parallel_workers > 0, 'Parallel workers less than 0';

  synchronization_stages := ARRAY[
    ('MASSIVE_PROCESSING', 11, 100000, '30 seconds'),
    hive.live_stage()
  ]::hive.application_stages;

  RAISE NOTICE 'proxy_whitelist will be installed in schema % with context %', __schema_name, __schema_name;

  CREATE TABLE IF NOT EXISTS proxy_whitelist_app.app_status (
    id SERIAL PRIMARY KEY,
    continue_processing BOOLEAN NOT NULL DEFAULT TRUE,
    parallel_workers INT NOT NULL DEFAULT 1,
    redis_url TEXT NOT NULL DEFAULT 'redis://localhost:6379',
    redis_password TEXT,
    start_block INT NOT NULL DEFAULT 1,
    poll_profile_images_every_n_batches INT NOT NULL DEFAULT 20,
    advisory_lock_namespace_begin INT NOT NULL DEFAULT 20000
  );

  IF NOT hive.app_context_exists(__schema_name) THEN
    PERFORM hive.app_create_context(
      _name => __schema_name,
      _schema => __schema_name,
      _is_forking => False,
      _stages => synchronization_stages
    );
  END IF;

  CREATE TABLE IF NOT EXISTS proxy_whitelist_app.version (
    schema_hash TEXT,
    runtime_hash TEXT
  );

  CREATE SEQUENCE IF NOT EXISTS proxy_whitelist_app.batch_seq START 1;

  CREATE TABLE IF NOT EXISTS proxy_whitelist_app.block_tasks (
    task_id     SERIAL PRIMARY KEY,
    batch_id    BIGINT   NOT NULL,
    shard       INT,
    first_block INT      NOT NULL,
    last_block  INT      NOT NULL,
    post_ids    INT[]    NOT NULL DEFAULT '{}',
    status      TEXT     NOT NULL DEFAULT 'pending',
    claimed_at  TIMESTAMPTZ,
    finished_at TIMESTAMPTZ
  );
  CREATE INDEX IF NOT EXISTS block_tasks_pending_idx
    ON proxy_whitelist_app.block_tasks (status, shard);

  -- Track which posts have been processed (for edit detection)
  CREATE TABLE IF NOT EXISTS proxy_whitelist_app.processed_posts (
    post_id INT PRIMARY KEY,
    last_block_num INT NOT NULL
  );

END
$BODY$;

INSERT INTO proxy_whitelist_app.app_status (
  id,
  continue_processing,
  parallel_workers,
  redis_url,
  redis_password,
  start_block,
  poll_profile_images_every_n_batches,
  advisory_lock_namespace_begin
)
VALUES (
  1,
  TRUE,
  current_setting('PG_TEMP.PARALLEL_WORKERS', TRUE)::INT,
  current_setting('PG_TEMP.REDIS_URL', TRUE)::TEXT,
  NULLIF(current_setting('PG_TEMP.REDIS_PASSWORD', TRUE), ''),
  current_setting('PG_TEMP.START_BLOCK', TRUE)::INT,
  current_setting('PG_TEMP.POLL_PROFILE_IMAGES_EVERY_N_BATCHES', TRUE)::INT,
  current_setting('PG_TEMP.ADVISORY_LOCK_NAMESPACE_BEGIN', TRUE)::INT
)
ON CONFLICT (id)
DO UPDATE SET
  redis_url = excluded.redis_url,
  redis_password = excluded.redis_password;

RESET ROLE;
