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
    start_block INT NOT NULL DEFAULT 1,
    advisory_lock_namespace_begin INT NOT NULL DEFAULT 20000
  );

  -- Migration: drop columns from previous versions
  ALTER TABLE proxy_whitelist_app.app_status
    DROP COLUMN IF EXISTS poll_profile_images_every_n_batches;
  ALTER TABLE proxy_whitelist_app.app_status
    DROP COLUMN IF EXISTS redis_url;
  ALTER TABLE proxy_whitelist_app.app_status
    DROP COLUMN IF EXISTS redis_password;

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

  -- Migration: drop tables from previous versions (parallel worker infrastructure)
  DROP TABLE IF EXISTS proxy_whitelist_app.block_tasks;
  DROP SEQUENCE IF EXISTS proxy_whitelist_app.batch_seq;

  -- Track which posts have been processed (for edit detection)
  CREATE TABLE IF NOT EXISTS proxy_whitelist_app.processed_posts (
    post_id INT PRIMARY KEY,
    last_block_num INT NOT NULL
  );

  -- App-local accounts table (same pattern as hivemind_app.hive_accounts).
  -- Populated as authors are encountered during processing.
  -- Keeps the app independent of hivemind's schema.
  CREATE TABLE IF NOT EXISTS proxy_whitelist_app.accounts (
    id SERIAL PRIMARY KEY,
    hive_id INT NOT NULL UNIQUE,
    name VARCHAR(16) NOT NULL UNIQUE
  );

  -- URL dictionary: text stored exactly once, referenced by integer ID everywhere else.
  -- Existence in this table = whitelisted.
  CREATE TABLE IF NOT EXISTS proxy_whitelist_app.urls (
    id BIGSERIAL PRIMARY KEY,
    url TEXT NOT NULL UNIQUE
  );

  -- Author-URL associations: two integers per row (~12 bytes vs ~120 bytes with TEXT)
  CREATE TABLE IF NOT EXISTS proxy_whitelist_app.url_authors (
    url_id BIGINT NOT NULL REFERENCES proxy_whitelist_app.urls(id) ON DELETE CASCADE,
    author_id INT NOT NULL REFERENCES proxy_whitelist_app.accounts(id),
    PRIMARY KEY (url_id, author_id)
  );
  CREATE INDEX IF NOT EXISTS url_authors_by_author
    ON proxy_whitelist_app.url_authors (author_id);

  -- Blacklisted accounts
  CREATE TABLE IF NOT EXISTS proxy_whitelist_app.blacklisted_accounts (
    account_id INT PRIMARY KEY REFERENCES proxy_whitelist_app.accounts(id)
  );

  -- Permanently blacklisted URLs
  CREATE TABLE IF NOT EXISTS proxy_whitelist_app.blacklisted_urls (
    url_id BIGINT PRIMARY KEY REFERENCES proxy_whitelist_app.urls(id)
  );

END
$BODY$;

INSERT INTO proxy_whitelist_app.app_status (
  id,
  continue_processing,
  parallel_workers,
  start_block,
  advisory_lock_namespace_begin
)
VALUES (
  1,
  TRUE,
  current_setting('PG_TEMP.PARALLEL_WORKERS', TRUE)::INT,
  current_setting('PG_TEMP.START_BLOCK', TRUE)::INT,
  current_setting('PG_TEMP.ADVISORY_LOCK_NAMESPACE_BEGIN', TRUE)::INT
)
ON CONFLICT (id) DO NOTHING;

RESET ROLE;
