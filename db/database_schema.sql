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

  -- Stream source identity: sync_uuid is minted once (the volatile default is
  -- evaluated only when the column is added or the row is first inserted) and
  -- tells a replica it is still reading the database it bootstrapped from.
  ALTER TABLE proxy_whitelist_app.app_status
    ADD COLUMN IF NOT EXISTS sync_uuid UUID NOT NULL DEFAULT gen_random_uuid();
  ALTER TABLE proxy_whitelist_app.app_status
    ADD COLUMN IF NOT EXISTS stream_schema_version INT NOT NULL DEFAULT 1;
  -- The block every row that predates the stream columns is labelled with.
  ALTER TABLE proxy_whitelist_app.app_status
    ADD COLUMN IF NOT EXISTS stream_sentinel_block INT;
  ALTER TABLE proxy_whitelist_app.app_status
    ADD COLUMN IF NOT EXISTS account_profiles_backfilled BOOLEAN NOT NULL DEFAULT FALSE;
  -- The last block whose rows the streams may have served (the end of the
  -- last processed range); the context falling behind it means the blocks
  -- above are re-derived, see check_stream_rewind().
  ALTER TABLE proxy_whitelist_app.app_status
    ADD COLUMN IF NOT EXISTS stream_head_block INT;
  -- Every hivemind account with an id up to this one has an accounts row
  -- (track_account_creations()); NULL until the install's backfill_accounts().
  ALTER TABLE proxy_whitelist_app.app_status
    ADD COLUMN IF NOT EXISTS accounts_tracked_hive_id INT;
  -- The version of the /stream/* contract this code serves (endpoints/stream.sql).
  ALTER TABLE proxy_whitelist_app.app_status
    ALTER COLUMN stream_schema_version SET DEFAULT 2;
  UPDATE proxy_whitelist_app.app_status SET stream_schema_version = 2
  WHERE stream_schema_version IS DISTINCT FROM 2;

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
  -- Uniqueness enforced via md5 expression index (btree UNIQUE fails for URLs >8KB).
  CREATE TABLE IF NOT EXISTS proxy_whitelist_app.urls (
    id BIGSERIAL PRIMARY KEY,
    url TEXT NOT NULL
  );

  -- Migration: replace btree UNIQUE on url text (fails for >8KB URLs) with md5 expression index
  ALTER TABLE proxy_whitelist_app.urls DROP CONSTRAINT IF EXISTS urls_url_key;
  CREATE UNIQUE INDEX IF NOT EXISTS urls_url_md5_idx
    ON proxy_whitelist_app.urls (md5(url));

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

-- Block coordinates for the replication streams (endpoints/stream.sql). Runs
-- once: the sentinel is recorded on the first install that has these columns,
-- and the columns are added only when missing. Adding a NOT NULL column with a
-- constant default is a catalog-only change in PostgreSQL 11+, so the existing
-- rows of urls/url_authors/accounts are not rewritten; the default is then
-- dropped so that every insert site has to supply the block.
DO $BODY$
DECLARE
  __schema_name VARCHAR;
  __sentinel INT;
  __table TEXT;
BEGIN
  SHOW SEARCH_PATH INTO __schema_name;

  SELECT stream_sentinel_block INTO __sentinel FROM proxy_whitelist_app.app_status;
  IF __sentinel IS NULL THEN
    __sentinel := COALESCE(hive.app_get_current_block_num(__schema_name), 0);
    UPDATE proxy_whitelist_app.app_status SET stream_sentinel_block = __sentinel;
    RAISE NOTICE 'Stream sentinel block: %', __sentinel;
  END IF;

  FOREACH __table IN ARRAY ARRAY['urls', 'url_authors', 'accounts'] LOOP
    IF NOT EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = 'proxy_whitelist_app' AND table_name = __table AND column_name = 'first_block'
    ) THEN
      EXECUTE format('ALTER TABLE proxy_whitelist_app.%I ADD COLUMN first_block INT NOT NULL DEFAULT %s', __table, __sentinel);
      EXECUTE format('ALTER TABLE proxy_whitelist_app.%I ALTER COLUMN first_block DROP DEFAULT', __table);
    END IF;
  END LOOP;
END
$BODY$;

CREATE INDEX IF NOT EXISTS urls_by_first_block
ON proxy_whitelist_app.urls (first_block, md5(url));
CREATE INDEX IF NOT EXISTS url_authors_by_first_block
ON proxy_whitelist_app.url_authors (first_block, url_id);
CREATE INDEX IF NOT EXISTS accounts_by_first_block
ON proxy_whitelist_app.accounts (first_block, name COLLATE "C");

-- Current avatar/cover per account (no history), from the latest
-- account_update/account_update2 seen for it.
CREATE TABLE IF NOT EXISTS proxy_whitelist_app.account_profiles (
  account_id INT PRIMARY KEY REFERENCES proxy_whitelist_app.accounts (id),
  avatar_url TEXT NULL,
  cover_url TEXT NULL,
  updated_block INT NOT NULL
);
CREATE INDEX IF NOT EXISTS account_profiles_by_updated_block
ON proxy_whitelist_app.account_profiles (updated_block, account_id);

-- Migration: hivemind's placeholder account (id 0, empty name) was backfilled
-- as an account by earlier installs; track_account_creations() skips it now.
DELETE FROM proxy_whitelist_app.account_profiles p
USING proxy_whitelist_app.accounts a
WHERE p.account_id = a.id AND (a.hive_id = 0 OR a.name = '');
DELETE FROM proxy_whitelist_app.blacklisted_accounts b
USING proxy_whitelist_app.accounts a
WHERE b.account_id = a.id AND (a.hive_id = 0 OR a.name = '');
DELETE FROM proxy_whitelist_app.url_authors ua
USING proxy_whitelist_app.accounts a
WHERE ua.author_id = a.id AND (a.hive_id = 0 OR a.name = '');
DELETE FROM proxy_whitelist_app.accounts
WHERE hive_id = 0 OR name = '';

RESET ROLE;
