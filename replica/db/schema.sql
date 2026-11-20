/*
 * The replica's copy of the tables the endpoint functions (the files under
 * endpoints/) read, with the primary's columns (db/database_schema.sql),
 * filled by replica/sync.py from the primary's replication streams.
 *
 * Rows are keyed by their natural keys (accounts by hive_id and name, urls by
 * md5(url), url_authors by (url, account), profiles by account): a HAF replay
 * renumbers the primary's serial ids, so the ids here are local and never
 * leave the replica. first_block/updated_block keep the source's block so the
 * replica could serve the streams itself later.
 */

CREATE SCHEMA IF NOT EXISTS proxy_whitelist_app AUTHORIZATION proxy_whitelist_owner;
CREATE SCHEMA IF NOT EXISTS proxy_whitelist_replica AUTHORIZATION proxy_whitelist_owner;

SET ROLE proxy_whitelist_owner;

CREATE TABLE IF NOT EXISTS proxy_whitelist_app.accounts (
  id SERIAL PRIMARY KEY,
  hive_id INT NOT NULL UNIQUE,
  -- the column name the endpoint functions read
  name VARCHAR(16) NOT NULL UNIQUE, -- noqa: RF04
  first_block INT NOT NULL
);
CREATE INDEX IF NOT EXISTS accounts_by_first_block
ON proxy_whitelist_app.accounts (first_block, name COLLATE "C");

CREATE TABLE IF NOT EXISTS proxy_whitelist_app.urls (
  id BIGSERIAL PRIMARY KEY,
  url TEXT NOT NULL,
  first_block INT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS urls_url_md5_idx
ON proxy_whitelist_app.urls (md5(url));
CREATE INDEX IF NOT EXISTS urls_by_first_block
ON proxy_whitelist_app.urls (first_block, md5(url));

CREATE TABLE IF NOT EXISTS proxy_whitelist_app.url_authors (
  url_id BIGINT NOT NULL REFERENCES proxy_whitelist_app.urls (id) ON DELETE CASCADE,
  author_id INT NOT NULL REFERENCES proxy_whitelist_app.accounts (id),
  first_block INT NOT NULL,
  PRIMARY KEY (url_id, author_id)
);
CREATE INDEX IF NOT EXISTS url_authors_by_author
ON proxy_whitelist_app.url_authors (author_id);
CREATE INDEX IF NOT EXISTS url_authors_by_first_block
ON proxy_whitelist_app.url_authors (first_block, url_id);

CREATE TABLE IF NOT EXISTS proxy_whitelist_app.account_profiles (
  account_id INT PRIMARY KEY REFERENCES proxy_whitelist_app.accounts (id),
  avatar_url TEXT NULL,
  cover_url TEXT NULL,
  updated_block INT NOT NULL
);
CREATE INDEX IF NOT EXISTS account_profiles_by_updated_block
ON proxy_whitelist_app.account_profiles (updated_block, account_id);

CREATE TABLE IF NOT EXISTS proxy_whitelist_app.blacklisted_accounts (
  account_id INT PRIMARY KEY REFERENCES proxy_whitelist_app.accounts (id)
);

CREATE TABLE IF NOT EXISTS proxy_whitelist_app.blacklisted_urls (
  url_id BIGINT PRIMARY KEY REFERENCES proxy_whitelist_app.urls (id)
);

-- url_authors rows whose url or account has not arrived yet: a url author's
-- block is the last edit of its post, so it can be lower than the block of
-- the url row. Moved into url_authors once both exist.
CREATE TABLE IF NOT EXISTS proxy_whitelist_replica.pending_url_authors (
  url_md5 TEXT NOT NULL,
  account_name TEXT NOT NULL,
  first_block INT NOT NULL,
  PRIMARY KEY (url_md5, account_name)
);

/*
 * The syncer's state, one row. The source identity (source_sync_uuid,
 * stream_schema_version) is stored on first contact and compared on every
 * poll. Each stream's cursor is (block, natural key of the last applied row):
 * a NULL key means every row up to and including the block is applied, so
 * the next page starts at block + 1; -1 is the cursor before block 0.
 * source_block_num/source_block_time are the source's head the replica has
 * fully replicated, i.e. what sync_status() reports. replica_sync_uuid is the
 * replica's own identity, minted with the row, so anew by every reset or
 * rebuild. rebuild_env_honoured records that a rebuild was done for
 * PROXY_WHITELIST_REPLICA_REBUILD=1, so later starts with it still set skip it.
 * purge_enabled is whether the running syncer purges Cloudflare (replica/purge.py).
 */
CREATE TABLE IF NOT EXISTS proxy_whitelist_replica.state (
  id BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
  source_url TEXT NULL,
  source_sync_uuid UUID NULL,
  stream_schema_version INT NULL,
  stream_sentinel_block INT NULL,
  accounts_block INT NOT NULL DEFAULT -1,
  accounts_after_name TEXT NULL,
  urls_block INT NOT NULL DEFAULT -1,
  urls_after_md5 TEXT NULL,
  url_authors_block INT NOT NULL DEFAULT -1,
  url_authors_after_md5 TEXT NULL,
  url_authors_after_account TEXT NULL,
  profiles_block INT NOT NULL DEFAULT -1,
  profiles_after_account TEXT NULL,
  source_block_num INT NULL,
  source_block_time TIMESTAMPTZ NULL,
  polled_at TIMESTAMPTZ NULL,
  sync_error TEXT NULL,
  bootstrap_complete BOOLEAN NOT NULL DEFAULT FALSE,
  replica_sync_uuid UUID NOT NULL DEFAULT gen_random_uuid(),
  rebuild_env_honoured BOOLEAN NOT NULL DEFAULT FALSE,
  purge_enabled BOOLEAN NOT NULL DEFAULT FALSE
);

-- Columns added after the first release, for replicas installed before them.
ALTER TABLE proxy_whitelist_replica.state
ADD COLUMN IF NOT EXISTS replica_sync_uuid UUID NOT NULL DEFAULT gen_random_uuid(),
ADD COLUMN IF NOT EXISTS rebuild_env_honoured BOOLEAN NOT NULL DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS purge_enabled BOOLEAN NOT NULL DEFAULT FALSE;

/*
 * The outbox of Cloudflare purges (replica/purge.py): an account whose
 * avatar_url or cover_url changed, written in the transaction that applied the
 * change, deleted once its redirect URLs are purged. A second change before
 * the purge raises block and ORs the flags. attempts/last_error record failed
 * purge calls.
 */
CREATE TABLE IF NOT EXISTS proxy_whitelist_replica.purge_queue (
  account_name TEXT PRIMARY KEY,
  block INT NOT NULL,
  avatar_changed BOOLEAN NOT NULL,
  cover_changed BOOLEAN NOT NULL,
  queued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  attempts INT NOT NULL DEFAULT 0,
  last_error TEXT NULL
);
CREATE INDEX IF NOT EXISTS purge_queue_by_block
ON proxy_whitelist_replica.purge_queue (block, account_name);

INSERT INTO proxy_whitelist_replica.state (id) VALUES (TRUE)
ON CONFLICT (id) DO NOTHING;

RESET ROLE;
