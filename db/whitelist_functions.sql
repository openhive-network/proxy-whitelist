SET ROLE proxy_whitelist_owner;

-- Ensure an account exists in the local accounts table.
-- Called during processing as authors are encountered.
-- Returns the local proxy_whitelist_app.accounts.id.
-- _block is the block that introduces the account to this app; an account
-- that already exists keeps the block it was first seen at.
DROP FUNCTION IF EXISTS proxy_whitelist_app.ensure_account(INT, VARCHAR);
CREATE OR REPLACE FUNCTION proxy_whitelist_app.ensure_account(
    _hive_id INT,
    _name VARCHAR(16),
    _block INT
)
RETURNS INT
LANGUAGE sql
AS $$
    INSERT INTO proxy_whitelist_app.accounts (hive_id, name, first_block)
    VALUES (_hive_id, _name, _block)
    ON CONFLICT (hive_id) DO UPDATE SET name = EXCLUDED.name
    RETURNING id;
$$;

-- Whitelist a batch of URLs for an author.
-- Replaces the old redis_whitelist_urls() plpython3u function.
-- Takes a local account ID (from ensure_account) instead of author name.
-- Set-based INSERTs instead of row-by-row Redis pipeline.
-- _block is the block of the post/account update the URLs come from; rows
-- that already exist keep the block they were first seen at.
DROP FUNCTION IF EXISTS proxy_whitelist_app.whitelist_urls(TEXT[], INT);
CREATE OR REPLACE FUNCTION proxy_whitelist_app.whitelist_urls(
    _urls TEXT[],
    _author_id INT,
    _block INT
)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    __count INT;
BEGIN
    -- Skip blacklisted accounts
    IF EXISTS (SELECT 1 FROM proxy_whitelist_app.blacklisted_accounts WHERE account_id = _author_id) THEN
        RETURN 0;
    END IF;

    -- Batch insert URLs (text stored once, md5 expression index for uniqueness)
    INSERT INTO proxy_whitelist_app.urls (url, first_block)
    SELECT DISTINCT u, _block FROM unnest(_urls) AS u
    WHERE u IS NOT NULL
    ON CONFLICT ((md5(url))) DO NOTHING;

    -- Batch insert author associations (integer IDs only)
    -- Join via md5 to use the expression index on urls(md5(url))
    INSERT INTO proxy_whitelist_app.url_authors (url_id, author_id, first_block)
    SELECT u.id, _author_id, _block
    FROM unnest(_urls) AS input_url
    JOIN proxy_whitelist_app.urls u ON md5(u.url) = md5(input_url) AND u.url = input_url
    ON CONFLICT DO NOTHING;

    GET DIAGNOSTICS __count = ROW_COUNT;
    RETURN __count;
END;
$$;

-- Record an account's current avatar/cover as of _block. A change is written
-- only when the values differ from the stored ones, so updated_block moves
-- only when the profile images do. An account that never had either image
-- gets no row (nor an accounts row); one that drops both keeps a row of NULLs
-- so the removal reaches the profile stream.
CREATE OR REPLACE FUNCTION proxy_whitelist_app.set_account_profile(
    _hive_id INT,
    _name VARCHAR(16),
    _avatar_url TEXT,
    _cover_url TEXT,
    _block INT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    IF _avatar_url IS NULL AND _cover_url IS NULL THEN
        UPDATE proxy_whitelist_app.account_profiles p
           SET avatar_url = NULL, cover_url = NULL, updated_block = _block
          FROM proxy_whitelist_app.accounts a
         WHERE a.hive_id = _hive_id
           AND p.account_id = a.id
           AND (p.avatar_url IS NOT NULL OR p.cover_url IS NOT NULL);
        RETURN;
    END IF;

    INSERT INTO proxy_whitelist_app.account_profiles (account_id, avatar_url, cover_url, updated_block)
    VALUES (proxy_whitelist_app.ensure_account(_hive_id, _name, _block), _avatar_url, _cover_url, _block)
    ON CONFLICT (account_id) DO UPDATE
        SET avatar_url = EXCLUDED.avatar_url,
            cover_url = EXCLUDED.cover_url,
            updated_block = EXCLUDED.updated_block
        WHERE (account_profiles.avatar_url, account_profiles.cover_url)
              IS DISTINCT FROM (EXCLUDED.avatar_url, EXCLUDED.cover_url);
END;
$$;

GRANT EXECUTE ON FUNCTION proxy_whitelist_app.ensure_account(INT, VARCHAR, INT)
    TO haf_admin WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION proxy_whitelist_app.ensure_account(INT, VARCHAR, INT)
    TO proxy_whitelist_user;

GRANT EXECUTE ON FUNCTION proxy_whitelist_app.whitelist_urls(TEXT[], INT, INT)
    TO haf_admin WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION proxy_whitelist_app.whitelist_urls(TEXT[], INT, INT)
    TO proxy_whitelist_user;

RESET ROLE;
