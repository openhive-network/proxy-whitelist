SET ROLE proxy_whitelist_owner;

-- Ensure an account exists in the local accounts table.
-- Called during processing as authors are encountered.
-- Returns the local proxy_whitelist_app.accounts.id.
CREATE OR REPLACE FUNCTION proxy_whitelist_app.ensure_account(
    _hive_id INT,
    _name VARCHAR(16)
)
RETURNS INT
LANGUAGE sql
AS $$
    INSERT INTO proxy_whitelist_app.accounts (hive_id, name)
    VALUES (_hive_id, _name)
    ON CONFLICT (hive_id) DO UPDATE SET name = EXCLUDED.name
    RETURNING id;
$$;

-- Whitelist a batch of URLs for an author.
-- Replaces the old redis_whitelist_urls() plpython3u function.
-- Takes a local account ID (from ensure_account) instead of author name.
-- Set-based INSERTs instead of row-by-row Redis pipeline.
CREATE OR REPLACE FUNCTION proxy_whitelist_app.whitelist_urls(
    _urls TEXT[],
    _author_id INT
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

    -- Batch insert URLs (text stored once)
    INSERT INTO proxy_whitelist_app.urls (url)
    SELECT DISTINCT u FROM unnest(_urls) AS u
    WHERE u IS NOT NULL
    ON CONFLICT (url) DO NOTHING;

    -- Batch insert author associations (integer IDs only)
    INSERT INTO proxy_whitelist_app.url_authors (url_id, author_id)
    SELECT u.id, _author_id
    FROM proxy_whitelist_app.urls u
    WHERE u.url = ANY(_urls)
    ON CONFLICT DO NOTHING;

    GET DIAGNOSTICS __count = ROW_COUNT;
    RETURN __count;
END;
$$;

GRANT EXECUTE ON FUNCTION proxy_whitelist_app.ensure_account(INT, VARCHAR)
    TO haf_admin WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION proxy_whitelist_app.ensure_account(INT, VARCHAR)
    TO proxy_whitelist_user;

GRANT EXECUTE ON FUNCTION proxy_whitelist_app.whitelist_urls(TEXT[], INT)
    TO haf_admin WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION proxy_whitelist_app.whitelist_urls(TEXT[], INT)
    TO proxy_whitelist_user;

RESET ROLE;
