SET ROLE proxy_whitelist_owner;

-- Called on every Varnish cache miss. Must be fast (indexed PK lookup).
CREATE OR REPLACE FUNCTION proxy_whitelist_endpoints.is_whitelisted("url" TEXT)
RETURNS BOOLEAN
LANGUAGE sql
STABLE
AS $$
    SELECT EXISTS(
        SELECT 1 FROM proxy_whitelist_app.urls u
        WHERE md5(u.url) = md5(is_whitelisted.url) AND u.url = is_whitelisted.url
    );
$$;

CREATE OR REPLACE FUNCTION proxy_whitelist_endpoints.is_url_blacklisted("url" TEXT)
RETURNS BOOLEAN
LANGUAGE sql
STABLE
AS $$
    SELECT EXISTS(
        SELECT 1 FROM proxy_whitelist_app.blacklisted_urls bu
        JOIN proxy_whitelist_app.urls u ON u.id = bu.url_id
        WHERE md5(u.url) = md5(is_url_blacklisted.url) AND u.url = is_url_blacklisted.url
    );
$$;

-- Drop the previous single-parameter signature so CREATE below replaces it
-- cleanly during in-place upgrades (CREATE OR REPLACE cannot change signature).
DROP FUNCTION IF EXISTS proxy_whitelist_endpoints.check_url(TEXT);

-- Combined check: returns 'blacklisted', 'whitelisted', or 'unknown' in one query.
-- Avoids two round-trips from the imagehoster.
--
-- The optional blacklisted_accounts list lets the caller apply its own
-- per-account blacklist as part of the same query: a URL whose only authors
-- are all in this list is reported as 'unknown' to that caller, even though
-- it is whitelisted in absolute terms. Operator-specific moderation policy
-- stays out of the (replay-reproducible) database this way.
CREATE OR REPLACE FUNCTION proxy_whitelist_endpoints.check_url(
    "url" TEXT,
    "blacklisted_accounts" TEXT[] DEFAULT NULL
)
RETURNS TEXT
LANGUAGE sql
STABLE
AS $$
    SELECT CASE
        WHEN EXISTS(
            SELECT 1 FROM proxy_whitelist_app.blacklisted_urls bu
            JOIN proxy_whitelist_app.urls u ON u.id = bu.url_id
            WHERE md5(u.url) = md5(check_url.url) AND u.url = check_url.url
        ) THEN 'blacklisted'
        WHEN EXISTS(
            SELECT 1 FROM proxy_whitelist_app.urls u
            WHERE md5(u.url) = md5(check_url.url) AND u.url = check_url.url
              AND (
                  -- treat both NULL and empty array as "no per-call blacklist"
                  coalesce(cardinality(check_url.blacklisted_accounts), 0) = 0
                  OR EXISTS (
                      SELECT 1
                      FROM proxy_whitelist_app.url_authors ua
                      JOIN proxy_whitelist_app.accounts a ON a.id = ua.author_id
                      WHERE ua.url_id = u.id
                        AND NOT (a.name = ANY(check_url.blacklisted_accounts))
                  )
              )
        ) THEN 'whitelisted'
        ELSE 'unknown'
    END;
$$;

GRANT EXECUTE ON FUNCTION proxy_whitelist_endpoints.is_whitelisted(TEXT)
    TO proxy_whitelist_user;
GRANT EXECUTE ON FUNCTION proxy_whitelist_endpoints.is_url_blacklisted(TEXT)
    TO proxy_whitelist_user;
GRANT EXECUTE ON FUNCTION proxy_whitelist_endpoints.check_url(TEXT, TEXT[])
    TO proxy_whitelist_user;

RESET ROLE;
