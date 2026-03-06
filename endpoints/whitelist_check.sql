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

-- Combined check: returns 'blacklisted', 'whitelisted', or 'unknown' in one query.
-- Avoids two round-trips from the imagehoster.
CREATE OR REPLACE FUNCTION proxy_whitelist_endpoints.check_url("url" TEXT)
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
        ) THEN 'whitelisted'
        ELSE 'unknown'
    END;
$$;

GRANT EXECUTE ON FUNCTION proxy_whitelist_endpoints.is_whitelisted(TEXT)
    TO proxy_whitelist_user;
GRANT EXECUTE ON FUNCTION proxy_whitelist_endpoints.is_url_blacklisted(TEXT)
    TO proxy_whitelist_user;
GRANT EXECUTE ON FUNCTION proxy_whitelist_endpoints.check_url(TEXT)
    TO proxy_whitelist_user;

RESET ROLE;
