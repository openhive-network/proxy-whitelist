SET ROLE proxy_whitelist_owner;

-- Get all URLs posted by an account
CREATE OR REPLACE FUNCTION proxy_whitelist_endpoints.account_urls("account" TEXT)
RETURNS TABLE(url TEXT)
LANGUAGE sql
STABLE
AS $$
    SELECT u.url
    FROM proxy_whitelist_app.url_authors ua
    JOIN proxy_whitelist_app.urls u ON u.id = ua.url_id
    JOIN proxy_whitelist_app.accounts a ON a.id = ua.author_id
    WHERE a.name = account_urls.account;
$$;

-- Get all accounts that posted a URL
CREATE OR REPLACE FUNCTION proxy_whitelist_endpoints.url_accounts("url" TEXT)
RETURNS TABLE(account TEXT)
LANGUAGE sql
STABLE
AS $$
    SELECT a.name
    FROM proxy_whitelist_app.url_authors ua
    JOIN proxy_whitelist_app.urls u ON u.id = ua.url_id
    JOIN proxy_whitelist_app.accounts a ON a.id = ua.author_id
    WHERE u.url = url_accounts.url;
$$;

GRANT EXECUTE ON FUNCTION proxy_whitelist_endpoints.account_urls(TEXT)
    TO proxy_whitelist_user;
GRANT EXECUTE ON FUNCTION proxy_whitelist_endpoints.url_accounts(TEXT)
    TO proxy_whitelist_user;

RESET ROLE;
