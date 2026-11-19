SET ROLE proxy_whitelist_owner;

-- The account's account_profiles row (all NULL when the account has none),
-- or HTTP 404 through PostgREST when this app does not know the account.
CREATE OR REPLACE FUNCTION proxy_whitelist_app.account_profile_by_name(_name TEXT)
RETURNS proxy_whitelist_app.account_profiles
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    __profile proxy_whitelist_app.account_profiles;
BEGIN
    SELECT p.* INTO __profile
    FROM proxy_whitelist_app.accounts a
    LEFT JOIN proxy_whitelist_app.account_profiles p ON p.account_id = a.id
    WHERE a.name = _name;
    IF NOT FOUND THEN
        RAISE SQLSTATE 'PT404' USING MESSAGE = format('unknown account: %s', _name);
    END IF;
    RETURN __profile;
END
$$;

-- GET /avatar/{name} — the account's profile_image URL as a JSON string, or
-- null when it has none.
CREATE OR REPLACE FUNCTION proxy_whitelist_endpoints.avatar("name" TEXT)
RETURNS TEXT
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    __url TEXT := (proxy_whitelist_app.account_profile_by_name(avatar.name)).avatar_url;
BEGIN
    PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=60"}]', true);
    RETURN __url;
END
$$;

-- GET /cover/{name} — the account's cover_image URL as a JSON string, or null
-- when it has none.
CREATE OR REPLACE FUNCTION proxy_whitelist_endpoints.cover("name" TEXT)
RETURNS TEXT
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    __url TEXT := (proxy_whitelist_app.account_profile_by_name(cover.name)).cover_url;
BEGIN
    PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=60"}]', true);
    RETURN __url;
END
$$;

GRANT EXECUTE ON FUNCTION proxy_whitelist_app.account_profile_by_name(TEXT) TO proxy_whitelist_user;
GRANT EXECUTE ON FUNCTION proxy_whitelist_endpoints.avatar(TEXT) TO proxy_whitelist_user;
GRANT EXECUTE ON FUNCTION proxy_whitelist_endpoints.cover(TEXT) TO proxy_whitelist_user;

RESET ROLE;
