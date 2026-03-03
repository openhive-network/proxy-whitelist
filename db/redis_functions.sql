-- plpython3u functions for writing URL whitelist data to Redis.
-- Follows the pattern from hivesense's ollama.sql (plpython3u external API calls).

-- Must be created before SET ROLE (plpython3u requires superuser)

CREATE OR REPLACE FUNCTION proxy_whitelist_app.redis_whitelist_urls(
    urls TEXT[],
    author TEXT,
    redis_url TEXT DEFAULT NULL
)
RETURNS INT
LANGUAGE plpython3u
AS $BODY$
    import redis as redis_lib

    _url = redis_url
    _password = None
    if _url is None:
        r = plpy.execute(
            "SELECT redis_url, redis_password FROM proxy_whitelist_app.app_status LIMIT 1"
        )
        _url = r[0]['redis_url']
        _password = r[0].get('redis_password')

    client = redis_lib.from_url(_url, password=_password)

    # Check if author is blacklisted
    if client.sismember('proxy:whitelist:blacklisted_accounts', author):
        return 0

    pipe = client.pipeline()
    count = 0

    for url in urls:
        if url is None:
            continue
        pipe.sadd('proxy:whitelist:urls', url)
        pipe.sadd('proxy:whitelist:account_urls:{}'.format(author), url)
        pipe.sadd('proxy:whitelist:url_accounts:{}'.format(url), author)
        count += 1

    if count > 0:
        pipe.execute()

    return count
$BODY$;

GRANT EXECUTE ON FUNCTION proxy_whitelist_app.redis_whitelist_urls(TEXT[], TEXT, TEXT)
    TO haf_admin WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION proxy_whitelist_app.redis_whitelist_urls(TEXT[], TEXT, TEXT)
    TO proxy_whitelist_user;
GRANT EXECUTE ON FUNCTION proxy_whitelist_app.redis_whitelist_urls(TEXT[], TEXT, TEXT)
    TO pg_database_owner WITH GRANT OPTION;
