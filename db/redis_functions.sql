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
    import time

    _url = redis_url
    _password = None
    if _url is None:
        r = plpy.execute(
            "SELECT redis_url, redis_password FROM proxy_whitelist_app.app_status LIMIT 1"
        )
        _url = r[0]['redis_url']
        _password = r[0].get('redis_password')

    # Reuse connection across calls within the same session
    cache_key = 'redis_client'
    client = SD.get(cache_key)
    if client is None:
        client = redis_lib.from_url(_url, password=_password,
                                    socket_connect_timeout=10,
                                    socket_timeout=30)
        SD[cache_key] = client

    max_retries = 30
    retry_delay = 2  # seconds

    for attempt in range(max_retries):
        try:
            # Check if author is blacklisted
            if client.sismember('proxy:whitelist:blacklisted_accounts', author):
                return 0

            pipe = client.pipeline()
            count = 0

            for url in urls:
                if url is None:
                    continue
                pipe.sadd('proxy:whitelist:urls', url)
                count += 1

            if count > 0:
                pipe.execute()

            return count

        except redis_lib.exceptions.BusyLoadingError:
            if attempt < max_retries - 1:
                plpy.notice('Redis is loading dataset, retrying in {}s (attempt {}/{})'.format(
                    retry_delay, attempt + 1, max_retries))
                time.sleep(retry_delay)
                continue
            raise
        except redis_lib.exceptions.ConnectionError:
            # Drop cached client so next call reconnects
            SD.pop(cache_key, None)
            if attempt < max_retries - 1:
                plpy.notice('Redis connection error, reconnecting in {}s (attempt {}/{})'.format(
                    retry_delay, attempt + 1, max_retries))
                time.sleep(retry_delay)
                client = redis_lib.from_url(_url, password=_password,
                                            socket_connect_timeout=10,
                                            socket_timeout=30)
                SD[cache_key] = client
                continue
            raise
$BODY$;

GRANT EXECUTE ON FUNCTION proxy_whitelist_app.redis_whitelist_urls(TEXT[], TEXT, TEXT)
    TO haf_admin WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION proxy_whitelist_app.redis_whitelist_urls(TEXT[], TEXT, TEXT)
    TO proxy_whitelist_user;
GRANT EXECUTE ON FUNCTION proxy_whitelist_app.redis_whitelist_urls(TEXT[], TEXT, TEXT)
    TO pg_database_owner WITH GRANT OPTION;
