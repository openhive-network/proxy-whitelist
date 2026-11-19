SET ROLE proxy_whitelist_owner;

/*
 * Replication streams: the app's tables as block-ordered pages a replica can
 * copy and then follow (the GET /stream/... paths of rewrite_rules.conf).
 *
 * Contract (stream_schema_version 2):
 *  - Chain-derived rows carry the block that introduced them (accounts, urls,
 *    url_authors: first_block) or last changed them (account_profiles:
 *    updated_block). Rows that predate the stream columns carry
 *    app_status.stream_sentinel_block. Serial ids are not part of the
 *    contract: they differ between replays and become visible out of order.
 *  - A page holds the rows of blocks [from_block, to_block] ordered by
 *    (block, natural key), the natural key being the account name for
 *    accounts and profiles, md5(url) for urls and (md5(url), account name)
 *    for url authors. md5 is lowercase hex; names compare bytewise
 *    (COLLATE "C"). An after_* cursor resumes strictly after that key within
 *    from_block; url_authors takes after_md5 and after_account together.
 *  - to_block defaults to, and is capped at, the last block the app has
 *    processed: a later block may still be in flight.
 *  - page_size (default 1000, capped at stream_page_cap()) is the number of
 *    rows the function selects; PostgREST's `limit` must not be used, as it
 *    truncates after the headers are set. A page shorter than page_size
 *    completes the range and its Stream-To-Block header is the applied upper
 *    bound; a full page's Stream-To-Block is its last row's block. A replica
 *    may always continue with from_block = Stream-To-Block and after_* set to
 *    the last row's natural key (or from Stream-To-Block + 1 without after_*
 *    after a short page), so no row is lost or repeated.
 *  - Stream-Sync-Uuid carries the source identity: a replica re-bootstraps
 *    when it changes. It is re-minted when the app context is moved back
 *    below blocks the streams have served (check_stream_rewind).
 *  - urls, url_authors and accounts are append-only; account_profiles keeps
 *    only the current value, so a profile reappears at its new block when it
 *    changes. Rows removed by the operator CLI (cli/blacklist.py remove-all)
 *    are not reported; the blacklists are served whole by /stream/blacklists.
 */

CREATE OR REPLACE FUNCTION proxy_whitelist_app.stream_page_cap()
RETURNS INT
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT 100000;
$$;

-- Validates a stream request's page size; returns it capped at stream_page_cap().
CREATE OR REPLACE FUNCTION proxy_whitelist_app.stream_page_size(_page_size INT)
RETURNS INT
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    IF _page_size IS NULL OR _page_size < 1 THEN
        RAISE EXCEPTION 'page_size must be >= 1, got %', _page_size
            USING ERRCODE = '22023';
    END IF;
    RETURN LEAST(_page_size, proxy_whitelist_app.stream_page_cap());
END
$$;

-- Validates a stream request's block range and returns the applied upper
-- block (see the contract above).
CREATE OR REPLACE FUNCTION proxy_whitelist_app.begin_stream_page(_from_block INT, _to_block INT)
RETURNS INT
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    __head INT;
BEGIN
    IF _from_block IS NULL OR _from_block < 0 THEN
        RAISE EXCEPTION 'from_block must be a block number >= 0, got %', _from_block
            USING ERRCODE = '22023';
    END IF;
    IF _to_block < _from_block THEN
        RAISE EXCEPTION 'to_block (%) must not be lower than from_block (%)', _to_block, _from_block
            USING ERRCODE = '22023';
    END IF;

    SELECT c.current_block_num INTO __head
    FROM hafd.contexts c
    WHERE c.name = 'proxy_whitelist_app';
    RETURN LEAST(COALESCE(_to_block, __head), __head);
END
$$;

-- Sets the stream response headers once a page has been selected: the
-- Stream-To-Block of the contract above and the source identity. _last_block
-- (the block of the page's last row) is only read for a full page.
CREATE OR REPLACE FUNCTION proxy_whitelist_app.end_stream_page(
    _applied_to INT, _rows_returned INT, _page_size INT, _last_block INT
)
RETURNS VOID
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    __sync_uuid UUID;
BEGIN
    SELECT s.sync_uuid INTO __sync_uuid FROM proxy_whitelist_app.app_status s;
    PERFORM set_config('response.headers', json_build_array(
        json_build_object('Stream-To-Block', (
            CASE WHEN _rows_returned < _page_size THEN _applied_to ELSE _last_block END
        )::TEXT),
        json_build_object('Stream-Sync-Uuid', __sync_uuid::TEXT)
    )::TEXT, true);
END
$$;

-- url_authors rows of blocks [_from, _to] in stream order, resuming after
-- (_after_md5, _after_account) within block _from. The url lookup is a
-- LATERAL behind OFFSET 0 so that it stays a per-row index probe: rows are
-- read in first_block order from url_authors_by_first_block and only one
-- block's worth needs sorting before LIMIT, instead of hashing all of urls.
CREATE OR REPLACE FUNCTION proxy_whitelist_app.url_author_rows(
    _from INT, _to INT, _after_md5 TEXT, _after_account TEXT, _limit INT
)
RETURNS TABLE (first_block INT, url_md5 TEXT, account_name TEXT)
LANGUAGE sql
STABLE
AS $$
    SELECT ua.first_block, u.md5, a.name::TEXT
    FROM proxy_whitelist_app.url_authors ua
    CROSS JOIN LATERAL (
        SELECT md5(x.url) AS md5
        FROM proxy_whitelist_app.urls x
        WHERE x.id = ua.url_id
        OFFSET 0
    ) u
    JOIN proxy_whitelist_app.accounts a ON a.id = ua.author_id
    WHERE ua.first_block BETWEEN _from AND _to
      AND (
          _after_md5 IS NULL
          OR ua.first_block > _from
          OR u.md5 > _after_md5
          OR (u.md5 = _after_md5 AND a.name COLLATE "C" > _after_account)
      )
    ORDER BY ua.first_block, u.md5, a.name COLLATE "C"
    LIMIT _limit;
$$;

-- The same rows for the single block _block, read by walking urls in md5
-- order (urls_url_md5_idx) and probing each url's authors, so a page costs
-- about its own size. The sentinel block holds every row that predates the
-- stream columns (tens of millions); sorting it for each page is not
-- affordable. OFFSET 0 keeps the planner from flattening the probe into a
-- hash join over the whole block.
CREATE OR REPLACE FUNCTION proxy_whitelist_app.url_author_rows_by_md5(
    _block INT, _after_md5 TEXT, _after_account TEXT, _limit INT
)
RETURNS TABLE (first_block INT, url_md5 TEXT, account_name TEXT)
LANGUAGE sql
STABLE
AS $$
    SELECT _block, md5(u.url), au.name
    FROM proxy_whitelist_app.urls u
    CROSS JOIN LATERAL (
        SELECT a.name::TEXT AS name
        FROM proxy_whitelist_app.url_authors ua
        JOIN proxy_whitelist_app.accounts a ON a.id = ua.author_id
        WHERE ua.url_id = u.id AND ua.first_block = _block
        OFFSET 0
    ) au
    WHERE md5(u.url) >= COALESCE(_after_md5, '')
      AND (
          _after_md5 IS NULL
          OR md5(u.url) > _after_md5
          OR au.name COLLATE "C" > _after_account
      )
    ORDER BY md5(u.url), au.name COLLATE "C"
    LIMIT _limit;
$$;

DROP FUNCTION IF EXISTS proxy_whitelist_endpoints.stream_accounts(INT);
DROP FUNCTION IF EXISTS proxy_whitelist_endpoints.stream_urls(INT, INT, TEXT);
DROP FUNCTION IF EXISTS proxy_whitelist_endpoints.stream_url_authors(INT, INT, TEXT, TEXT);
DROP FUNCTION IF EXISTS proxy_whitelist_endpoints.stream_profiles(INT, INT, TEXT);

/*
 * GET /stream/accounts?from_block=&to_block=&after_name=&page_size= — accounts
 * by the block that introduced them to this app. hive_id is deterministic,
 * so a replica keys accounts on it.
 */
CREATE OR REPLACE FUNCTION proxy_whitelist_endpoints.stream_accounts(
    "from_block" INT DEFAULT 0,
    "to_block" INT DEFAULT NULL,
    "after_name" TEXT DEFAULT NULL,
    "page_size" INT DEFAULT 1000
)
RETURNS TABLE (first_block INT, hive_id INT, name TEXT)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    __to INT := proxy_whitelist_app.begin_stream_page(stream_accounts.from_block, stream_accounts.to_block);
    __size INT := proxy_whitelist_app.stream_page_size(stream_accounts.page_size);
    __rows INT := 0;
BEGIN
    -- Two statements rather than "after_name IS NULL OR ...", so that the row
    -- comparison reaches the (first_block, name) index as its scan start.
    IF stream_accounts.after_name IS NULL THEN
        FOR first_block, hive_id, name IN
            SELECT a.first_block, a.hive_id, a.name::TEXT
            FROM proxy_whitelist_app.accounts a
            WHERE a.first_block BETWEEN stream_accounts.from_block AND __to
            ORDER BY a.first_block, a.name COLLATE "C"
            LIMIT __size
        LOOP
            __rows := __rows + 1;
            RETURN NEXT;
        END LOOP;
    ELSE
        FOR first_block, hive_id, name IN
            SELECT a.first_block, a.hive_id, a.name::TEXT
            FROM proxy_whitelist_app.accounts a
            WHERE (a.first_block, a.name COLLATE "C") > (stream_accounts.from_block, stream_accounts.after_name)
              AND a.first_block <= __to
            ORDER BY a.first_block, a.name COLLATE "C"
            LIMIT __size
        LOOP
            __rows := __rows + 1;
            RETURN NEXT;
        END LOOP;
    END IF;
    PERFORM proxy_whitelist_app.end_stream_page(__to, __rows, __size, first_block);
END
$$;

-- GET /stream/urls?from_block=&to_block=&after_md5=&page_size=
CREATE OR REPLACE FUNCTION proxy_whitelist_endpoints.stream_urls(
    "from_block" INT DEFAULT 0,
    "to_block" INT DEFAULT NULL,
    "after_md5" TEXT DEFAULT NULL,
    "page_size" INT DEFAULT 1000
)
RETURNS TABLE (first_block INT, md5 TEXT, url TEXT)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    __to INT := proxy_whitelist_app.begin_stream_page(stream_urls.from_block, stream_urls.to_block);
    __size INT := proxy_whitelist_app.stream_page_size(stream_urls.page_size);
    __rows INT := 0;
BEGIN
    -- Two statements rather than "after_md5 IS NULL OR ...", so that the row
    -- comparison reaches the (first_block, md5(url)) index as its scan start.
    IF stream_urls.after_md5 IS NULL THEN
        FOR first_block, md5, url IN
            SELECT u.first_block, md5(u.url), u.url
            FROM proxy_whitelist_app.urls u
            WHERE u.first_block BETWEEN stream_urls.from_block AND __to
            ORDER BY u.first_block, md5(u.url)
            LIMIT __size
        LOOP
            __rows := __rows + 1;
            RETURN NEXT;
        END LOOP;
    ELSE
        FOR first_block, md5, url IN
            SELECT u.first_block, md5(u.url), u.url
            FROM proxy_whitelist_app.urls u
            WHERE (u.first_block, md5(u.url)) > (stream_urls.from_block, stream_urls.after_md5)
              AND u.first_block <= __to
            ORDER BY u.first_block, md5(u.url)
            LIMIT __size
        LOOP
            __rows := __rows + 1;
            RETURN NEXT;
        END LOOP;
    END IF;
    PERFORM proxy_whitelist_app.end_stream_page(__to, __rows, __size, first_block);
END
$$;

/*
 * GET /stream/url-authors?from_block=&to_block=&after_md5=&after_account=&page_size=
 * The range is read in three block-disjoint parts, in order: before the
 * sentinel block, the sentinel block (read by md5, see
 * url_author_rows_by_md5; skipped when no row carries it, so a fresh install
 * does not walk urls for nothing), after it. The resume cursor applies only
 * to the part that starts at from_block.
 */
CREATE OR REPLACE FUNCTION proxy_whitelist_endpoints.stream_url_authors(
    "from_block" INT DEFAULT 0,
    "to_block" INT DEFAULT NULL,
    "after_md5" TEXT DEFAULT NULL,
    "after_account" TEXT DEFAULT NULL,
    "page_size" INT DEFAULT 1000
)
RETURNS TABLE (first_block INT, url_md5 TEXT, account_name TEXT)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    __from INT := stream_url_authors.from_block;
    __to INT := proxy_whitelist_app.begin_stream_page(stream_url_authors.from_block, stream_url_authors.to_block);
    __size INT := proxy_whitelist_app.stream_page_size(stream_url_authors.page_size);
    __sentinel INT;
    __rows INT := 0;
BEGIN
    IF (stream_url_authors.after_md5 IS NULL) != (stream_url_authors.after_account IS NULL) THEN
        RAISE EXCEPTION 'after_md5 and after_account must be given together'
            USING ERRCODE = '22023';
    END IF;

    SELECT s.stream_sentinel_block INTO __sentinel FROM proxy_whitelist_app.app_status s;

    IF __from < __sentinel THEN
        FOR first_block, url_md5, account_name IN
            SELECT * FROM proxy_whitelist_app.url_author_rows(
                __from, LEAST(__to, __sentinel - 1),
                stream_url_authors.after_md5, stream_url_authors.after_account, __size
            )
        LOOP
            __rows := __rows + 1;
            RETURN NEXT;
        END LOOP;
    END IF;

    IF __rows < __size AND __sentinel BETWEEN __from AND __to
       AND EXISTS (SELECT 1 FROM proxy_whitelist_app.url_authors ua WHERE ua.first_block = __sentinel) THEN
        FOR first_block, url_md5, account_name IN
            SELECT * FROM proxy_whitelist_app.url_author_rows_by_md5(
                __sentinel,
                CASE WHEN __from = __sentinel THEN stream_url_authors.after_md5 END,
                CASE WHEN __from = __sentinel THEN stream_url_authors.after_account END,
                __size - __rows
            )
        LOOP
            __rows := __rows + 1;
            RETURN NEXT;
        END LOOP;
    END IF;

    IF __rows < __size AND __to > __sentinel THEN
        FOR first_block, url_md5, account_name IN
            SELECT * FROM proxy_whitelist_app.url_author_rows(
                GREATEST(__from, __sentinel + 1), __to,
                CASE WHEN __from > __sentinel THEN stream_url_authors.after_md5 END,
                CASE WHEN __from > __sentinel THEN stream_url_authors.after_account END,
                __size - __rows
            )
        LOOP
            __rows := __rows + 1;
            RETURN NEXT;
        END LOOP;
    END IF;
    PERFORM proxy_whitelist_app.end_stream_page(__to, __rows, __size, first_block);
END
$$;

-- GET /stream/profiles?from_block=&to_block=&after_account=&page_size=
CREATE OR REPLACE FUNCTION proxy_whitelist_endpoints.stream_profiles(
    "from_block" INT DEFAULT 0,
    "to_block" INT DEFAULT NULL,
    "after_account" TEXT DEFAULT NULL,
    "page_size" INT DEFAULT 1000
)
RETURNS TABLE (updated_block INT, account_name TEXT, avatar_url TEXT, cover_url TEXT)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    __to INT := proxy_whitelist_app.begin_stream_page(stream_profiles.from_block, stream_profiles.to_block);
    __size INT := proxy_whitelist_app.stream_page_size(stream_profiles.page_size);
    __rows INT := 0;
BEGIN
    FOR updated_block, account_name, avatar_url, cover_url IN
        SELECT p.updated_block, a.name::TEXT, p.avatar_url, p.cover_url
        FROM proxy_whitelist_app.account_profiles p
        JOIN proxy_whitelist_app.accounts a ON a.id = p.account_id
        WHERE p.updated_block BETWEEN stream_profiles.from_block AND __to
          AND (
              stream_profiles.after_account IS NULL
              OR p.updated_block > stream_profiles.from_block
              OR a.name COLLATE "C" > stream_profiles.after_account
          )
        ORDER BY p.updated_block, a.name COLLATE "C"
        LIMIT __size
    LOOP
        __rows := __rows + 1;
        RETURN NEXT;
    END LOOP;
    PERFORM proxy_whitelist_app.end_stream_page(__to, __rows, __size, updated_block);
END
$$;

-- GET /stream/blacklists — the operator's blacklists, whole: they are small
-- and not chain-derived, so they are not block-paged.
CREATE OR REPLACE FUNCTION proxy_whitelist_endpoints.stream_blacklists()
RETURNS JSON
LANGUAGE sql
STABLE
AS $$
    SELECT json_build_object(
        'urls', COALESCE((
            SELECT json_agg(u.url ORDER BY md5(u.url))
            FROM proxy_whitelist_app.blacklisted_urls bu
            JOIN proxy_whitelist_app.urls u ON u.id = bu.url_id
        ), '[]'::JSON),
        'accounts', COALESCE((
            SELECT json_agg(a.name ORDER BY a.name COLLATE "C")
            FROM proxy_whitelist_app.blacklisted_accounts ba
            JOIN proxy_whitelist_app.accounts a ON a.id = ba.account_id
        ), '[]'::JSON)
    );
$$;

GRANT EXECUTE ON FUNCTION proxy_whitelist_app.stream_page_cap() TO proxy_whitelist_user;
GRANT EXECUTE ON FUNCTION proxy_whitelist_app.stream_page_size(INT) TO proxy_whitelist_user;
GRANT EXECUTE ON FUNCTION proxy_whitelist_app.begin_stream_page(INT, INT) TO proxy_whitelist_user;
GRANT EXECUTE ON FUNCTION proxy_whitelist_app.end_stream_page(INT, INT, INT, INT) TO proxy_whitelist_user;
GRANT EXECUTE ON FUNCTION proxy_whitelist_app.url_author_rows(INT, INT, TEXT, TEXT, INT) TO proxy_whitelist_user;
GRANT EXECUTE ON FUNCTION proxy_whitelist_app.url_author_rows_by_md5(INT, TEXT, TEXT, INT) TO proxy_whitelist_user;
GRANT EXECUTE ON FUNCTION proxy_whitelist_endpoints.stream_accounts(INT, INT, TEXT, INT) TO proxy_whitelist_user;
GRANT EXECUTE ON FUNCTION proxy_whitelist_endpoints.stream_urls(INT, INT, TEXT, INT) TO proxy_whitelist_user;
GRANT EXECUTE ON FUNCTION proxy_whitelist_endpoints.stream_url_authors(INT, INT, TEXT, TEXT, INT) TO proxy_whitelist_user;
GRANT EXECUTE ON FUNCTION proxy_whitelist_endpoints.stream_profiles(INT, INT, TEXT, INT) TO proxy_whitelist_user;
GRANT EXECUTE ON FUNCTION proxy_whitelist_endpoints.stream_blacklists() TO proxy_whitelist_user;

RESET ROLE;
