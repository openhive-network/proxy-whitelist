-- The replication streams (endpoints/stream.sql), the block-labelled writers
-- that feed them (ensure_account, whitelist_urls, set_account_profile), the
-- stream source identity on a context rewind (check_stream_rewind) and the
-- avatar/cover lookup. Run against an installed app by .aidev/run-checks.sh's install
-- step; any failed case raises. Everything it writes is rolled back.
BEGIN;
SET ROLE proxy_whitelist_owner;

DO $$
DECLARE
    __s CONSTANT INT := (SELECT stream_sentinel_block FROM proxy_whitelist_app.app_status);
    __b CONSTANT INT := __s + 10;
    __head CONSTANT INT := __s + 100;
    -- Five accounts introduced in one block, their profiles in the next.
    __a CONSTANT INT := __s + 40;
    __five CONSTANT TEXT[] := ARRAY['stream-p1', 'stream-p2', 'stream-p3', 'stream-p4', 'stream-p5'];
    __case RECORD;
    __img RECORD;
    __alice INT;
    __bob INT;
    __carol INT;
    __got TEXT[];
    __want TEXT[];
    __page TEXT[];
    __from INT;
    __after_md5 TEXT;
    __after_account TEXT;
    __count INT;
    __sizes INT[];
    __heads INT[];
    __to_block INT;
    __md5_scans BIGINT;
    __uuid UUID;
    __reminted BOOLEAN;
    __profile proxy_whitelist_app.account_profiles;
BEGIN
    PERFORM hive.app_set_current_block_num('proxy_whitelist_app', __head);

    -- extract_profile_images: posting_json_metadata wins per key, the parse
    -- rules are extract_profile_urls'.
    FOR __case IN
        SELECT *
        FROM (VALUES
            ('both from posting',
             '{"profile":{"profile_image":"https://p/a.png","cover_image":"https://p/c.png"}}', NULL,
             'https://p/a.png', 'https://p/c.png'),
            ('posting avatar, json cover',
             '{"profile":{"profile_image":"https://p/a.png"}}',
             '{"profile":{"profile_image":"https://j/a.png","cover_image":"https://j/c.png"}}',
             'https://p/a.png', 'https://j/c.png'),
            ('unparsable posting, flat json', '{not json', '{"profile_image":"https://j/a.png"}',
             'https://j/a.png', NULL),
            ('non-http values are ignored', '{"profile":{"profile_image":"ipfs://x","cover_image":7}}', NULL,
             NULL, NULL),
            ('no metadata', NULL, NULL, NULL, NULL)
        ) AS c (name, posting, json, avatar, cover)
    LOOP
        __img := proxy_whitelist_app.extract_profile_images(__case.posting, __case.json);
        IF (__img.avatar_url, __img.cover_url) IS DISTINCT FROM (__case.avatar, __case.cover) THEN
            RAISE EXCEPTION 'extract_profile_images: %: expected (%, %), got (%, %)',
                __case.name, __case.avatar, __case.cover, __img.avatar_url, __img.cover_url;
        END IF;
    END LOOP;

    __alice := proxy_whitelist_app.ensure_account(2147000201, 'stream-alice', __b);
    __bob := proxy_whitelist_app.ensure_account(2147000202, 'stream-bob', __b);
    __carol := proxy_whitelist_app.ensure_account(2147000203, 'stream-carol', __b);
    IF proxy_whitelist_app.ensure_account(2147000201, 'stream-alice', __b + 1) != __alice
       OR (SELECT first_block FROM proxy_whitelist_app.accounts WHERE id = __alice) != __b THEN
        RAISE EXCEPTION 'ensure_account: a known account changed its id or lost its first_block';
    END IF;

    -- whitelist_urls labels new rows with its block; existing rows keep theirs.
    PERFORM proxy_whitelist_app.whitelist_urls(
        ARRAY(SELECT format('https://stream.invalid/%s.png', i) FROM generate_series(1, 5) i), __alice, __b
    );
    PERFORM proxy_whitelist_app.whitelist_urls(ARRAY['https://stream.invalid/1.png'], __bob, __b + 5);
    IF (SELECT first_block FROM proxy_whitelist_app.urls WHERE url = 'https://stream.invalid/1.png') != __b THEN
        RAISE EXCEPTION 'whitelist_urls: a URL seen again at a later block lost its first_block';
    END IF;
    IF (SELECT ua.first_block FROM proxy_whitelist_app.url_authors ua
        JOIN proxy_whitelist_app.urls u ON u.id = ua.url_id
        WHERE u.url = 'https://stream.invalid/1.png' AND ua.author_id = __bob) != __b + 5 THEN
        RAISE EXCEPTION 'whitelist_urls: a new author association does not carry its own block';
    END IF;

    -- stream_urls: pages of 2 over [__b, __b + 3], whose rows are the 5 URLs
    -- of block __b, are 2, 2 and 1 rows; a full page's Stream-To-Block is its
    -- last row's block, the short one's the applied upper bound, and
    -- following (Stream-To-Block, last md5) covers each URL exactly once.
    __got := '{}';
    __sizes := '{}';
    __heads := '{}';
    __from := __b;
    __after_md5 := NULL;
    LOOP
        __page := ARRAY(
            SELECT p.first_block || '|' || p.md5
            FROM proxy_whitelist_endpoints.stream_urls(__from, __b + 3, __after_md5, 2) p
        );
        __to_block := (current_setting('response.headers')::JSON -> 0 ->> 'Stream-To-Block')::INT;
        __got := __got || __page;
        __sizes := __sizes || cardinality(__page);
        __heads := __heads || __to_block;
        EXIT WHEN cardinality(__page) < 2;
        __from := __to_block;
        __after_md5 := split_part(__page[2], '|', 2);
    END LOOP;
    __want := ARRAY(
        SELECT __b || '|' || md5(format('https://stream.invalid/%s.png', i))
        FROM generate_series(1, 5) i
        ORDER BY 1
    );
    IF __got IS DISTINCT FROM __want THEN
        RAISE EXCEPTION 'stream_urls paging: expected %, got %', __want, __got;
    END IF;
    IF (__sizes, __heads) IS DISTINCT FROM (ARRAY[2, 2, 1], ARRAY[__b, __b, __b + 3]) THEN
        RAISE EXCEPTION 'stream_urls pages: expected sizes (2, 2, 1) and Stream-To-Block (%, %, %), got % and %',
            __b, __b, __b + 3, __sizes, __heads;
    END IF;

    -- url_authors: the 5 rows of block __b in pages of 2, as for urls.
    __sizes := '{}';
    __heads := '{}';
    __from := __b;
    __after_md5 := NULL;
    __after_account := NULL;
    LOOP
        __page := ARRAY(
            SELECT p.first_block || '|' || p.url_md5 || '|' || p.account_name
            FROM proxy_whitelist_endpoints.stream_url_authors(__from, __b + 3, __after_md5, __after_account, 2) p
        );
        __to_block := (current_setting('response.headers')::JSON -> 0 ->> 'Stream-To-Block')::INT;
        __sizes := __sizes || cardinality(__page);
        __heads := __heads || __to_block;
        EXIT WHEN cardinality(__page) < 2;
        __from := __to_block;
        __after_md5 := split_part(__page[2], '|', 2);
        __after_account := split_part(__page[2], '|', 3);
    END LOOP;
    IF (__sizes, __heads) IS DISTINCT FROM (ARRAY[2, 2, 1], ARRAY[__b, __b, __b + 3]) THEN
        RAISE EXCEPTION 'stream_url_authors pages: expected sizes (2, 2, 1) and Stream-To-Block (%, %, %), got % and %',
            __b, __b, __b + 3, __sizes, __heads;
    END IF;

    -- A sentinel block no url_authors row carries is not read by md5: a page
    -- over it does not touch urls_url_md5_idx and still returns the rows
    -- around it.
    UPDATE proxy_whitelist_app.app_status SET stream_sentinel_block = __b + 2;
    __md5_scans := pg_stat_get_xact_numscans('proxy_whitelist_app.urls_url_md5_idx'::REGCLASS);
    __got := ARRAY(
        SELECT p.first_block || '|' || p.account_name
        FROM proxy_whitelist_endpoints.stream_url_authors(__b, __b + 5) p
    );
    IF pg_stat_get_xact_numscans('proxy_whitelist_app.urls_url_md5_idx'::REGCLASS) != __md5_scans THEN
        RAISE EXCEPTION 'stream_url_authors read urls by md5 for a sentinel block without rows';
    END IF;
    __want := ARRAY[__b || '|stream-alice', __b || '|stream-alice', __b || '|stream-alice',
                    __b || '|stream-alice', __b || '|stream-alice', (__b + 5) || '|stream-bob'];
    IF __got IS DISTINCT FROM __want THEN
        RAISE EXCEPTION 'stream_url_authors around an empty sentinel block: expected %, got %', __want, __got;
    END IF;
    UPDATE proxy_whitelist_app.app_status SET stream_sentinel_block = __s;

    -- The url_authors tiebreak is given whole or not at all.
    BEGIN
        PERFORM proxy_whitelist_endpoints.stream_url_authors(__b, __b, md5('https://stream.invalid/1.png'));
        RAISE EXCEPTION 'stream_url_authors with after_md5 but no after_account did not raise';
    EXCEPTION WHEN invalid_parameter_value THEN
        NULL;
    END;

    -- url_authors at the sentinel block are read by md5, the rest sorted;
    -- paging across both must still cover every row once, in stream order.
    INSERT INTO proxy_whitelist_app.urls (url, first_block)
    SELECT format('https://stream.invalid/old-%s.png', i), __s FROM generate_series(1, 3) i;
    INSERT INTO proxy_whitelist_app.url_authors (url_id, author_id, first_block)
    SELECT u.id, a.id, __s
    FROM proxy_whitelist_app.urls u
    CROSS JOIN (VALUES (__alice), (__carol)) a (id)
    WHERE u.url LIKE 'https://stream.invalid/old-%';

    __want := ARRAY(
        SELECT ua.first_block || '|' || md5(u.url) || '|' || a.name
        FROM proxy_whitelist_app.url_authors ua
        JOIN proxy_whitelist_app.urls u ON u.id = ua.url_id
        JOIN proxy_whitelist_app.accounts a ON a.id = ua.author_id
        WHERE ua.first_block BETWEEN __s AND __b + 5
        ORDER BY ua.first_block, md5(u.url), a.name COLLATE "C"
    );
    IF cardinality(__want) < 6 + 5 + 1 THEN
        RAISE EXCEPTION 'url_authors seed: expected at least 12 rows, found %', cardinality(__want);
    END IF;
    __got := '{}';
    __from := __s;
    __after_md5 := NULL;
    __after_account := NULL;
    LOOP
        __page := ARRAY(
            SELECT p.first_block || '|' || p.url_md5 || '|' || p.account_name
            FROM proxy_whitelist_endpoints.stream_url_authors(__from, __b + 5, __after_md5, __after_account, 2) p
        );
        __got := __got || __page;
        EXIT WHEN cardinality(__page) < 2;
        __from := (current_setting('response.headers')::JSON -> 0 ->> 'Stream-To-Block')::INT;
        __after_md5 := split_part(__page[2], '|', 2);
        __after_account := split_part(__page[2], '|', 3);
    END LOOP;
    IF __got IS DISTINCT FROM __want THEN
        RAISE EXCEPTION 'stream_url_authors paging: expected %, got %', __want, __got;
    END IF;

    -- Rows above the app's processed block are not served yet.
    PERFORM proxy_whitelist_app.whitelist_urls(ARRAY['https://stream.invalid/future.png'], __alice, __head + 1);
    IF EXISTS (SELECT 1 FROM proxy_whitelist_endpoints.stream_urls(__head, __head + 10) p
               WHERE p.url = 'https://stream.invalid/future.png') THEN
        RAISE EXCEPTION 'stream_urls served a row above the processed block';
    END IF;

    -- set_account_profile: a change moves updated_block, an unchanged
    -- profile keeps it, removing both images keeps a NULL row, an account
    -- that never had one gets no row.
    PERFORM proxy_whitelist_app.set_account_profile(2147000201, 'stream-alice', 'https://p/a1.png', 'https://p/c1.png', __b);
    PERFORM proxy_whitelist_app.set_account_profile(2147000201, 'stream-alice', 'https://p/a1.png', 'https://p/c1.png', __b + 1);
    SELECT * INTO __profile FROM proxy_whitelist_app.account_profiles WHERE account_id = __alice;
    IF __profile.updated_block != __b THEN
        RAISE EXCEPTION 'set_account_profile: an unchanged profile moved to block %', __profile.updated_block;
    END IF;
    PERFORM proxy_whitelist_app.set_account_profile(2147000201, 'stream-alice', 'https://p/a2.png', 'https://p/c1.png', __b + 2);
    __got := ARRAY(
        SELECT p.updated_block || '|' || p.account_name || '|' || p.avatar_url || '|' || p.cover_url
        FROM proxy_whitelist_endpoints.stream_profiles(__b + 2, __b + 2) p
    );
    IF __got IS DISTINCT FROM ARRAY[(__b + 2) || '|stream-alice|https://p/a2.png|https://p/c1.png'] THEN
        RAISE EXCEPTION 'stream_profiles after a change: got %', __got;
    END IF;
    IF EXISTS (SELECT 1 FROM proxy_whitelist_endpoints.stream_profiles(__b, __b + 1) p WHERE p.account_name = 'stream-alice') THEN
        RAISE EXCEPTION 'stream_profiles: a changed profile is still served at its old block';
    END IF;
    IF proxy_whitelist_endpoints.avatar('stream-alice') IS DISTINCT FROM 'https://p/a2.png'
       OR proxy_whitelist_endpoints.cover('stream-alice') IS DISTINCT FROM 'https://p/c1.png' THEN
        RAISE EXCEPTION 'avatar/cover do not return the current profile of stream-alice';
    END IF;

    PERFORM proxy_whitelist_app.set_account_profile(2147000201, 'stream-alice', NULL, NULL, __b + 3);
    SELECT * INTO __profile FROM proxy_whitelist_app.account_profiles WHERE account_id = __alice;
    IF (__profile.avatar_url, __profile.cover_url, __profile.updated_block) IS DISTINCT FROM (NULL::TEXT, NULL::TEXT, __b + 3) THEN
        RAISE EXCEPTION 'set_account_profile: removing both images left %', __profile;
    END IF;
    PERFORM proxy_whitelist_app.set_account_profile(2147000204, 'stream-dave', NULL, NULL, __b);
    IF EXISTS (SELECT 1 FROM proxy_whitelist_app.accounts WHERE hive_id = 2147000204) THEN
        RAISE EXCEPTION 'set_account_profile: an account without images was added';
    END IF;

    -- Profile paging tiebreak on the account name.
    PERFORM proxy_whitelist_app.set_account_profile(2147000202, 'stream-bob', 'https://p/b.png', NULL, __b + 4);
    PERFORM proxy_whitelist_app.set_account_profile(2147000203, 'stream-carol', NULL, 'https://p/c.png', __b + 4);
    __got := ARRAY(
        SELECT p.account_name
        FROM proxy_whitelist_endpoints.stream_profiles(__b + 4, __b + 4, 'stream-bob') p
    );
    IF __got IS DISTINCT FROM ARRAY['stream-carol'] THEN
        RAISE EXCEPTION 'stream_profiles after_account: expected {stream-carol}, got %', __got;
    END IF;

    -- avatar/cover: null for a known account without one, 404 for an unknown one.
    IF proxy_whitelist_endpoints.cover('stream-bob') IS NOT NULL THEN
        RAISE EXCEPTION 'cover of an account without a cover is not null';
    END IF;
    BEGIN
        PERFORM proxy_whitelist_endpoints.avatar('stream-nobody');
        RAISE EXCEPTION 'avatar of an unknown account did not raise';
    EXCEPTION WHEN SQLSTATE 'PT404' THEN
        NULL;
    END;

    -- stream_accounts pages accounts by (first_block, name), not by id: an
    -- account whose id is lower than those already streamed (as when a side
    -- session's insert commits late) is still delivered at its block.
    FOR __count IN 1..4 LOOP
        PERFORM proxy_whitelist_app.ensure_account(2147000210 + __count, __five[__count], __a);
    END LOOP;
    INSERT INTO proxy_whitelist_app.accounts (id, hive_id, name, first_block)
    VALUES (-2147000215, 2147000215, __five[5], __a);
    __got := '{}';
    __sizes := '{}';
    __heads := '{}';
    __from := __a;
    __after_account := NULL;
    LOOP
        __page := ARRAY(
            SELECT p.first_block || '|' || p.name
            FROM proxy_whitelist_endpoints.stream_accounts(__from, __a + 2, __after_account, 2) p
        );
        __to_block := (current_setting('response.headers')::JSON -> 0 ->> 'Stream-To-Block')::INT;
        __got := __got || __page;
        __sizes := __sizes || cardinality(__page);
        __heads := __heads || __to_block;
        EXIT WHEN cardinality(__page) < 2;
        __from := __to_block;
        __after_account := split_part(__page[2], '|', 2);
    END LOOP;
    __want := ARRAY(SELECT __a || '|' || n FROM unnest(__five) n ORDER BY n COLLATE "C");
    IF __got IS DISTINCT FROM __want THEN
        RAISE EXCEPTION 'stream_accounts paging: expected %, got %', __want, __got;
    END IF;
    IF (__sizes, __heads) IS DISTINCT FROM (ARRAY[2, 2, 1], ARRAY[__a, __a, __a + 2]) THEN
        RAISE EXCEPTION 'stream_accounts pages: expected sizes (2, 2, 1) and Stream-To-Block (%, %, %), got % and %',
            __a, __a, __a + 2, __sizes, __heads;
    END IF;

    -- stream_profiles: the 5 profiles of block __a + 1 in pages of 2.
    FOR __count IN 1..5 LOOP
        PERFORM proxy_whitelist_app.set_account_profile(
            2147000210 + __count, __five[__count], format('https://p/%s.png', __count), NULL, __a + 1
        );
    END LOOP;
    __got := '{}';
    __sizes := '{}';
    __heads := '{}';
    __from := __a + 1;
    __after_account := NULL;
    LOOP
        __page := ARRAY(
            SELECT p.updated_block || '|' || p.account_name
            FROM proxy_whitelist_endpoints.stream_profiles(__from, __a + 3, __after_account, 2) p
        );
        __to_block := (current_setting('response.headers')::JSON -> 0 ->> 'Stream-To-Block')::INT;
        __got := __got || __page;
        __sizes := __sizes || cardinality(__page);
        __heads := __heads || __to_block;
        EXIT WHEN cardinality(__page) < 2;
        __from := __to_block;
        __after_account := split_part(__page[2], '|', 2);
    END LOOP;
    __want := ARRAY(SELECT (__a + 1) || '|' || n FROM unnest(__five) n ORDER BY n COLLATE "C");
    IF __got IS DISTINCT FROM __want THEN
        RAISE EXCEPTION 'stream_profiles paging: expected %, got %', __want, __got;
    END IF;
    IF (__sizes, __heads) IS DISTINCT FROM (ARRAY[2, 2, 1], ARRAY[__a + 1, __a + 1, __a + 3]) THEN
        RAISE EXCEPTION 'stream_profiles pages: expected sizes (2, 2, 1) and Stream-To-Block (%, %, %), got % and %',
            __a + 1, __a + 1, __a + 3, __sizes, __heads;
    END IF;

    -- Range and page size validation.
    BEGIN
        PERFORM proxy_whitelist_endpoints.stream_urls(__b, __b, NULL, 0);
        RAISE EXCEPTION 'stream_urls with page_size 0 did not raise';
    EXCEPTION WHEN invalid_parameter_value THEN
        NULL;
    END;
    FOR __case IN
        SELECT * FROM (VALUES (-1, NULL::INT), (10, 9)) AS c (from_block, to_block)
    LOOP
        BEGIN
            PERFORM proxy_whitelist_app.begin_stream_page(__case.from_block, __case.to_block);
            RAISE EXCEPTION 'begin_stream_page(%, %) did not raise', __case.from_block, __case.to_block;
        EXCEPTION WHEN invalid_parameter_value THEN
            NULL;
        END;
    END LOOP;

    -- The account_profiles backfill ran at install and does not run again.
    SELECT proxy_whitelist_app.backfill_account_profiles() INTO __count;
    IF __count != 0 OR NOT (SELECT account_profiles_backfilled FROM proxy_whitelist_app.app_status) THEN
        RAISE EXCEPTION 'backfill_account_profiles: not a no-op after install (wrote % rows)', __count;
    END IF;

    -- check_stream_rewind: a restart at or past the recorded stream head keeps
    -- sync_uuid; the context moved back below it re-mints it once. (The check
    -- is a statement of its own: a query reads app_status with the snapshot
    -- it started with.)
    UPDATE proxy_whitelist_app.app_status SET stream_head_block = __head;
    __uuid := (SELECT sync_uuid FROM proxy_whitelist_app.app_status);
    __reminted := proxy_whitelist_app.check_stream_rewind(hive.app_get_current_block_num('proxy_whitelist_app'));
    IF __reminted OR (SELECT sync_uuid FROM proxy_whitelist_app.app_status) != __uuid THEN
        RAISE EXCEPTION 'check_stream_rewind: re-minted sync_uuid on a normal restart';
    END IF;
    PERFORM hive.app_set_current_block_num('proxy_whitelist_app', __head - 50);
    __reminted := proxy_whitelist_app.check_stream_rewind(hive.app_get_current_block_num('proxy_whitelist_app'));
    IF NOT __reminted
       OR (SELECT sync_uuid FROM proxy_whitelist_app.app_status) = __uuid
       OR (SELECT stream_head_block FROM proxy_whitelist_app.app_status) != __head - 50 THEN
        RAISE EXCEPTION 'check_stream_rewind: the context moved back below the stream head kept sync_uuid';
    END IF;
    IF proxy_whitelist_endpoints.sync_status() ->> 'sync_uuid' = __uuid::TEXT
       OR (proxy_whitelist_endpoints.sync_status() ->> 'stream_schema_version')::INT != 2 THEN
        RAISE EXCEPTION 'sync_status: % after a rewind', proxy_whitelist_endpoints.sync_status();
    END IF;
    __uuid := (SELECT sync_uuid FROM proxy_whitelist_app.app_status);
    IF proxy_whitelist_app.check_stream_rewind(hive.app_get_current_block_num('proxy_whitelist_app')) THEN
        RAISE EXCEPTION 'check_stream_rewind: re-minted twice for one rewind';
    END IF;

    RAISE NOTICE 'streams: all cases pass';
END
$$;

ROLLBACK;
