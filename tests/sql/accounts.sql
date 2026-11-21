-- Every hivemind account has an accounts row: the per-range creation tracking
-- of process_blocks (track_account_creations), the install's backfill_accounts
-- and its stream label rule, and the avatar/cover lookup that follows from
-- them. Seeds hivemind_app.hive_accounts rows in the stack's HAF database. Run
-- against an installed app by .aidev/run-checks.sh's install step; any failed
-- case raises. Everything it writes is rolled back.
BEGIN;

-- Above every id of the stack's hivemind, so that they are its newest accounts.
INSERT INTO hivemind_app.hive_accounts (id, name, created_at)
SELECT
    2147000300 + i AS hive_id,
    format('acct-new-%s', i) AS account_name,
    now() AS created_at
FROM generate_series(1, 5) AS i;

-- Hivemind's placeholder account, which no backfill gives an accounts row.
INSERT INTO hivemind_app.hive_accounts (id, name, created_at)
VALUES (0, '', now())
ON CONFLICT DO NOTHING;

SET ROLE proxy_whitelist_owner;

DO $$
DECLARE
    __head CONSTANT INT := (SELECT stream_head_block FROM proxy_whitelist_app.app_status);
    __cursor CONSTANT INT := (SELECT accounts_tracked_hive_id FROM proxy_whitelist_app.app_status);
    __range hive.BLOCKS_RANGE := (__head + 1, __head + 3);
    __known INT;
    __done INT;
    __count INT;
    __got TEXT[];
    __profile proxy_whitelist_app.account_profiles;
BEGIN
    IF __cursor IS NULL OR __cursor >= 2147000301 THEN
        RAISE EXCEPTION 'install: accounts_tracked_hive_id is %, expected the highest hivemind id', __cursor;
    END IF;
    IF EXISTS (
        SELECT 1 FROM hivemind_app.hive_accounts ha
        WHERE ha.id <= __cursor AND ha.id != 0 AND ha.name != ''
          AND NOT EXISTS (SELECT 1 FROM proxy_whitelist_app.accounts a WHERE a.hive_id = ha.id)
    ) THEN
        RAISE EXCEPTION 'install: a hivemind account has no accounts row after backfill_accounts';
    END IF;

    -- One of the new accounts is already known (as an author at a later
    -- block): it keeps its id, name and first_block.
    __known := proxy_whitelist_app.ensure_account(2147000303, 'acct-new-3', __head + 2);

    -- A processed range adds the others, labelled with its first block,
    -- without a post or account update by them.
    CALL proxy_whitelist_app.process_blocks('proxy_whitelist_app', __range, 1, __done);
    __got := ARRAY(
        SELECT a.name || '|' || a.first_block
        FROM proxy_whitelist_app.accounts a
        WHERE a.hive_id > 2147000300
        ORDER BY a.name
    );
    IF __got IS DISTINCT FROM ARRAY[
        'acct-new-1|' || __head + 1, 'acct-new-2|' || __head + 1, 'acct-new-3|' || __head + 2,
        'acct-new-4|' || __head + 1, 'acct-new-5|' || __head + 1
    ] THEN
        RAISE EXCEPTION 'process_blocks: new hivemind accounts as %', __got;
    END IF;
    IF (SELECT id FROM proxy_whitelist_app.accounts WHERE hive_id = 2147000303) != __known THEN
        RAISE EXCEPTION 'process_blocks: an account known before its creation was tracked changed its id';
    END IF;
    IF (SELECT accounts_tracked_hive_id FROM proxy_whitelist_app.app_status) != 2147000305 THEN
        RAISE EXCEPTION 'process_blocks: the tracking cursor did not reach the newest hivemind account';
    END IF;
    IF proxy_whitelist_app.track_account_creations(__head + 4) != 0 THEN
        RAISE EXCEPTION 'track_account_creations: wrote rows with no new hivemind account';
    END IF;

    -- An existing account without a profile has an all-NULL profile; a name
    -- hivemind does not have is a 404.
    __profile := proxy_whitelist_app.account_profile_by_name('acct-new-1');
    IF (__profile.avatar_url, __profile.cover_url) IS DISTINCT FROM (NULL::TEXT, NULL::TEXT) THEN
        RAISE EXCEPTION 'account_profile_by_name: an account without a profile has %', __profile;
    END IF;
    BEGIN
        PERFORM proxy_whitelist_app.account_profile_by_name('acct-new-none');
        RAISE EXCEPTION 'account_profile_by_name: a name hivemind does not have did not raise';
    EXCEPTION WHEN SQLSTATE 'PT404' THEN
        NULL;
    END;

    -- The install backfill of a store whose accounts lack hivemind accounts
    -- (none tracked yet): every one gets a row above the stream head it
    -- found, and a second run writes nothing.
    DELETE FROM proxy_whitelist_app.accounts WHERE hive_id IN (2147000301, 2147000302, 2147000304, 2147000305);
    UPDATE proxy_whitelist_app.app_status SET accounts_tracked_hive_id = NULL, stream_head_block = __head + 10;
    __count := proxy_whitelist_app.backfill_accounts();
    IF __count != 4 THEN
        RAISE EXCEPTION 'backfill_accounts: wrote % rows, expected the 4 missing accounts', __count;
    END IF;
    IF EXISTS (
        SELECT 1 FROM hivemind_app.hive_accounts ha
        WHERE ha.id != 0 AND ha.name != ''
          AND NOT EXISTS (SELECT 1 FROM proxy_whitelist_app.accounts a WHERE a.hive_id = ha.id)
    ) THEN
        RAISE EXCEPTION 'backfill_accounts: a hivemind account still has no accounts row';
    END IF;
    IF EXISTS (SELECT 1 FROM proxy_whitelist_app.accounts WHERE hive_id = 0 OR name = '') THEN
        RAISE EXCEPTION 'backfill_accounts: hivemind''s placeholder account has an accounts row';
    END IF;
    __got := ARRAY(
        SELECT a.name || '|' || a.first_block
        FROM proxy_whitelist_app.accounts a
        WHERE a.hive_id > 2147000300 AND a.first_block <= __head + 10
        ORDER BY a.name
    );
    IF __got IS DISTINCT FROM ARRAY['acct-new-3|' || __head + 2] THEN
        RAISE EXCEPTION 'backfill_accounts: rows at or below the stream head: %', __got;
    END IF;
    IF (SELECT count(*) FROM proxy_whitelist_app.accounts WHERE first_block = __head + 11) != 4 THEN
        RAISE EXCEPTION 'backfill_accounts: the backfilled rows are not labelled stream_head_block + 1';
    END IF;
    IF proxy_whitelist_app.backfill_accounts() != 0 THEN
        RAISE EXCEPTION 'backfill_accounts: a second run wrote rows';
    END IF;

    RAISE NOTICE 'accounts: all cases pass';
END
$$;

ROLLBACK;
