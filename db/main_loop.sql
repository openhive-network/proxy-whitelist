SET ROLE proxy_whitelist_owner;

-- Process a batch of posts: extract image URLs, resolve proxy URLs, write to PostgreSQL.
CREATE OR REPLACE FUNCTION proxy_whitelist_app.process_posts(
    _post_ids INT[],
    _worker INT
)
RETURNS INT
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    rec RECORD;
    __urls TEXT[];
    __resolved TEXT[];
    __all_resolved TEXT[];
    __total INT := 0;
    __batch_count INT;
    __local_account_id INT;
BEGIN
    __batch_count := 0;

    FOR rec IN
        SELECT hp.id AS post_id,
               hpd.body,
               hpd.json,
               ha.id AS hive_account_id,
               ha.name AS author,
               hp.block_num
        FROM unnest(_post_ids) AS sel(id)
        JOIN hivemind_app.hive_posts hp ON hp.id = sel.id
        JOIN hivemind_app.hive_post_data hpd ON hpd.id = hp.id
        JOIN hivemind_app.hive_accounts ha ON ha.id = hp.author_id
        WHERE hp.counter_deleted = 0
    LOOP
        -- Ensure author exists in local accounts table
        __local_account_id := proxy_whitelist_app.ensure_account(rec.hive_account_id, rec.author);

        -- Extract raw image URLs from body + json_metadata
        __urls := proxy_whitelist_app.extract_image_urls(rec.body, rec.json);

        -- Resolve each URL to original upstream URL
        __all_resolved := ARRAY[]::TEXT[];
        FOR i IN 1..COALESCE(array_length(__urls, 1), 0) LOOP
            __resolved := proxy_whitelist_app.resolve_proxy_url(__urls[i]);
            IF __resolved IS NOT NULL THEN
                __all_resolved := __all_resolved || __resolved;
            END IF;
        END LOOP;

        -- Write to PostgreSQL
        IF array_length(__all_resolved, 1) > 0 THEN
            __total := __total + proxy_whitelist_app.whitelist_urls(
                __all_resolved, __local_account_id
            );
        END IF;

        -- Track processed post
        INSERT INTO proxy_whitelist_app.processed_posts (post_id, last_block_num)
        VALUES (rec.post_id, rec.block_num)
        ON CONFLICT (post_id) DO UPDATE
            SET last_block_num = EXCLUDED.last_block_num;

        __batch_count := __batch_count + 1;
    END LOOP;

    RETURN __total;
END;
$$;

-- Full-scan fallback: process profile images from ALL accounts.
-- Kept for manual backfill/recovery. Use process_profile_images_incremental() for normal operation.
CREATE OR REPLACE FUNCTION proxy_whitelist_app.process_profile_images_full_scan()
RETURNS INT
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    rec RECORD;
    __profile_urls TEXT[];
    __resolved TEXT[];
    __all_resolved TEXT[];
    __total INT := 0;
    __local_account_id INT;
BEGIN
    FOR rec IN
        SELECT ha.id AS hive_account_id,
               ha.name,
               ha.posting_json_metadata,
               ha.json_metadata
        FROM hivemind_app.hive_accounts ha
        WHERE ha.posting_json_metadata ~ '"profile_image"'
           OR ha.json_metadata ~ '"profile_image"'
           OR ha.posting_json_metadata ~ '"cover_image"'
           OR ha.json_metadata ~ '"cover_image"'
    LOOP
        __profile_urls := proxy_whitelist_app.extract_profile_urls(
            rec.posting_json_metadata, rec.json_metadata
        );

        IF __profile_urls IS NULL THEN
            CONTINUE;
        END IF;

        -- Ensure author exists in local accounts table
        __local_account_id := proxy_whitelist_app.ensure_account(rec.hive_account_id, rec.name);

        -- Resolve each profile URL
        __all_resolved := ARRAY[]::TEXT[];
        FOR i IN 1..COALESCE(array_length(__profile_urls, 1), 0) LOOP
            __resolved := proxy_whitelist_app.resolve_proxy_url(__profile_urls[i]);
            IF __resolved IS NOT NULL THEN
                __all_resolved := __all_resolved || __resolved;
            END IF;
        END LOOP;

        IF array_length(__all_resolved, 1) > 0 THEN
            __total := __total + proxy_whitelist_app.whitelist_urls(
                __all_resolved, __local_account_id
            );
        END IF;
    END LOOP;

    RAISE NOTICE 'Profile images (full scan): whitelisted % URLs', __total;
    RETURN __total;
END;
$$;

-- Drop the old name if it exists (renamed to _full_scan above).
DROP FUNCTION IF EXISTS proxy_whitelist_app.process_profile_images();

-- Incremental profile image processing: only process accounts that had
-- account_update or account_update2 operations in the given block range.
-- Uses hafd.operations directly (not operations_view) for performance.
CREATE OR REPLACE FUNCTION proxy_whitelist_app.process_profile_images_incremental(
    _first_block INT, _last_block INT
)
RETURNS INT
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    __rec RECORD;
    __urls TEXT[];
    __resolved TEXT[];
    __all_urls TEXT[];
    __count INT := 0;
    __total INT := 0;
    __local_account_id INT;
BEGIN
    FOR __rec IN
        SELECT DISTINCT ha.id AS hive_account_id,
               ha.name, ha.posting_json_metadata, ha.json_metadata
        FROM hafd.operations ho
        JOIN hivemind_app.hive_accounts ha
            ON ha.name = ho.body_value->>'account'
        WHERE hafd.operation_id_to_block_num(ho.id) BETWEEN _first_block AND _last_block
          AND ho.op_type_id IN (10, 43)
    LOOP
        __urls := proxy_whitelist_app.extract_profile_urls(
            __rec.posting_json_metadata, __rec.json_metadata
        );
        IF __urls IS NULL OR array_length(__urls, 1) IS NULL THEN
            CONTINUE;
        END IF;

        -- Ensure author exists in local accounts table
        __local_account_id := proxy_whitelist_app.ensure_account(__rec.hive_account_id, __rec.name);

        __all_urls := '{}';
        FOR i IN 1..array_length(__urls, 1) LOOP
            __resolved := proxy_whitelist_app.resolve_proxy_url(__urls[i]);
            IF __resolved IS NOT NULL THEN
                __all_urls := __all_urls || __resolved;
            END IF;
        END LOOP;

        IF array_length(__all_urls, 1) IS NOT NULL THEN
            __count := proxy_whitelist_app.whitelist_urls(
                __all_urls, __local_account_id
            );
            __total := __total + __count;
        END IF;
    END LOOP;

    IF __total > 0 THEN
        RAISE NOTICE 'Profile images (blocks %-%) : whitelisted % URLs',
            _first_block, _last_block, __total;
    END IF;

    RETURN __total;
END;
$$;

-- Block range processing wrapper for massive processing stage.
CREATE OR REPLACE FUNCTION proxy_whitelist_app.block_range_data(
    _from INT, _to INT, _logs BOOLEAN, _worker INT
)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    __total INT := 0;
    __post_ids INT[];
BEGIN
    -- Get all posts in this block range
    SELECT ARRAY_AGG(hp.id ORDER BY hp.id)
    INTO __post_ids
    FROM hivemind_app.hive_posts hp
    WHERE (
        hp.block_num_created BETWEEN _from AND _to
        OR hp.block_num BETWEEN _from AND _to
    )
    AND hp.counter_deleted = 0;

    IF __post_ids IS NOT NULL AND array_length(__post_ids, 1) > 0 THEN
        __total := proxy_whitelist_app.process_posts(__post_ids, _worker);
        IF _logs THEN
            RAISE NOTICE 'worker % blocks %-% : % posts, % URLs whitelisted',
                         _worker, _from, _to,
                         array_length(__post_ids, 1), __total;
        END IF;
    END IF;

    RETURN __total;
END;
$$;

-- Re-extract image URLs from every post created or edited in a block range.
-- Use after an extraction change (e.g. adding markdown image support) so that
-- posts processed by older code pick up the URLs it missed. Work is done in
-- chunks of _chunk_size blocks, each committed separately, so it can run for
-- hours and be interrupted/resumed by block number. Safe to run alongside live
-- block processing: it only does ON CONFLICT DO NOTHING inserts and idempotent
-- upserts. _to_block defaults to the app's current block.
--
--   CALL proxy_whitelist_app.backfill_posts(<first block>);
--   CALL proxy_whitelist_app.backfill_posts_since('3 months');
CREATE OR REPLACE PROCEDURE proxy_whitelist_app.backfill_posts(
    IN _from_block INT,
    IN _to_block INT DEFAULT NULL,
    IN _chunk_size INT DEFAULT 10000
)
LANGUAGE plpgsql
AS $$
DECLARE
    __from INT := _from_block;
    __to INT;
    __chunk_to INT;
    __chunk_done INT;
    __total INT := 0;
    __attempt INT;
    __started TIMESTAMPTZ := clock_timestamp();
BEGIN
    IF _chunk_size < 1 THEN
        RAISE EXCEPTION 'backfill_posts: _chunk_size must be >= 1';
    END IF;
    __to := COALESCE(_to_block, hive.app_get_current_block_num('proxy_whitelist_app'));
    IF __to IS NULL OR __to < __from THEN
        RAISE NOTICE 'backfill_posts: nothing to do (from % to %)', __from, __to;
        RETURN;
    END IF;

    RAISE NOTICE 'backfill_posts: blocks %-% in chunks of %', __from, __to, _chunk_size;

    WHILE __from <= __to LOOP
        __chunk_to := LEAST(__from + _chunk_size - 1, __to);
        __attempt := 0;

        -- Retry a chunk on transient failures (e.g. deadlock with the live
        -- block processor). The failed subtransaction is rolled back by the
        -- exception handler; COMMIT below only ever sees a clean chunk.
        LOOP
            BEGIN
                __chunk_done := proxy_whitelist_app.block_range_data(__from, __chunk_to, false, 0);
                EXIT;
            EXCEPTION WHEN OTHERS THEN
                __attempt := __attempt + 1;
                IF __attempt >= 3 THEN
                    RAISE;
                END IF;
                RAISE WARNING 'backfill_posts: blocks %-% failed (attempt %): %; retrying',
                              __from, __chunk_to, __attempt, SQLERRM;
                PERFORM pg_sleep(2);
            END;
        END LOOP;
        COMMIT;

        __total := __total + COALESCE(__chunk_done, 0);
        RAISE NOTICE 'backfill_posts: blocks %-% done, % new URL associations (% total, % elapsed, % blocks/s)',
                     __from, __chunk_to, COALESCE(__chunk_done, 0), __total,
                     date_trunc('second', clock_timestamp() - __started),
                     round((__chunk_to - _from_block + 1)
                           / GREATEST(EXTRACT(EPOCH FROM clock_timestamp() - __started), 1));
        __from := __chunk_to + 1;
    END LOOP;

    RAISE NOTICE 'backfill_posts: finished blocks %-%, % new URL associations in %',
                 _from_block, __to, __total, date_trunc('second', clock_timestamp() - __started);
END
$$;

-- Convenience wrapper: backfill posts from the first block at or after
-- now() - _since up to the app's current block.
CREATE OR REPLACE PROCEDURE proxy_whitelist_app.backfill_posts_since(
    IN _since INTERVAL,
    IN _chunk_size INT DEFAULT 10000
)
LANGUAGE plpgsql
AS $$
DECLARE
    __from_block INT;
BEGIN
    -- ORDER BY created_at so the planner walks hive_blocks_created_at_idx
    -- from the cutoff instead of scanning the primary key from block 1.
    SELECT num INTO __from_block
    FROM hafd.blocks
    WHERE created_at >= now() - _since
    ORDER BY created_at
    LIMIT 1;

    IF __from_block IS NULL THEN
        RAISE NOTICE 'backfill_posts_since: no blocks newer than % ago', _since;
        RETURN;
    END IF;

    CALL proxy_whitelist_app.backfill_posts(__from_block, NULL, _chunk_size);
END
$$;

CREATE OR REPLACE PROCEDURE proxy_whitelist_app.massive_processing(
    IN _from INT, IN _to INT, IN _logs BOOLEAN, IN _worker INT, OUT _done INT
)
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM set_config('synchronous_commit', 'OFF', false);
    SELECT proxy_whitelist_app.block_range_data(_from, _to, _logs, _worker) INTO _done;
END
$$;

CREATE OR REPLACE PROCEDURE proxy_whitelist_app.single_processing(
    IN _from INT, IN _to INT, IN _logs BOOLEAN, IN _worker INT, OUT _done INT
)
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM set_config('synchronous_commit', 'ON', false);
    SELECT proxy_whitelist_app.block_range_data(_from, _to, _logs, _worker) INTO _done;
END
$$;

CREATE OR REPLACE PROCEDURE proxy_whitelist_app.process_blocks(
    _context_name hive.CONTEXT_NAME,
    _block_range hive.BLOCKS_RANGE,
    IN _worker INT,
    OUT _done INT,
    _logs BOOLEAN = true
)
LANGUAGE plpgsql
AS $$
BEGIN
    IF hive.get_current_stage_name(_context_name) = 'MASSIVE_PROCESSING' THEN
        CALL proxy_whitelist_app.massive_processing(
            _block_range.first_block, _block_range.last_block, _logs, _worker, _done
        );
        RETURN;
    END IF;

    CALL proxy_whitelist_app.single_processing(
        _block_range.first_block, _block_range.last_block, _logs, _worker, _done
    );
END
$$;

-- Check if processing should stop.
CREATE OR REPLACE FUNCTION proxy_whitelist_app.isBreakingPending(
    _appContext hive.CONTEXT_NAME,
    _maxBlockLimit INT,
    _blocks_range hive.BLOCKS_RANGE
)
RETURNS TEXT  -- NULL means no break
LANGUAGE plpgsql
PARALLEL SAFE
AS $$
BEGIN
    IF _blocks_range IS NULL AND _maxBlockLimit IS NOT NULL THEN
        IF hive.app_get_current_block_num(_appContext) >= _maxBlockLimit THEN
            RAISE NOTICE 'Worker % reached blocks limit. Exiting at block: %.',
                         _appContext, hive.app_get_current_block_num(_appContext);
            RETURN 'BLOCK_LIMIT_REACHED';
        END IF;
    END IF;

    IF NOT proxy_whitelist_app.continueProcessing() THEN
        RAISE NOTICE 'Worker % exiting at block: %.',
                     _appContext, hive.app_get_current_block_num(_appContext);
        RETURN 'BREAK_ON_USER_REQUEST';
    END IF;

    RETURN NULL;
END
$$;

-- Wait for start block to be available.
CREATE OR REPLACE FUNCTION proxy_whitelist_app.wait_for_start_block(
    _start_block INT, _worker INT, _context hafd.CONTEXT_NAME
)
RETURNS BOOLEAN
LANGUAGE plpgsql
PARALLEL SAFE
STABLE
AS $$
DECLARE
    __head_of_irreversible_block BIGINT;
BEGIN
    IF _start_block != 0 THEN
        SELECT hir.consistent_block INTO __head_of_irreversible_block
        FROM hafd.hive_state hir;

        IF _start_block > __head_of_irreversible_block THEN
            PERFORM pg_sleep(5);
            IF _worker = 1 THEN
                RAISE INFO 'Waiting for start block(%). HAF head is %',
                           _start_block, __head_of_irreversible_block;
            END IF;
            RETURN FALSE;
        END IF;
    END IF;

    RETURN TRUE;
END
$$;

-- Advisory lock helper with timeout (same as hivesense).
CREATE OR REPLACE PROCEDURE proxy_whitelist_app.wait_for_advisory_lock(
    _namespace INT, _worker INT, _timeout_ms INT DEFAULT 5000
)
LANGUAGE plpgsql
AS $$
BEGIN
  LOOP
    COMMIT;
    PERFORM set_config('lock_timeout', _timeout_ms::text, true);

    BEGIN
      PERFORM pg_advisory_lock(_namespace, _worker);
      PERFORM set_config('lock_timeout', '0', true);
      EXIT;
    EXCEPTION
      WHEN SQLSTATE '55P03' OR SQLSTATE '57014' THEN
        IF EXISTS (SELECT FROM pg_locks
           WHERE locktype = 'advisory'
             AND classid  = _namespace
             AND objid    = _worker
             AND pid      = pg_backend_pid()
        ) THEN
          PERFORM set_config('lock_timeout', '0', true);
          EXIT;
        END IF;
    END;
  END LOOP;
END;
$$;


-- Single-threaded block processor: gets ranges from HAF, waits for hivemind,
-- processes posts directly. No workers, no advisory locks.
CREATE OR REPLACE PROCEDURE proxy_whitelist_app.scheduler(
    IN  _app_context_base_name  hive.context_name,
    IN  _workers                INT,
    IN  _max_block_limit        INT DEFAULT null
)
LANGUAGE plpgsql
AS $$
DECLARE
    __hivemind_current_block        INT;
    __context_name                  hive.context_name := _app_context_base_name;
    __start_block                   INT := 0;
    __blocks_range                  hive.blocks_range := (0,0);
    __planned_range                 hive.blocks_range;
    __breaking_reason               TEXT := null;
    __done                          INT;
    __hivemind_wait_start           TIMESTAMPTZ;
    __hivemind_wait_timeout CONSTANT INTERVAL := '300 seconds';
BEGIN
    -- Block until any active hivemind/proxy-whitelist installer releases its
    -- exclusive lock; held by this session until the scheduler returns.
    PERFORM hive.acquire_app_block_processor_locks(ARRAY['hivemind', 'proxy-whitelist']);

    SELECT start_block INTO __start_block
    FROM proxy_whitelist_app.app_status;

    PERFORM proxy_whitelist_app.allowProcessing();

    RAISE NOTICE 'Last block processed by %: %',
                 __context_name,
                 hive.app_get_current_block_num(__context_name);

    IF _max_block_limit IS NOT NULL THEN
        RAISE NOTICE 'Max block limit: %', _max_block_limit;
    END IF;

    IF hive.app_get_current_block_num(__context_name) < __start_block - 1 THEN
        PERFORM hive.app_set_current_block_num(__context_name, __start_block - 1);
    END IF;

    RAISE NOTICE 'Entering main loop...';

    <<main_loop>>
    LOOP
        -- Wait for start block
        IF NOT proxy_whitelist_app.wait_for_start_block(__start_block, 1, __context_name) THEN
            PERFORM pg_sleep(0.1);
            CONTINUE;
        END IF;

        -- Request next range from HAF, then rollback to free xmin
        CALL hive.app_next_iteration(
            __context_name,
            __blocks_range,
            _override_max_batch => NULL,
            _limit              => _max_block_limit
        );

        __planned_range := __blocks_range;
        ROLLBACK;

        IF __blocks_range IS NULL THEN
            CONTINUE;
        END IF;

        -- Wait for hivemind to fully process through this range.
        --
        -- In live mode, hivemind updates last_completed_block_num AFTER all
        -- phases (hive_posts, hive_post_data, notifications) are committed.
        -- This is the safe signal that all data for the block is visible.
        --
        -- In massive sync mode, last_completed_block_num is NOT updated
        -- (it stays at the pre-massive-sync value until finalization).
        -- We detect this by checking if app_get_current_block_num is far
        -- ahead of last_completed_block_num, and fall back to
        -- app_get_current_block_num in that case. The multi-transaction
        -- race is harmless during massive sync because we trail hivemind
        -- by many batches, so PostDataCache.flush() has long since
        -- committed by the time we read a block's data.
        __hivemind_wait_start := clock_timestamp();
        LOOP
            COMMIT;  -- fresh snapshot each iteration

            SELECT last_completed_block_num
              INTO __hivemind_current_block
              FROM hivemind_app.hive_state;

            -- If last_completed is current enough, use it (live mode)
            EXIT WHEN __hivemind_current_block >= __planned_range.last_block;

            -- Massive sync fallback: last_completed is stale but
            -- app_get_current_block_num is well past our target.
            -- The 1000-block margin ensures PostDataCache has flushed.
            EXIT WHEN hive.app_get_current_block_num('hivemind_app')
                      >= __planned_range.last_block + 1000
                  AND __hivemind_current_block < hive.app_get_current_block_num('hivemind_app') - 10000;

            IF clock_timestamp() - __hivemind_wait_start > __hivemind_wait_timeout THEN
                RAISE NOTICE 'Hivemind wait timeout after % for block % (hivemind at %). Re-requesting range.',
                             __hivemind_wait_timeout, __planned_range.last_block, __hivemind_current_block;
                CONTINUE main_loop;
            END IF;

            RAISE NOTICE 'Waiting for hivemind to reach block % (at %)',
                         __planned_range.last_block,
                         __hivemind_current_block;
            PERFORM pg_sleep(1);

            __breaking_reason := proxy_whitelist_app.isBreakingPending(
                __context_name, _max_block_limit, NULL
            );
            IF __breaking_reason IS NOT NULL THEN
                RETURN;
            END IF;
        END LOOP;

        -- Re-claim the block range inside a write transaction
        CALL hive.app_next_iteration(
            __context_name,
            __blocks_range,
            _override_max_batch => NULL,
            _limit              => _max_block_limit
        );

        __breaking_reason := proxy_whitelist_app.isBreakingPending(
            __context_name, _max_block_limit, __blocks_range
        );
        IF __breaking_reason IS NOT NULL THEN
            ROLLBACK;
            RETURN;
        END IF;

        IF __blocks_range IS NULL THEN
            CONTINUE;
        END IF;

        -- Process posts directly (no worker dispatch)
        CALL proxy_whitelist_app.process_blocks(
            __context_name, __blocks_range, 1, __done
        );

        -- Process profile images for accounts updated in this block range
        BEGIN
            PERFORM proxy_whitelist_app.process_profile_images_incremental(
                __blocks_range.first_block, __blocks_range.last_block
            );
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Profile image processing failed for blocks %-%, will retry next cycle: %',
                          __blocks_range.first_block, __blocks_range.last_block, SQLERRM;
        END;

    END LOOP;

    ASSERT FALSE, 'Scheduler: unreachable';
END
$$;


-- ============================================================================
-- GENERIC DRIVER ENTRY POINT AND APPLICATION REGISTRY (haf#341)
-- ============================================================================
-- Entry point for the generic HAF block-processing driver (haf_app_driver.py):
-- processes one range delivered by hive.app_next_iteration. The driver owns the
-- transaction, so this must not COMMIT. Waiting for hivemind is no longer done
-- here: the application is registered as depending on hivemind_app, so
-- hive.app_next_iteration never delivers a block hivemind has not committed
-- (hivemind reports its committed position through its completed-block
-- function, which is exact during massive sync as well).
CREATE OR REPLACE PROCEDURE proxy_whitelist_app.process_blocks( _block_range hive.blocks_range )
LANGUAGE plpgsql
AS $$
DECLARE
    __done INT;
BEGIN
    CALL proxy_whitelist_app.process_blocks( 'proxy_whitelist_app', _block_range, 1, __done );

    -- Process profile images for accounts updated in this block range
    BEGIN
        PERFORM proxy_whitelist_app.process_profile_images_incremental(
            _block_range.first_block, _block_range.last_block
        );
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Profile image processing failed for blocks %-%, will retry next cycle: %',
                      _block_range.first_block, _block_range.last_block, SQLERRM;
    END;
END
$$;

-- Register with the HAF application registry (driver entry point above) and
-- declare the dependency on hivemind, which must already be registered (the
-- hivemind install runs first).
SELECT hive.app_register( 'proxy_whitelist_app', ARRAY[ 'proxy_whitelist_app' ]::hive.contexts_group, 'proxy_whitelist_app.process_blocks' );
SELECT hive.app_add_dependency( 'proxy_whitelist_app', 'hivemind_app' );


-- Worker loop: claims tasks from block_tasks and processes them.
-- Legacy code kept for future parallel processing support.
CREATE OR REPLACE PROCEDURE proxy_whitelist_app.worker_loop(
    IN _worker            INT,
    IN _app_context_name  hive.context_name,
    IN _max_block_limit   INT DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    __task                          RECORD;
    __urls_whitelisted              INT;
    __breaking_reason               TEXT := NULL;
    __advisory_lock_namespace_begin INT;
    __start_key_namespace           INT;
    __done_key_namespace            INT;
    __ack_key_namespace             INT;
BEGIN
    SELECT advisory_lock_namespace_begin INTO __advisory_lock_namespace_begin
    FROM proxy_whitelist_app.app_status;

    __start_key_namespace := __advisory_lock_namespace_begin;
    __done_key_namespace := __advisory_lock_namespace_begin + 1;
    __ack_key_namespace := __advisory_lock_namespace_begin + 2;

    LOOP
        -- Take done_key (never blocks for long)
        PERFORM pg_advisory_lock(__done_key_namespace, _worker);

        -- Block until scheduler says "go"
        CALL proxy_whitelist_app.wait_for_advisory_lock(__start_key_namespace, _worker);

        -- Release start_key
        PERFORM pg_advisory_unlock(__start_key_namespace, _worker);

        LOOP
            -- Claim a pending task
            SELECT task_id, post_ids, first_block, last_block
              INTO __task
            FROM proxy_whitelist_app.block_tasks
            WHERE status = 'pending'
            FOR UPDATE SKIP LOCKED
            LIMIT 1;

            IF NOT FOUND THEN
                ROLLBACK;
                EXIT;
            END IF;

            -- Mark running
            UPDATE proxy_whitelist_app.block_tasks
               SET status     = 'running',
                   shard      = _worker,
                   claimed_at = clock_timestamp()
             WHERE task_id    = __task.task_id;

            BEGIN
                __urls_whitelisted := proxy_whitelist_app.process_posts(
                    __task.post_ids, _worker
                );
            EXCEPTION WHEN OTHERS THEN
                RAISE WARNING 'worker % task % failed: %, retrying after delay',
                              _worker, __task.task_id, SQLERRM;
                ROLLBACK;
                PERFORM pg_sleep(5);
                CONTINUE;
            END;

            RAISE NOTICE 'worker % blocks %-% : % posts, % URLs',
                         _worker, __task.first_block, __task.last_block,
                         array_length(__task.post_ids, 1), __urls_whitelisted;

            -- Mark done
            UPDATE proxy_whitelist_app.block_tasks
               SET status      = 'done',
                   finished_at = clock_timestamp()
             WHERE task_id = __task.task_id;

            COMMIT;
        END LOOP;

        -- Signal "done" to scheduler
        PERFORM pg_advisory_unlock(__done_key_namespace, _worker);

        -- Wait for scheduler ack
        CALL proxy_whitelist_app.wait_for_advisory_lock(__ack_key_namespace, _worker);
        PERFORM pg_advisory_unlock(__ack_key_namespace, _worker);

        __breaking_reason := proxy_whitelist_app.isBreakingPending(
            _app_context_name, _max_block_limit, NULL
        );
        IF __breaking_reason IS NOT NULL THEN
            RETURN;
        END IF;
    END LOOP;

    ASSERT FALSE, 'Worker loop: unreachable';
END
$$;

RESET ROLE;
