-- proxy_whitelist_app.should_log_hivemind_wait: when the scheduler logs
-- 'Waiting for hivemind to reach block ...'. Run against an installed app by
-- .aidev/run-checks.sh's install step; any failed case raises.
DO $$
DECLARE
    __t0 CONSTANT TIMESTAMPTZ := '2026-11-19 12:00:00+00';
    __case RECORD;
    __got BOOLEAN;
BEGIN
    FOR __case IN
        SELECT *
        FROM (VALUES
            ('first poll of a wait logs', 100, NULL::INT, NULL::TIMESTAMPTZ, __t0, TRUE),
            ('same block 1 s later is silent', 100, 100, __t0, __t0 + INTERVAL '1 second', FALSE),
            ('same block 9.9 s later is silent', 100, 100, __t0, __t0 + INTERVAL '9.9 seconds', FALSE),
            ('same block 10 s later is a heartbeat', 100, 100, __t0, __t0 + INTERVAL '10 seconds', TRUE),
            ('same block 25 s later is a heartbeat', 100, 100, __t0, __t0 + INTERVAL '25 seconds', TRUE),
            ('next awaited block logs at once', 101, 100, __t0, __t0 + INTERVAL '1 second', TRUE),
            ('a re-requested earlier block logs', 99, 100, __t0, __t0 + INTERVAL '1 second', TRUE)
        ) AS c (name, awaited, last_block, last_at, now_at, expected)
    LOOP
        __got := proxy_whitelist_app.should_log_hivemind_wait(
            __case.awaited, __case.last_block, __case.last_at, __case.now_at
        );
        IF __got IS DISTINCT FROM __case.expected THEN
            RAISE EXCEPTION 'should_log_hivemind_wait: %: expected %, got %',
                            __case.name, __case.expected, __got;
        END IF;
    END LOOP;

    -- A wait of 35 one-second polls for one block logs on entry and at 10, 20 and 30 s.
    DECLARE
        __logged_block INT;
        __logged_at TIMESTAMPTZ;
        __lines INT := 0;
        __now TIMESTAMPTZ;
    BEGIN
        FOR __i IN 0..34 LOOP
            __now := __t0 + make_interval(secs => __i);
            IF proxy_whitelist_app.should_log_hivemind_wait(200, __logged_block, __logged_at, __now) THEN
                __lines := __lines + 1;
                __logged_block := 200;
                __logged_at := __now;
            END IF;
        END LOOP;
        IF __lines != 4 THEN
            RAISE EXCEPTION 'should_log_hivemind_wait: a 35 s wait for one block logged % lines, expected 4', __lines;
        END IF;
    END;

    RAISE NOTICE 'should_log_hivemind_wait: all cases pass';
END
$$;
