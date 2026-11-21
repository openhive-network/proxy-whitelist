-- The app's row in the HAF application registry: registered under its lock
-- name, and check_registered_name's refusal of a row left under another name.
-- Run against an installed app by .aidev/run-checks.sh's install step; any
-- failed case raises. Everything it writes is rolled back.
BEGIN;

DO $$
DECLARE
    __names TEXT[];
BEGIN
    __names := ARRAY(
        SELECT a.name FROM hafd.applications a
        WHERE a.contexts @> ARRAY['proxy_whitelist_app']
        ORDER BY a.name
    );
    IF __names IS DISTINCT FROM ARRAY['proxy-whitelist'] THEN
        RAISE EXCEPTION 'registration: context proxy_whitelist_app is registered by %, expected {proxy-whitelist}', __names;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM hafd.application_dependencies d
        JOIN hafd.applications h ON h.name = d.depends_on
        WHERE d.application = 'proxy-whitelist' AND h.contexts @> ARRAY['hivemind_app']
    ) THEN
        RAISE EXCEPTION 'registration: proxy-whitelist does not depend on hivemind';
    END IF;
    IF hive._app_broken_reason('proxy-whitelist') IS NOT NULL THEN
        RAISE EXCEPTION 'registration: HAF finds the install broken: %', hive._app_broken_reason('proxy-whitelist');
    END IF;

    PERFORM proxy_whitelist_app.check_registered_name();
END
$$;

-- As installs before the rename registered it.
DELETE FROM hafd.applications
WHERE name = 'proxy-whitelist';
INSERT INTO hafd.applications (name, contexts, process_procedure, owner)
VALUES ('proxy_whitelist_app', ARRAY['proxy_whitelist_app'], 'proxy_whitelist_app.process_blocks', 'proxy_whitelist_owner');

DO $$
BEGIN
    PERFORM proxy_whitelist_app.check_registered_name();
    RAISE EXCEPTION 'registration: check_registered_name accepted a leftover proxy_whitelist_app row';
EXCEPTION WHEN raise_exception THEN
    IF SQLERRM LIKE 'registration: %' THEN
        RAISE;
    END IF;
    IF SQLERRM NOT LIKE 'HAF application proxy_whitelist_app holds context proxy_whitelist_app%'
       OR strpos(SQLERRM, 'SELECT hive.app_unregister(''proxy_whitelist_app'');') = 0 THEN
        RAISE EXCEPTION 'registration: unexpected refusal of a leftover row: %', SQLERRM;
    END IF;
END
$$;

ROLLBACK;
