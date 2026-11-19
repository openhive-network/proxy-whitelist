#!/usr/bin/env bash
# The step functions are called indirectly, through `step`.
# shellcheck disable=SC2317,SC2329
# The checks AIDEV's verification slots run (.aidev/project.yaml), as one junit
# report per suite: each named step is a test case, its log the failure body.
#
#   .aidev/run-checks.sh <suite> <step>...
#
#   - shellcheck      ShellCheck at warning severity on every *.sh (CI's
#                     lint_bash_scripts), at default severity on .aidev/'s own
#   - sqlfluff        `sqlfluff lint` with the repository's .sqlfluff (CI's
#                     lint_sql_scripts; a violation .sqlfluff does not list under
#                     `warnings` fails the step)
#   - unit            tests/unit with pytest (no database, no network)
#   - sql-registered  every .sql file under db/ and endpoints/ is applied by
#                     scripts/install_app.sh (a file it misses never reaches a database)
#   - install         scripts/install_app.sh into the stack's HAF database
#                     (PROXY_WHITELIST_TEST_DB_HOST, default `haf`), which already
#                     has hivemind installed, through install_with_app_lock.py;
#                     then the schema, the endpoint schema and the HAF context exist,
#                     and every tests/sql/*.sql check passes against the installed app
#   - reinstall       install_app.sh a second time over the installed app (there is
#                     no uninstall script; the installer is idempotent), the
#                     endpoint schema is still there and the stream source
#                     identity (app_status.sync_uuid) is unchanged
#   - api-smoke       .aidev/api_smoke.py: PostgREST on the installed schema as
#                     production runs it, then check_url through unknown →
#                     whitelisted → blacklisted, the blacklisted_accounts form,
#                     url_accounts, sync_status, the root spec, the /stream/*
#                     pages and avatar/cover (one junit case per call in
#                     $out/api-smoke.xml)
#   - replica-install replica/install.sh into a fresh database on the stack's plain
#                     Postgres (PROXY_WHITELIST_TEST_REPLICA_DB_HOST, default
#                     `replica-db`), twice; then the replicated tables, the state
#                     row and the endpoint functions exist and the stream ones do not
#   - replica-sync    .aidev/replica_smoke.py sync: replica/sync.py --once against
#                     the installed primary served as api-smoke serves it, until
#                     bootstrap_complete with the primary's counts
#   - replica-smoke   .aidev/replica_smoke.py smoke: the primary seeded and followed
#                     cycle by cycle (paging, new and quiet blocks, a pending url
#                     author, an unreachable source, a changed sync_uuid and
#                     --reset, --rebuild), the replica's answers compared byte for byte
#   - replica-health  .aidev/replica_smoke.py health: the replica rebuilt from the
#                     primary, then replica/healthcheck/proxy_whitelist_replica.sh
#                     (haproxy's agent check) up in sync, down when stale,
#                     unbootstrapped or PostgREST not live
#
# Reports go to test-results/<suite>/: junit.xml (one case per step) plus one log
# per step, and the per-call junit of api-smoke.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
suite="${1:?usage: $0 <suite> <step>...}"; shift
out="test-results/$suite"
rm -rf "$out"; mkdir -p "$out"
cases="$out/cases.tsv"; : > "$cases"
DB_HOST="${PROXY_WHITELIST_TEST_DB_HOST:-haf}"
ADMIN_URL="postgresql://haf_admin@$DB_HOST/haf_block_log"
export PROXY_WHITELIST_TEST_ADMIN_URL="$ADMIN_URL"
export PROXY_WHITELIST_TEST_USER_URL="postgresql://proxy_whitelist_user@$DB_HOST/haf_block_log"
REPLICA_DB_HOST="${PROXY_WHITELIST_TEST_REPLICA_DB_HOST:-replica-db}"
REPLICA_ADMIN_URL="postgresql://postgres@$REPLICA_DB_HOST/proxy_whitelist"
export PROXY_WHITELIST_TEST_REPLICA_ADMIN_URL="$REPLICA_ADMIN_URL"
export PROXY_WHITELIST_TEST_REPLICA_USER_URL="postgresql://proxy_whitelist_user@$REPLICA_DB_HOST/proxy_whitelist"
status=0

# record CASES_FILE NAME RC SECONDS LOG
record() {
    if [ "$3" -eq 0 ]; then
        printf 'case\t%s\tpass\t%s\t\n' "$2" "$4" >> "$1"
    else
        printf 'case\t%s\tfail\t%s\texit %s\t%s\n' "$2" "$4" "$3" "$5" >> "$1"
    fi
}
step() {
    local name="$1"; shift
    local log="$out/$name.log" t0=$SECONDS rc=0
    echo "== $name" >&2
    "$@" > "$log" 2>&1 < /dev/null || rc=$?
    [ "$rc" -eq 0 ] || { status=1; tail -40 "$log" >&2; }
    record "$cases" "$name" "$rc" "$((SECONDS - t0))" "$log"
}

run_shellcheck() {
    shellcheck --version | head -2
    find . -name .git -prune -o -name .aidev -prune -o -name test-results -prune -o -type f -name '*.sh' -print0 \
        | xargs -0 shellcheck -S warning -f gcc || return 1
    shellcheck -f gcc .aidev/*.sh
}
run_sqlfluff() {
    sqlfluff --version
    sqlfluff lint --nofail --format yaml --write-output "$out/sqlfluff.yaml" . || return 1
    if grep -q 'warning: false' "$out/sqlfluff.yaml"; then
        cat "$out/sqlfluff.yaml"
        return 1
    fi
    echo "no sqlfluff violations"
}
run_unit() {
    python3 -m pytest tests/unit -q --junitxml="$out/unit-tests.xml"
}
sql_registered() {
    local f missing=0
    while IFS= read -r f; do
        if ! grep -qF "\$SRCPATH/$f\"" scripts/install_app.sh; then
            echo "not applied by scripts/install_app.sh: $f"
            missing=1
        fi
    done < <(find db endpoints -name '*.sql' | sort)
    [ "$missing" -eq 0 ] && echo "every .sql file under db/ and endpoints/ is applied"
    return "$missing"
}

wait_for_db() {
    local i found
    for i in $(seq 60); do
        found="$(psql "$ADMIN_URL" -Atc "select 1 from pg_namespace where nspname = 'hivemind_endpoints'" 2>/dev/null)" || true
        [ "$found" = 1 ] && return 0
        echo "waiting for hivemind in $ADMIN_URL ($i)"
        sleep 5
    done
    echo "no hivemind_endpoints schema in $ADMIN_URL: is the stack up?"
    return 1
}
# query SQL: one value, as haf_admin
query() {
    psql "$ADMIN_URL" -v ON_ERROR_STOP=on -Atc "$1"
}
install_app() {
    bash scripts/install_app.sh --postgres-url="$ADMIN_URL" "$@" 2>&1 | tee "$out/install_app.log"
    return "${PIPESTATUS[0]}"
}
installed_ok() {
    [ "$(query "select 1 from pg_namespace where nspname = 'proxy_whitelist_app'")" = 1 ] || { echo "no proxy_whitelist_app schema"; return 1; }
    [ "$(query "select 1 from pg_namespace where nspname = 'proxy_whitelist_endpoints'")" = 1 ] || { echo "no proxy_whitelist_endpoints schema"; return 1; }
    [ "$(query "select hive.app_context_exists('proxy_whitelist_app')")" = t ] || { echo "HAF context proxy_whitelist_app does not exist"; return 1; }
    [ "$(query "select to_regprocedure('proxy_whitelist_endpoints.check_url(text, text[])') is not null")" = t ] || { echo "check_url(text, text[]) is not installed"; return 1; }
    echo "schema, endpoint schema, HAF context and check_url present"
}
sql_checks() {
    local f
    for f in tests/sql/*.sql; do
        echo "-- $f"
        psql "$ADMIN_URL" -v ON_ERROR_STOP=on -f "$f" || return 1
    done
}
install() {
    wait_for_db || return 1
    install_app "$@" || return 1
    if grep -q "WARNING: install_with_app_lock.py wrapper not found" "$out/install_app.log"; then
        echo "FAIL: install_app.sh ran without install_with_app_lock.py: the runtime image lacks /usr/local/bin/install_with_app_lock.py or install_app.sh no longer finds it"
        return 1
    fi
    installed_ok || return 1
    sql_checks
}
reinstall() {
    local before after
    before="$(query "select sync_uuid from proxy_whitelist_app.app_status")" || return 1
    install_app || return 1
    installed_ok || return 1
    after="$(query "select sync_uuid from proxy_whitelist_app.app_status")" || return 1
    [ -n "$before" ] && [ "$before" = "$after" ] || { echo "sync_uuid changed on reinstall: '$before' -> '$after'"; return 1; }
    echo "sync_uuid kept: $after"
}

# replica_query SQL: one value, as the replica's superuser
replica_query() {
    psql "$REPLICA_ADMIN_URL" -v ON_ERROR_STOP=on -Atc "$1"
}
replica_install() {
    local i maintenance="postgresql://postgres@$REPLICA_DB_HOST/postgres"
    for i in $(seq 60); do
        psql "$maintenance" -Atc "select 1" > /dev/null 2>&1 && break
        echo "waiting for $maintenance ($i)"
        sleep 2
    done
    psql "$maintenance" -v ON_ERROR_STOP=on \
        -c "drop database if exists proxy_whitelist with (force)" \
        -c "create database proxy_whitelist" || return 1
    bash replica/install.sh --postgres-url="$REPLICA_ADMIN_URL" || return 1
    replica_query "update proxy_whitelist_replica.state set sync_error = 'kept by a reinstall'" || return 1
    bash replica/install.sh --postgres-url="$REPLICA_ADMIN_URL" || return 1
    [ "$(replica_query "select sync_error from proxy_whitelist_replica.state")" = "kept by a reinstall" ] \
        || { echo "a reinstall replaced the syncer's state"; return 1; }
    replica_query "update proxy_whitelist_replica.state set sync_error = null" || return 1
    local t
    for t in accounts urls url_authors account_profiles blacklisted_urls blacklisted_accounts; do
        [ "$(replica_query "select to_regclass('proxy_whitelist_app.$t') is not null")" = t ] || { echo "no proxy_whitelist_app.$t"; return 1; }
    done
    [ "$(replica_query "select to_regprocedure('proxy_whitelist_endpoints.check_url(text, text[])') is not null")" = t ] \
        || { echo "check_url(text, text[]) is not installed"; return 1; }
    [ "$(replica_query "select count(*) from pg_proc where proname like 'stream\_%'")" = 0 ] \
        || { echo "stream functions installed on the replica"; return 1; }
    [ "$(replica_query "select proxy_whitelist_endpoints.sync_status()->>'bootstrap_complete'")" = false ] \
        || { echo "sync_status() of a fresh replica is not bootstrap_complete = false"; return 1; }
    [ "$(replica_query "select (s->>'sync_uuid') is not null and s->>'source_sync_uuid' is null
                        from proxy_whitelist_endpoints.sync_status() s")" = t ] \
        || { echo "sync_status() of a fresh replica lacks its own sync_uuid or has a source's"; return 1; }
    echo "replica schema, state and endpoints present, no stream endpoints"
}

for s in "$@"; do
    case "$s" in
        shellcheck) step shellcheck run_shellcheck ;;
        sqlfluff) step sqlfluff run_sqlfluff ;;
        unit) step unit run_unit ;;
        sql-registered) step sql-registered sql_registered ;;
        install) step install install ;;
        reinstall) step reinstall reinstall ;;
        api-smoke) step api-smoke python3 .aidev/api_smoke.py "$out" ;;
        replica-install) step replica-install replica_install ;;
        replica-sync) step replica-sync python3 .aidev/replica_smoke.py sync "$out" ;;
        replica-smoke) step replica-smoke python3 .aidev/replica_smoke.py smoke "$out" ;;
        replica-health) step replica-health python3 .aidev/replica_smoke.py health "$out" ;;
        *) echo "unknown step: $s" >&2; exit 2 ;;
    esac
done
python3 .aidev/junit_cases.py "$out/junit.xml" "$suite" "$cases"
exit "$status"
