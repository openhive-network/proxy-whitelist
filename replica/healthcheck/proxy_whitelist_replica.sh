#! /bin/sh
# haproxy agent check of a proxy-whitelist replica (replica/README.md, Health):
# haf_api_node's healthchecks/checks/proxy_whitelist.sh without its HAF gate.
# check_haf_lib queries hive.* and hafd.*, which the replica's plain Postgres
# does not have, so the stock agent answers "down" whatever the replica's state.
#
# check_http_sync_age.sh and format_seconds.sh next to this script are
# haf_api_node's healthchecks/checks/ files, vendored unchanged from its
# develop at 0a619f2e. They expect PSQL_CHECK_TIMEOUT, TIME_OFFSET and
# adjust_age_for_ci from check_haf_lib.sh, defined here without its CI offset.
#
# The sync-status query is this script's own, not check_sync_status's: that one
# is "BEGIN; SET LOCAL search_path ...; SELECT ...; COMMIT" in one --command,
# whose SELECT result psql before 15 does not print (only the last statement's,
# COMMIT's), and the image's psql is 14. Here it is a single statement with the
# search_path as a startup option, safe on the replica's plain Postgres (there is
# no pgbouncer to drop it). The liveness and age checks are still the vendored ones.
#
# Prints "up", or "down #<reason>" and exits 10, like the stock agents. A
# Cloudflare purge backlog (purge_pending > 0) stays "up", with the backlog as
# its reason: a stale avatar redirect is no reason to route whitelist traffic away.
#
#   REPLICA_LIVE_URL      PostgREST's admin /live (default http://replica-postgrest:3001/live)
#   REPLICA_POSTGRES_URL  the replica database as proxy_whitelist_user (required)
#   PSQL_CHECK_TIMEOUT    seconds the sync-status query may take (default 5)
#   HTTP_CHECK_TIMEOUT    seconds /live may take (default 5)
set -e

export PSQL_CHECK_TIMEOUT="${PSQL_CHECK_TIMEOUT:-5}"
export TIME_OFFSET=0
adjust_age_for_ci() {
  echo "$1"
}

# shellcheck source=replica/healthcheck/check_http_sync_age.sh
. "$(dirname "$0")/check_http_sync_age.sh"

if [ -z "${REPLICA_POSTGRES_URL:-}" ]; then
  echo "down #proxy_whitelist_replica REPLICA_POSTGRES_URL is not set"
  exit 10
fi

# Kill a pending sync-status query when the agent connection is torn down.
trap "trap - 2 15 && kill -- -\$$" 2 15

if ! _result=$(
    _name=proxy_whitelist_replica
    check_postgrest_live "$_name" "${REPLICA_LIVE_URL:-http://replica-postgrest:3001/live}"
    # to_json of the FROM alias, like check_sync_status: the object whether the
    # function returns json or a row.
    _sync_status=$(PGOPTIONS="-c search_path=proxy_whitelist_endpoints,proxy_whitelist_app,proxy_whitelist_replica" \
        timeout "$PSQL_CHECK_TIMEOUT" psql "$REPLICA_POSTGRES_URL" --quiet --no-align --tuples-only \
        --command="SELECT to_json(s) FROM proxy_whitelist_endpoints.sync_status() s LIMIT 1") || {
      case "$?" in
        124|143) echo "down #$_name sync-status query timed out after ${PSQL_CHECK_TIMEOUT}s" ;;
        *)       echo "down #$_name sync-status query failed" ;;
      esac
      exit 16
    }
    _block_num=$(echo "$_sync_status" | sed -n 's/.*"last_block_num"[[:space:]]*:[[:space:]]*\([0-9]*\).*/\1/p')
    _block_time=$(echo "$_sync_status" | sed -n 's/.*"last_block_time"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')
    if [ -z "$_block_time" ]; then
      if [ -z "$_block_num" ] || [ "$_block_num" -eq 0 ]; then
        echo "down #$_name has no processed block yet"
        exit 14
      fi
      echo "down #$_name block $_block_num has no timestamp in sync-status"
      exit 15
    fi
    check_http_block_time "$_name" 60 "$_block_time"
    _purge_pending=$(echo "$_sync_status" | sed -n 's/.*"purge_pending"[[:space:]]*:[[:space:]]*\([0-9]*\).*/\1/p')
    if [ -n "$_purge_pending" ] && [ "$_purge_pending" -gt 0 ]; then
      _purge_oldest=$(echo "$_sync_status" | sed -n 's/.*"purge_oldest_block"[[:space:]]*:[[:space:]]*\([0-9]*\).*/\1/p')
      echo "#$_name purge backlog: $_purge_pending accounts since block $_purge_oldest"
    fi
  ); then
  echo "$_result"
  exit 10
fi
echo "up${_result:+ $_result}"
