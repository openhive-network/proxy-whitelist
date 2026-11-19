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
# Prints "up", or "down #<reason>" and exits 10, like the stock agents.
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

if ! _result=$(check_sync_status "proxy_whitelist_replica" 60 \
    "${REPLICA_LIVE_URL:-http://replica-postgrest:3001/live}" \
    "$REPLICA_POSTGRES_URL" \
    proxy_whitelist_endpoints.sync_status "proxy_whitelist_endpoints, proxy_whitelist_app, proxy_whitelist_replica"); then
  echo "$_result"
  exit 10
fi
echo "up"
