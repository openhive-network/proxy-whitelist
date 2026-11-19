#!/bin/bash
# Installs the replica into a plain Postgres database: the replicated tables and
# the syncer's state (replica/db/), then the primary's own endpoint functions
# (endpoints/) over them and the replica's sync_status(). Idempotent: running it
# again keeps the replicated rows and the syncer's state.

set -e
set -o pipefail

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 || exit 1; pwd -P )"
SRCPATH="${SCRIPTPATH}/.."

print_help () {
    echo "Usage: $0 [OPTION[=VALUE]]..."
    echo
    echo "Installs the proxy_whitelist replica schema and endpoints into a plain Postgres."
    echo "OPTIONS:"
    echo "  --postgres-url=URL   PostgreSQL URL of a superuser (default: \$POSTGRES_URL)"
    echo "  --help               Display this help screen and exit"
    echo
    echo "PROXY_WHITELIST_USER_PASSWORD, when set, becomes the password of proxy_whitelist_user,"
    echo "the role PostgREST connects as."
}

POSTGRES_URL=${POSTGRES_URL:-""}

while [ $# -gt 0 ]; do
  case "$1" in
    --postgres-url=*)
        POSTGRES_URL="${1#*=}"
        ;;
    --help)
        print_help
        exit 0
        ;;
    *)
        echo "ERROR: '$1' is not a valid option"
        echo
        print_help
        exit 1
        ;;
    esac
    shift
done

if [ -z "$POSTGRES_URL" ]; then
    echo "ERROR: no --postgres-url and no POSTGRES_URL"
    exit 1
fi

apply() {
    psql "$POSTGRES_URL" -v ON_ERROR_STOP=on -f "$1"
}

echo "Installing the proxy_whitelist replica..."

apply "$SRCPATH/replica/db/roles.sql"
apply "$SRCPATH/replica/db/schema.sql"

# The primary's endpoint functions, unchanged. sync_status.sql (HAF's
# contexts and blocks) is replaced by the replica's own, and stream.sql is not
# installed: a replica is not a stream source.
apply "$SRCPATH/endpoints/endpoint_schema.sql"
apply "$SRCPATH/endpoints/whitelist_check.sql"
apply "$SRCPATH/endpoints/blacklist_ops.sql"
apply "$SRCPATH/endpoints/avatar.sql"
apply "$SRCPATH/replica/db/replica_sync_status.sql"

psql "$POSTGRES_URL" -v ON_ERROR_STOP=on \
  -c "SET ROLE proxy_whitelist_owner; GRANT USAGE ON SCHEMA proxy_whitelist_app TO proxy_whitelist_user;" \
  -c "SET ROLE proxy_whitelist_owner; GRANT SELECT ON ALL TABLES IN SCHEMA proxy_whitelist_app TO proxy_whitelist_user;"

if [ -n "${PROXY_WHITELIST_USER_PASSWORD:-}" ]; then
    psql "$POSTGRES_URL" -v ON_ERROR_STOP=on -v password="$PROXY_WHITELIST_USER_PASSWORD" \
      <<< "ALTER ROLE proxy_whitelist_user PASSWORD :'password';"
fi

# PostgREST caches the schema: have a running one reload it.
psql "$POSTGRES_URL" -v ON_ERROR_STOP=on -c "NOTIFY pgrst, 'reload schema';"

echo "proxy_whitelist replica installed successfully."
