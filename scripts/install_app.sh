#!/bin/bash

set -e
set -o pipefail

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 || exit 1; pwd -P )"
SRCPATH="${SCRIPTPATH}/../"

# Capture original args for the install-lock re-exec below.
ORIGINAL_ARGS=("$@")

echo "All arguments: $*"

print_help () {
    echo "Usage: $0 [OPTION[=VALUE]]..."
    echo
    echo "Sets up a HAF database to work with the proxy_whitelist application."
    echo "OPTIONS:"
    echo "  --host=VALUE                 PostgreSQL host (defaults to /var/run/postgresql)"
    echo "  --port=NUMBER                PostgreSQL port (defaults to 5432)"
    echo "  --postgres-url=URL           PostgreSQL URL (alternative to --host and --port)"
    echo "  --start_block=NUMBER         Start block to process (default: 1)"
    echo "  --parallel_workers=NUMBER    Number of parallel worker processes (default: 4)"
    echo "  --advisory-lock-namespace=NUMBER  Advisory lock namespace start (default: 20000)"
    echo "  --help                       Display this help screen and exit"
    echo
}

POSTGRES_USER=${POSTGRES_USER:-"haf_admin"}
POSTGRES_HOST=${POSTGRES_HOST:-"haf"}
POSTGRES_PORT=${POSTGRES_PORT:-5432}
POSTGRES_URL=${POSTGRES_URL:-""}
PROXY_WHITELIST_SCHEMA=${PROXY_WHITELIST_SCHEMA:-"proxy_whitelist_app"}
POSTGRES_APP_NAME=proxy_whitelist_install
PARALLEL_WORKERS=4
START_BLOCK=1
ADVISORY_LOCK_NAMESPACE_BEGIN=20000

while [ $# -gt 0 ]; do
  case "$1" in
    --host=*)
        POSTGRES_HOST="${1#*=}"
        ;;
    --port=*)
        POSTGRES_PORT="${1#*=}"
        ;;
    --postgres-url=*)
        POSTGRES_URL="${1#*=}"
        ;;
    --start_block=*)
        START_BLOCK="${1#*=}"
        ;;
    --parallel_workers=*)
        PARALLEL_WORKERS="${1#*=}"
        ;;
    --advisory-lock-namespace=*)
        ADVISORY_LOCK_NAMESPACE_BEGIN="${1#*=}"
        ;;
    --schema=*)
        PROXY_WHITELIST_SCHEMA="${1#*=}"
        ;;
    --help)
        print_help
        exit 0
        ;;
    -*)
        echo "ERROR: '$1' is not a valid option"
        echo
        print_help
        exit 1
        ;;
    *)
        echo "ERROR: '$1' is not a valid argument"
        echo
        print_help
        exit 2
        ;;
    esac
    shift
done

POSTGRES_ACCESS=${POSTGRES_URL:-"postgresql://$POSTGRES_USER@$POSTGRES_HOST:$POSTGRES_PORT/haf_block_log?application_name=${POSTGRES_APP_NAME}"}

# Re-exec under the install-lock wrapper if not already running under it AND
# the wrapper plus python3 are actually available (e.g. inside the production
# install image). The wrapper holds an exclusive advisory lock on
# `proxy-whitelist` for the lifetime of this script. When the wrapper isn't
# installed (e.g. running this script directly on a CI runner host outside
# the production image), print a WARNING and run without the lock -- the lock
# is a production safety mechanism, not a correctness requirement for tests.
if [[ -z "${HAF_INSTALL_LOCK_HELD:-}" ]]; then
  if command -v python3 >/dev/null 2>&1 && [[ -f /usr/local/bin/install_with_app_lock.py ]]; then
    export HAF_INSTALL_LOCK_HELD=1
    exec python3 /usr/local/bin/install_with_app_lock.py proxy-whitelist "$POSTGRES_ACCESS" "$0" "${ORIGINAL_ARGS[@]}"
  else
    echo "WARNING: install_with_app_lock.py wrapper not found; running install without HAF advisory lock (expected in CI test setups, not in production install images)." >&2
    export HAF_INSTALL_LOCK_HELD=1
  fi
fi

echo "Installing proxy_whitelist app..."
echo "Number of workers: ${PARALLEL_WORKERS}"
echo "Start block: ${START_BLOCK}"
echo "Advisory lock namespace: ${ADVISORY_LOCK_NAMESPACE_BEGIN}"

psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -f "$SRCPATH/db/builtin_roles.sql"

psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -c "CREATE EXTENSION IF NOT EXISTS plpython3u;"

# Create schema
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on \
  -c "SET ROLE proxy_whitelist_owner; CREATE SCHEMA IF NOT EXISTS ${PROXY_WHITELIST_SCHEMA} AUTHORIZATION proxy_whitelist_owner;"

# Install database schema with config injection via pg_temp
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on -c "
  SET pg_temp.PARALLEL_WORKERS TO ${PARALLEL_WORKERS};
  SET pg_temp.START_BLOCK TO ${START_BLOCK};
  SET pg_temp.ADVISORY_LOCK_NAMESPACE_BEGIN TO ${ADVISORY_LOCK_NAMESPACE_BEGIN};
  SET SEARCH_PATH TO ${PROXY_WHITELIST_SCHEMA};
" -f "$SRCPATH/db/database_schema.sql"

# Install helpers
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on \
  -c "SET SEARCH_PATH TO ${PROXY_WHITELIST_SCHEMA};" \
  -f "$SRCPATH/db/helpers.sql"

# Install whitelist functions (pure SQL, no Redis dependency)
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on \
  -c "SET SEARCH_PATH TO ${PROXY_WHITELIST_SCHEMA};" \
  -f "$SRCPATH/db/whitelist_functions.sql"

# Install URL extraction (plpython3u)
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on \
  -c "SET SEARCH_PATH TO ${PROXY_WHITELIST_SCHEMA};" \
  -f "$SRCPATH/db/url_extraction.sql"

# Install main loop (scheduler + workers)
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on \
  -c "SET SEARCH_PATH TO ${PROXY_WHITELIST_SCHEMA};" \
  -f "$SRCPATH/db/main_loop.sql"

# Install endpoint schema (PostgREST API)
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on \
  -c "SET SEARCH_PATH TO ${PROXY_WHITELIST_SCHEMA};" \
  -f "$SRCPATH/endpoints/endpoint_schema.sql"

psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on \
  -c "SET SEARCH_PATH TO ${PROXY_WHITELIST_SCHEMA};" \
  -f "$SRCPATH/endpoints/whitelist_check.sql"

psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on \
  -c "SET SEARCH_PATH TO ${PROXY_WHITELIST_SCHEMA};" \
  -f "$SRCPATH/endpoints/blacklist_ops.sql"

# Grant permissions
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on \
  -c "SET ROLE proxy_whitelist_owner; GRANT USAGE ON SCHEMA ${PROXY_WHITELIST_SCHEMA} TO proxy_whitelist_user;"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on \
  -c "SET ROLE proxy_whitelist_owner; GRANT SELECT ON ALL TABLES IN SCHEMA ${PROXY_WHITELIST_SCHEMA} TO proxy_whitelist_user;"

echo "proxy_whitelist app installed successfully."
