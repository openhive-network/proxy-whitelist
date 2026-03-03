#!/bin/bash

set -e
set -o pipefail

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 || exit 1; pwd -P )"
SRCPATH="${SCRIPTPATH}/../"

echo "All arguments: $*"

print_help () {
    echo "Usage: $0 [OPTION[=VALUE]]..."
    echo
    echo "Sets up a HAF database to work with the proxy_whitelist application."
    echo "OPTIONS:"
    echo "  --host=VALUE                 PostgreSQL host (defaults to /var/run/postgresql)"
    echo "  --port=NUMBER                PostgreSQL port (defaults to 5432)"
    echo "  --postgres-url=URL           PostgreSQL URL (alternative to --host and --port)"
    echo "  --redis-url=URL              Redis URL for whitelist storage (defaults to redis://localhost:6379)"
    echo "  --redis-password=PASSWORD     Redis password (optional, for ACL auth)"
    echo "  --start_block=NUMBER         Start block to process (default: 1)"
    echo "  --parallel_workers=NUMBER    Number of parallel worker processes (default: 4)"
    echo "  --poll-profile-every=NUMBER  Process profile images every N batches (default: 20)"
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
REDIS_URL='redis://localhost:6379'
REDIS_PASSWORD=''
PARALLEL_WORKERS=4
START_BLOCK=1
POLL_PROFILE_IMAGES_EVERY_N_BATCHES=20
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
    --redis-url=*)
        REDIS_URL="${1#*=}"
        ;;
    --redis-password=*)
        REDIS_PASSWORD="${1#*=}"
        ;;
    --start_block=*)
        START_BLOCK="${1#*=}"
        ;;
    --parallel_workers=*)
        PARALLEL_WORKERS="${1#*=}"
        ;;
    --poll-profile-every=*)
        POLL_PROFILE_IMAGES_EVERY_N_BATCHES="${1#*=}"
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

echo "Installing proxy_whitelist app..."
echo "Number of workers: ${PARALLEL_WORKERS}"
echo "Redis URL: ${REDIS_URL}"
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
  SET pg_temp.REDIS_URL TO '${REDIS_URL}';
  SET pg_temp.REDIS_PASSWORD TO '${REDIS_PASSWORD}';
  SET pg_temp.START_BLOCK TO ${START_BLOCK};
  SET pg_temp.POLL_PROFILE_IMAGES_EVERY_N_BATCHES TO ${POLL_PROFILE_IMAGES_EVERY_N_BATCHES};
  SET pg_temp.ADVISORY_LOCK_NAMESPACE_BEGIN TO ${ADVISORY_LOCK_NAMESPACE_BEGIN};
  SET SEARCH_PATH TO ${PROXY_WHITELIST_SCHEMA};
" -f "$SRCPATH/db/database_schema.sql"

# Install helpers
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on \
  -c "SET SEARCH_PATH TO ${PROXY_WHITELIST_SCHEMA};" \
  -f "$SRCPATH/db/helpers.sql"

# Install plpython3u functions (Redis, URL extraction)
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on \
  -c "SET SEARCH_PATH TO ${PROXY_WHITELIST_SCHEMA};" \
  -f "$SRCPATH/db/redis_functions.sql"

psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on \
  -c "SET SEARCH_PATH TO ${PROXY_WHITELIST_SCHEMA};" \
  -f "$SRCPATH/db/url_extraction.sql"

# Install main loop (scheduler + workers)
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on \
  -c "SET SEARCH_PATH TO ${PROXY_WHITELIST_SCHEMA};" \
  -f "$SRCPATH/db/main_loop.sql"

# Grant permissions
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on \
  -c "SET ROLE proxy_whitelist_owner; GRANT USAGE ON SCHEMA ${PROXY_WHITELIST_SCHEMA} TO proxy_whitelist_user;"
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on \
  -c "SET ROLE proxy_whitelist_owner; GRANT SELECT ON ALL TABLES IN SCHEMA ${PROXY_WHITELIST_SCHEMA} TO proxy_whitelist_user;"

echo "proxy_whitelist app installed successfully."
