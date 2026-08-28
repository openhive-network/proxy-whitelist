#!/bin/bash
set -e
set -o pipefail

print_help () {
    echo "Usage: $0 [OPTION[=VALUE]]..."
    echo
    echo "Starts the proxy_whitelist block processing."
    echo "OPTIONS:"
    echo "  --host=VALUE         PostgreSQL host (defaults to localhost)"
    echo "  --port=NUMBER        PostgreSQL port (defaults to 5432)"
    echo "  --postgres-url=URL   PostgreSQL URL (alternative to --host and --port)"
    echo "  --stop-at-block=NUM  Stop processing at given block"
    echo "  --help               Display this help screen and exit"
    echo
}

POSTGRES_USER=${POSTGRES_USER:-"proxy_whitelist_owner"}
POSTGRES_HOST=${POSTGRES_HOST:-"localhost"}
POSTGRES_PORT=${POSTGRES_PORT:-5432}
POSTGRES_URL=${POSTGRES_URL:-""}
PROCESS_BLOCK_LIMIT=${PROCESS_BLOCK_LIMIT:-null}
PROXY_WHITELIST_SCHEMA=${PROXY_WHITELIST_SCHEMA:-"proxy_whitelist_app"}

while [ $# -gt 0 ]; do
  case "$1" in
    --host=*)
        POSTGRES_HOST="${1#*=}"
        ;;
    --port=*)
        POSTGRES_PORT="${1#*=}"
        ;;
    --user=*)
        POSTGRES_USER="${1#*=}"
        ;;
    --postgres-url=*)
        POSTGRES_URL="${1#*=}"
        ;;
    --stop-at-block=*)
        PROCESS_BLOCK_LIMIT="${1#*=}"
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

postgres_access(){
  echo "${POSTGRES_URL:-"postgresql://$POSTGRES_USER@$POSTGRES_HOST:$POSTGRES_PORT/haf_block_log?application_name=$1"}"
}

echo "Starting proxy_whitelist..."

# Record startup time for health checks
date -uIseconds > /tmp/proxy_whitelist_startup_time.txt

# The generic HAF block-processing driver (haf#341) runs the registered
# ${PROXY_WHITELIST_SCHEMA}.process_blocks procedure per delivered range and
# idles on its own connection between blocks; hive.app_next_iteration withholds
# blocks hivemind has not committed (registered dependency). It ships with the
# psql base image. exec it directly: as PID 1 it must receive SIGTERM itself.
if command -v haf_app_driver.py >/dev/null 2>&1; then
    limit_arg=()
    [ "$PROCESS_BLOCK_LIMIT" != "null" ] && limit_arg=(--stop-at-block="$PROCESS_BLOCK_LIMIT")
    exec haf_app_driver.py --app="${PROXY_WHITELIST_SCHEMA}" --postgres-url="$(postgres_access proxy_whitelist_block_processing)" \
        --lock=hivemind --lock=proxy-whitelist --startup-marker=/tmp/proxy_whitelist_startup_time.txt "${limit_arg[@]}"
fi
echo "WARNING: haf_app_driver.py not found, falling back to the legacy CALL scheduler() loop"

# Single-threaded: scheduler processes blocks directly, no worker processes needed.
# The _workers parameter is kept for API compatibility but ignored.
psql "$(postgres_access proxy_whitelist_scheduler)" \
  -v ON_ERROR_STOP=on \
  -c "\timing" \
  -c "SET SEARCH_PATH TO ${PROXY_WHITELIST_SCHEMA};" \
  -c "CALL ${PROXY_WHITELIST_SCHEMA}.scheduler('${PROXY_WHITELIST_SCHEMA}', 0, ${PROCESS_BLOCK_LIMIT});" \
|| \
psql "$(postgres_access proxy_whitelist_scheduler)" \
  -v ON_ERROR_STOP=on \
  -c "SET SEARCH_PATH TO ${PROXY_WHITELIST_SCHEMA};" \
  -c "SELECT ${PROXY_WHITELIST_SCHEMA}.stopProcessing();"

echo "Scheduler stopped"
