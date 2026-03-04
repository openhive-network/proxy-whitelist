#!/bin/bash
set -e
set -o pipefail

print_help () {
    echo "Usage: $0 [OPTION[=VALUE]]..."
    echo
    echo "Starts the proxy_whitelist block processing (scheduler + workers)."
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

NUMBER_OF_WORKERS="$(psql "$(postgres_access proxy_whitelist_block_processing)" -v "ON_ERROR_STOP=on" -t -c "SELECT parallel_workers FROM ${PROXY_WHITELIST_SCHEMA}.app_status" | xargs)"

echo "Starting proxy_whitelist with ${NUMBER_OF_WORKERS} workers..."

launch_worker() {
    local n_blocks="${2:-null}"
    local worker=${1}

    trap '' SIGINT SIGTERM

    setsid bash <<EOF
    psql "$(postgres_access "proxy_whitelist_worker_${worker}")" \
      -v ON_ERROR_STOP=on \
      -c "\\timing" \
      -c "SET SEARCH_PATH TO ${PROXY_WHITELIST_SCHEMA};" \
      -c "CALL ${PROXY_WHITELIST_SCHEMA}.worker_loop(${worker}, '${PROXY_WHITELIST_SCHEMA}', ${n_blocks});" \
    || \
    psql "$(postgres_access "proxy_whitelist_worker_${worker}")" \
      -v ON_ERROR_STOP=on \
      -c "SET SEARCH_PATH TO ${PROXY_WHITELIST_SCHEMA};" \
      -c "SELECT ${PROXY_WHITELIST_SCHEMA}.stopProcessing();"
EOF
    echo "Worker ${worker} stopped"
}

launch_scheduler() {
    local n_blocks="${2:-null}"
    local num_workers=${1}

    trap '' SIGINT SIGTERM

    setsid bash <<EOF
    psql "$(postgres_access proxy_whitelist_scheduler)" \
      -v ON_ERROR_STOP=on \
      -c "\\timing" \
      -c "SET SEARCH_PATH TO ${PROXY_WHITELIST_SCHEMA};" \
      -c "CALL ${PROXY_WHITELIST_SCHEMA}.scheduler('${PROXY_WHITELIST_SCHEMA}', ${num_workers}, ${n_blocks});" \
    || \
    psql "$(postgres_access proxy_whitelist_scheduler)" \
      -v ON_ERROR_STOP=on \
      -c "SET SEARCH_PATH TO ${PROXY_WHITELIST_SCHEMA};" \
      -c "SELECT ${PROXY_WHITELIST_SCHEMA}.stopProcessing();"
EOF
    echo "Scheduler stopped"
}

# Record startup time for health checks
date -uIseconds > /tmp/proxy_whitelist_startup_time.txt

# Wait for Redis to be ready (loading RDB can take minutes for large datasets)
echo "Waiting for Redis to accept commands..."
while true; do
    if psql "$(postgres_access proxy_whitelist_redis_check)" -v "ON_ERROR_STOP=on" -t -c \
        "SELECT proxy_whitelist_app.redis_whitelist_urls(ARRAY[]::TEXT[], '', NULL);" \
        >/dev/null 2>&1; then
        echo "Redis is ready."
        break
    fi
    echo "Redis not ready yet, retrying in 5s..."
    sleep 5
done

launch_scheduler "$NUMBER_OF_WORKERS" "$PROCESS_BLOCK_LIMIT" &

i=1
while [ "$i" -le "$NUMBER_OF_WORKERS" ]; do
    launch_worker "$i" "$PROCESS_BLOCK_LIMIT" &
    i=$((i + 1))
done

terminate_jobs() {
    echo "Stopping proxy_whitelist workers..."
    psql "$(postgres_access proxy_whitelist_breaker)" -v "ON_ERROR_STOP=on" -t \
      -c "SET SEARCH_PATH TO ${PROXY_WHITELIST_SCHEMA}; SELECT ${PROXY_WHITELIST_SCHEMA}.stopProcessing();"
    wait
}

trap 'terminate_jobs' INT TERM

wait
