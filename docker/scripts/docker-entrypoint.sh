#!/bin/sh
set -e
cd /app/scripts

if [ "$1" = "install_app" ]; then
  shift
  exec /app/scripts/install_app.sh --host="${POSTGRES_HOST:-haf}" "$@"
elif [ "$1" = "process_blocks" ]; then
  shift
  exec /app/scripts/process_blocks.sh --host="${POSTGRES_HOST:-haf}" "$@"
elif [ "$1" = "replica_install" ]; then
  shift
  exec /app/replica/install.sh "$@"
elif [ "$1" = "replica_sync" ]; then
  shift
  exec python3 /app/replica/sync.py "$@"
elif [ "$1" = "replica_agent" ]; then
  # haproxy's agent-check port: one run of the check per connection
  exec nc -lk -p "${REPLICA_AGENT_PORT:-7015}" -e /app/replica/healthcheck/proxy_whitelist_replica.sh
else
  echo "usage: $0 install_app|process_blocks|replica_install|replica_sync|replica_agent"
  exit 1
fi
