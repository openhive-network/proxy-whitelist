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
else
  echo "usage: $0 install_app|process_blocks|replica_install|replica_sync"
  exit 1
fi
