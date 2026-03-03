#!/bin/sh
set -e
cd /app/scripts

if [ "$1" = "install_app" ]; then
  shift
  exec /app/scripts/install_app.sh --host="${POSTGRES_HOST:-haf}" "$@"
elif [ "$1" = "process_blocks" ]; then
  shift
  exec /app/scripts/process_blocks.sh --host="${POSTGRES_HOST:-haf}" "$@"
else
  echo "usage: $0 install_app|process_blocks"
  exit 1
fi
