#!/usr/bin/env bash
set -euo pipefail

echo "[start] Using DATABASE_URL=${DATABASE_URL:-"(unset)"}"

# Always refresh runtime DB from the repo copy
if [ -f "/opt/render/project/src/data/racing.db" ]; then
  echo "[start] Seeding /data/racing.db from repo copy (force overwrite)"
  mkdir -p /data
  cp -f /opt/render/project/src/data/racing.db /data/racing.db
  ls -lh /data/racing.db
fi

# Start the API
exec uvicorn api.main:app --host 0.0.0.0 --port ${PORT:-10000}
