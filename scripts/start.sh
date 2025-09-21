#!/usr/bin/env bash
set -euo pipefail

echo "[start] ensuring /data exists"
mkdir -p /data

# Always refresh the runtime DB from the repo copy
if [ -f /opt/render/project/src/data/racing.db ]; then
  echo "[start] updating runtime DB from repo copy"
  cp -f /opt/render/project/src/data/racing.db /data/racing.db
  ls -lh /data/racing.db || true
fi

echo "[start] launching API"
exec uvicorn api.main:app --host 0.0.0.0 --port "${PORT:-10000}"
