#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="/opt/render/project/src"
SEED_DB="$APP_ROOT/data/racing.db"

RUN_DB="/data/racing.db"

echo "[start] DATABASE_URL=${DATABASE_URL:-<unset>}"
echo "[start] checking runtime DB…"

copy_needed=0

# If no runtime DB (or empty), seed once from the repo copy
if [ ! -s "$RUN_DB" ] && [ -s "$SEED_DB" ]; then
  echo "[start] /data/racing.db missing or empty → will seed from repo"
  copy_needed=1
fi

# Manual override: FORCE_SEED=1 will reseed even if DB exists
if [ "${FORCE_SEED:-0}" = "1" ]; then
  echo "[start] FORCE_SEED=1 → will reseed from repo"
  copy_needed=1
fi

if [ "$copy_needed" = "1" ]; then
  mkdir -p /data
  cp -f "$SEED_DB" "$RUN_DB"
  chmod 664 "$RUN_DB"
  echo "[start] seeded /data/racing.db"
else
  echo "[start] keeping existing /data/racing.db"
fi

# finally start the API
exec uvicorn api.main:app --host 0.0.0.0 --port "${PORT:-10000}"
