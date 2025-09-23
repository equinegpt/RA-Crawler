#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="/opt/render/project/src"
SEED_DB="$APP_ROOT/data/racing.db"
SEED_VER_FILE="$APP_ROOT/data/seed.version"

RUN_DB="/data/racing.db"
RUN_VER_FILE="/data/.seed_version"

echo "[start] DATABASE_URL=${DATABASE_URL:-<unset>}"
echo "[start] checking seed DB…"

copy_needed=0

# if no runtime DB, we must seed
if [ ! -f "$RUN_DB" ]; then
  echo "[start] /data/racing.db missing → will seed"
  copy_needed=1
else
  # compare checksums
  seed_sum=$(sha256sum "$SEED_DB" | awk '{print $1}')
  run_sum=$(sha256sum "$RUN_DB"  | awk '{print $1}')
  if [ "$seed_sum" != "$run_sum" ]; then
    echo "[start] checksum differs → will seed"
    copy_needed=1
  fi
fi

# version bump switch: if version differs, force reseed even if checksum same
seed_ver="(none)"
run_ver="(none)"
if [ -f "$SEED_VER_FILE" ]; then seed_ver=$(cat "$SEED_VER_FILE"); fi
if [ -f "$RUN_VER_FILE" ];  then run_ver=$(cat "$RUN_VER_FILE");  fi

if [ "$seed_ver" != "$run_ver" ]; then
  echo "[start] version mismatch ($run_ver → $seed_ver) → will seed"
  copy_needed=1
fi

if [ "${FORCE_SEED:-0}" = "1" ]; then
  echo "[start] FORCE_SEED=1 → will seed"
  copy_needed=1
fi

if [ "$copy_needed" = "1" ]; then
  mkdir -p /data
  cp -f "$SEED_DB" "$RUN_DB"
  chmod 664 "$RUN_DB"
  # remember version so we don’t copy next deploy unless bumped
  if [ -f "$SEED_VER_FILE" ]; then
    cp -f "$SEED_VER_FILE" "$RUN_VER_FILE"
  else
    echo "no-version" > "$RUN_VER_FILE"
  fi
  echo "[start] seeded /data/racing.db"
else
  echo "[start] keeping existing /data/racing.db"
fi

# finally start the API
exec uvicorn api.main:app --host 0.0.0.0 --port "${PORT:-10000}"
