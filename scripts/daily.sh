#!/usr/bin/env bash
set -euo pipefail

echo "[daily.sh] Start $(date -Is)"
echo "[daily.sh] DATABASE_URL=${DATABASE_URL:-unset}"

# Ensure we have a DB at /data; if it's empty and a seed exists, copy it once.
if [ ! -s /data/racing.db ] && [ -s /opt/render/project/src/data/racing.db ]; then
  echo "[daily.sh] Seeding /data/racing.db from repo copy…"
  cp -f /opt/render/project/src/data/racing.db /data/racing.db
fi

# Run the Python daily job (uses DATABASE_URL, defaults to sqlite:////data/racing.db)
python -m api.daily_job

echo "[daily.sh] Done  $(date -Is)"
