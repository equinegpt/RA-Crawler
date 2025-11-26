#!/usr/bin/env bash
set -euo pipefail

echo "[results_daily.sh] Start $(date -Is)"
echo "[results_daily.sh] DATABASE_URL=${DATABASE_URL:-unset}"

# For Postgres (non-sqlite) we don't need any /data seeding logic.
# If you ever ran this against sqlite locally, you could copy the seeding
# logic from scripts/daily.sh, but Render is using Postgres for ra-crawler.

python -m api.results_daily_job

echo "[results_daily.sh] Done  $(date -Is)"
