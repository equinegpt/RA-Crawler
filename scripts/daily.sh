# scripts/daily.sh
#!/usr/bin/env bash
set -euo pipefail

echo "[daily.sh] Start $(date -Is)"
echo "[daily.sh] DATABASE_URL=${DATABASE_URL:-unset}"

# Only bother with /data/racing.db when using sqlite
if [[ "${DATABASE_URL:-}" == sqlite:* ]]; then
  echo "[daily.sh] sqlite mode → ensuring /data/racing.db"
  mkdir -p /data
  if [ ! -s /data/racing.db ] && [ -s /opt/render/project/src/data/racing.db ]; then
    echo "[daily.sh] Seeding /data/racing.db from repo copy…"
    cp -f /opt/render/project/src/data/racing.db /data/racing.db
  fi
else
  echo "[daily.sh] non-sqlite DATABASE_URL → running schema migrations"
  python -m api.init_pg_schema || echo "[daily.sh] WARNING: schema migration failed, continuing anyway"
fi

python -m api.daily_job

echo "[daily.sh] Done  $(date -Is)"
