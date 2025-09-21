# scripts/start.sh
#!/usr/bin/env bash
set -euo pipefail

# ALWAYS replace the runtime DB with the seed at boot
if [ -f "/opt/render/project/src/data/racing.db" ]; then
  mkdir -p /data
  cp -f /opt/render/project/src/data/racing.db /data/racing.db
fi

# sanity logs (you’ll see these in Render logs)
echo "DATABASE_URL=${DATABASE_URL:-sqlite:////data/racing.db}"
sqlite3 /data/racing.db "SELECT COUNT(*) FROM race_program;" || true
sqlite3 /data/racing.db "SELECT COUNT(*) FROM race_program WHERE distance_m IS NULL;" || true

# start API
exec uvicorn api.main:app --host 0.0.0.0 --port "$PORT"
