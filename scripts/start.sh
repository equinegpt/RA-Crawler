#!/usr/bin/env bash
set -euo pipefail

# Always replace the runtime DB from the repo seed
if [ -f "/opt/render/project/src/data/racing.db" ]; then
  mkdir -p /data
  cp -f /opt/render/project/src/data/racing.db /data/racing.db
fi

# Show what DB the app will use
echo "DATABASE_URL=${DATABASE_URL:-sqlite:////data/racing.db}"
sqlite3 /data/racing.db "SELECT COUNT(*) FROM race_program;" || true
sqlite3 /data/racing.db "SELECT COUNT(*) FROM race_program WHERE distance_m IS NULL;" || true

# Start the API
exec uvicorn api.main:app --host 0.0.0.0 --port "$PORT"
