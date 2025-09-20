cat > start.sh <<'SH'
#!/usr/bin/env bash
set -euo pipefail

# Ensure the persistent disk exists
mkdir -p /data

# Seed /data/racing.db once, if it doesn't exist yet
if [ -f "/opt/render/project/src/data/racing.db" ] && [ ! -f "/data/racing.db" ]; then
  echo "[boot] Seeding /data/racing.db from repo copy…"
  cp /opt/render/project/src/data/racing.db /data/racing.db
fi

# Always point the app at the persistent DB path
export DATABASE_URL="sqlite:////data/racing.db"

# Start the API
exec uvicorn api.main:app --host 0.0.0.0 --port "${PORT:-8000}"
SH
chmod +x start.sh
