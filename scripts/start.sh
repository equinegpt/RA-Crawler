#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/opt/render/project/src"
REPO_DB="$APP_DIR/data/racing.db"
DISK_DB="/data/racing.db"

mkdir -p /data

# Seed if missing or zero bytes
if [ ! -s "$DISK_DB" ]; then
  echo "[startup] Seeding DB -> $DISK_DB"
  cp "$REPO_DB" "$DISK_DB"
else
  echo "[startup] DB already present at $DISK_DB (skipping seed)"
fi

export DATABASE_URL="sqlite:////data/racing.db"

# sanity log
python - <<'PY'
import sqlite3
con=sqlite3.connect("/data/racing.db")
cur=con.cursor()
cur.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='race_program'")
if cur.fetchone()[0]:
    cur.execute("SELECT COUNT(*) FROM race_program")
    print("[startup] race_program rows:", cur.fetchone()[0])
else:
    print("[startup] WARNING: race_program table not found!")
con.close()
PY

exec uvicorn api.main:app --host 0.0.0.0 --port "$PORT"
