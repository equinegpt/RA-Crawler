#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/opt/render/project/src"
REPO_DB="$APP_DIR/data/racing.db"
DISK_DB="/data/racing.db"

mkdir -p /data

if [ ! -s "$DISK_DB" ]; then
  if [ -s "$REPO_DB" ]; then
    echo "[startup] Seeding DB -> $DISK_DB"
    cp "$REPO_DB" "$DISK_DB"
  else:
    echo "[startup] ERROR: seed DB missing at $REPO_DB"
    ls -l "$APP_DIR/data" || true
  fi
else
  echo "[startup] DB already present at $DISK_DB (skipping seed)"
fi

export DATABASE_URL="sqlite:////data/racing.db"

python - <<'PY'
import sqlite3
db="/data/racing.db"
con=sqlite3.connect(db)
cur=con.cursor()
cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='race_program'")
if cur.fetchone():
    cur.execute("SELECT COUNT(*) FROM race_program")
    print(f"[startup] race_program rows:", cur.fetchone()[0])
else:
    print("[startup] WARNING: race_program table not found!")
con.close()
PY

exec uvicorn api.main:app --host 0.0.0.0 --port "$PORT"
