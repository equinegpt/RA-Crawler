#!/usr/bin/env bash
set -euo pipefail

echo "[start] PORT=${PORT:-unset}"
echo "[start] DATABASE_URL=${DATABASE_URL:-unset}"

# Show both files BEFORE copy
echo "[start] repo DB at /opt/render/project/src/data/racing.db:"
ls -lh /opt/render/project/src/data/racing.db || true
shasum -a 256 /opt/render/project/src/data/racing.db || true
sqlite3 /opt/render/project/src/data/racing.db "SELECT COUNT(*) FROM race_program;" || true

echo "[start] current runtime DB at /data/racing.db:"
ls -lh /data/racing.db || true
shasum -a 256 /data/racing.db || true
sqlite3 /data/racing.db "SELECT COUNT(*) FROM race_program;" || true

# Seed (force)
mkdir -p /data
cp -f /opt/render/project/src/data/racing.db /data/racing.db

# Show both AFTER copy
echo "[start] AFTER COPY -> runtime DB at /data/racing.db:"
ls -lh /data/racing.db
shasum -a 256 /data/racing.db
sqlite3 /data/racing.db "SELECT COUNT(*) FROM race_program;"

# One-time date normalization (safe no-op if already normalized)
sqlite3 /data/racing.db "
UPDATE race_program
SET date = date(date)
WHERE date IS NOT NULL AND trim(date) <> '';
"

# Show date span and per-day counts (first 5/last 5 for brevity)
echo "[start] Date span:"
sqlite3 /data/racing.db "SELECT MIN(date), MAX(date) FROM race_program;"
echo "[start] Sample per-day counts:"
sqlite3 /data/racing.db "
SELECT date(date) d, COUNT(*)
FROM race_program
GROUP BY d
ORDER BY d
LIMIT 5;
"
sqlite3 /data/racing.db "
SELECT date(date) d, COUNT(*)
FROM race_program
GROUP BY d
ORDER BY d DESC
LIMIT 5;
"

# Start API
exec uvicorn api.main:app --host 0.0.0.0 --port "$PORT"
