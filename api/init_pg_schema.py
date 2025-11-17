# api/init_pg_schema.py
from __future__ import annotations

import os
import sys

from sqlalchemy import create_engine, text


TABLE_SQL_POSTGRES = """
CREATE TABLE IF NOT EXISTS race_program (
    id          SERIAL PRIMARY KEY,
    race_no     INTEGER,
    date        DATE,
    state       TEXT,
    track       TEXT,
    meeting_id  TEXT,
    type        TEXT,
    description TEXT,
    prize       INTEGER,
    condition   TEXT,
    class       TEXT,
    age         TEXT,
    sex         TEXT,
    distance_m  INTEGER,
    bonus       TEXT,
    url         TEXT
);
"""

# optional: if you ever run this against sqlite for some reason
TABLE_SQL_SQLITE = """
CREATE TABLE IF NOT EXISTS race_program (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    race_no     INTEGER,
    date        TEXT,
    state       TEXT,
    track       TEXT,
    meeting_id  TEXT,
    type        TEXT,
    description TEXT,
    prize       INTEGER,
    condition   TEXT,
    class       TEXT,
    age         TEXT,
    sex         TEXT,
    distance_m  INTEGER,
    bonus       TEXT,
    url         TEXT
);
"""

# matches what maintenance/dedupe expects
INDEX_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS ux_race_program_ident
ON race_program (date, state, track, race_no, url);
"""


def main() -> int:
    url = os.getenv("DATABASE_URL")
    if not url:
        print("[init_pg_schema] ERROR: DATABASE_URL is not set", file=sys.stderr)
        return 1

    print(f"[init_pg_schema] Using DATABASE_URL={url}")

    eng = create_engine(url, future=True)

    with eng.begin() as conn:
        dialect = eng.dialect.name
        print(f"[init_pg_schema] Detected dialect={dialect}")

        if dialect == "postgresql":
            conn.execute(text(TABLE_SQL_POSTGRES))
        elif dialect == "sqlite":
            conn.execute(text(TABLE_SQL_SQLITE))
        else:
            print(f"[init_pg_schema] WARNING: unknown dialect {dialect}, "
                  f"defaulting to Postgres-style DDL")
            conn.execute(text(TABLE_SQL_POSTGRES))

        conn.execute(text(INDEX_SQL))

    print("[init_pg_schema] Schema ensured OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
