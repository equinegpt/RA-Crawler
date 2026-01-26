# api/init_pg_schema.py
from __future__ import annotations

import os
import sys

from sqlalchemy import create_engine, text


# ---------------------------------------------------------------------------
# Existing race_program table
# ---------------------------------------------------------------------------

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
    url         TEXT,
    race_time   TEXT
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
    url         TEXT,
    race_time   TEXT
);
"""

# matches what maintenance/dedupe expects
INDEX_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS ux_race_program_ident
ON race_program (date, state, track, race_no, url);
"""


# ---------------------------------------------------------------------------
# NEW: ra_results table (Racing Australia official results)
# ---------------------------------------------------------------------------

RA_RESULTS_TABLE_SQL_POSTGRES = """
CREATE TABLE IF NOT EXISTS ra_results (
    id             SERIAL PRIMARY KEY,
    meeting_date   DATE    NOT NULL,
    state          TEXT    NOT NULL,
    track          TEXT    NOT NULL,
    race_no        INTEGER NOT NULL,
    horse_number   INTEGER NOT NULL,
    horse_name     TEXT    NOT NULL,
    trainer        TEXT,
    jockey         TEXT,
    finishing_pos  INTEGER,
    is_scratched   BOOLEAN NOT NULL DEFAULT FALSE,
    margin_lens    NUMERIC(5,2),
    starting_price NUMERIC(8,2),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

RA_RESULTS_TABLE_SQL_SQLITE = """
CREATE TABLE IF NOT EXISTS ra_results (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    meeting_date   TEXT    NOT NULL,
    state          TEXT    NOT NULL,
    track          TEXT    NOT NULL,
    race_no        INTEGER NOT NULL,
    horse_number   INTEGER NOT NULL,
    horse_name     TEXT    NOT NULL,
    trainer        TEXT,
    jockey         TEXT,
    finishing_pos  INTEGER,
    is_scratched   INTEGER NOT NULL DEFAULT 0,
    margin_lens    REAL,
    starting_price REAL,
    created_at     TEXT,
    updated_at     TEXT
);
"""

# Unique runner key: date + state + track + race + horse_number
RA_RESULTS_UNIQUE_INDEX_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS ux_ra_results_runner
ON ra_results (meeting_date, state, track, race_no, horse_number);
"""

# Helpful filter indexes
RA_RESULTS_INDEX_MEETING_DATE_SQL = """
CREATE INDEX IF NOT EXISTS ix_ra_results_meeting_date
ON ra_results (meeting_date);
"""

RA_RESULTS_INDEX_STATE_SQL = """
CREATE INDEX IF NOT EXISTS ix_ra_results_state
ON ra_results (state);
"""

RA_RESULTS_INDEX_TRACK_SQL = """
CREATE INDEX IF NOT EXISTS ix_ra_results_track
ON ra_results (track);
"""

# ---------------------------------------------------------------------------
# Migration: Add race_time column to existing race_program table
# ---------------------------------------------------------------------------

ADD_RACE_TIME_COLUMN_SQL = """
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'race_program' AND column_name = 'race_time'
    ) THEN
        ALTER TABLE race_program ADD COLUMN race_time TEXT;
    END IF;
END $$;
"""

# ---------------------------------------------------------------------------
# Migration: Add trainer and jockey columns to existing ra_results table
# ---------------------------------------------------------------------------

ADD_TRAINER_JOCKEY_COLUMNS_SQL = """
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'ra_results' AND column_name = 'trainer'
    ) THEN
        ALTER TABLE ra_results ADD COLUMN trainer TEXT;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'ra_results' AND column_name = 'jockey'
    ) THEN
        ALTER TABLE ra_results ADD COLUMN jockey TEXT;
    END IF;
END $$;
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
            # race_program
            conn.execute(text(TABLE_SQL_POSTGRES))
            conn.execute(text(INDEX_SQL))
            # Migration: add race_time column if missing
            conn.execute(text(ADD_RACE_TIME_COLUMN_SQL))

            # ra_results
            conn.execute(text(RA_RESULTS_TABLE_SQL_POSTGRES))
            conn.execute(text(RA_RESULTS_UNIQUE_INDEX_SQL))
            conn.execute(text(RA_RESULTS_INDEX_MEETING_DATE_SQL))
            conn.execute(text(RA_RESULTS_INDEX_STATE_SQL))
            conn.execute(text(RA_RESULTS_INDEX_TRACK_SQL))
            # Migration: add trainer/jockey columns if missing
            conn.execute(text(ADD_TRAINER_JOCKEY_COLUMNS_SQL))

        elif dialect == "sqlite":
            # race_program
            conn.execute(text(TABLE_SQL_SQLITE))
            conn.execute(text(INDEX_SQL))

            # ra_results
            conn.execute(text(RA_RESULTS_TABLE_SQL_SQLITE))
            conn.execute(text(RA_RESULTS_UNIQUE_INDEX_SQL))
            conn.execute(text(RA_RESULTS_INDEX_MEETING_DATE_SQL))
            conn.execute(text(RA_RESULTS_INDEX_STATE_SQL))
            conn.execute(text(RA_RESULTS_INDEX_TRACK_SQL))

        else:
            print(
                f"[init_pg_schema] WARNING: unknown dialect {dialect}, "
                f"defaulting to Postgres-style DDL"
            )
            conn.execute(text(TABLE_SQL_POSTGRES))
            conn.execute(text(INDEX_SQL))

            conn.execute(text(RA_RESULTS_TABLE_SQL_POSTGRES))
            conn.execute(text(RA_RESULTS_UNIQUE_INDEX_SQL))
            conn.execute(text(RA_RESULTS_INDEX_MEETING_DATE_SQL))
            conn.execute(text(RA_RESULTS_INDEX_STATE_SQL))
            conn.execute(text(RA_RESULTS_INDEX_TRACK_SQL))

    print("[init_pg_schema] Schema ensured OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
