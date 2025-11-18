# api/db.py
from __future__ import annotations

import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# -------------------------------------------------------------------
# Single source of truth for the DB URL
# -------------------------------------------------------------------

# 1) Prefer DATABASE_URL from the environment (Render / shell)
DATABASE_URL = os.getenv("DATABASE_URL")

# 2) Fallback ONLY if it's completely unset (for local dev)
if not DATABASE_URL:
    # Local dev default – you can change this if you like
    DATABASE_URL = "sqlite:///./data/racing.db"

# Create global engine
engine: Engine = create_engine(
    DATABASE_URL,
    future=True,
    pool_pre_ping=True,
)

def get_engine() -> Engine:
    """Return the global SQLAlchemy engine."""
    return engine


def ensure_schema() -> None:
    """
    Very simple schema creator.

    - Works for both SQLite and Postgres.
    - DOES NOT use SQLite-specific PRAGMA calls (which break on Postgres).
    - We rely on api/init_pg_schema.py for richer PG setup.
    """
    ddl = """
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
    try:
        with engine.begin() as c:
            c.exec_driver_sql(ddl)
    except Exception:
        # If running against an existing schema this is effectively a no-op.
        # We don't care if this fails on PG as long as the table already exists.
        pass
