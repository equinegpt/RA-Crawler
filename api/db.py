# api/db.py
import os
from sqlalchemy import create_engine

# One path for *everything* in local dev. Change only if you really must.
_DEFAULT_URL = "sqlite:////Users/andrewholmes/web-crawl-db-api/data/racing.db"
DATABASE_URL = os.getenv("DATABASE_URL", _DEFAULT_URL)

engine = create_engine(
    DATABASE_URL,
    future=True,
    pool_pre_ping=True,
)


def get_engine():
    return engine


def ensure_schema():
    """
    Ensure the race_program table exists for local SQLite dev.

    On Postgres (Render) we don't do any DDL here; the schema is created
    separately via api.init_pg_schema.
    """
    # Only run this on SQLite
    if engine.dialect.name != "sqlite":
        return

    ddl = """
    CREATE TABLE IF NOT EXISTS race_program (
      id          INTEGER PRIMARY KEY,
      race_no     INTEGER,
      date        TEXT,
      state       TEXT,
      track       TEXT,
      type        TEXT,
      description TEXT,
      prize       INTEGER,
      condition   TEXT,
      class       TEXT,
      age         TEXT,
      sex         TEXT,
      distance_m  INTEGER,
      bonus       TEXT,
      meeting_id  TEXT,
      url         TEXT
    );
    """
    with engine.begin() as c:
        # Make sure table exists (no-op if it already does)
        c.exec_driver_sql(ddl)

        # For existing DBs, ensure meeting_id column exists (SQLite-specific)
        rows = c.exec_driver_sql("PRAGMA table_info(race_program)").fetchall()
        col_names = {r[1] for r in rows}
        if "meeting_id" not in col_names:
            c.exec_driver_sql("ALTER TABLE race_program ADD COLUMN meeting_id TEXT")
