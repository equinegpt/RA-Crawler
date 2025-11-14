# api/db.py
import os
from sqlalchemy import create_engine

# One path for *everything*. Change only this if you really must.
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
    ddl = """
    CREATE TABLE IF NOT EXISTS race_program (
      id          INTEGER PRIMARY KEY,
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
    with engine.begin() as c:
        c.exec_driver_sql(ddl)
        # For existing DBs, ensure meeting_id column exists
        rows = c.exec_driver_sql("PRAGMA table_info(race_program)").fetchall()
        col_names = {r[1] for r in rows}
        if "meeting_id" not in col_names:
            c.exec_driver_sql("ALTER TABLE race_program ADD COLUMN meeting_id TEXT")
