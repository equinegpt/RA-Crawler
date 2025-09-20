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
