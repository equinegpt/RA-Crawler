# api/crawler.py
"""
DB writer helpers for the RA crawler.

Back-compat for callers that invoke:
  - upsert_program_rows(rows)
  - upsert_program_rows(engine, rows)
  - upsert_program_rows(connection, rows)

Behavior:
  * Ensures race_program table + unique index exist
  * Normalizes date to YYYY-MM-DD
  * UPDATE first; if no match, INSERT
  * Safe transaction handling for Engine/Connection/no-arg modes
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, Iterable, List, Tuple, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine, Connection

from .db import get_engine

RA_DB_VERBOSE = bool(int(os.getenv("RA_DB_VERBOSE", "0")))

# --------------------------- Schema ---------------------------

CREATE_TABLE_SQL = """
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

CREATE_UNIQUE_INDEX_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS ix_race_program_identity
ON race_program (date, state, track, race_no);
"""

def _ensure_schema_via_engine(eng: Engine) -> None:
    with eng.begin() as conn:
        conn.exec_driver_sql(CREATE_TABLE_SQL)
        conn.exec_driver_sql(CREATE_UNIQUE_INDEX_SQL)

def _ensure_schema_via_connection(conn: Connection) -> None:
    manage_tx = not conn.in_transaction()
    if manage_tx:
        tx = conn.begin()
    try:
        conn.exec_driver_sql(CREATE_TABLE_SQL)
        conn.exec_driver_sql(CREATE_UNIQUE_INDEX_SQL)
        if manage_tx:
            tx.commit()
    except Exception:
        if manage_tx:
            tx.rollback()
        raise

# ------------------------ Normalization -----------------------

def _norm_date(d: Any) -> Optional[str]:
    if not d:
        return None
    s = str(d).strip()
    if not s:
        return None

    # Common formats
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass

    # Handle 'YYYY-M-D' or time suffixes
    base = s.split(" ", 1)[0].split("T", 1)[0]
    parts = base.split("-")
    if len(parts) == 3 and all(parts):
        y, m, d2 = parts
        if len(m) == 1: m = "0" + m
        if len(d2) == 1: d2 = "0" + d2
        return f"{y}-{m}-{d2}"

    try:
        return datetime.fromisoformat(base).strftime("%Y-%m-%d")
    except Exception:
        return s  # last resort: preserve

def _coerce_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, int):
        return v
    s = str(v).strip()
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return None

def _clean_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None

# ------------------------- SQL bits ---------------------------

UPDATE_SQL = text("""
    UPDATE race_program
    SET
        meeting_id  = :meeting_id,
        type        = :type,
        description = :description,
        prize       = :prize,
        condition   = :condition,
        class       = :class,
        age         = :age,
        sex         = :sex,
        distance_m  = :distance_m,
        bonus       = :bonus,
        url         = :url
    WHERE date    = :date
      AND state   = :state
      AND track   = :track
      AND race_no = :race_no
""")

INSERT_SQL = text("""
    INSERT INTO race_program (
        race_no, date, state, track, meeting_id, type, description, prize,
        condition, class, age, sex, distance_m, bonus, url
    ) VALUES (
        :race_no, :date, :state, :track, :meeting_id, :type, :description, :prize,
        :condition, :class, :age, :sex, :distance_m, :bonus, :url
    )
""")

def _prep_params(r: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "race_no":    _coerce_int(r.get("race_no")),
        "date":       _norm_date(r.get("date")),
        "state":      (_clean_str(r.get("state")) or ""),
        "track":      (_clean_str(r.get("track")) or ""),
        "meeting_id": _clean_str(r.get("meeting_id")),   # ← new
        "type":       _clean_str(r.get("type")),
        "description":(_clean_str(r.get("description")) or ""),
        "prize":      _coerce_int(r.get("prize")),
        "condition":  _clean_str(r.get("condition")),
        "class":      _clean_str(r.get("class")),
        "age":        _clean_str(r.get("age")),
        "sex":        _clean_str(r.get("sex")),
        "distance_m": _coerce_int(r.get("distance_m")),
        "bonus":      _clean_str(r.get("bonus")),
        "url":        (_clean_str(r.get("url")) or ""),
    }

def _ensure_meeting_id_column(conn) -> None:
    """Add meeting_id column if missing (for existing SQLite DBs)."""
    try:
        rows = conn.exec_driver_sql("PRAGMA table_info(race_program)").fetchall()
        col_names = {r[1] for r in rows}
        if "meeting_id" not in col_names:
            conn.exec_driver_sql("ALTER TABLE race_program ADD COLUMN meeting_id TEXT")
            if RA_DB_VERBOSE:
                print("[crawler] Added meeting_id column to race_program")
    except Exception as e:
        if RA_DB_VERBOSE:
            print(f"[crawler] WARNING: could not ensure meeting_id column: {e}")

def _ensure_schema_via_engine(eng: Engine) -> None:
    with eng.begin() as conn:
        conn.exec_driver_sql(CREATE_TABLE_SQL)
        conn.exec_driver_sql(CREATE_UNIQUE_INDEX_SQL)
        _ensure_meeting_id_column(conn)

def _ensure_schema_via_connection(conn: Connection) -> None:
    manage_tx = not conn.in_transaction()
    if manage_tx:
        tx = conn.begin()
    try:
        conn.exec_driver_sql(CREATE_TABLE_SQL)
        conn.exec_driver_sql(CREATE_UNIQUE_INDEX_SQL)
        _ensure_meeting_id_column(conn)
        if manage_tx:
            tx.commit()
    except Exception:
        if manage_tx:
            tx.rollback()
        raise

# ---------------------- Public entrypoint ---------------------

def upsert_program_rows(*args) -> Tuple[int, int]:
    """
    Back-compat signature:

      upsert_program_rows(rows)
      upsert_program_rows(engine, rows)
      upsert_program_rows(connection, rows)

    Returns: (saved_count, updated_count)
    """
    # Parse args
    eng: Optional[Engine] = None
    conn: Optional[Connection] = None
    rows: Optional[Iterable[Dict[str, Any]]] = None

    if len(args) == 1:
        # rows only
        rows = args[0]
    elif len(args) == 2:
        first, second = args
        if isinstance(first, Engine):
            eng = first
            rows = second
        elif isinstance(first, Connection):
            conn = first
            rows = second
        else:
            # tolerate callers passing a random object; try to treat first as rows
            # and ignore second (but this is unusual). Better to raise:
            raise TypeError("upsert_program_rows expects (rows) or (Engine, rows) or (Connection, rows)")
    else:
        raise TypeError("upsert_program_rows expects 1 or 2 arguments")

    # Materialize rows
    rows_list = list(rows or [])
    if not rows_list:
        return (0, 0)

    # Acquire engine/connection as needed
    if conn is not None:
        # Use provided connection; ensure schema with this connection
        _ensure_schema_via_connection(conn)
        manage_tx = not conn.in_transaction()
        if manage_tx:
            tx = conn.begin()
        saved = 0
        updated = 0
        try:
            for r in rows_list:
                p = _prep_params(r)
                if not p["date"] or not p["state"] or not p["track"] or p["race_no"] is None:
                    if RA_DB_VERBOSE:
                        print(f"[crawler] SKIP malformed row via-conn: {p}")
                    continue
                res = conn.execute(UPDATE_SQL, p)
                if res.rowcount and res.rowcount > 0:
                    updated += res.rowcount
                else:
                    conn.execute(INSERT_SQL, p)
                    saved += 1
            if manage_tx:
                tx.commit()
        except Exception:
            if manage_tx:
                tx.rollback()
            raise
        if RA_DB_VERBOSE:
            print(f"[crawler] upsert saved={saved} updated={updated}")
        return (saved, updated)

    # No explicit connection: use engine (provided or default)
    if eng is None:
        eng = get_engine()

    _ensure_schema_via_engine(eng)

    saved = 0
    updated = 0
    with eng.begin() as c:
        for r in rows_list:
            p = _prep_params(r)
            if not p["date"] or not p["state"] or not p["track"] or p["race_no"] is None:
                if RA_DB_VERBOSE:
                    print(f"[crawler] SKIP malformed row via-eng: {p}")
                continue
            res = c.execute(UPDATE_SQL, p)
            if res.rowcount and res.rowcount > 0:
                updated += res.rowcount
            else:
                c.execute(INSERT_SQL, p)
                saved += 1

    if RA_DB_VERBOSE:
        print(f"[crawler] upsert saved={saved} updated={updated}")
    return (saved, updated)

# ---------------------- Optional utility ----------------------

def count_rows() -> int:
    try:
        eng = get_engine()
        with eng.connect() as c:
            return int(c.exec_driver_sql("SELECT COUNT(*) FROM race_program").scalar() or 0)
    except Exception:
        return 0
