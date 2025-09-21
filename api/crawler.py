# api/crawler.py
from __future__ import annotations

from typing import Any, Dict, Iterable, Optional, Tuple, Union
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine
from .db import get_engine

# ---------------- Schema bootstrap ----------------

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS race_program (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
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

def _ensure_table_exists(conn: Connection) -> None:
    conn.exec_driver_sql(CREATE_TABLE_SQL)

# ---------------- Normalization ----------------

def _to_int_or_none(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        return None

def _normalize_row(r: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "race_no":     _to_int_or_none(r.get("race_no")),
        "date":        r.get("date"),
        "state":       r.get("state"),
        "track":       r.get("track"),
        "type":        r.get("type"),
        "description": r.get("description"),
        "prize":       _to_int_or_none(r.get("prize")),
        "condition":   r.get("condition"),
        "class":       r.get("class"),
        "age":         r.get("age"),
        "sex":         r.get("sex"),
        "distance_m":  _to_int_or_none(r.get("distance_m")),
        "bonus":       r.get("bonus"),
        "url":         r.get("url"),
    }

# ---------------- SQL ----------------

_UPDATE_SQL = text("""
UPDATE race_program
   SET type        = :type,
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
   AND (description = :description OR :description IS NULL)
""")

_INSERT_SQL = text("""
INSERT INTO race_program (
    race_no, date, state, track, type, description, prize,
    condition, class, age, sex, distance_m, bonus, url
) VALUES (
    :race_no, :date, :state, :track, :type, :description, :prize,
    :condition, :class, :age, :sex, :distance_m, :bonus, :url
)
""")

# ---------------- Upsert (backwards compatible) ----------------

def upsert_program_rows(
    conn_or_rows: Union[Connection, Iterable[Dict[str, Any]]],
    maybe_rows: Optional[Iterable[Dict[str, Any]]] = None,
    debug: bool = False,
) -> Tuple[int, int]:
    """
    Supports both:
      - upsert_program_rows(rows, debug=False)
      - upsert_program_rows(conn, rows, debug=False)

    Returns (inserted_count, updated_count).
    """
    # Case A: new-style call: only rows passed
    if maybe_rows is None:
        rows = conn_or_rows  # type: ignore[assignment]
        eng: Engine = get_engine()
        inserted = 0
        updated = 0
        with eng.begin() as conn:
            _ensure_table_exists(conn)
            for r in rows:  # type: ignore[assignment]
                row = _normalize_row(r)
                res = conn.execute(_UPDATE_SQL, row)
                if res.rowcount and res.rowcount > 0:
                    updated += res.rowcount
                    if debug:
                        print(f"[upsert] updated -> {row.get('date')},{row.get('state')},{row.get('track')} R{row.get('race_no')}")
                else:
                    conn.execute(_INSERT_SQL, row)
                    inserted += 1
                    if debug:
                        print(f"[upsert] inserted -> {row.get('date')},{row.get('state')},{row.get('track')} R{row.get('race_no')}")
        return inserted, updated

    # Case B: old-style call: (conn, rows)
    conn: Connection = conn_or_rows  # type: ignore[assignment]
    rows = maybe_rows
    inserted = 0
    updated = 0

    # If caller already has a transaction open, DON'T start another one.
    if getattr(conn, "in_transaction", None) and conn.in_transaction():
        _ensure_table_exists(conn)
        for r in rows:
            row = _normalize_row(r)
            res = conn.execute(_UPDATE_SQL, row)
            if res.rowcount and res.rowcount > 0:
                updated += res.rowcount
                if debug:
                    print(f"[upsert] updated -> {row.get('date')},{row.get('state')},{row.get('track')} R{row.get('race_no')}")
            else:
                conn.execute(_INSERT_SQL, row)
                inserted += 1
                if debug:
                    print(f"[upsert] inserted -> {row.get('date')},{row.get('state')},{row.get('track')} R{row.get('race_no')}")
        return inserted, updated
    else:
        # No active txn -> we can manage our own
        with conn.begin():
            _ensure_table_exists(conn)
            for r in rows:
                row = _normalize_row(r)
                res = conn.execute(_UPDATE_SQL, row)
                if res.rowcount and res.rowcount > 0:
                    updated += res.rowcount
                    if debug:
                        print(f"[upsert] updated -> {row.get('date')},{row.get('state')},{row.get('track')} R{row.get('race_no')}")
                else:
                    conn.execute(_INSERT_SQL, row)
                    inserted += 1
                    if debug:
                        print(f"[upsert] inserted -> {row.get('date')},{row.get('state')},{row.get('track')} R{row.get('race_no')}")
        return inserted, updated

# ---------------- Utility ----------------

def count_rows() -> int:
    eng: Engine = get_engine()
    with eng.connect() as conn:
        _ensure_table_exists(conn)
        return conn.execute(text("SELECT COUNT(*) FROM race_program")).scalar_one()
