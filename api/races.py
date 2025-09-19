# api/races.py
from __future__ import annotations

from datetime import date
from typing import Optional, Dict, Any, List

from fastapi import APIRouter, Query
from sqlalchemy import text
from sqlalchemy.engine import Engine

# Try to get your project's engine; fall back to a local maker if needed.
try:
    from .db import engine as _engine  # type: ignore
except Exception:  # pragma: no cover
    _engine = None  # type: ignore

try:
    from .db import get_engine as _get_engine  # type: ignore
except Exception:  # pragma: no cover
    _get_engine = None  # type: ignore

races_router = APIRouter(tags=["races"])


def _resolve_engine() -> Engine:
    if _get_engine:
        return _get_engine()  # type: ignore[return-value]
    if _engine is None:
        raise RuntimeError("Could not resolve database engine from api.db")
    return _engine  # type: ignore[return-value]


def _table_columns(conn) -> List[str]:
    # Introspect columns from SQLite; portable enough for this case
    cols = []
    for row in conn.execute(text("PRAGMA table_info(race_program)")):
        # row: (cid, name, type, notnull, dflt_value, pk)
        cols.append(row[1])
    return cols


def _select_cols(conn) -> str:
    present = set(_table_columns(conn))

    # Preferred order. Only include if present in the DB.
    ordered = [
        "id",
        "race_no",
        "date",
        "state",
        "track",
        "type",          # may not exist in older DBs
        "description",
        "prize",
        "condition",
        "class",
        "age",
        "sex",
        "distance_m",
        "bonus",
        "url",
    ]
    cols = [c for c in ordered if c in present]
    if not cols:
        # safety net, but this should never happen
        cols = ["*"]
    return ", ".join(cols)


@races_router.get("/races")
def list_races(
    # NOTE: limit=0 means "no limit"
    limit: int = Query(0, ge=0, le=100_000),
    offset: int = Query(0, ge=0),
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    state: Optional[str] = Query(None),
    track: Optional[str] = Query(None),
    q: Optional[str] = Query(None, description="substring match on track/description"),
) -> List[Dict[str, Any]]:
    """
    List races from race_program.

    Defaults:
    - **No date filter** unless date_from/date_to are provided.
    - **No limit** when limit=0 (or omitted).
    """
    eng = _resolve_engine()
    with eng.connect() as conn:
        select_cols = _select_cols(conn)

        wheres = []
        params: Dict[str, Any] = {}

        if date_from:
            wheres.append("date >= :date_from")
            params["date_from"] = date_from.isoformat()
        if date_to:
            wheres.append("date <= :date_to")
            params["date_to"] = date_to.isoformat()
        if state:
            wheres.append("state = :state")
            params["state"] = state
        if track:
            wheres.append("track = :track")
            params["track"] = track
        if q:
            wheres.append("(track LIKE :q OR description LIKE :q)")
            params["q"] = f"%{q}%"

        where_sql = f" WHERE {' AND '.join(wheres)}" if wheres else ""

        sql = f"""
            SELECT {select_cols}
            FROM race_program
            {where_sql}
            ORDER BY CASE WHEN date IS NULL THEN 1 ELSE 0 END,
                     date, state, track, race_no
        """.strip()

        if limit and limit > 0:
            sql += " LIMIT :limit OFFSET :offset"
            params["limit"] = limit
            params["offset"] = offset

        rows = conn.execute(text(sql), params).mappings().all()
        return [dict(r) for r in rows]


@races_router.get("/races/count")
def count_races(
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    state: Optional[str] = Query(None),
    track: Optional[str] = Query(None),
    q: Optional[str] = Query(None),
) -> Dict[str, int]:
    """
    Count races matching optional filters.
    """
    eng = _resolve_engine()
    with eng.connect() as conn:
        wheres = []
        params: Dict[str, Any] = {}

        if date_from:
            wheres.append("date >= :date_from")
            params["date_from"] = date_from.isoformat()
        if date_to:
            wheres.append("date <= :date_to")
            params["date_to"] = date_to.isoformat()
        if state:
            wheres.append("state = :state")
            params["state"] = state
        if track:
            wheres.append("track = :track")
            params["track"] = track
        if q:
            wheres.append("(track LIKE :q OR description LIKE :q)")
            params["q"] = f"%{q}%"

        where_sql = f" WHERE {' AND '.join(wheres)}" if wheres else ""

        sql = f"SELECT COUNT(*) AS n FROM race_program {where_sql}"
        n = conn.execute(text(sql), params).scalar_one()
        return {"count": int(n)}
