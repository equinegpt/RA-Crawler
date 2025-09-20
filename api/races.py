# api/races.py
from __future__ import annotations

from datetime import date, timedelta, datetime
from typing import List, Optional, Dict, Any

from fastapi import APIRouter, Query
from sqlalchemy import text
from sqlalchemy.engine import Engine

# We rely on your project's DB helper.
# This must exist: api/db.py -> get_engine() -> returns SQLAlchemy Engine
from .db import get_engine

races_router = APIRouter()


def _default_window() -> tuple[str, str]:
    """Return (from_date, to_date) ISO strings for today -> today+30d."""
    # If you want Melbourne-local ‘today’, uncomment the ZoneInfo line:
    # from zoneinfo import ZoneInfo
    # today = datetime.now(ZoneInfo("Australia/Melbourne")).date()
    today = date.today()
    return today.isoformat(), (today + timedelta(days=30)).isoformat()


@races_router.get("/races", response_model=List[Dict[str, Any]])
def list_races(
    state: Optional[str] = Query(None, description="Filter by state code (e.g. VIC, NSW)"),
    track: Optional[str] = Query(None, description="Filter by track name (exact match)"),
    type: Optional[str] = Query(None, alias="type", description="Filter by track type (M/P/C)"),
    klass: Optional[str] = Query(None, alias="class", description="Filter by race class (e.g. BM64, G1, Listed)"),
    from_date: Optional[str] = Query(None, description="Inclusive start date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, description="Inclusive end date (YYYY-MM-DD)"),
    q: Optional[str] = Query(None, description="Free text search over description/bonus"),
    limit: int = Query(200, ge=1, le=1000, description="Max rows to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
):
    """
    Return races from `race_program` sorted by date (NULLs last), then state, track, race_no.
    Defaults to a rolling 30-day window (today -> today+30d) unless from_date/to_date are provided.
    """

    # Default window if none supplied
    if not from_date or not to_date:
        d_from, d_to = _default_window()
        from_date = from_date or d_from
        to_date = to_date or d_to

    # Build SQL with safe parameter binding
    # NOTE: SQLite stores date as TEXT (ISO 8601) – string compare works for YYYY-MM-DD.
    # NULLs last via CASE
    base_sql = """
        SELECT
            id, race_no, date, state, track, type, description,
            prize, condition, class, age, sex, distance_m, bonus, url
        FROM race_program
        WHERE 1=1
          AND (:state     IS NULL OR state = :state)
          AND (:track     IS NULL OR track = :track)
          AND (:type      IS NULL OR type  = :type)
          AND (:class     IS NULL OR class = :class)
          AND (:from_date IS NULL OR date >= :from_date)
          AND (:to_date   IS NULL OR date <= :to_date)
    """

    params: Dict[str, Any] = {
        "state": state,
        "track": track,
        "type": type,
        "class": klass,
        "from_date": from_date,
        "to_date": to_date,
        "limit": limit,
        "offset": offset,
    }

    # Optional free-text query (SQLite: case-insensitive with lower())
    # Search in description and bonus
    if q:
        base_sql += " AND (LOWER(description) LIKE :q OR LOWER(COALESCE(bonus,'')) LIKE :q) "
        params["q"] = f"%{q.lower()}%"

    order_sql = """
        ORDER BY
          CASE WHEN date IS NULL THEN 1 ELSE 0 END,
          date ASC,
          state ASC,
          track ASC,
          race_no ASC
        LIMIT :limit OFFSET :offset
    """

    sql = base_sql + order_sql

    eng: Engine = get_engine()
    with eng.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
        # Convert MappingResult rows to plain dicts
        return [dict(r) for r in rows]
