# api/races.py
from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Query
from sqlalchemy import text

from .db import get_engine

races_router = APIRouter(prefix="", tags=["races"])

def _row_to_dict(row) -> dict:
    return {
        "id": row["id"],
        "race_no": row["race_no"],
        "date": row["date"],
        "state": row["state"],
        "track": row["track"],
        "type": row.get("type"),
        "description": row["description"],
        "prize": row["prize"],
        "condition": row["condition"],
        "class": row.get("class"),
        "age": row["age"],
        "sex": row["sex"],
        "distance_m": row.get("distance_m"),
        "bonus": row.get("bonus"),
        "url": row["url"],
    }

@races_router.get("/races")
def list_races(
    days: int = Query(30, ge=1, le=120),
    include_past: int = Query(0, ge=0, le=14),
    limit: int = Query(5000, ge=1, le=20000),
    offset: int = Query(0, ge=0),
    state: Optional[str] = None,
    track: Optional[str] = None,
):
    """
    Returns races sorted by date (then state, track, race_no).
    Default window: today..today+30d; can include past days via include_past.
    """
    eng = get_engine()
    today = date.today()
    start_date = today - timedelta(days=include_past)
    end_date = today + timedelta(days=days)

    where = ["1=1", "date >= :start", "date < :end"]
    params = {
        "start": start_date.isoformat(),
        "end": end_date.isoformat(),
        "limit": limit,
        "offset": offset,
    }
    if state:
        where.append("state = :state")
        params["state"] = state
    if track:
        where.append("track = :track")
        params["track"] = track

    sql = f"""
        SELECT id, race_no, date, state, track, type, description, prize, condition, class, age, sex, distance_m, bonus, url
        FROM race_program
        WHERE {" AND ".join(where)}
        ORDER BY
          CASE WHEN date IS NULL THEN 1 ELSE 0 END,
          date, state, track, race_no
        LIMIT :limit OFFSET :offset
    """

    with eng.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
        return [_row_to_dict(r) for r in rows]
