# api/races.py
from __future__ import annotations

from fastapi import APIRouter, Query
from sqlalchemy import text
from .db import get_engine

races_router = APIRouter()


def _row_to_dict(row) -> dict:
    d = dict(row)
    return {
        "id": d.get("id"),
        "race_no": d.get("race_no"),
        "date": d.get("date"),
        "state": d.get("state"),
        "track": d.get("track"),
        "type": d.get("type"),
        "description": d.get("description"),
        "prize": d.get("prize"),
        "condition": d.get("condition"),
        "class": d.get("class"),
        "age": d.get("age"),
        "sex": d.get("sex"),
        "distance_m": d.get("distance_m"),
        "bonus": d.get("bonus"),
        "url": d.get("url"),
    }


@races_router.get("/races")
def list_races(
    # Pagination
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    # Optional filters (non-breaking; ignore if not provided)
    state: str | None = Query(None, description="Filter by state (e.g., VIC)"),
    track: str | None = Query(None, description="Filter by track name"),
    date_from: str | None = Query(None, description="YYYY-MM-DD"),
    date_to: str | None = Query(None, description="YYYY-MM-DD"),
):
    """
    Return races sorted by date (NULLs last), then state, track, race_no.
    Optional filters for state, track, and date range.
    """
    where = ["1=1"]
    params: dict[str, object] = {"limit": limit, "offset": offset}

    if state:
        where.append("state = :state")
        params["state"] = state
    if track:
        where.append("track = :track")
        params["track"] = track
    if date_from:
        where.append("date >= :date_from")
        params["date_from"] = date_from
    if date_to:
        where.append("date <= :date_to")
        params["date_to"] = date_to

    sql = f"""
        SELECT
            id, race_no, date, state, track, type, description, prize,
            condition, class, age, sex, distance_m, bonus, url
        FROM race_program
        WHERE {" AND ".join(where)}
        ORDER BY
            CASE WHEN date IS NULL THEN 1 ELSE 0 END,
            date ASC,
            state ASC,
            track ASC,
            race_no ASC
        LIMIT :limit OFFSET :offset
    """

    eng = get_engine()
    with eng.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
    return [_row_to_dict(r) for r in rows]
