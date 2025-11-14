from __future__ import annotations

from datetime import date, timedelta
from typing import Optional, Dict, Any, List

from fastapi import APIRouter, Query
from sqlalchemy import text

from .db import get_engine

races_router = APIRouter()


def _row_to_dict(row):
    d = dict(row)

    # Map "No Restrictions" (and close variants) to the string "None"
    raw_age = d.get("age")
    age_out = d.get("age")
    if raw_age is not None:
        s = str(raw_age).strip().lower()
        if s in {"no restrictions", "no restriction"}:
            age_out = "None"

    return {
        "id": d.get("id"),
        "race_no": d.get("race_no"),
        "date": d.get("date"),
        "state": d.get("state"),
        "meetingId": d.get("meeting_id"),   # ← NEW FIELD
        "track": d.get("track"),
        "type": d.get("type"),
        "description": d.get("description"),
        "prize": d.get("prize"),
        "condition": d.get("condition"),
        "class": d.get("class"),
        "age": age_out,                     # "None" instead of "No Restrictions"
        "sex": d.get("sex"),
        "distance_m": d.get("distance_m"),
        "bonus": d.get("bonus"),
        "url": d.get("url"),
    }


@races_router.get("/races")
def list_races(
    start: Optional[str] = Query(None, description="Start date (YYYY-MM-DD). Defaults to today."),
    end: Optional[str] = Query(None, description="End date (YYYY-MM-DD). Defaults to today + 30 days."),
    limit: Optional[int] = Query(None, ge=1, le=50000, description="Optional cap on rows."),
    offset: int = Query(0, ge=0, description="Optional offset for pagination."),
) -> List[Dict[str, Any]]:
    """
    Return race_program rows, normalized by SQLite date() so mixed YYYY-M-D formats
    sort/filter correctly. Defaults to a rolling 30-day window starting today.
    """
    # Defaults: today -> today + 30 days
    if start is None:
        start = date.today().isoformat()
    if end is None:
        end = (date.today() + timedelta(days=30)).isoformat()

    # Build SQL with date() normalization in WHERE/ORDER to fix gaps/mis-sorts
    sql = """
        SELECT
            id, race_no, date, state, track, meeting_id, type, description, prize,
            condition, class, age, sex, distance_m, bonus, url
        FROM race_program
        WHERE date(date) >= date(:start)
          AND date(date) <= date(:end)
        ORDER BY
            date(date), state, track, race_no
    """
    if limit is not None:
        sql += " LIMIT :limit OFFSET :offset"

    params = {
        "start": start,
        "end": end,
        "limit": limit if limit is not None else 0,
        "offset": offset,
    }

    eng = get_engine()
    with eng.connect() as conn:
        if limit is not None:
            rows = conn.execute(text(sql), params).mappings().all()
        else:
            # when no LIMIT, don't pass :limit/:offset to avoid param mismatch
            rows = conn.execute(
                text(sql.replace(" LIMIT :limit OFFSET :offset", "")),
                {"start": start, "end": end},
            ).mappings().all()

    return [_row_to_dict(r) for r in rows]
