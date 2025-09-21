# api/races.py
from __future__ import annotations

from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Query
from sqlalchemy import text

from .db import get_engine

races_router = APIRouter()


def _row_to_dict(row: dict) -> dict:
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


def _today_in_tz(tz_name: str = "Australia/Melbourne") -> date:
    try:
        return datetime.now(ZoneInfo(tz_name)).date()
    except Exception:
        # Fallback to system local date if zoneinfo not available for some reason
        return datetime.now().date()


@races_router.get("/races")
def list_races(
    # Optional explicit window (if omitted we use "today -> today+29" in Melbourne time)
    start: str | None = Query(None, description="YYYY-MM-DD (local to Australia/Melbourne by default)"),
    end: str | None = Query(None, description="YYYY-MM-DD (inclusive)"),
    days: int = Query(30, ge=1, le=60, description="Used only when start/end not provided"),
    tz: str = Query("Australia/Melbourne", description="IANA timezone for 'today' defaulting"),
    # Optional pagination if a client *wants* it; by default we do NOT limit
    limit: int | None = Query(None, ge=1, le=20000),
    offset: int = Query(0, ge=0),
):
    """
    Returns races sorted by date ascending, then state, track, race_no.

    Default window: [today (Melbourne) .. today+29] with NO row cap.

    You can override with:
      - ?start=YYYY-MM-DD&end=YYYY-MM-DD  (inclusive)
      - or keep defaults and optionally add &limit=&offset= for paging.
    """
    # Resolve date window
    if start and end:
        start_date = start
        end_date = end
    elif start and not end:
        start_date = start
        # if only start given, honor 'days' from that start
        sd = datetime.strptime(start, "%Y-%m-%d").date()
        end_date = (sd + timedelta(days=days - 1)).strftime("%Y-%m-%d")
    elif not start and end:
        # if only end given, backfill 'days' backwards (keep simple: last N days up to end)
        ed = datetime.strptime(end, "%Y-%m-%d").date()
        start_date = (ed - timedelta(days=days - 1)).strftime("%Y-%m-%d")
        end_date = end
    else:
        # neither provided -> use Melbourne "today" and 30-day window
        today = _today_in_tz(tz)
        start_date = today.strftime("%Y-%m-%d")
        end_date = (today + timedelta(days=days - 1)).strftime("%Y-%m-%d")

    where = ["date >= :start", "date <= :end"]
    params: dict[str, object] = {"start": start_date, "end": end_date}

    sql = f"""
        SELECT
            id, race_no, date, state, track, type, description, prize,
            condition, class, age, sex, distance_m, bonus, url
        FROM race_program
        WHERE {' AND '.join(where)}
        ORDER BY
            CASE WHEN date IS NULL THEN 1 ELSE 0 END,
            date, state, track, race_no
    """

    # Only apply LIMIT/OFFSET if a limit was explicitly requested
    if limit is not None:
        sql += "\nLIMIT :limit OFFSET :offset"
        params["limit"] = limit
        params["offset"] = offset

    eng = get_engine()
    with eng.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()

    return [_row_to_dict(r) for r in rows]


@races_router.get("/")
def healthcheck():
    return {"status": "ok"}
