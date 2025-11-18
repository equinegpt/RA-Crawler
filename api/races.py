# api/races.py
from __future__ import annotations

from typing import List, Any, Dict

from fastapi import APIRouter
from sqlalchemy import text
import os

from .db import get_engine

races_router = APIRouter()


@races_router.get("/races")
def list_races() -> List[Dict[str, Any]]:
    """
    Return all races from race_program.

    NOTE:
    - meeting_id (DB column) is exposed as meetingId (camelCase) in JSON.
    - date is returned as "YYYY-MM-DD" string.
    """
    eng = get_engine()
    with eng.connect() as c:
        rows = c.execute(
            text(
                """
                SELECT
                    id,
                    race_no,
                    date,
                    state,
                    meeting_id,
                    track,
                    type,
                    description,
                    prize,
                    condition,
                    class,
                    age,
                    sex,
                    distance_m,
                    bonus,
                    url
                FROM race_program
                ORDER BY date, state, track, race_no, id
                """
            )
        ).mappings().all()

    out: List[Dict[str, Any]] = []
    for r in rows:
        # SQLAlchemy .mappings() gives dict-like rows
        dt = r["date"]
        if hasattr(dt, "isoformat"):
            date_str = dt.isoformat()
        else:
            date_str = str(dt) if dt is not None else None

        out.append(
            {
                "id": r["id"],
                "race_no": r["race_no"],
                "date": date_str,
                "state": r["state"],
                # meeting_id → meetingId
                "meetingId": r["meeting_id"],
                "track": r["track"],
                "type": r["type"],
                "description": r["description"],
                "prize": r["prize"],
                "condition": r["condition"],
                "class": r["class"],
                "age": r["age"],
                "sex": r["sex"],
                "distance_m": r["distance_m"],
                "bonus": r["bonus"],
                "url": r["url"],
            }
        )

    return out


@races_router.get("/races/debug-db")
def debug_db():
    """
    Debug endpoint to see exactly which DB the API is using,
    and what meeting_id looks like for a handful of known meetings.
    """
    eng = get_engine()
    url_str = str(eng.url)
    env_url = os.getenv("DATABASE_URL")

    with eng.connect() as c:
        min_date = c.execute(text("SELECT MIN(date) FROM race_program")).scalar()
        max_date = c.execute(text("SELECT MAX(date) FROM race_program")).scalar()

        sample_rows = c.execute(
            text(
                """
                SELECT id, date, state, track, meeting_id
                FROM race_program
                WHERE date IN ('2025-11-18','2025-11-19','2025-11-20')
                  AND track IN (
                    'bet365 Park Kyneton',
                    'Canterbury Park',
                    'Doomben',
                    'Kilcoy',
                    'Newcastle'
                  )
                ORDER BY date, state, track, id
                """
            )
        ).mappings().all()

    def _date_str(d):
        if hasattr(d, "isoformat"):
            return d.isoformat()
        return str(d) if d is not None else None

    sample = []
    for r in sample_rows:
        sample.append(
            {
                "id": r["id"],
                "date": _date_str(r["date"]),
                "state": r["state"],
                "track": r["track"],
                "meeting_id": r["meeting_id"],
            }
        )

    return {
        "engine_url": url_str,
        "env_DATABASE_URL": env_url,
        "min_date": _date_str(min_date),
        "max_date": _date_str(max_date),
        "sample": sample,
    }
