# api/races.py
from __future__ import annotations

from typing import List, Any, Dict

from fastapi import APIRouter
from sqlalchemy import text

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
                # 👇 THIS is the important bit
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
