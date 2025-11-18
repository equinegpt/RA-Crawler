# api/main.py
from datetime import date, datetime
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

from .db import ensure_schema, get_engine
from .races import races_router


# Make sure the DB schema exists (safe for Postgres too)
ensure_schema()

app = FastAPI(title="RA Program API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def health():
    return {"status": "ok"}


# Keep any existing routes defined in api.races (for back-compat)
app.include_router(races_router)


@app.get("/races")
def list_races(
    start: Optional[str] = Query(
        None, description="Filter from this date (YYYY-MM-DD, inclusive)"
    ),
    end: Optional[str] = Query(
        None, description="Filter up to this date (YYYY-MM-DD, inclusive)"
    ),
    state: Optional[str] = Query(
        None, description="Filter by state, e.g. VIC/NSW/QLD"
    ),
):
    """
    List races, exposing DB `meeting_id` as JSON `meetingId`.

    This reads directly from the DB via get_engine(), so as long as the
    meeting_id is in race_program, it will appear here.
    """
    eng = get_engine()

    sql = """
        SELECT
            id,
            race_no,
            date,
            state,
            track,
            meeting_id,
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
        WHERE 1=1
    """
    params: dict[str, object] = {}

    if start:
        sql += " AND date >= :start"
        params["start"] = start
    if end:
        sql += " AND date <= :end"
        params["end"] = end
    if state:
        sql += " AND state = :state"
        params["state"] = state

    sql += " ORDER BY date, state, track, race_no"

    with eng.connect() as c:
        rows = c.execute(text(sql), params).mappings().all()

    out = []
    for r in rows:
        d = dict(r)

        raw_date = d.get("date")
        if isinstance(raw_date, datetime):
            iso_date = raw_date.date().isoformat()
        elif isinstance(raw_date, date):
            iso_date = raw_date.isoformat()
        else:
            iso_date = str(raw_date) if raw_date is not None else None

        out.append(
            {
                "id": d["id"],
                "race_no": d["race_no"],
                "date": iso_date,
                "state": d["state"],
                # 👇 The important bit: DB meeting_id → JSON meetingId
                "meetingId": d.get("meeting_id"),
                "track": d["track"],
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
        )

    return out


# Optional local run:
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api.main:app", host="0.0.0.0", port=8001, reload=True)
