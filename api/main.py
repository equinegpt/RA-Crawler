from __future__ import annotations

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


@app.get("/results")
def list_results(
    meeting_date: Optional[str] = Query(
        None, alias="date", description="Filter by meeting date (YYYY-MM-DD)"
    ),
    state: Optional[str] = Query(
        None, description="Filter by state, e.g. VIC/NSW/QLD"
    ),
    track: Optional[str] = Query(
        None, description="Filter by exact track name"
    ),
):
    """
    List official RA results from ra_results table.

    This is what the Tips Results Service calls.
    """
    eng = get_engine()

    sql = """
        SELECT
            id,
            meeting_date,
            state,
            track,
            race_no,
            horse_number,
            horse_name,
            trainer,
            jockey,
            finishing_pos,
            is_scratched,
            margin_lens,
            starting_price
        FROM ra_results
        WHERE 1=1
    """
    params: dict[str, object] = {}

    if meeting_date:
        sql += " AND meeting_date = :meeting_date"
        params["meeting_date"] = meeting_date
    if state:
        sql += " AND state = :state"
        params["state"] = state
    if track:
        sql += " AND track = :track"
        params["track"] = track

    sql += " ORDER BY meeting_date, state, track, race_no, horse_number"

    with eng.connect() as c:
        rows = c.execute(text(sql), params).mappings().all()

    out = []
    for r in rows:
        d = dict(r)

        raw_date = d.get("meeting_date")
        if isinstance(raw_date, datetime):
            iso_date = raw_date.date().isoformat()
        elif isinstance(raw_date, date):
            iso_date = raw_date.isoformat()
        else:
            iso_date = str(raw_date) if raw_date is not None else None

        margin_lens = d.get("margin_lens")
        starting_price = d.get("starting_price")

        out.append(
            {
                "id": d["id"],
                "meeting_date": iso_date,
                "state": d["state"],
                "track": d["track"],
                "race_no": d["race_no"],
                "horse_number": d["horse_number"],
                "horse_name": d["horse_name"],
                "trainer": d.get("trainer"),
                "jockey": d.get("jockey"),
                "finishing_pos": d.get("finishing_pos"),
                "is_scratched": bool(d.get("is_scratched")),
                "margin_lens": float(margin_lens) if margin_lens is not None else None,
                "starting_price": float(starting_price) if starting_price is not None else None,
            }
        )

    return out


@app.post("/results/refresh")
def refresh_results(
    meeting_date: str = Query(..., alias="date", description="Meeting date (YYYY-MM-DD)"),
):
    """
    Re-crawl RA results for a specific date to update trainer/jockey data.
    """
    from .ra_results_crawler import RAResultsCrawler

    try:
        target_date = date.fromisoformat(meeting_date)
    except ValueError:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD")

    crawler = RAResultsCrawler()
    try:
        crawler.fetch_for_date(target_date)
    except Exception as exc:
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=f"results crawl failed: {exc}")

    return {"ok": True, "date": meeting_date}


# Optional local run:
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api.main:app", host="0.0.0.0", port=8001, reload=True)
