from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

from .db import ensure_schema, get_engine
from .races import races_router
from .backfill_meeting_ids import canonical_track_name

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

        # Normalise track name so RA names match PF names
        # e.g. "Ladbrokes Cannon Park" → "Cairns"
        raw_track = d["track"] or ""
        normalised = canonical_track_name(raw_track)
        display_track = normalised.title() if normalised else raw_track

        out.append(
            {
                "id": d["id"],
                "race_no": d["race_no"],
                "date": iso_date,
                "state": d["state"],
                "meetingId": d.get("meeting_id"),
                "track": display_track,
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


@app.get("/dividends")
def list_dividends(
    meeting_date: Optional[str] = Query(
        None, alias="date", description="Filter by meeting date (YYYY-MM-DD)"
    ),
    track: Optional[str] = Query(
        None, description="Filter by track name"
    ),
):
    """
    List exotic dividends (Q/T/Quaddie) from race_dividends table.
    """
    eng = get_engine()

    sql = """
        SELECT meeting_date, state, track, race_no,
               dividend_type, dividend_amount, combination
        FROM race_dividends
        WHERE 1=1
    """
    params: dict[str, object] = {}

    if meeting_date:
        sql += " AND meeting_date = :meeting_date"
        params["meeting_date"] = meeting_date
    if track:
        sql += " AND track = :track"
        params["track"] = track

    sql += " ORDER BY meeting_date, track, race_no, dividend_type"

    with eng.connect() as c:
        rows = c.execute(text(sql), params).mappings().all()

    out = []
    for d in rows:
        raw_date = d.get("meeting_date")
        if hasattr(raw_date, "isoformat"):
            iso_date = raw_date.isoformat()
        else:
            iso_date = str(raw_date) if raw_date is not None else None

        amount = d.get("dividend_amount")

        out.append({
            "meeting_date": iso_date,
            "state": d["state"],
            "track": d["track"],
            "race_no": d["race_no"],
            "dividend_type": d["dividend_type"],
            "dividend_amount": float(amount) if amount is not None else None,
            "combination": d.get("combination"),
        })

    return out


@app.post("/cache-sb-events")
def cache_sb_events(
    meeting_date: Optional[str] = Query(None, alias="date"),
):
    """
    Fetch Sportsbet schedule page and cache event IDs for today's races.
    Call this during the day (e.g., 8am) when the schedule still shows today.
    The 11pm results cron will read from this cache.
    """
    from datetime import date as _date, datetime
    from zoneinfo import ZoneInfo
    from .sb_exotics_crawler import _fetch_sb_event_map

    if meeting_date:
        target = _date.fromisoformat(meeting_date)
    else:
        target = datetime.now(ZoneInfo("Australia/Melbourne")).date()

    event_map = _fetch_sb_event_map(target)
    if not event_map:
        return {"ok": False, "cached": 0, "detail": "No events found on SB schedule"}

    eng = get_engine()
    cached = 0

    with eng.begin() as conn:
        # Ensure table exists
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS sb_event_cache (
                id SERIAL PRIMARY KEY,
                meeting_date DATE NOT NULL,
                track_slug TEXT NOT NULL,
                race_no INTEGER NOT NULL,
                event_id TEXT NOT NULL,
                url_path TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(meeting_date, track_slug, race_no)
            )
        """))

        for (track_slug, race_no), (event_id, url_path) in event_map.items():
            conn.execute(text("""
                INSERT INTO sb_event_cache (meeting_date, track_slug, race_no, event_id, url_path)
                VALUES (:d, :slug, :rno, :eid, :path)
                ON CONFLICT (meeting_date, track_slug, race_no)
                DO UPDATE SET event_id = :eid, url_path = :path
            """), {
                "d": target, "slug": track_slug, "rno": race_no,
                "eid": event_id, "path": url_path,
            })
            cached += 1

    return {"ok": True, "date": target.isoformat(), "cached": cached}


@app.post("/backfill-meetings")
def backfill_meetings():
    """
    Trigger a meeting_id backfill from Punting Form (same logic as the cron job).
    """
    import os
    from .backfill_meeting_ids import backfill as _backfill_meeting_ids

    url = os.getenv("DATABASE_URL", "sqlite:////data/racing.db")

    try:
        meetings_updated, rows_updated = _backfill_meeting_ids(
            url, dry_run=False, limit=None,
        )
    except Exception as exc:
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=f"backfill failed: {exc}")

    return {
        "ok": True,
        "meetings_updated": meetings_updated,
        "rows_updated": rows_updated,
    }


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
