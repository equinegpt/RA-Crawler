# api/routes/results.py
from __future__ import annotations

from datetime import date
from typing import List

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from ..db import SessionLocal
from ..models import RAResult
from ..ra_results_crawler import RAResultsCrawler


router = APIRouter(prefix="/results", tags=["results"])


class ResultRow(BaseModel):
    meeting_date: date
    state: str
    track: str
    race_no: int
    horse_number: int
    horse_name: str
    finishing_pos: int | None
    is_scratched: bool
    margin_lens: float | None = None
    starting_price: float | None = None


def _get_db_session() -> Session:
    """
    Simple local helper instead of assuming a get_db() dependency
    exists in db.py. Keeps this router self-contained.
    """
    return SessionLocal()


@router.get("", response_model=List[ResultRow])
def list_results(
    date: date = Query(..., description="Meeting date (YYYY-MM-DD)"),
    state: str | None = Query(None, description="State code, e.g. VIC, NSW"),
    track: str | None = Query(None, description="Track name, e.g. Flemington"),
):
    """
    List results for a given date, optionally filtered by state/track.
    This is what the Tips Results Service will call.
    """
    db = _get_db_session()
    try:
        q = db.query(RAResult).filter(RAResult.meeting_date == date)

        if state:
            q = q.filter(RAResult.state == state)
        if track:
            q = q.filter(RAResult.track == track)

        rows = q.all()

        return [
            ResultRow(
                meeting_date=r.meeting_date,
                state=r.state,
                track=r.track,
                race_no=r.race_no,
                horse_number=r.horse_number,
                horse_name=r.horse_name,
                finishing_pos=r.finishing_pos,
                is_scratched=r.is_scratched,
                margin_lens=float(r.margin_lens) if r.margin_lens is not None else None,
                starting_price=float(r.starting_price)
                if r.starting_price is not None
                else None,
            )
            for r in rows
        ]
    finally:
        db.close()


@router.post("/refresh")
def refresh_results(
    date: date = Query(..., description="Meeting date (YYYY-MM-DD)"),
):
    """
    Trigger a crawl of RA results for a given date.

    This is what your 6pm and 11pm Render crons will hit:
    POST /results/refresh?date=2025-11-26
    """
    crawler = RAResultsCrawler()

    try:
        crawler.fetch_for_date(date)
    except NotImplementedError as nie:
        # Make it obvious in logs & API if the HTML parser isn't done yet.
        raise HTTPException(status_code=500, detail=str(nie))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"results crawl failed: {exc}")

    return {"ok": True, "date": str(date)}
