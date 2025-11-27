# app/api/routes_ui_results.py
from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.database import get_db
from app.results_summary import (
    build_results_summary,
    build_results_daily_stats,
)
from app.models import Meeting

templates = Jinja2Templates(directory="templates")
router = APIRouter()


@router.get("/ui/results", response_class=HTMLResponse)
def results_overview(
    request: Request,
    from_date: Optional[date] = Query(None),
    to_date: Optional[date] = Query(None),
    track_code: Optional[str] = Query(
        None,
        description="Optional track filter as 'STATE|Track Name', e.g. 'VIC|Flemington'",
    ),
    bet_focus: str = Query(
        "all",
        description="Tip focus: 'all', 'ai_best', or 'danger'",
    ),
    db: Session = Depends(get_db),
):
    """
    RA-based results overview:
    - Uses TipOutcome (provider='RA') + RaceResult
    - Computes ROI, strike rate, etc.
    """

    # Default range: last 7 days ending today (AU time logic can be refined later)
    today = date.today()
    if from_date is None:
        from_date = today - timedelta(days=7)
    if to_date is None:
        to_date = today

    summary = build_results_summary(db, from_date, to_date, track_code, bet_focus)
    daily_stats = build_results_daily_stats(
        db, from_date, to_date, track_code, bet_focus
    )

    # Optional: build a list of available tracks for a dropdown
    tracks = (
        db.query(Meeting.state, Meeting.track_name)
        .distinct()
        .order_by(Meeting.state, Meeting.track_name)
        .all()
    )
    track_options = [
        {
            "code": f"{state}|{track_name}",
            "label": f"{state} – {track_name}",
        }
        for (state, track_name) in tracks
    ]

    return templates.TemplateResponse(
        "results_overview.html",
        {
            "request": request,
            "from_date": from_date,
            "to_date": to_date,
            "track_code": track_code,
            "bet_focus": bet_focus,
            "summary": summary,
            "daily_stats": daily_stats,
            "track_options": track_options,
        },
    )
