# app/results_summary.py
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from datetime import date
from typing import Optional, List, Dict, Any

from sqlalchemy.orm import Session

from app.models import Meeting, Race, Tip, TipOutcome


@dataclass
class DaySummary:
    date: date
    total_tips: int
    winners: int
    places: int
    scratched: int
    pending: int
    total_staked: Decimal
    total_return: Decimal
    roi_pct: float
    win_strike_rate: float
    place_strike_rate: float


@dataclass
class OverallSummary:
    total_tips: int
    winners: int
    places: int
    scratched: int
    pending: int
    total_staked: Decimal
    total_return: Decimal
    roi_pct: float
    win_strike_rate: float
    place_strike_rate: float


def _filter_base_query(
    db: Session,
    from_date: date,
    to_date: date,
    track_code: Optional[str],
    bet_focus: str,
):
    """
    Base query joining TipOutcome → Tip → Race → Meeting, filtered by date/track/bet_focus.
    """
    q = (
        db.query(TipOutcome, Tip, Race, Meeting)
        .join(Tip, TipOutcome.tip_id == Tip.id)
        .join(Race, Tip.race_id == Race.id)
        .join(Meeting, Race.meeting_id == Meeting.id)
        .filter(Meeting.date >= from_date, Meeting.date <= to_date)
        .filter(TipOutcome.provider == "RA")
    )

    # Track filter: we use a "state|track" style code like "VIC|Flemington"
    if track_code:
        try:
            state, track_name = track_code.split("|", 1)
            q = q.filter(
                Meeting.state == state,
                Meeting.track_name == track_name,
            )
        except ValueError:
            # If it's malformed, ignore the track filter
            pass

    # Bet focus: 'all', 'ai_best', 'danger' etc.
    bf = (bet_focus or "all").lower()
    if bf in ("ai_best", "ai-best", "best"):
        q = q.filter(Tip.tip_type == "AI_BEST")
    elif bf in ("danger",):
        q = q.filter(Tip.tip_type == "DANGER")

    return q


def _accumulate(
    rows: List[tuple[TipOutcome, Tip, Race, Meeting]]
) -> OverallSummary:
    total_tips = 0
    winners = 0
    places = 0
    scratched = 0
    pending = 0
    total_staked = Decimal("0")
    total_return = Decimal("0")

    for outcome, tip, race, meeting in rows:
        total_tips += 1

        stake = Decimal(str(tip.stake_units or 1))
        total_staked += stake

        status = (outcome.outcome_status or "PENDING").upper()

        if status == "SCRATCHED":
            scratched += 1
            # convention: scratched returns stake, but for ROI we can treat as 0 staked
            # or just ignore; for simplicity we count stake but 0 return
        elif status == "PENDING":
            pending += 1
        else:
            if status == "WIN":
                winners += 1
            if status in ("WIN", "PLACE"):
                places += 1

            if status == "WIN" and outcome.starting_price is not None:
                sp = Decimal(str(outcome.starting_price))
                total_return += stake * sp
            # Note: we're not modelling place dividends yet; easy to extend later.

    if total_staked > 0:
        roi_pct = float((total_return - total_staked) / total_staked * 100)
    else:
        roi_pct = 0.0

    bettable = total_tips - scratched - pending
    if bettable > 0:
        win_sr = float(winners / bettable * 100)
        place_sr = float(places / bettable * 100)
    else:
        win_sr = 0.0
        place_sr = 0.0

    return OverallSummary(
        total_tips=total_tips,
        winners=winners,
        places=places,
        scratched=scratched,
        pending=pending,
        total_staked=total_staked,
        total_return=total_return,
        roi_pct=roi_pct,
        win_strike_rate=win_sr,
        place_strike_rate=place_sr,
    )


def build_results_summary(
    db: Session,
    from_date: date,
    to_date: date,
    track_code: Optional[str],
    bet_focus: str,
) -> OverallSummary:
    """
    Overall summary for a date range, based purely on RA-settled TipOutcome.
    """
    q = _filter_base_query(db, from_date, to_date, track_code, bet_focus)
    rows = q.all()
    return _accumulate(rows)


def build_results_daily_stats(
    db: Session,
    from_date: date,
    to_date: date,
    track_code: Optional[str],
    bet_focus: str,
) -> List[DaySummary]:
    """
    Break down stats by day (for charts / tables).
    """
    q = _filter_base_query(db, from_date, to_date, track_code, bet_focus)
    rows = q.all()

    rows_by_date: Dict[date, List[Any]] = {}
    for outcome, tip, race, meeting in rows:
        rows_by_date.setdefault(meeting.date, []).append(
            (outcome, tip, race, meeting)
        )

    day_summaries: List[DaySummary] = []
    for d in sorted(rows_by_date.keys()):
        summary = _accumulate(rows_by_date[d])
        day_summaries.append(
            DaySummary(
                date=d,
                total_tips=summary.total_tips,
                winners=summary.winners,
                places=summary.places,
                scratched=summary.scratched,
                pending=summary.pending,
                total_staked=summary.total_staked,
                total_return=summary.total_return,
                roi_pct=summary.roi_pct,
                win_strike_rate=summary.win_strike_rate,
                place_strike_rate=summary.place_strike_rate,
            )
        )

    return day_summaries
