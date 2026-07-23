# api/racenet_dividends.py
"""
Exotic dividends (Quinella / Exacta / Trifecta / FirstFour) from Racenet
meeting-results pages.

Why Racenet: RA's own Results.aspx pages do NOT publish exotic dividends
(verified 2026-07-23), and the earlier TAB.com.au path (sb_exotics_crawler)
needs SPA rendering plus a same-day event-id cache and never populated.
Racenet's results page embeds the FULL meeting state — every race's
dividends across three totes (S-TAB / NSW / Ubet) — in its window.__NUXT__
payload, which evaluates in ~50ms with the pure-python `quickjs` package.
One fetch per meeting per day, historical pages persist (backfillable).

Rows land in the existing (previously empty) race_dividends table:
    (meeting_date, state, track, race_no, dividend_type, dividend_amount,
     combination)
dividend_amount is the MINIMUM across the reporting totes — the
conservative "worst tote" figure, so any backtested edge survives the
real-world tote lottery. dividend_type keeps Racenet's names:
Quinella / Exacta / Trifecta / FirstFour.
"""
from __future__ import annotations

import re
from datetime import date
from typing import Dict, List, Optional

import quickjs
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from .scraper_proxy import scraper_get
from .db import get_engine

_engine = get_engine()
SessionLocal = sessionmaker(bind=_engine, autoflush=False, autocommit=False)

RACENET_BASE = "https://www.racenet.com.au/results/horse-racing"

_SPONSORS = [
    "sportsbet-", "sportsbet ", "ladbrokes ", "bet365 ", "picklebet park ",
    "thomas farms rc ", "aquis park ", "aquis ", "tabtouch ", "southside ",
    "royal ",
]

# Track name -> racenet slug where simple normalisation is not enough.
# Extend as backfill misses surface (misses are logged, never fatal).
_SLUG_MAP = {
    "rosehill gardens": "rosehill",
    "royal randwick": "randwick",
    "canberra acton": "canberra",
    "murray bridge gh": "murray-bridge",
    "sandown-hillside": "sandown",
    "sandown-lakeside": "sandown",
    "cannon park": "cairns",
    "pioneer park": "darwin",
    "morphettville parks": "morphettville",
    "pinjarra scarpside": "pinjarra",
    "gold coast poly": "gold-coast",
}


def _slug(track: str) -> str:
    s = (track or "").lower().strip()
    for p in _SPONSORS:
        if s.startswith(p):
            s = s[len(p):]
            break
    s = s.strip()
    if s in _SLUG_MAP:
        return _SLUG_MAP[s]
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s


def _extract_nuxt_state(html: str) -> Optional[dict]:
    """Evaluate the embedded window.__NUXT__ IIFE and return it as a dict."""
    i = html.find("window.__NUXT__=")
    if i < 0:
        return None
    j = html.find("</script>", i)
    js = html[i:j].replace("window.__NUXT__=", "var __NUXT__=", 1)
    try:
        import json as _json
        ctx = quickjs.Context()
        ctx.eval(js)
        return _json.loads(ctx.eval("JSON.stringify(__NUXT__)"))
    except Exception:
        return None


def fetch_meeting_dividends(meeting_date: date, track: str,
                            state: Optional[str] = None) -> List[Dict]:
    """Fetch + parse one meeting's exotic dividends. Returns row dicts."""
    slug = _slug(track)
    url = f"{RACENET_BASE}/{slug}-{meeting_date.strftime('%Y%m%d')}"
    resp = scraper_get(url, timeout=60)
    if resp is None or resp.status_code != 200:
        print(f"[racenet-div] {url} -> HTTP {getattr(resp, 'status_code', '?')}")
        return []
    nuxt = _extract_nuxt_state(resp.text)
    if not nuxt:
        print(f"[racenet-div] {url} -> no __NUXT__ state")
        return []

    rows: List[Dict] = []
    for blob in nuxt.get("data") or []:
        meeting = (blob or {}).get("meeting") or {}
        for ev in meeting.get("events") or []:
            race_no = ev.get("eventNumber")
            exotics = ev.get("exoticResult") or []
            if not race_no or not exotics:
                continue
            # group by market type; conservative MIN across totes
            by_type: Dict[str, Dict] = {}
            for x in exotics:
                mkt = x.get("exoticMarket")
                try:
                    amt = float(x.get("amount"))
                except (TypeError, ValueError):
                    continue
                combo = (x.get("results") or "").replace(",", "-")
                cur = by_type.get(mkt)
                if cur is None or amt < cur["dividend_amount"]:
                    by_type[mkt] = {"dividend_amount": amt, "combination": combo}
            for mkt, v in by_type.items():
                rows.append({
                    "meeting_date": meeting_date.isoformat(),
                    "state": state or "",
                    "track": track,
                    "race_no": int(race_no),
                    "dividend_type": mkt,
                    "dividend_amount": round(v["dividend_amount"], 2),
                    "combination": v["combination"],
                })
    print(f"[racenet-div] {track} {meeting_date}: {len(rows)} dividend rows")
    return rows


def save_dividends(rows: List[Dict]) -> int:
    if not rows:
        return 0
    session = SessionLocal()
    try:
        for r in rows:
            session.execute(text("""
                INSERT INTO race_dividends
                    (meeting_date, state, track, race_no, dividend_type,
                     dividend_amount, combination)
                VALUES (:meeting_date, :state, :track, :race_no,
                        :dividend_type, :dividend_amount, :combination)
                ON CONFLICT (meeting_date, track, race_no, dividend_type)
                DO UPDATE SET dividend_amount = EXCLUDED.dividend_amount,
                              combination = EXCLUDED.combination
            """), r)
        session.commit()
        return len(rows)
    finally:
        session.close()


def fetch_for_date(meeting_date: date) -> int:
    """All meetings for a date (from ra_results tracks) -> race_dividends."""
    session = SessionLocal()
    try:
        tracks = session.execute(text("""
            SELECT DISTINCT state, track FROM ra_results
            WHERE meeting_date = :d
        """), {"d": meeting_date.isoformat()}).fetchall()
    finally:
        session.close()
    total = 0
    for state, track in tracks:
        try:
            rows = fetch_meeting_dividends(meeting_date, track, state)
            total += save_dividends(rows)
        except Exception as e:
            print(f"[racenet-div] {track} {meeting_date} FAILED: {e}")
    return total
