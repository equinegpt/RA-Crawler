# api/sb_exotics_crawler.py
"""
Scrapes exotic dividends (Quinella, Trifecta, Quaddie) from TAB.com.au
for races where we generated tips.

TAB URL pattern:
  https://www.tab.com.au/racing/{YYYY-MM-DD}/{TRACK}/{M|P|C}/R/{raceNo}

The page has tabs: Results | Dividends | Exotics | Multiples | Deductions
We need the Exotics tab content which shows Q/T/First4 dividends.

All fetches go through Scrape.do with render=true (TAB is a SPA).
"""
from __future__ import annotations

import re
import time
from datetime import date
from typing import Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from .scraper_proxy import scraper_get
from .db import get_engine

_engine = get_engine()
SessionLocal = sessionmaker(bind=_engine, autoflush=False, autocommit=False)

TAB_BASE = "https://www.tab.com.au"


# ---------------------------------------------------------------------------
# Track name → TAB URL format
# ---------------------------------------------------------------------------

# TAB uses UPPERCASE track names in URLs, mostly matching RA names
# Known differences where our track name differs from TAB's URL
_TAB_TRACK_MAP = {
    "southside pakenham": "PAKENHAM",
    "southside cranbourne": "CRANBOURNE",
    "rosehill gardens": "ROSEHILL GARDENS",
    "royal randwick": "RANDWICK",
    "aquis park gold coast": "GOLD COAST",
    "aquis park gold coast poly": "GOLD COAST POLY",
    "ladbrokes pioneer park": "DARWIN",
    "ladbrokes cannon park": "CAIRNS",
    "morphettville parks": "MORPHETTVILLE PARKS",
}

# Sponsor prefixes to strip
_SPONSORS = [
    "sportsbet", "ladbrokes", "bet365", "picklebet",
    "thomas farms", "aquis park", "aquis", "tabtouch",
]


def _track_to_tab_name(track_name: str) -> str:
    """Convert our track name to TAB URL format (uppercase)."""
    s = track_name.lower().strip()

    # Check alias map first
    if s in _TAB_TRACK_MAP:
        return _TAB_TRACK_MAP[s]

    # Strip sponsors
    for sp in _SPONSORS:
        s = s.replace(sp, "").strip()

    # Check again after stripping
    if s in _TAB_TRACK_MAP:
        return _TAB_TRACK_MAP[s]

    return s.upper()


def _type_to_tab(meeting_type: str) -> str:
    """Convert meeting type to TAB URL format."""
    t = (meeting_type or "").upper()
    if t in ("M", "P", "C"):
        return t
    return "C"  # default to Country


# ---------------------------------------------------------------------------
# Scrape a single TAB race page for exotic dividends
# ---------------------------------------------------------------------------

def _scrape_tab_exotics(
    target_date: date,
    track: str,
    meeting_type: str,
    race_no: int,
) -> Dict[str, Optional[Tuple[float, str]]]:
    """
    Fetch a TAB race page and extract Q/T dividends.

    URL: /racing/{date}/{TRACK}/{type}/R/{raceNo}

    Returns dict e.g. {"Q": (4.40, "8/1"), "T": (58.00, "8/1/5")}
    """
    tab_track = _track_to_tab_name(track)
    tab_type = _type_to_tab(meeting_type)
    date_str = target_date.isoformat()

    # TAB URL uses URL-encoded spaces for multi-word tracks
    tab_track_url = tab_track.replace(" ", "%20")
    url = f"{TAB_BASE}/racing/{date_str}/{tab_track_url}/{tab_type}/R/{race_no}"
    print(f"[TABExotics] Fetching {url}")

    try:
        resp = scraper_get(url, timeout=60, render=True)
        if resp.status_code != 200:
            print(f"[TABExotics] HTTP {resp.status_code}")
            return {}
    except Exception as e:
        print(f"[TABExotics] ERROR: {e}")
        return {}

    page_text = resp.text
    dividends: Dict[str, Optional[Tuple[float, str]]] = {}

    # Parse Quinella: look for "Quinella" near numbers and a dollar amount
    # TAB format: Quinella [8] [1] ... 4.40
    q_match = re.search(
        r'Quinella.*?(\d{1,2})\D+(\d{1,2})\D+?([\d,]+\.?\d{0,2})\s*$',
        page_text, re.IGNORECASE | re.MULTILINE
    )
    if not q_match:
        # Try simpler pattern
        q_match = re.search(
            r'Quinella[^0-9]*?(\d{1,2})[^0-9]+(\d{1,2})[^0-9]+([\d,]+\.\d{2})',
            page_text, re.IGNORECASE
        )
    if q_match:
        try:
            combo = f"{q_match.group(1)}/{q_match.group(2)}"
            amount = float(q_match.group(3).replace(',', ''))
            dividends["Q"] = (amount, combo)
            print(f"[TABExotics]   Quinella: ${amount} ({combo})")
        except ValueError:
            pass

    # Parse Trifecta
    t_match = re.search(
        r'Trifecta[^0-9]*?(\d{1,2})[^0-9]+(\d{1,2})[^0-9]+(\d{1,2})[^0-9]+([\d,]+\.\d{2})',
        page_text, re.IGNORECASE
    )
    if t_match:
        try:
            combo = f"{t_match.group(1)}/{t_match.group(2)}/{t_match.group(3)}"
            amount = float(t_match.group(4).replace(',', ''))
            dividends["T"] = (amount, combo)
            print(f"[TABExotics]   Trifecta: ${amount} ({combo})")
        except ValueError:
            pass

    if not dividends:
        # Debug: show what we got
        text_len = len(page_text)
        has_exotic = "exotic" in page_text.lower()
        has_quinella = "quinella" in page_text.lower()
        print(f"[TABExotics]   No dividends parsed (page={text_len}b, "
              f"has_exotic={has_exotic}, has_quinella={has_quinella})")

    return dividends


# ---------------------------------------------------------------------------
# Get tipped meetings
# ---------------------------------------------------------------------------

def _get_tipped_meetings(target_date: date) -> List[Dict]:
    """Get meetings we generated tips for (M/P + big-maiden C)."""
    session = SessionLocal()
    try:
        rows = session.execute(text("""
            SELECT DISTINCT track, state, type
            FROM race_program
            WHERE date = :d AND meeting_id IS NOT NULL AND type IN ('M', 'P')
        """), {"d": target_date.isoformat()}).fetchall()

        country_rows = session.execute(text("""
            SELECT DISTINCT track, state, type
            FROM race_program
            WHERE date = :d AND meeting_id IS NOT NULL AND type = 'C'
              AND LOWER(class) LIKE '%%maiden%%' AND prize > 29000
        """), {"d": target_date.isoformat()}).fetchall()

        meetings = []
        seen = set()
        for r in list(rows) + list(country_rows):
            key = (r[0], r[1])
            if key not in seen:
                seen.add(key)
                meetings.append({"track": r[0], "state": r[1], "type": r[2]})
        return meetings
    finally:
        session.close()


def _get_race_count(target_date: date, track: str) -> int:
    """Get number of races for a meeting."""
    session = SessionLocal()
    try:
        row = session.execute(text("""
            SELECT MAX(race_no) FROM race_program
            WHERE date = :d AND track = :t
        """), {"d": target_date.isoformat(), "t": track}).fetchone()
        return row[0] if row and row[0] else 0
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Upsert dividend
# ---------------------------------------------------------------------------

def _upsert_dividend(session, meeting_date, state, track, race_no,
                     dividend_type, amount, combination) -> int:
    existing = session.execute(text("""
        SELECT id FROM race_dividends
        WHERE meeting_date = :d AND track = :t AND race_no = :r AND dividend_type = :dt
    """), {"d": meeting_date, "t": track, "r": race_no, "dt": dividend_type}).fetchone()

    if existing:
        session.execute(text("""
            UPDATE race_dividends SET dividend_amount = :amt, combination = :combo WHERE id = :id
        """), {"amt": amount, "combo": combination, "id": existing[0]})
    else:
        session.execute(text("""
            INSERT INTO race_dividends (meeting_date, state, track, race_no, dividend_type, dividend_amount, combination)
            VALUES (:d, :s, :t, :r, :dt, :amt, :combo)
        """), {"d": meeting_date, "s": state, "t": track, "r": race_no,
               "dt": dividend_type, "amt": amount, "combo": combination})
    return 1


# ---------------------------------------------------------------------------
# Main crawler
# ---------------------------------------------------------------------------

class SBExoticsCrawler:
    """Fetches exotic dividends from TAB.com.au for tipped meetings."""

    def fetch_for_date(self, target_date: date) -> int:
        """Main entry point. Returns number of dividends upserted."""
        meetings = _get_tipped_meetings(target_date)
        if not meetings:
            print(f"[TABExotics] No tipped meetings for {target_date}")
            return 0

        print(f"[TABExotics] {len(meetings)} tipped meetings for {target_date}")

        session = SessionLocal()
        total = 0

        try:
            for meeting in meetings:
                track = meeting["track"]
                state = meeting["state"]
                mtype = meeting["type"]
                num_races = _get_race_count(target_date, track)

                if num_races == 0:
                    print(f"[TABExotics] {track}: no races found in DB")
                    continue

                print(f"[TABExotics] {track} ({state}) {mtype}: {num_races} races")

                for race_no in range(1, num_races + 1):
                    try:
                        divs = _scrape_tab_exotics(target_date, track, mtype, race_no)
                        for div_type, div_data in divs.items():
                            if div_data:
                                amount, combo = div_data
                                total += _upsert_dividend(
                                    session, target_date, state, track,
                                    race_no, div_type, amount, combo
                                )
                        time.sleep(3)  # rate limit — be nice
                    except Exception as e:
                        print(f"[TABExotics] ERROR {track} R{race_no}: {e}")

            session.commit()
        finally:
            session.close()

        print(f"[TABExotics] Done for {target_date}: {total} dividends upserted")
        return total
