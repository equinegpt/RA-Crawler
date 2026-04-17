# api/sb_exotics_crawler.py
"""
Scrapes exotic dividends (Quinella, Trifecta, Quaddie) from Sportsbet
for races where we generated tips.

Steps:
  1. Fetch Sportsbet /racing-schedule → extract track-slug + eventId map
  2. Match to our tipped meetings (M/P/big-maiden C)
  3. For each matched race: fetch /exotics page via Scrape.do → parse Q/T
  4. For Quaddie: fetch /multiples page → parse Quaddie dividend
  5. Upsert into race_dividends table
"""
from __future__ import annotations

import os
import re
import time
from datetime import date, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from .scraper_proxy import scraper_get
from .db import get_engine
from .models import RaceDividend

_engine = get_engine()
SessionLocal = sessionmaker(bind=_engine, autoflush=False, autocommit=False)

# Sportsbet base URL
SB_BASE = "https://www.sportsbet.com.au"


# ---------------------------------------------------------------------------
# Step 1: Discover Sportsbet eventIds from the racing schedule page
# ---------------------------------------------------------------------------

def _fetch_sb_event_map(target_date: date) -> Dict[Tuple[str, int], Tuple[str, str]]:
    """
    Fetch the Sportsbet racing schedule and extract track/race → eventId mapping.

    Returns dict of (track_slug, race_no) → (event_id, full_url_path)
    e.g. ("cranbourne", 3) → ("10376442", "horse-racing/australia-nz/cranbourne/race-3-10376442")

    This is a plain HTML fetch (no Scrape.do needed — the schedule page
    embeds race links directly in the HTML).
    """
    url = f"{SB_BASE}/racing-schedule"

    try:
        resp = scraper_get(url, timeout=45, render=False)
        if resp.status_code != 200:
            print(f"[SBExotics] HTTP {resp.status_code} fetching racing schedule")
            return {}
    except Exception as e:
        print(f"[SBExotics] ERROR fetching racing schedule: {e}")
        return {}

    html = resp.text
    event_map: Dict[Tuple[str, int], Tuple[str, str]] = {}

    # Pattern: horse-racing/australia-nz/{track-slug}/race-{raceNo}-{eventId}
    pattern = re.compile(
        r'horse-racing/australia-nz/([a-z0-9-]+)/race-(\d+)-(\d+)'
    )

    for m in pattern.finditer(html):
        track_slug = m.group(1)
        race_no = int(m.group(2))
        event_id = m.group(3)
        full_path = m.group(0)

        key = (track_slug, race_no)
        if key not in event_map:
            event_map[key] = (event_id, full_path)

    print(f"[SBExotics] Found {len(event_map)} race events on schedule page")
    return event_map


# ---------------------------------------------------------------------------
# Step 2: Map our track names to Sportsbet slugs
# ---------------------------------------------------------------------------

def _track_to_slug(track_name: str) -> str:
    """Convert a track name to Sportsbet URL slug format.
    e.g. "Moonee Valley" → "moonee-valley", "Eagle Farm" → "eagle-farm"
    """
    slug = track_name.lower().strip()
    slug = re.sub(r'[^a-z0-9]+', '-', slug)
    slug = slug.strip('-')
    return slug


def _get_tipped_meetings(target_date: date) -> List[Dict]:
    """
    Get the list of meetings we generated tips for on this date.
    Queries the RA crawler's race_program table for M/P meetings
    (and Country with big Maiden, but we simplify to just what has tips).

    Actually, query TRS for which meetings have tips for this date.
    Falls back to RA crawler race_program data.
    """
    session = SessionLocal()
    try:
        # Get unique meetings from race_program for this date
        rows = session.execute(text("""
            SELECT DISTINCT track, state, type
            FROM race_program
            WHERE date = :d
              AND meeting_id IS NOT NULL
              AND type IN ('M', 'P')
        """), {"d": target_date.isoformat()}).fetchall()

        # Also include Country with big maiden
        country_rows = session.execute(text("""
            SELECT DISTINCT track, state, type
            FROM race_program
            WHERE date = :d
              AND meeting_id IS NOT NULL
              AND type = 'C'
              AND LOWER(class) LIKE '%%maiden%%'
              AND prize > 29000
        """), {"d": target_date.isoformat()}).fetchall()

        meetings = []
        seen = set()
        for r in list(rows) + list(country_rows):
            key = (r[0], r[1])
            if key not in seen:
                seen.add(key)
                meetings.append({
                    "track": r[0],
                    "state": r[1],
                    "type": r[2],
                })

        return meetings
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Step 3: Scrape exotics page for a single race
# ---------------------------------------------------------------------------

def _scrape_race_exotics(
    event_url_path: str,
    timeout: int = 45,
) -> Dict[str, Optional[Tuple[float, str]]]:
    """
    Scrape the /exotics tab for a race via Scrape.do.

    Returns dict of dividend_type → (amount, combination) or None.
    e.g. {"Q": (12.40, "3/7"), "T": (87.20, "3/7/1")}
    """
    url = f"{SB_BASE}/{event_url_path}/exotics"
    print(f"[SBExotics] Scraping {url}")

    try:
        resp = scraper_get(url, timeout=timeout, render=True)
        if resp.status_code != 200:
            print(f"[SBExotics] HTTP {resp.status_code} for {url}")
            return {}
    except Exception as e:
        print(f"[SBExotics] ERROR scraping {url}: {e}")
        return {}

    soup = BeautifulSoup(resp.text, "html.parser")
    dividends: Dict[str, Optional[Tuple[float, str]]] = {}

    # Look for dividend rows — Sportsbet uses classes like:
    #   exoticRow, dividendRow, exoticDividend
    # The structure is typically:
    #   <div class="exoticRow...">
    #     <span>Quinella</span>
    #     <span class="exoticDividend...">$12.40</span>
    #     <span>3/7</span>
    #   </div>

    # Strategy: find all text on the page, look for dividend patterns
    page_text = resp.text

    # Pattern 1: Look for "Quinella" near a dollar amount
    q_match = re.search(
        r'Quinella[^$]*?\$\s*([\d,]+\.?\d*)',
        page_text, re.IGNORECASE
    )
    if q_match:
        try:
            amount = float(q_match.group(1).replace(',', ''))
            # Try to find the combination nearby
            combo = _find_combination_near(page_text, q_match.end(), 2)
            dividends["Q"] = (amount, combo or "")
            print(f"[SBExotics] Quinella: ${amount} ({combo})")
        except ValueError:
            pass

    # Pattern 2: Trifecta
    t_match = re.search(
        r'Trifecta[^$]*?\$\s*([\d,]+\.?\d*)',
        page_text, re.IGNORECASE
    )
    if t_match:
        try:
            amount = float(t_match.group(1).replace(',', ''))
            combo = _find_combination_near(page_text, t_match.end(), 3)
            dividends["T"] = (amount, combo or "")
            print(f"[SBExotics] Trifecta: ${amount} ({combo})")
        except ValueError:
            pass

    return dividends


def _scrape_quaddie(
    event_url_path: str,
    timeout: int = 45,
) -> Optional[Tuple[float, str]]:
    """
    Scrape the /multiples tab for Quaddie dividend.

    Returns (amount, combination) or None.
    """
    url = f"{SB_BASE}/{event_url_path}/multiples"
    print(f"[SBExotics] Scraping Quaddie from {url}")

    try:
        resp = scraper_get(url, timeout=timeout, render=True)
        if resp.status_code != 200:
            print(f"[SBExotics] HTTP {resp.status_code} for {url}")
            return None
    except Exception as e:
        print(f"[SBExotics] ERROR scraping {url}: {e}")
        return None

    page_text = resp.text

    # Look for "Quaddie" near a dollar amount (not "Early Quaddie")
    # Match "Quaddie" but not "Early Quaddie"
    q_match = re.search(
        r'(?<!Early\s)Quaddie[^$]*?\$\s*([\d,]+\.?\d*)',
        page_text, re.IGNORECASE
    )
    if q_match:
        try:
            amount = float(q_match.group(1).replace(',', ''))
            combo = _find_combination_near(page_text, q_match.end(), 4)
            print(f"[SBExotics] Quaddie: ${amount} ({combo})")
            return (amount, combo or "")
        except ValueError:
            pass

    return None


def _find_combination_near(text: str, start_pos: int, num_runners: int) -> Optional[str]:
    """Find a runner combination like '3/7' or '3/7/1' near the given position."""
    # Look in the next 200 chars for a pattern like "3/7" or "3/7/1" or "2/5/8/1"
    snippet = text[start_pos:start_pos + 300]
    sep = r'[/\-,]'
    pattern = r'(\d{1,2})' + (sep + r'\s*(\d{1,2})') * (num_runners - 1)
    m = re.search(pattern, snippet)
    if m:
        return '/'.join(g for g in m.groups() if g)
    return None


# ---------------------------------------------------------------------------
# Step 4: Main entry point
# ---------------------------------------------------------------------------

class SBExoticsCrawler:
    """Fetches exotic dividends from Sportsbet for tipped meetings."""

    def fetch_for_date(self, target_date: date) -> int:
        """
        Main entry point. Fetches Q/T/Quaddie dividends for all tipped
        meetings on the given date.

        Returns the number of dividend rows inserted/updated.
        """
        # 1) Get Sportsbet event map
        event_map = _fetch_sb_event_map(target_date)
        if not event_map:
            print(f"[SBExotics] No events found on Sportsbet schedule")
            return 0

        # 2) Get our tipped meetings
        meetings = _get_tipped_meetings(target_date)
        if not meetings:
            print(f"[SBExotics] No tipped meetings for {target_date}")
            return 0

        print(f"[SBExotics] {len(meetings)} tipped meetings, "
              f"{len(event_map)} SB events")

        # 3) Match and scrape
        session = SessionLocal()
        total_upserted = 0

        try:
            for meeting in meetings:
                track = meeting["track"]
                state = meeting["state"]
                slug = _track_to_slug(track)

                # Find all race events for this track slug
                track_events = {
                    rno: (eid, path)
                    for (s, rno), (eid, path) in event_map.items()
                    if s == slug
                }

                if not track_events:
                    # Try fuzzy slug matching (e.g., "southside-cranbourne" vs "cranbourne")
                    for (s, rno), (eid, path) in event_map.items():
                        if slug in s or s in slug:
                            track_events[rno] = (eid, path)

                if not track_events:
                    print(f"[SBExotics] No SB events for {track} (slug={slug})")
                    continue

                print(f"[SBExotics] {track} ({state}): "
                      f"{len(track_events)} races on SB")

                # Scrape exotics for each race
                for race_no, (event_id, url_path) in sorted(track_events.items()):
                    try:
                        divs = _scrape_race_exotics(url_path)

                        for div_type, div_data in divs.items():
                            if div_data is None:
                                continue
                            amount, combo = div_data
                            total_upserted += _upsert_dividend(
                                session, target_date, state, track,
                                race_no, div_type, amount, combo
                            )

                        # Rate limit: be nice to Scrape.do
                        time.sleep(2)

                    except Exception as e:
                        print(f"[SBExotics] ERROR {track} R{race_no}: {e}")

                # Scrape Quaddie (last 4 races)
                if len(track_events) >= 4:
                    last_race_no = max(track_events.keys())
                    _, last_url_path = track_events[last_race_no]

                    try:
                        quad = _scrape_quaddie(last_url_path)
                        if quad:
                            amount, combo = quad
                            total_upserted += _upsert_dividend(
                                session, target_date, state, track,
                                last_race_no, "QUAD", amount, combo
                            )
                        time.sleep(2)
                    except Exception as e:
                        print(f"[SBExotics] ERROR Quaddie {track}: {e}")

            session.commit()
        finally:
            session.close()

        print(f"[SBExotics] Done for {target_date}: "
              f"{total_upserted} dividends upserted")
        return total_upserted


def _upsert_dividend(
    session: Session,
    meeting_date: date,
    state: str,
    track: str,
    race_no: int,
    dividend_type: str,
    amount: float,
    combination: str,
) -> int:
    """Insert or update a single dividend row. Returns 1 if upserted, 0 if skipped."""
    existing = session.execute(text("""
        SELECT id FROM race_dividends
        WHERE meeting_date = :d AND track = :t AND race_no = :r AND dividend_type = :dt
    """), {"d": meeting_date, "t": track, "r": race_no, "dt": dividend_type}).fetchone()

    if existing:
        session.execute(text("""
            UPDATE race_dividends
            SET dividend_amount = :amt, combination = :combo
            WHERE id = :id
        """), {"amt": amount, "combo": combination, "id": existing[0]})
    else:
        session.execute(text("""
            INSERT INTO race_dividends (meeting_date, state, track, race_no, dividend_type, dividend_amount, combination)
            VALUES (:d, :s, :t, :r, :dt, :amt, :combo)
        """), {
            "d": meeting_date, "s": state, "t": track, "r": race_no,
            "dt": dividend_type, "amt": amount, "combo": combination,
        })

    return 1
