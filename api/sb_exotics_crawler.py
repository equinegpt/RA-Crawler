# api/sb_exotics_crawler.py
"""
Scrapes exotic dividends (Quinella, Trifecta, Quaddie) from Sportsbet
for races where we generated tips.

Approach:
  1. Get our tipped meetings for the date (M/P/big-maiden C)
  2. For each meeting: fetch its Sportsbet track page to discover event IDs
  3. For each race: fetch /exotics page → parse Q/T dividends
  4. For Quaddie: fetch /multiples page on the last race
  5. Upsert into race_dividends table

All Sportsbet fetches go through Scrape.do (JS rendering where needed).
"""
from __future__ import annotations

import re
import time
from datetime import date
from typing import Dict, List, Optional, Tuple

from bs4 import BeautifulSoup
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from .scraper_proxy import scraper_get
from .db import get_engine

_engine = get_engine()
SessionLocal = sessionmaker(bind=_engine, autoflush=False, autocommit=False)

SB_BASE = "https://www.sportsbet.com.au"


# ---------------------------------------------------------------------------
# Track name → Sportsbet slug
# ---------------------------------------------------------------------------

# Known aliases where our track name differs from SB slug
_TRACK_ALIASES = {
    "southside pakenham": "pakenham",
    "southside cranbourne": "cranbourne",
    "cannon park": "cairns",
    "ladbrokes cannon park": "cairns",
    "aquis park gold coast": "gold-coast",
    "aquis park gold coast poly": "gold-coast-poly",
    "ladbrokes pioneer park": "pioneer-park",
    "morphettville parks": "morphettville",
    "royal randwick": "randwick",
    "rosehill gardens": "rosehill",
    "beaumont newcastle": "newcastle",
}

# Sponsor prefixes to strip
_SPONSORS = [
    "sportsbet", "ladbrokes", "bet365", "picklebet",
    "thomas farms", "aquis park", "aquis", "tabtouch",
]


def _track_to_slug(track_name: str) -> str:
    """Convert track name to Sportsbet URL slug."""
    s = track_name.lower().strip()

    # Check alias map first (before stripping)
    if s in _TRACK_ALIASES:
        return _TRACK_ALIASES[s]

    # Strip sponsors
    for sp in _SPONSORS:
        s = s.replace(sp, "").strip()

    # Check alias again after stripping
    if s in _TRACK_ALIASES:
        return _TRACK_ALIASES[s]

    # Convert to slug
    slug = re.sub(r'[^a-z0-9]+', '-', s).strip('-')
    return slug


# ---------------------------------------------------------------------------
# Fetch event IDs from a track's Sportsbet page
# ---------------------------------------------------------------------------

def _fetch_track_event_ids(track_slug: str) -> Dict[int, Tuple[str, str]]:
    """
    Fetch a Sportsbet track page and extract race event IDs.

    Returns dict of race_no → (event_id, url_path)
    e.g. {1: ("10376449", "horse-racing/australia-nz/cranbourne/race-1-10376449"), ...}
    """
    url = f"{SB_BASE}/horse-racing/australia-nz/{track_slug}"
    print(f"[SBExotics] Fetching track page: {url}")

    try:
        resp = scraper_get(url, timeout=45, render=True)
        if resp.status_code != 200:
            print(f"[SBExotics] HTTP {resp.status_code} for {url}")
            return {}
    except Exception as e:
        print(f"[SBExotics] ERROR fetching {url}: {e}")
        return {}

    # Extract event URLs for this track
    pattern = re.compile(
        rf'horse-racing/australia-nz/{re.escape(track_slug)}/race-(\d+)-(\d+)'
    )

    events: Dict[int, Tuple[str, str]] = {}
    for m in pattern.finditer(resp.text):
        race_no = int(m.group(1))
        event_id = m.group(2)
        full_path = m.group(0)
        if race_no not in events:
            events[race_no] = (event_id, full_path)

    print(f"[SBExotics] {track_slug}: found {len(events)} race events")
    return events


# ---------------------------------------------------------------------------
# Scrape exotics / dividends
# ---------------------------------------------------------------------------

def _scrape_race_exotics(event_url_path: str) -> Dict[str, Optional[Tuple[float, str]]]:
    """
    Scrape the /exotics tab for Q/T dividends via Scrape.do.
    Returns dict e.g. {"Q": (12.40, "3/7"), "T": (87.20, "3/7/1")}
    """
    url = f"{SB_BASE}/{event_url_path}/exotics"
    print(f"[SBExotics] Scraping {url}")

    try:
        resp = scraper_get(url, timeout=60, render=True)
        if resp.status_code != 200:
            print(f"[SBExotics] HTTP {resp.status_code} for exotics")
            return {}
    except Exception as e:
        print(f"[SBExotics] ERROR scraping exotics: {e}")
        return {}

    page_text = resp.text
    dividends: Dict[str, Optional[Tuple[float, str]]] = {}

    # Quinella
    q_match = re.search(r'Quinella[^$]*?\$\s*([\d,]+\.?\d*)', page_text, re.IGNORECASE)
    if q_match:
        try:
            amount = float(q_match.group(1).replace(',', ''))
            combo = _find_combination_near(page_text, q_match.end(), 2)
            dividends["Q"] = (amount, combo or "")
            print(f"[SBExotics]   Quinella: ${amount}")
        except ValueError:
            pass

    # Trifecta
    t_match = re.search(r'Trifecta[^$]*?\$\s*([\d,]+\.?\d*)', page_text, re.IGNORECASE)
    if t_match:
        try:
            amount = float(t_match.group(1).replace(',', ''))
            combo = _find_combination_near(page_text, t_match.end(), 3)
            dividends["T"] = (amount, combo or "")
            print(f"[SBExotics]   Trifecta: ${amount}")
        except ValueError:
            pass

    if not dividends:
        print(f"[SBExotics]   No dividends found on page")

    return dividends


def _scrape_quaddie(event_url_path: str) -> Optional[Tuple[float, str]]:
    """Scrape /multiples tab for Quaddie dividend."""
    url = f"{SB_BASE}/{event_url_path}/multiples"
    print(f"[SBExotics] Scraping Quaddie: {url}")

    try:
        resp = scraper_get(url, timeout=60, render=True)
        if resp.status_code != 200:
            return None
    except Exception as e:
        print(f"[SBExotics] ERROR scraping multiples: {e}")
        return None

    q_match = re.search(
        r'(?<!Early\s)Quaddie[^$]*?\$\s*([\d,]+\.?\d*)',
        resp.text, re.IGNORECASE
    )
    if q_match:
        try:
            amount = float(q_match.group(1).replace(',', ''))
            combo = _find_combination_near(resp.text, q_match.end(), 4)
            print(f"[SBExotics]   Quaddie: ${amount}")
            return (amount, combo or "")
        except ValueError:
            pass
    return None


def _find_combination_near(text: str, start_pos: int, num_runners: int) -> Optional[str]:
    """Find a runner combination like '3/7' near the given position."""
    snippet = text[start_pos:start_pos + 300]
    sep = r'[/\-,]'
    pattern = r'(\d{1,2})' + (sep + r'\s*(\d{1,2})') * (num_runners - 1)
    m = re.search(pattern, snippet)
    if m:
        return '/'.join(g for g in m.groups() if g)
    return None


# ---------------------------------------------------------------------------
# Get tipped meetings
# ---------------------------------------------------------------------------

def _get_tipped_meetings(target_date: date) -> List[Dict]:
    """Get meetings we generated tips for on this date (M/P + big-maiden C)."""
    session = SessionLocal()
    try:
        # M and P meetings
        rows = session.execute(text("""
            SELECT DISTINCT track, state, type
            FROM race_program
            WHERE date = :d
              AND meeting_id IS NOT NULL
              AND type IN ('M', 'P')
        """), {"d": target_date.isoformat()}).fetchall()

        # Country with big maiden
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
                meetings.append({"track": r[0], "state": r[1], "type": r[2]})
        return meetings
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Upsert dividend
# ---------------------------------------------------------------------------

def _upsert_dividend(session, meeting_date, state, track, race_no,
                     dividend_type, amount, combination) -> int:
    """Insert or update a dividend row. Returns 1."""
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

def _load_cached_events(target_date: date) -> Dict[Tuple[str, int], Tuple[str, str]]:
    """Load event IDs from sb_event_cache table."""
    session = SessionLocal()
    try:
        rows = session.execute(text("""
            SELECT track_slug, race_no, event_id, url_path
            FROM sb_event_cache
            WHERE meeting_date = :d
        """), {"d": target_date}).fetchall()
        result = {}
        for r in rows:
            result[(r[0], r[1])] = (r[2], r[3])
        if result:
            print(f"[SBExotics] Loaded {len(result)} cached events for {target_date}")
        return result
    except Exception as e:
        print(f"[SBExotics] Cache read error (table may not exist): {e}")
        return {}
    finally:
        session.close()


def _get_cached_events_for_slug(
    cached: Dict[Tuple[str, int], Tuple[str, str]],
    slug: str,
) -> Dict[int, Tuple[str, str]]:
    """Filter cached events for a specific track slug."""
    events = {}
    for (s, rno), (eid, path) in cached.items():
        if s == slug:
            events[rno] = (eid, path)
    return events


class SBExoticsCrawler:
    """Fetches exotic dividends from Sportsbet for tipped meetings."""

    def fetch_for_date(self, target_date: date) -> int:
        """Main entry point. Returns number of dividends upserted."""

        meetings = _get_tipped_meetings(target_date)
        if not meetings:
            print(f"[SBExotics] No tipped meetings for {target_date}")
            return 0

        print(f"[SBExotics] {len(meetings)} tipped meetings for {target_date}")

        # Try cached events first (populated at 8am when races are live)
        cached = _load_cached_events(target_date)
        if cached:
            print(f"[SBExotics] Using {len(cached)} cached events")

        session = SessionLocal()
        total = 0

        try:
            for meeting in meetings:
                track = meeting["track"]
                state = meeting["state"]
                slug = _track_to_slug(track)

                # Step 1: Get event IDs — from cache or live track page
                events = _get_cached_events_for_slug(cached, slug) if cached else {}
                if not events:
                    events = _fetch_track_event_ids(slug)
                if not events:
                    print(f"[SBExotics] Skipping {track} — no events found for slug={slug}")
                    time.sleep(2)
                    continue

                # Step 2: Scrape exotics for each race
                for race_no, (event_id, url_path) in sorted(events.items()):
                    try:
                        divs = _scrape_race_exotics(url_path)
                        for div_type, div_data in divs.items():
                            if div_data:
                                amount, combo = div_data
                                total += _upsert_dividend(
                                    session, target_date, state, track,
                                    race_no, div_type, amount, combo
                                )
                        time.sleep(2)  # rate limit
                    except Exception as e:
                        print(f"[SBExotics] ERROR {track} R{race_no}: {e}")

                # Step 3: Scrape Quaddie (last race)
                if len(events) >= 4:
                    last_race = max(events.keys())
                    _, last_path = events[last_race]
                    try:
                        quad = _scrape_quaddie(last_path)
                        if quad:
                            amount, combo = quad
                            total += _upsert_dividend(
                                session, target_date, state, track,
                                last_race, "QUAD", amount, combo
                            )
                        time.sleep(2)
                    except Exception as e:
                        print(f"[SBExotics] ERROR Quaddie {track}: {e}")

            session.commit()
        finally:
            session.close()

        print(f"[SBExotics] Done for {target_date}: {total} dividends upserted")
        return total
