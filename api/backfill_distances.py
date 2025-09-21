# api/backfill_distances.py
from __future__ import annotations

import os
import re
import sys
import time
import urllib.parse
from collections import defaultdict
from typing import Dict, Any, List, Tuple

import requests
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# -------------------- HTTP helpers --------------------

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

REQ_TIMEOUT = 20  # seconds

def http_get(url: str, referer: str | None = None) -> str:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    if referer:
        headers["Referer"] = referer
    r = requests.get(url, headers=headers, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    r.encoding = r.apparent_encoding or "utf-8"
    return r.text

# -------------------- DB helpers --------------------

def get_engine(url: str | None = None) -> Engine:
    db_url = url or os.getenv("DATABASE_URL") or "sqlite:///./racing.db"
    if os.getenv("RA_DB_VERBOSE"):
        print(f"[backfill_distances] Using DATABASE_URL: {db_url}")
    return create_engine(db_url, future=True)

def load_rows_needing_distance(eng: Engine, limit: int | None = None) -> List[Dict[str, Any]]:
    sql = """
        SELECT id, race_no, url
        FROM race_program
        WHERE distance_m IS NULL
        ORDER BY url, race_no
    """
    params: Dict[str, Any] = {}
    if limit:
        sql += " LIMIT :lim"
        params["lim"] = limit
    with eng.connect() as c:
        return [dict(r) for r in c.execute(text(sql), params).mappings().all()]

# -------------------- URL/Key helpers --------------------

def key_from_url(url: str) -> str | None:
    """Extract ?Key=... from a Program URL."""
    try:
        q = urllib.parse.urlsplit(url).query
        params = urllib.parse.parse_qs(q)
        key = params.get("Key", [None])[0]
        if key:
            return urllib.parse.unquote(key)
        return None
    except Exception:
        return None

# -------------------- Distance parsing --------------------

# Examples we must catch:
#   "Race 6 - 4:35PM ... (1400 METRES)"
#   "Race 3 - ... (1,600 METRES)"
#   "Race 5 - ... (1000m)"
# Case-insensitive, commas allowed, 'm' or 'METRE(S)'
RACE_HEADER_DISTANCE_RE = re.compile(
    r"(?i)\bRace\s*(\d+)[^\n\r]*?\(\s*([0-9][0-9,\.]*)\s*(?:m|metre|metres)\s*\)"
)

def extract_race_distances_from_html(html: str) -> Dict[int, int]:
    """
    Returns {race_no: distance_m} parsed from race titles.
    """
    mapping: Dict[int, int] = {}
    for m in RACE_HEADER_DISTANCE_RE.finditer(html):
        try:
            rno = int(m.group(1))
            raw = m.group(2).replace(",", "")
            # Sometimes you'll see "1.200" (unlikely), handle '.' by strip:
            raw = raw.replace(".", "")
            dm = int(raw)
            if dm > 0:
                mapping[rno] = dm
        except Exception:
            continue
    return mapping

# -------------------- Meeting updater --------------------

def update_distances_for_meeting(
    eng: Engine,
    meeting_url: str,
    rows_for_meeting: List[Dict[str, Any]],
    debug: bool = False,
) -> Tuple[int, int]:
    """
    Fetch the meeting Program page, parse distances by race header,
    and update entries (only distance_m) for rows that are NULL.

    Returns (updated_count, total_attempted).
    """
    if debug:
        print(f"[backfill_distances] Fetching {meeting_url}")

    try:
        html = http_get(meeting_url, referer="https://www.racingaustralia.horse/")
    except Exception as e:
        if debug:
            print(f"[backfill_distances] ERROR fetching {meeting_url}: {e}")
        return (0, len(rows_for_meeting))

    dist_map = extract_race_distances_from_html(html)
    if debug:
        print(f"[backfill_distances] Parsed distances: {dist_map}")

    updated = 0
    with eng.begin() as c:
        for row in rows_for_meeting:
            rid = row["id"]
            rn = int(row["race_no"])
            dm = dist_map.get(rn)
            if isinstance(dm, int) and dm > 0:
                c.execute(text("UPDATE race_program SET distance_m = :d WHERE id = :id"),
                          {"d": dm, "id": rid})
                updated += 1
            elif debug:
                print(f"[backfill_distances]   no distance for race_no={rn} url={meeting_url}")

    return (updated, len(rows_for_meeting))

# -------------------- Orchestrator --------------------

def backfill_all(
    eng: Engine,
    limit: int | None = None,
    debug: bool = False
) -> Tuple[int, int, int]:
    """
    Returns (meetings_processed, rows_attempted, rows_updated).
    """
    missing = load_rows_needing_distance(eng, limit=limit)
    if not missing:
        print("[backfill_distances] No rows with NULL distance_m. Nothing to do.")
        return (0, 0, 0)

    # Group rows by Program URL so we only fetch each page once
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in missing:
        groups[r["url"]].append(r)

    meetings = 0
    attempted = 0
    updated = 0

    for url, rows in groups.items():
        meetings += 1
        upd, att = update_distances_for_meeting(eng, url, rows, debug=debug)
        updated += upd
        attempted += att
        if debug:
            print(f"[backfill_distances] meeting={url} attempted={att} updated={upd}")

        # small politeness delay
        time.sleep(0.5)

    print(f"[backfill_distances] meetings={meetings} attempted={attempted} updated={updated}")
    return (meetings, attempted, updated)

# -------------------- CLI --------------------

def main() -> None:
    import argparse
    ap = argparse.ArgumentParser(
        description="Backfill missing distance_m by scraping race headers like '(1400 METRES)'."
    )
    ap.add_argument("--url", help="DATABASE_URL override (default from env or sqlite:///./racing.db)")
    ap.add_argument("--limit", type=int, help="Limit number of NULL-distance rows to process")
    ap.add_argument("--debug", action="store_true", help="Verbose logs")
    args = ap.parse_args()

    eng = get_engine(args.url)
    backfill_all(eng, limit=args.limit, debug=args.debug)

if __name__ == "__main__":
    main()
