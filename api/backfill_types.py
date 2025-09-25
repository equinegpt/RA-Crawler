# api/backfill_types.py

from __future__ import annotations

import argparse
import os
import re
import time
from typing import Optional

import requests
from sqlalchemy import create_engine, text

from .track_types import infer_type

REQ_TIMEOUT = 20
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; RA-Crawler/1.0; +https://example.com)",
}

MEETING_TYPE_RE = re.compile(r"Meeting Type:\s*(Metro|Metropolitan|Provincial|Country)", re.I)

def db_engine(url: Optional[str] = None):
    url = url or os.getenv("DATABASE_URL", "sqlite:///./racing.db")
    eng = create_engine(url, future=True)
    return eng

def fetch(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    return r.text

def parse_meeting_type_from_html(html: str) -> Optional[str]:
    """
    Returns 'M' | 'P' | 'C' if a Meeting Type header is found, else None.
    """
    # Cheap text search (fast, robust against small markup changes)
    m = MEETING_TYPE_RE.search(html)
    if not m:
        return None
    word = m.group(1).lower()
    if word.startswith("metro"):
        return "M"
    if word.startswith("prov"):
        return "P"
    if word.startswith("country"):
        return "C"
    return None

def normalize_type_letter(word: str) -> Optional[str]:
    if not word:
        return None
    w = word.strip().upper()
    if w in ("M", "METRO", "METROPOLITAN"):
        return "M"
    if w in ("P", "PROVINCIAL"):
        return "P"
    if w in ("C", "COUNTRY"):
        return "C"
    return None

def backfill(url: Optional[str], dry_run: bool = False, limit: Optional[int] = None) -> int:
    eng = db_engine(url)
    updated = 0

    # We dedupe meetings by (date,state,track) because each meeting has multiple races.
    sel = text("""
        SELECT date, state, track, MIN(url) as any_url
        FROM race_program
        WHERE type IS NULL OR TRIM(COALESCE(type,'')) = ''
        GROUP BY date, state, track
        ORDER BY date, state, track
    """)
    with eng.connect() as conn:
        meetings = conn.execute(sel).fetchall()

    if limit:
        meetings = meetings[:limit]

    print(f"[types] candidate meetings: {len(meetings)}")

    for i, (date, state, track, any_url) in enumerate(meetings, start=1):
        # 1) Try table lookup first (fast)
        t = infer_type(state, track)
        # 2) If missing, try parsing the Program page heading (“Meeting Type:”)
        if not t:
            try:
                html = fetch(any_url)
                t = parse_meeting_type_from_html(html)
            except Exception as e:
                # network hiccup — skip gracefully
                t = None

        if not t:
            # Still nothing, skip
            if i % 25 == 0:
                print(f"[types] progress: {i}/{len(meetings)} (updated={updated})")
            continue

        if dry_run:
            updated += 1
            if i % 25 == 0:
                print(f"[types] progress: {i}/{len(meetings)} (updated would be {updated})")
            continue

        with eng.begin() as tx:
            # Set type for all races in the meeting
            u = text("""
                UPDATE race_program
                SET type = :t
                WHERE date = :d AND state = :s AND track = :trk
            """)
            tx.execute(u, {"t": t, "d": date, "s": state, "trk": track})
        updated += 1

        if i % 25 == 0:
            print(f"[types] progress: {i}/{len(meetings)} (updated={updated})")
        # be polite to RA
        time.sleep(0.25)

    print(f"[types] updated meetings: {updated}")
    return updated

def main():
    ap = argparse.ArgumentParser(description="Backfill race_program.type using lookup + page heading parsing.")
    ap.add_argument("--url", help="DATABASE_URL (defaults to env DATABASE_URL or sqlite:///./racing.db)")
    ap.add_argument("--dry-run", action="store_true", help="Do not write; just report.")
    ap.add_argument("--limit", type=int, help="Only process this many meetings.")
    args = ap.parse_args()

    n = backfill(args.url, dry_run=args.dry_run, limit=args.limit)
    if args.dry_run:
        print(f"[types] (dry-run) would update {n} meetings.")
    else:
        print(f"[types] done: updated {n} meetings.")

if __name__ == "__main__":
    main()
