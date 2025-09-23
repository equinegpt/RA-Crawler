# api/backfill_distance.py
from __future__ import annotations

import argparse
import re
import time
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import requests
from sqlalchemy import create_engine, text

REQ_TIMEOUT = 30
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; RA-Crawler/1.0)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Example line on Program page:
#   Race 6 - 4:35PM BUDGET CAR AND TRUCK RENTAL HANDICAP (1400 METRES)
# We'll extract both race number and distance from near that text.
RE_RACE_LINE = re.compile(
    r"Race\s*(\d{1,2})\b.*?\(\s*([0-9][0-9,]{2,4})\s*(?:M|METRE|METRES|METERS)\s*\)",
    re.IGNORECASE | re.DOTALL,
)

def fetch(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    # Don’t slice: keep full HTML so we don’t miss late-page headers
    return r.text

def parse_race_distances_from_program(html: str) -> Dict[int, int]:
    """
    Return a mapping {race_no -> distance_m} parsed from the Program page.
    """
    out: Dict[int, int] = {}
    for m in RE_RACE_LINE.finditer(html):
        race_no_str, dist_str = m.group(1), m.group(2)
        try:
            race_no = int(race_no_str)
            dist = int(dist_str.replace(",", ""))
            if 600 <= dist <= 7000:  # sanity
                # Don’t overwrite if already seen; first match per race is fine
                out.setdefault(race_no, dist)
        except Exception:
            continue
    return out

def collect_candidates(conn, limit: Optional[int]) -> List[Tuple[int, str, int]]:
    """
    Return list of (id, url, race_no) where distance_m is NULL/0 and url is present.
    """
    base = """
        SELECT id, url, race_no
        FROM race_program
        WHERE (distance_m IS NULL OR distance_m = 0)
          AND url IS NOT NULL AND TRIM(url) <> ''
    """
    if limit:
        rows = conn.execute(text(base + " LIMIT :limit"), {"limit": limit}).fetchall()
    else:
        rows = conn.execute(text(base)).fetchall()
    return [(r[0], r[1], r[2]) for r in rows]

def backfill(db_url: str, limit: Optional[int], sleep_sec: float, dry_run: bool) -> Tuple[int, int]:
    eng = create_engine(db_url, future=True)
    with eng.begin() as conn:
        cand = collect_candidates(conn, limit)
    total = len(cand)
    if total == 0:
        print("[backfill] Candidates: 0")
        return 0, 0

    # Group by program URL so we fetch each meeting once
    by_url: Dict[str, List[Tuple[int, int]]] = defaultdict(list)
    for rid, url, race_no in cand:
        by_url[url].append((rid, race_no))

    print(f"[backfill] Candidates: {total} (meetings: {len(by_url)})")

    scanned_urls = 0
    updated = 0

    for url, items in by_url.items():
        scanned_urls += 1
        try:
            html = fetch(url)
        except Exception as e:
            if scanned_urls % 5 == 0:
                print(f"[backfill] meetings_scanned={scanned_urls}, updated={updated} (fetch error: {e})")
            time.sleep(sleep_sec)
            continue

        # Parse distances for all races in this meeting
        map_rno_dist = parse_race_distances_from_program(html)
        if not map_rno_dist:
            if scanned_urls % 10 == 0:
                print(f"[backfill] meetings_scanned={scanned_urls}, updated={updated} (no distances found)")
            time.sleep(sleep_sec)
            continue

        # Update rows that have a matching race_no
        to_update = []
        for rid, rno in items:
            d = map_rno_dist.get(rno)
            if d:
                to_update.append({"id": rid, "d": d})

        if to_update and not dry_run:
            # Bulk update in one transaction
            with eng.begin() as conn:
                for chunk_start in range(0, len(to_update), 200):
                    chunk = to_update[chunk_start:chunk_start+200]
                    conn.execute(
                        text("UPDATE race_program SET distance_m = :d WHERE id = :id"),
                        chunk,
                    )
            updated += len(to_update)

        if scanned_urls % 10 == 0:
            print(f"[backfill] meetings_scanned={scanned_urls}, updated={updated}")

        time.sleep(sleep_sec)

    print(f"[backfill] Updated rows: {updated}")
    return scanned_urls, updated

def main():
    ap = argparse.ArgumentParser(description="Backfill distance_m by parsing Program pages once per meeting.")
    ap.add_argument("--url", required=True, help="SQLAlchemy DB URL, e.g. sqlite:////abs/path/to/racing.db")
    ap.add_argument("--limit", type=int, default=None, help="Limit candidate rows")
    ap.add_argument("--sleep", type=float, default=0.15, help="Sleep between meeting fetches")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    print(f"[backfill] Using DATABASE_URL: {args.url}")
    backfill(args.url, limit=args.limit, sleep_sec=args.sleep, dry_run=args.dry_run)

if __name__ == "__main__":
    main()
