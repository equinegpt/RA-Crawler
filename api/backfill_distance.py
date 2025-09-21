# api/backfill_distance.py
from __future__ import annotations

import argparse
import os
import re
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter, Retry
from selectolax.parser import HTMLParser
from sqlalchemy import create_engine, text

# ---------------- Config ----------------
USER_AGENT = "Mozilla/5.0 (compatible; RA-Crawler/1.0; +https://example.org)"
REQ_TIMEOUT = (5, 15)        # (connect, read)
RETRY_TOTAL = 3
RETRY_BACKOFF = 0.5
POOL_MAX = 100               # requests connection pool size
# ----------------------------------------

# Examples we aim to match:
# "Race 6 - 4:35PM BUDGET ... (1400 METRES)"
# "Race 2 – ... (1200m)"
# Allow hyphen or en-dash, optional time, anything, then (#### METRES|M)
RACE_DIST_RE = re.compile(
    r"Race\s*(\d+)\s*[-–]\s*.*?\((\d{3,4})\s*(?:METRES|M)\)",
    re.IGNORECASE | re.DOTALL,
)

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    retries = Retry(
        total=RETRY_TOTAL,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=POOL_MAX, pool_maxsize=POOL_MAX)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

def extract_map_race_to_distance(html: str) -> Dict[int, int]:
    """
    Return a mapping {race_no: distance_m} parsed from the Program page.
    We search visible text broadly because RA uses different containers/templates.
    """
    out: Dict[int, int] = {}

    tree = HTMLParser(html)

    # Aggregate candidate text blobs that likely contain "Race N - ... (#### METRES)"
    chunks: List[str] = []

    # Titles / headings first
    for sel in ("h1", "h2", "h3", ".content h2", ".content h3", "#content h2", "#content h3"):
        for n in tree.css(sel) or []:
            txt = n.text(separator=" ").strip()
            if txt:
                chunks.append(txt)

    # Links / strong / table cells sometimes hold the full “Race …” line
    for sel in ("a", "strong", "td", "div"):
        for n in tree.css(sel) or []:
            t = n.text(separator=" ").strip()
            # cheap filter: keep only lines that contain "Race" to reduce noise
            if t and ("Race" in t or "RACE" in t):
                # Avoid accumulating gigantic page text repeatedly
                if len(t) > 400:
                    t = t[:400]
                chunks.append(t)

    # Final fallback: body text (first N chars to keep it cheap)
    if tree.body is not None:
        body = tree.body.text(separator=" ")
        if body:
            body = body[:20000]
            chunks.append(body)

    # Scan all chunks
    for blob in chunks:
        for m in RACE_DIST_RE.finditer(blob):
            try:
                rno = int(m.group(1))
                dist = int(m.group(2))
                # only set first time; later matches for same race_no are ignored
                out.setdefault(rno, dist)
            except Exception:
                continue

    return out

def fetch_race_map(session: requests.Session, url: str) -> Dict[int, int]:
    try:
        r = session.get(url, timeout=REQ_TIMEOUT)
        if r.status_code != 200 or not r.text:
            return {}
        return extract_map_race_to_distance(r.text)
    except requests.RequestException:
        return {}

def backfill(db_url: str, dry_run: bool, limit: int, max_workers: int) -> int:
    eng = create_engine(db_url, future=True)

    # Pull all candidates with id, race_no, url
    with eng.connect() as conn:
        rows: List[Tuple[int, int, str]] = conn.execute(
            text("""
                 SELECT id, race_no, url
                 FROM race_program
                 WHERE distance_m IS NULL
                 LIMIT :lim
            """),
            {"lim": limit},
        ).fetchall()

    if not rows:
        print("[backfill] Nothing to update.")
        return 0

    print(f"[backfill] Candidates: {len(rows)}")

    # Group rows by Program URL so we only fetch each page once
    by_url: Dict[str, List[Tuple[int, int]]] = defaultdict(list)
    for rid, rno, url in rows:
        by_url[url].append((rid, rno))

    session = make_session()

    updated = 0
    updates_batch: List[Dict[str, int]] = []
    start = time.time()

    # Fetch pages concurrently
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = {pool.submit(fetch_race_map, session, url): url for url in by_url.keys()}

        for fut in as_completed(futs):
            url = futs[fut]
            race_map: Dict[int, int] = {}
            try:
                race_map = fut.result()
            except Exception:
                race_map = {}

            pairs = by_url[url]
            for rid, rno in pairs:
                dist = race_map.get(rno)
                if dist:
                    if dry_run:
                        print(f"[dry-run] would set id={rid} race_no={rno} distance_m={dist} ({url})")
                    else:
                        updates_batch.append({"d": dist, "i": rid})
                        updated += 1

                if not dry_run and len(updates_batch) >= 100:
                    with eng.begin() as conn:
                        conn.execute(
                            text("UPDATE race_program SET distance_m=:d WHERE id=:i"),
                            updates_batch,
                        )
                    updates_batch.clear()

    # Final flush
    if not dry_run and updates_batch:
        with eng.begin() as conn:
            conn.execute(
                text("UPDATE race_program SET distance_m=:d WHERE id=:i"),
                updates_batch,
            )

    dur = time.time() - start
    print(f"[backfill] Updated rows: {updated} in {dur:.1f}s")
    return updated

def main():
    ap = argparse.ArgumentParser(description="Backfill race_program.distance_m by parsing each race's heading on Program pages.")
    ap.add_argument("--url", default=os.getenv("DATABASE_URL", "sqlite:///./racing.db"),
                    help="SQLAlchemy DB URL (default env DATABASE_URL or sqlite:///./racing.db)")
    ap.add_argument("--dry-run", action="store_true", help="Do not write changes")
    ap.add_argument("--limit", type=int, default=10000, help="Max candidate rows to process")
    ap.add_argument("--max-workers", type=int, default=12, help="Concurrent fetch workers")
    args = ap.parse_args()

    print(f"[backfill] Using DATABASE_URL: {args.url}")
    try:
        n = backfill(args.url, dry_run=args.dry_run, limit=args.limit, max_workers=max(1, args.max_workers))
        if args.dry_run:
            print("[backfill] Dry-run complete.")
    except KeyboardInterrupt:
        print("\n[backfill] Cancelled by user.", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print("[backfill] ERROR:", e, file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
