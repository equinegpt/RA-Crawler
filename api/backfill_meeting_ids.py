# api/backfill_meeting_ids.py
from __future__ import annotations

import argparse
import os
import re
import time
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests
from sqlalchemy import create_engine, text

PF_API_URL = "https://api.puntingform.com.au/v2/form/meetingslist"
# Allow override via env; fall back to the key you provided
PF_API_KEY_DEFAULT = "c867b2f9-d740-4cce-b772-801708c8191d"
PF_API_KEY = os.getenv("PF_API_KEY", PF_API_KEY_DEFAULT)

REQ_TIMEOUT = 20
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; RA-MeetingsBackfill/1.0)",
    "Accept": "application/json",
}


def db_engine(url: Optional[str] = None):
    url = url or os.getenv("DATABASE_URL", "sqlite:///./racing.db")
    return create_engine(url, future=True)


def _norm_track_name(name: str) -> str:
    # Normalise spacing/case to make matching robust
    s = re.sub(r"\s+", " ", (name or "").strip())
    return s.upper()


def _fetch_meetings_for_date(date_iso: str) -> List[Dict]:
    """
    Call Punting Form meetingslist for a given YYYY-MM-DD date.
    meetingDate must be 'D MMM YYYY' (e.g. '30 May 2025').
    """
    if not PF_API_KEY:
        raise RuntimeError("PF_API_KEY is not configured")

    dt = datetime.strptime(date_iso, "%Y-%m-%d")
    # 'D MMM YYYY' → e.g. '14 Nov 2025'
    meeting_date_str = f"{dt.day} {dt.strftime('%b')} {dt.year}"

    params = {
        "apiKey": PF_API_KEY,
        "meetingDate": meeting_date_str,
    }
    resp = requests.get(PF_API_URL, params=params, headers=HEADERS, timeout=REQ_TIMEOUT)
    resp.raise_for_status()
    data = resp.json() or {}
    payload = data.get("payLoad") or data.get("payload") or []
    return payload


def backfill(
    url: Optional[str] = None,
    dry_run: bool = False,
    limit: Optional[int] = None,
    sleep_sec: float = 0.3,
) -> Tuple[int, int]:
    eng = db_engine(url)

    # Ensure meeting_id column exists (for older DBs)
    with eng.begin() as conn:
        rows = conn.exec_driver_sql("PRAGMA table_info(race_program)").fetchall()
        col_names = {r[1] for r in rows}
        if "meeting_id" not in col_names:
            conn.exec_driver_sql("ALTER TABLE race_program ADD COLUMN meeting_id TEXT")
            print("[meeting_ids] Added meeting_id column to race_program")

    # Get distinct meetings that don't have meeting_id yet
    sel = text("""
        SELECT date, state, track
        FROM race_program
        WHERE meeting_id IS NULL OR TRIM(COALESCE(meeting_id, '')) = ''
        GROUP BY date, state, track
        ORDER BY date, state, track
    """)

    with eng.connect() as conn:
        meetings = conn.execute(sel).fetchall()

    if limit is not None:
        meetings = meetings[:limit]

    if not meetings:
        print("[meeting_ids] No meetings without meeting_id found.")
        return 0, 0

    # Group by date so we only call PF once per date
    by_date: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
    for d, state, track in meetings:
        by_date[str(d)].append((state, track))

    print(f"[meeting_ids] candidate meetings: {len(meetings)} across {len(by_date)} date(s)")

    meetings_updated = 0
    rows_updated = 0

    for idx, (date_iso, mts) in enumerate(sorted(by_date.items()), start=1):
        print(f"[meeting_ids] ({idx}/{len(by_date)}) date={date_iso} meetings={len(mts)}")
        try:
            pf_meetings = _fetch_meetings_for_date(date_iso)
        except Exception as e:
            print(f"[meeting_ids] ERROR fetching PF meetings for {date_iso}: {e}")
            time.sleep(sleep_sec)
            continue

        # Build (state, norm_track_name) -> meetingId map
        pf_map: Dict[Tuple[str, str], str] = {}
        for m in pf_meetings:
            tinfo = m.get("track") or {}
            tname = tinfo.get("name")
            tstate = tinfo.get("state")
            mid = m.get("meetingId")
            if not (tname and tstate and mid):
                continue
            key = (str(tstate).upper(), _norm_track_name(tname))
            pf_map[key] = str(mid)

        if not pf_map:
            print(f"[meeting_ids] No PF meetings map built for {date_iso}, skipping.")
            time.sleep(sleep_sec)
            continue

        # Update all matching meetings for this date
        with eng.begin() as tx:
            for state, track in mts:
                key = (str(state).upper(), _norm_track_name(track))
                mid = pf_map.get(key)
                if not mid:
                    # No match found for this (date,state,track); skip quietly
                    continue

                if dry_run:
                    meetings_updated += 1
                    continue

                res = tx.execute(
                    text("""
                        UPDATE race_program
                        SET meeting_id = :mid
                        WHERE date = :d AND state = :s AND track = :trk
                          AND (meeting_id IS NULL OR TRIM(COALESCE(meeting_id,'')) = '')
                    """),
                    {"mid": mid, "d": date_iso, "s": state, "trk": track},
                )
                if res.rowcount:
                    meetings_updated += 1
                    rows_updated += res.rowcount

        print(f"[meeting_ids] date={date_iso} → meetings_updated so far={meetings_updated}, rows={rows_updated}")
        time.sleep(sleep_sec)

    print(f"[meeting_ids] DONE meetings_updated={meetings_updated}, rows_updated={rows_updated}")
    return meetings_updated, rows_updated


def main():
    ap = argparse.ArgumentParser(description="Backfill race_program.meeting_id from Punting Form meetingslist.")
    ap.add_argument("--url", help="DATABASE_URL (defaults to env DATABASE_URL or sqlite:///./racing.db)")
    ap.add_argument("--dry-run", action="store_true", help="Do not write; just report.")
    ap.add_argument("--limit", type=int, help="Process only this many meetings.")
    args = ap.parse_args()

    meetings_updated, rows_updated = backfill(
        url=args.url,
        dry_run=args.dry_run,
        limit=args.limit,
    )
    if args.dry_run:
        print(f"[meeting_ids] (dry-run) would update {meetings_updated} meetings / {rows_updated} rows.")
    else:
        print(f"[meeting_ids] done: meetings_updated={meetings_updated}, rows_updated={rows_updated}")


if __name__ == "__main__":
    main()
