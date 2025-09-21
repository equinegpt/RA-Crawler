# api/smoke_harvest.py
from __future__ import annotations
import argparse
from typing import List
from sqlalchemy import text

from .ra_discover import discover_meeting_keys
from .ra_harvest import harvest_program_from_key
from .crawler import upsert_program_rows
from .db import get_engine

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--include-past", type=int, default=2)
    ap.add_argument("--limit", type=int, default=10, help="how many meetings to try")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    keys = sorted(discover_meeting_keys(days=args.days, include_past=args.include_past, debug=args.debug))
    print(f"[smoke] discovered {len(keys)} keys; testing first {min(args.limit, len(keys))}")

    eng = get_engine()
    with eng.connect() as c:
        try:
            n = c.execute(text("SELECT COUNT(*) FROM race_program")).scalar_one()
            print(f"[smoke] row count BEFORE: {n}")
        except Exception as e:
            print("[smoke] WARNING: couldn't read race_program:", e)

    total_saved = 0
    total_updated = 0

    for i, key in enumerate(keys[:args.limit], 1):
        print(f"\n[smoke] ({i}/{args.limit}) Harvesting: {key}")
        try:
            rows = harvest_program_from_key(key, force=args.force, debug=args.debug)
            print(f"[smoke] rows parsed: {len(rows)}")
            if not rows:
                continue
            eng = get_engine()
            with eng.begin() as c:
                s, u = upsert_program_rows(c, rows)
                print(f"[smoke] upsert saved={s} updated={u}")
                total_saved += s
                total_updated += u
        except Exception as e:
            print(f"[smoke] ERROR on {key}: {e}")

    with get_engine().connect() as c:
        try:
            n2 = c.execute(text("SELECT COUNT(*) FROM race_program")).scalar_one()
            print(f"\n[smoke] row count AFTER: {n2}  (delta saved={total_saved}, updated={total_updated})")
        except Exception as e:
            print("[smoke] WARNING: couldn't read race_program:", e)

if __name__ == "__main__":
    main()
