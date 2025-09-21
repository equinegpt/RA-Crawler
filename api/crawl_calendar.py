# api/crawl_calendar.py
from __future__ import annotations
import argparse
from typing import Tuple
from sqlalchemy import text

from .ra_discover import discover_meeting_keys
from .ra_harvest import harvest_program_from_key
from .crawler import upsert_program_rows
from .db import get_engine


def crawl_next(
    days: int,
    *,
    include_past: int = 0,
    force: bool = False,
    debug: bool = False,
) -> Tuple[int, int]:
    """
    Discover meeting keys for the window and harvest+upsert each meeting.
    Returns (total_saved, total_updated).
    """
    keys = sorted(discover_meeting_keys(days=days, include_past=include_past, debug=debug))
    print(f"[crawl] discovered {len(keys)} keys for days={days}, include_past={include_past}")

    eng = get_engine()
    try:
        with eng.connect() as c:
            n0 = c.execute(text("SELECT COUNT(*) FROM race_program")).scalar_one()
            print(f"[crawl] row count BEFORE: {n0}")
    except Exception as e:
        print("[crawl] WARNING: could not read race_program before:", e)

    total_saved = 0
    total_updated = 0

    for idx, key in enumerate(keys, 1):
        print(f"\n[crawl] ({idx}/{len(keys)}) Harvesting: {key}")
        try:
            rows = harvest_program_from_key(key, force=force, debug=debug)
            print(f"[crawl] parsed rows: {len(rows)}")
            if not rows:
                continue
            with eng.begin() as c:
                s, u = upsert_program_rows(c, rows)
                print(f"[crawl] upsert saved={s} updated={u}")
                total_saved += s
                total_updated += u
        except Exception as e:
            # One bad meeting shouldn't break the run
            print(f"[crawl] ERROR on {key}: {e}")

    try:
        with eng.connect() as c:
            n1 = c.execute(text("SELECT COUNT(*) FROM race_program")).scalar_one()
            print(f"\n[crawl] row count AFTER: {n1}  (delta saved={total_saved}, updated={total_updated})")
    except Exception as e:
        print("[crawl] WARNING: could not read race_program after:", e)

    return total_saved, total_updated


def _main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--include-past", type=int, default=2)
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--debug", action="store_true")
    ns = ap.parse_args()

    s, u = crawl_next(
        ns.days,
        include_past=ns.include_past,
        force=ns.force,
        debug=ns.debug,
    )
    print(f"TOTAL saved {s}, updated {u}")


if __name__ == "__main__":
    _main()
