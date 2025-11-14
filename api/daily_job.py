# api/daily_job.py
from __future__ import annotations
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, text

# Core crawler
from api.crawl_calendar import crawl_next

# Optional maintenance helpers (if present)
try:
    from api.maintenance import backfill_race_dates, dedupe_race_program
except Exception:
    backfill_race_dates = None
    dedupe_race_program = None

# Optional distance backfill
try:
    from api.backfill_distance import backfill as backfill_distance
except Exception:
    backfill_distance = None

# Optional type (M/P/C) backfill
try:
    from api.backfill_types import backfill as backfill_types
except Exception:
    backfill_types = None

# Optional meeting_id backfill (Punting Form)
try:
    from api.backfill_meeting_ids import backfill as backfill_meeting_ids
except Exception:
    backfill_meeting_ids = None


def _get_engine():
    # Default is the Render volume; overridden by DATABASE_URL when set
    url = os.getenv("DATABASE_URL", "sqlite:////data/racing.db")
    eng = create_engine(url, future=True)
    return eng, url


def _cleanup_past_rows(eng) -> int:
    """Delete races older than 'today' in AU/Melbourne (so UI never shows stale)."""
    today_melb = datetime.now(ZoneInfo("Australia/Melbourne")).date()
    with eng.begin() as conn:
        n = conn.execute(
            text("DELETE FROM race_program WHERE date < :today"),
            {"today": today_melb.isoformat()},
        ).rowcount or 0
    return n


def _count_rows(eng) -> int:
    with eng.connect() as c:
        try:
            return c.execute(text("SELECT COUNT(*) FROM race_program")).scalar() or 0
        except Exception:
            return 0


def run_daily(
    days: int = 30,
    include_past: int = 2,
    force: bool = True,
    debug: bool = False,
) -> None:
    eng, url = _get_engine()
    print(f"[daily] DATABASE_URL={url}")

    # 1) Crawl from RA
    print(f"[daily] Crawling next {days} day(s) (include_past={include_past})…")
    saved, updated = crawl_next(days, force=force, include_past=include_past, debug=debug)
    print(f"[daily] crawl_next: saved={saved}, updated={updated}")

    # 2) Optional: backfill dates (defensive; cheap and safe)
    if backfill_race_dates is not None:
        try:
            print("[daily] Backfilling NULL dates (if any)…")
            n = backfill_race_dates(eng)
            print(f"[daily] backfill_race_dates: fixed={n}")
        except Exception as e:
            print(f"[daily] backfill_race_dates skipped (error: {e})", file=sys.stderr)

    # 3) Optional: de-duplicate by (date,state,track,race_no,url)
    if dedupe_race_program is not None:
        try:
            print("[daily] De-duplicating race_program…")
            stats = dedupe_race_program(eng, dry_run=False, add_unique_index=True)
            print(f"[daily] dedupe: {stats}")
        except Exception as e:
            print(f"[daily] dedupe skipped (error: {e})", file=sys.stderr)

    # 4) Backfill distance_m from Program pages (your existing rule set)
    if backfill_distance is not None:
        try:
            print("[daily] Backfilling distance_m…")
            scanned, updated_rows = backfill_distance(
                url,          # DB URL
                limit=None,   # no explicit limit → use your internal query
                sleep_sec=0.15,
                dry_run=False,
            )
            print(f"[daily] backfill_distance: meetings_scanned={scanned}, updated_rows={updated_rows}")
        except Exception as e:
            print(f"[daily] backfill_distance skipped (error: {e})", file=sys.stderr)
    else:
        print("[daily] backfill_distance not available (module missing)")

    # 5) Backfill type (M/P/C) per meeting
    if backfill_types is not None:
        try:
            print("[daily] Backfilling type (M/P/C)…")
            updated_meetings = backfill_types(
                url,          # DB URL
                dry_run=False,
                limit=None,
            )
            print(f"[daily] backfill_types: updated_meetings={updated_meetings}")
        except Exception as e:
            print(f"[daily] backfill_types skipped (error: {e})", file=sys.stderr)
    else:
        print("[daily] backfill_types not available (module missing)")

    # 6) Backfill meeting_id from Punting Form
    if backfill_meeting_ids is not None:
        try:
            print("[daily] Backfilling meeting_id from Punting Form…")
            meetings_updated, rows_updated = backfill_meeting_ids(
                url,          # DB URL
                dry_run=False,
                limit=None,
            )
            print(f"[daily] backfill_meeting_ids: meetings_updated={meetings_updated}, rows_updated={rows_updated}")
        except Exception as e:
            print(f"[daily] backfill_meeting_ids skipped (error: {e})", file=sys.stderr)
    else:
        print("[daily] backfill_meeting_ids not available (module missing)")

    # 7) Remove past rows relative to AU/Melbourne “today”
    try:
        removed = _cleanup_past_rows(eng)
        print(f"[daily] cleanup_past_rows: removed={removed}")
    except Exception as e:
        print(f"[daily] cleanup_past_rows skipped (error: {e})", file=sys.stderr)

    # 8) Report final row count
    rows = _count_rows(eng)
    print(f"[daily] race_program rows now: {rows}")


if __name__ == "__main__":
    # Defaults: 30 days forward, keep 2 days behind “today”
    run_daily(days=30, include_past=2, force=True, debug=False)
