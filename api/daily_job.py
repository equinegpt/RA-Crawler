# api/daily_job.py
from __future__ import annotations
import os
import time
import traceback
from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, text

# Core crawler
from api.crawl_calendar import crawl_next

# Optional maintenance helpers (safety nets — should rarely trigger)
try:
    from api.maintenance import backfill_race_dates, dedupe_race_program
except Exception:
    backfill_race_dates = None
    dedupe_race_program = None

# Meeting_id backfill (Punting Form — separate data source, stays as its own step)
try:
    from api.backfill_meeting_ids import backfill as backfill_meeting_ids
except Exception:
    backfill_meeting_ids = None


def _get_engine():
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


def _step(label: str, fn):
    """Run a pipeline step with timing and error handling. Returns the result or None on failure."""
    t0 = time.monotonic()
    try:
        result = fn()
        elapsed = time.monotonic() - t0
        print(f"[daily] {label}: done ({elapsed:.1f}s)")
        return result
    except Exception as e:
        elapsed = time.monotonic() - t0
        print(f"[daily] {label}: FAILED after {elapsed:.1f}s — {e}")
        traceback.print_exc()
        return None


def run_daily(
    days: int = 30,
    include_past: int = 2,
    force: bool = True,
    debug: bool = False,
) -> None:
    eng, url = _get_engine()
    print(f"[daily] DATABASE_URL={url}")
    pipeline_start = time.monotonic()

    # ── Step 1: Crawl from RA ──────────────────────────────────────────
    # Harvest now extracts distance_m and type (M/P/C) in a single pass.
    # No separate backfill steps needed for those fields.
    print(f"[daily] Step 1: Crawling next {days} day(s) (include_past={include_past})…")
    result = _step("crawl", lambda: crawl_next(days, force=force, include_past=include_past, debug=debug))
    if result:
        saved, updated = result
        print(f"[daily]   saved={saved}, updated={updated}")

    # ── Step 2: Backfill meeting_id from Punting Form ──────────────────
    # This stays as a separate step because it calls an external API (PF),
    # not the RA HTML pages that the crawler already fetches.
    if backfill_meeting_ids is not None:
        print("[daily] Step 2: Backfilling meeting_id from Punting Form…")
        result = _step("backfill_meeting_ids", lambda: backfill_meeting_ids(url, dry_run=False, limit=None))
        if result:
            meetings_updated, rows_updated = result
            print(f"[daily]   meetings_updated={meetings_updated}, rows_updated={rows_updated}")
    else:
        print("[daily] Step 2: backfill_meeting_ids not available (module missing)")

    # ── Step 3: Safety nets (should rarely trigger) ────────────────────
    if backfill_race_dates is not None:
        result = _step("safety_backfill_dates", lambda: backfill_race_dates(eng))
        if result and result > 0:
            print(f"[daily]   WARNING: fixed {result} NULL dates — this shouldn't happen")

    if dedupe_race_program is not None:
        result = _step("safety_dedupe", lambda: dedupe_race_program(eng, dry_run=False, add_unique_index=True))
        if result:
            print(f"[daily]   dedupe stats: {result}")

    # ── Step 4: Cleanup past rows ──────────────────────────────────────
    result = _step("cleanup_past_rows", lambda: _cleanup_past_rows(eng))
    if result is not None:
        print(f"[daily]   removed={result}")

    # ── Report ─────────────────────────────────────────────────────────
    rows = _count_rows(eng)
    elapsed = time.monotonic() - pipeline_start
    print(f"[daily] race_program rows now: {rows}")
    print(f"[daily] pipeline complete in {elapsed:.1f}s")


if __name__ == "__main__":
    run_daily(days=30, include_past=2, force=True, debug=False)
