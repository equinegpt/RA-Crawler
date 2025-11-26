# api/manual_backfill_program.py
from __future__ import annotations

import sys
from datetime import date
from typing import Tuple

from api.crawl_calendar import crawl_next


def _compute_window(start: date, end: date, today: date) -> Tuple[int, int]:
    """
    Compute (include_past, days) parameters for crawl_next so that the
    [start, end] window is covered, relative to `today`.

    crawl_next(days, include_past, ...) typically covers the range:
      [today - include_past, today + days - 1]

    We choose include_past and days such that [start, end] ⊆ that range.
    """
    if end < start:
        raise ValueError("end must be >= start")

    # How far back is `start` from today?
    days_back = (today - start).days
    include_past = max(days_back, 0)

    # How far forward is `end` from today?
    days_fwd = (end - today).days
    days_param = max(days_fwd + 1, 1)  # at least 1 day

    return include_past, days_param


def main() -> int:
    if len(sys.argv) != 3:
        print(
            "Usage: python -m api.manual_backfill_program YYYY-MM-DD YYYY-MM-DD",
            file=sys.stderr,
        )
        return 1

    start_str, end_str = sys.argv[1], sys.argv[2]
    start = date.fromisoformat(start_str)
    end = date.fromisoformat(end_str)
    today = date.today()

    if end < start:
        print("End date must be >= start date", file=sys.stderr)
        return 1

    include_past, days_param = _compute_window(start, end, today)

    print(
        f"[manual_backfill_program] today={today}, "
        f"start={start}, end={end}, "
        f"include_past={include_past}, days={days_param}"
    )

    saved, updated = crawl_next(
        days=days_param,
        force=True,
        include_past=include_past,
        debug=False,
    )
    print(
        f"[manual_backfill_program] crawl_next: saved={saved}, updated={updated}"
    )
    print("[manual_backfill_program] Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
