# api/manual_backfill_program.py
from __future__ import annotations

import sys
from datetime import date, timedelta

from .ra_program_crawler import RAProgramCrawler  # 👈 adjust name if your crawler file/class differs


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

    if end < start:
        print("End date must be >= start date", file=sys.stderr)
        return 1

    crawler = RAProgramCrawler()

    d = start
    while d <= end:
        print(f"[manual_backfill_program] Crawling race program for {d}")
        try:
            crawler.fetch_for_date(d)
        except Exception as exc:
            print(f"[manual_backfill_program] ERROR for {d}: {exc}")
        d += timedelta(days=1)

    print("[manual_backfill_program] Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
