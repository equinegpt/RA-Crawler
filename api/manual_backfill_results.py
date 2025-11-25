# api/manual_backfill_results.py
from __future__ import annotations

import sys
from datetime import date, timedelta

from .ra_results_crawler import RAResultsCrawler


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print("Usage: python -m api.manual_backfill_results YYYY-MM-DD YYYY-MM-DD")
        return 1

    start_str, end_str = argv[1], argv[2]
    try:
        start = date.fromisoformat(start_str)
        end = date.fromisoformat(end_str)
    except ValueError:
        print("ERROR: dates must be in ISO format YYYY-MM-DD")
        return 1

    if end < start:
        print("ERROR: end date must be >= start date")
        return 1

    crawler = RAResultsCrawler()

    cur = start
    while cur <= end:
        print(f"[manual_backfill] Crawling results for {cur}")
        crawler.fetch_for_date(cur)
        cur += timedelta(days=1)

    print("[manual_backfill] Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
