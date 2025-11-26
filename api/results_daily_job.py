# api/results_daily_job.py
from __future__ import annotations

from datetime import date, timedelta

from .ra_results_crawler import RAResultsCrawler


def main() -> int:
    today = date.today()
    yesterday = today - timedelta(days=1)

    crawler = RAResultsCrawler()

    for d in (yesterday, today):
        print(f"[results_daily_job] Crawling results for {d}")
        try:
            crawler.fetch_for_date(d)
        except Exception as exc:
            # Don't let one date kill the whole run
            print(f"[results_daily_job] ERROR for {d}: {exc}")

    print("[results_daily_job] Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
