# api/results_daily_job.py
from __future__ import annotations

from datetime import date, timedelta

from .ra_results_crawler import RAResultsCrawler
from .sb_exotics_crawler import SBExoticsCrawler


def main() -> int:
    today = date.today()
    yesterday = today - timedelta(days=1)

    # Step 1: Crawl RA results
    ra_crawler = RAResultsCrawler()
    for d in (yesterday, today):
        print(f"[results_daily_job] Crawling RA results for {d}")
        try:
            ra_crawler.fetch_for_date(d)
        except Exception as exc:
            print(f"[results_daily_job] RA ERROR for {d}: {exc}")

    # Step 2: Scrape exotic dividends from Sportsbet (yesterday only)
    print(f"[results_daily_job] Scraping SB exotics for {yesterday}")
    sb_crawler = SBExoticsCrawler()
    try:
        count = sb_crawler.fetch_for_date(yesterday)
        print(f"[results_daily_job] SB exotics: {count} dividends")
    except Exception as exc:
        print(f"[results_daily_job] SB exotics ERROR for {yesterday}: {exc}")

    print("[results_daily_job] Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
