# api/results_daily_job.py
from __future__ import annotations

import os
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

    # Step 2: Scrape exotic dividends from Sportsbet
    # Use the SB results page /racing-schedule/results/YYYY-MM-DD
    # Scrape both today and yesterday to catch any missed days.
    sb_crawler = SBExoticsCrawler()
    for d in (today, yesterday):
        print(f"[results_daily_job] Scraping SB exotics for {d}")
        try:
            count = sb_crawler.fetch_for_date(d)
            print(f"[results_daily_job] SB exotics for {d}: {count} dividends")
        except Exception as exc:
            print(f"[results_daily_job] SB exotics ERROR for {d}: {exc}")

    # Step 2b (2026-07-23): exotic dividends from Racenet — the path that
    # actually populates race_dividends. One page per meeting, __NUXT__ state
    # eval via quickjs, all races x Q/E/T/FirstFour, min-across-totes. The SB
    # path above has never written a row (SPA + event-cache dependencies);
    # kept for reference until Racenet has a fortnight of clean nightly runs.
    from .racenet_dividends import fetch_for_date as racenet_fetch_for_date
    for d in (today, yesterday):
        try:
            count = racenet_fetch_for_date(d)
            print(f"[results_daily_job] Racenet dividends for {d}: {count} rows")
        except Exception as exc:
            print(f"[results_daily_job] Racenet dividends ERROR for {d}: {exc}")

    # Step 3: Trigger TRS to match results to tips (creates TipOutcome rows)
    import httpx
    trs_base = os.environ.get("TRS_BASE_URL", "https://tips-results-service.onrender.com")
    for d in (today, yesterday):
        for endpoint in ("fetch-ra-results", "fetch-pf-results"):
            try:
                with httpx.Client(timeout=120.0) as client:
                    resp = client.post(f"{trs_base}/cron/{endpoint}?date={d}")
                    print(f"[results_daily_job] TRS {endpoint} {d}: HTTP {resp.status_code}")
            except Exception as exc:
                print(f"[results_daily_job] TRS {endpoint} ERROR for {d}: {exc}")

    print("[results_daily_job] Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
