# scripts/refresh_results_for_date.py
from __future__ import annotations

import datetime as dt
import os

import httpx


BASE_URL = os.environ.get("RA_CRAWLER_BASE_URL", "https://ra-crawler.onrender.com")


def main():
    # Adjust offset if you want to run this shortly after last race
    today = dt.date.today()

    with httpx.Client(timeout=60) as client:
        resp = client.post(
            f"{BASE_URL}/results/refresh",
            params={"date": today.isoformat()},
        )
        resp.raise_for_status()
        print("Results refreshed:", resp.json())


if __name__ == "__main__":
    main()
