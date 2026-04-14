# api/scraper_proxy.py
"""
Shared helper to route requests through a scraping proxy.

Racing Australia blocks direct requests from cloud IPs (Render, etc).
Supports both ScraperAPI and Scrape.do via SCRAPER_PROVIDER env var.

Usage:
    from .scraper_proxy import scraper_get

    html = scraper_get("https://www.racingaustralia.horse/...", timeout=30)
"""
from __future__ import annotations

import os
import urllib.parse

import requests

SCRAPER_PROVIDER = os.getenv("SCRAPER_PROVIDER", "scrapedo")
SCRAPER_API_KEY = os.getenv("SCRAPER_API_KEY", "")


def scraper_get(
    target_url: str,
    *,
    timeout: int = 30,
    session: requests.Session | None = None,
    render: bool = False,
) -> requests.Response:
    """
    GET a URL through scraping proxy (ScraperAPI or Scrape.do).

    Returns the full requests.Response (caller can check .status_code, .text, etc).
    Raises on HTTP errors only if the caller calls .raise_for_status().
    """
    requester = session or requests

    if SCRAPER_PROVIDER == "scraperapi":
        params = {
            "api_key": SCRAPER_API_KEY,
            "url": target_url,
        }
        if render:
            params["render"] = "true"
        return requester.get("http://api.scraperapi.com", params=params, timeout=timeout)
    else:
        # Scrape.do
        params = {
            "token": SCRAPER_API_KEY,
            "url": target_url,
        }
        if render:
            params["render"] = "true"
        return requester.get("https://api.scrape.do", params=params, timeout=timeout)
