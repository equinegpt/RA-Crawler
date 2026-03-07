# api/scraper_proxy.py
"""
Shared helper to route requests through ScraperAPI proxy.

Racing Australia blocks direct requests from cloud IPs (Render, etc).
ScraperAPI rotates IPs automatically and handles retries.

Usage:
    from .scraper_proxy import scraper_get

    html = scraper_get("https://www.racingaustralia.horse/...", timeout=30)
"""
from __future__ import annotations

import os

import requests

SCRAPER_API_KEY = os.getenv("SCRAPER_API_KEY", "")
SCRAPER_API_URL = "http://api.scraperapi.com"


def scraper_get(
    target_url: str,
    *,
    timeout: int = 30,
    session: requests.Session | None = None,
    render: bool = False,
) -> requests.Response:
    """
    GET a URL through ScraperAPI proxy.

    Returns the full requests.Response (caller can check .status_code, .text, etc).
    Raises on HTTP errors only if the caller calls .raise_for_status().
    """
    params = {
        "api_key": SCRAPER_API_KEY,
        "url": target_url,
    }
    if render:
        params["render"] = "true"

    requester = session or requests
    return requester.get(SCRAPER_API_URL, params=params, timeout=timeout)
