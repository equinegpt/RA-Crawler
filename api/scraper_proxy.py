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


# Anti-bot challenge statuses (2026-08-01: racenet.com.au started returning
# 202 Kasada-style challenges to datacenter IPs; Scrape.do render=true also
# fails there, but super=true residential IPs pass clean).
_CHALLENGE_STATUSES = {202, 401, 403, 429}
_AUTO_SUPER = os.getenv("SCRAPEDO_SUPER_FALLBACK", "1") == "1"


def scraper_get(
    target_url: str,
    *,
    timeout: int = 30,
    session: requests.Session | None = None,
    render: bool = False,
    super_: bool = False,
    auto_super: bool = True,
) -> requests.Response:
    """
    GET a URL through scraping proxy (ScraperAPI or Scrape.do).

    Returns the full requests.Response (caller can check .status_code, .text, etc).
    Raises on HTTP errors only if the caller calls .raise_for_status().

    super_=True routes via Scrape.do residential proxies (~10 credits vs 1).
    auto_super: when the plain fetch comes back with a challenge status
    (202/401/403/429), retry once on residential automatically — callers
    doing many fetches should instead detect the first challenge and pass
    super_=True for the rest of the run (see racenet_odds.sweep).
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

    # Scrape.do
    params = {
        "token": SCRAPER_API_KEY,
        "url": target_url,
    }
    if render:
        params["render"] = "true"
    if super_:
        params["super"] = "true"
    resp = requester.get("https://api.scrape.do", params=params, timeout=timeout)
    if (not super_ and auto_super and _AUTO_SUPER
            and resp is not None and resp.status_code in _CHALLENGE_STATUSES):
        params["super"] = "true"
        resp = requester.get("https://api.scrape.do", params=params, timeout=timeout)
    return resp
