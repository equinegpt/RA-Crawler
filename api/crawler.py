# api/crawler.py
from __future__ import annotations
import time
import requests
from .settings import settings

def fetch(url: str, *, timeout: int | float | None = None, delay_ms: int | None = None) -> str:
    """Fetch a URL and return HTML. Raises for HTTP errors."""
    headers = {
        "User-Agent": settings.USER_AGENT,
        "Referer": settings.RA_BASE_URL,
        "Accept-Language": "en-AU,en;q=0.9",
    }
    t = timeout if timeout is not None else settings.REQUEST_TIMEOUT
    resp = requests.get(url, headers=headers, timeout=t)
    resp.raise_for_status()
    if (d := (delay_ms if delay_ms is not None else settings.REQUEST_DELAY_MS)) and d > 0:
        time.sleep(d / 1000.0)
    return resp.text
