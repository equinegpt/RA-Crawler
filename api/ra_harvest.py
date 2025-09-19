# api/ra_harvest.py
#
# Stable, production-safe Program harvester
# ----------------------------------------
# - No "Type" logic (we'll add/maintain that elsewhere)
# - Works with either a raw RA meeting Key or a full Program URL
# - Single GET per page with short timeouts + gentle retries (won't hang)
# - Populates missing date from Key
# - Infers class from the SAME ROW text only (no cross-row leakage)
# - Handles common AUS patterns: Group / Listed / Maiden / BMxx / BMxx+ /
#   ratings bands ("0 - 55") / WA "RTG 66+" -> "BM66+"
# - No DB writes here—pure fetch → parse → normalize → return rows

import re
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import unquote_plus

import requests

from .program_parser import parse_program  # existing parser the project already uses

# ---------------------------- HTTP CONFIG ------------------------------------

REQ_TIMEOUT_CONNECT = 6   # seconds
REQ_TIMEOUT_READ = 12     # seconds
REQ_TIMEOUT = (REQ_TIMEOUT_CONNECT, REQ_TIMEOUT_READ)

_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
}

# We’ll try to use urllib3’s Retry via requests if available; fall back gracefully if not.
_Retry = None
_HTTPAdapter = None
try:
    from urllib3.util.retry import Retry as _Retry  # type: ignore
    from requests.adapters import HTTPAdapter as _HTTPAdapter  # type: ignore
except Exception:
    _Retry = None
    _HTTPAdapter = None


def _build_session() -> requests.Session:
    s = requests.Session()
    if _Retry and _HTTPAdapter:
        retry = _Retry(
            total=2,
            connect=2,
            read=2,
            backoff_factor=0.5,
            status_forcelist=(500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "HEAD"]),
            raise_on_status=False,
            respect_retry_after_header=True,
        )
        adapter = _HTTPAdapter(max_retries=retry, pool_maxsize=10)
        s.mount("http://", adapter)
        s.mount("https://", adapter)
    return s


# Single module-level session to benefit from keep-alive pools
_SESS = _build_session()

# ----------------------------- KEY / URL HELPERS -----------------------------

def _extract_key(url_or_key: str) -> Optional[str]:
    """
    Returns 'YYYYMonDD,STATE,Track Name' from either a full URL or a raw key string.
    Handles URL-escaped parts. Returns None on empty.
    """
    s = (url_or_key or "").strip()
    if not s:
        return None
    # Already a URL with Key=…
    if "Key=" in s:
        key = s.split("Key=", 1)[1]
        key = key.split("&", 1)[0]
        return unquote_plus(key).strip()
    # Looks like a raw key
    return unquote_plus(s).strip()


def _date_from_key(url_or_key: str) -> Optional[str]:
    """
    Extract ISO date (YYYY-MM-DD) from the 'YYYYMonDD' prefix embedded in the Key or URL.
    """
    key = _extract_key(url_or_key)
    if not key:
        return None
    ymd = key.split(",", 1)[0].strip()
    try:
        dt = datetime.strptime(ymd, "%Y%b%d")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def _url_from_key_or_url(url_or_key: str) -> str:
    """
    If given a raw Key, construct the Program URL; if given a URL, return as-is.
    """
    s = (url_or_key or "").strip()
    if s.lower().startswith("http"):
        return s
    if "Key=" in s:
        return s
    base = "https://www.racingaustralia.horse/FreeFields/RaceProgram.aspx?Key="
    return base + s


# ----------------------------- FETCH -----------------------------------------

def _fetch_html(url: str, *, force: bool = False, referer: Optional[str] = None) -> str:
    """
    One-shot GET with short timeouts + no-cache headers. If force=True, add a
    cache-buster query parameter so we don’t get stale or cached responses.
    """
    headers = dict(_DEFAULT_HEADERS)
    if referer:
        headers["Referer"] = referer

    u = url
    if force:
        ts = int(datetime.utcnow().timestamp())
        u = f"{url}{'&' if '?' in url else '?'}_ts={ts}"

    r = _SESS.get(u, headers=headers, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    return r.text


# ----------------------------- CLASS INFERENCE -------------------------------

# Precedence: Group (1→3) > Listed > Maiden > BMxx+ > BMxx > Ratings band (0-55 etc.) > WA RTG → BMxx(+)
_GROUP_RE   = re.compile(r"\bGROUP\s*([1-3])\b", re.I)
_LISTED_RE  = re.compile(r"\bLISTED\b", re.I)
_MAIDEN_RE  = re.compile(r"\bMAIDEN\b", re.I)
_BMPLUS_RE  = re.compile(r"\bBM\s?(\d{2})\+\b", re.I)
_BM_RE      = re.compile(r"\bBM\s?(\d{2})\b", re.I)
_RATINGS_RE = re.compile(r"\bRATINGS?\s*(?:BAND\s*)?(\d{1,2})\s*[-–]\s*(\d{1,2})\b", re.I)
_RTG_RE     = re.compile(r"\bRTG\s?(\d{2})(\+?)\b", re.I)

# Signals that *aren’t* class (avoid false positives)
# e.g. “Open Handicap” often means “no class restriction, Hcp” — do not set class from “Open”
_OPEN_WORD  = re.compile(r"\bOPEN\b", re.I)


def _infer_class_from_text(text: str) -> Optional[str]:
    """
    Infer a grading/class label from a single row’s text (description + bonus).
    Returns None if nothing conclusive is found.
    """
    t = (text or "").strip()
    if not t:
        return None

    # Highest precedence
    m = _GROUP_RE.search(t)
    if m:
        return f"Group {m.group(1)}"
    if _LISTED_RE.search(t):
        return "Listed"

    # Maiden before BM/ratings—some titles include both “Maiden Handicap”
    if _MAIDEN_RE.search(t):
        return "Maiden"

    # Australian benchmark forms
    m = _BMPLUS_RE.search(t)
    if m:
        return f"BM{m.group(1)}+"
    m = _BM_RE.search(t)
    if m:
        return f"BM{m.group(1)}"

    # RATINGS BAND 0 - 55
    m = _RATINGS_RE.search(t)
    if m:
        lo, hi = m.group(1), m.group(2)
        # Emit exactly like “0-55” so downstream filters can map to min/max easily.
        return f"{lo}-{hi}"

    # WA RTG 66+ -> treat as BM66(+)
    m = _RTG_RE.search(t)
    if m:
        num, plus = m.group(1), m.group(2)
        return f"BM{num}{'+' if plus else ''}"

    # Explicit “Open” appears often but is *not* a class restriction; skip emitting “Open”
    if _OPEN_WORD.search(t):
        return None

    return None


# ----------------------------- TEXT UTILS ------------------------------------

_WS_RE = re.compile(r"\s+")


def _s(x: Optional[str]) -> str:
    return _WS_RE.sub(" ", (x or "").strip())


# ----------------------------- NORMALIZATION ---------------------------------

def _normalize_rows(rows: List[Dict[str, Any]], url_or_key: str, *, debug: bool = False) -> List[Dict[str, Any]]:
    """
    Safe normalization:
    - Backfill missing `date` from Key
    - Infer `class` from this row’s text only (no global/page bleed)
    - Leave all other fields as produced by the parser
    """
    iso_date = _date_from_key(url_or_key)
    out: List[Dict[str, Any]] = []

    for r in rows:
        row = dict(r)  # copy — never mutate parser output in place

        # Normalize fields we rely on to infer
        desc = _s(row.get("description"))
        bonus = _s(row.get("bonus"))
        row["description"] = desc or row.get("description")
        row["bonus"] = bonus or row.get("bonus")

        # If parser didn’t populate date, fill from Key
        if not row.get("date"):
            row["date"] = iso_date

        # Class: only look at this row’s own text (desc+bonus)
        have = _s(row.get("class"))
        if not have or have.lower() in {"open", "no restrictions", "no class restriction"}:
            inferred = _infer_class_from_text(f"{desc} {bonus}".strip())
            if inferred:
                row["class"] = inferred
            elif not have:
                row["class"] = None  # keep explicit None if nothing to infer

        out.append(row)

    if debug:
        print(f"[normalize] key={url_or_key} date={iso_date} rows={len(out)}")
    return out


# ----------------------------- PUBLIC API ------------------------------------

def harvest_program(url_or_key: str, *, force: bool = False, debug: bool = False) -> List[Dict[str, Any]]:
    """
    Fetch a Race Program (by raw Key or full URL), parse, normalize, and return rows.

    Parameters
    ----------
    url_or_key : str
        Either the full Program URL or a raw Key like '2025Sep20,WA,Belmont'.
    force : bool
        If True, appends a cache-busting query string to avoid stale responses.
    debug : bool
        If True, prints debug information to stdout.

    Returns
    -------
    List[Dict[str, Any]]
        One dict per race with fields produced by parse_program and normalized here.
    """
    url = _url_from_key_or_url(url_or_key)
    html = _fetch_html(url, force=force)
    # parse_program(html, url) MUST be idempotent and not rely on global state
    rows = parse_program(html, url)
    return _normalize_rows(rows, url_or_key, debug=debug)


def harvest_program_from_key(key: str, *, force: bool = False, debug: bool = False) -> List[Dict[str, Any]]:
    """
    Convenience wrapper when the caller has a meeting key such as '2025Sep27,ACT,Canberra'.
    """
    return harvest_program(key, force=force, debug=debug)
