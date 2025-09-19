# api/ra_discover.py
# Robust discovery of meeting keys from Racing Australia.
# Safe networking, liberal extraction, and resilient pagination.
#
# Public API:
#   discover_meeting_keys(days: int, include_past: int = 0, debug: bool = False) -> set[str]
#
# Returns normalized keys: "YYYYMonDD,STATE,TRACK"
# Example: "2025Sep22,VIC,Warrnambool"

from __future__ import annotations
import os
import re
import time
import html
import urllib.parse as up
from datetime import date, datetime, timedelta
from typing import Optional, Tuple, Set, List, Dict

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# ============================ Networking config ==============================

REQ_CONNECT_TIMEOUT = float(os.environ.get("RA_REQ_CONNECT_TIMEOUT", "6.0"))
REQ_READ_TIMEOUT    = float(os.environ.get("RA_REQ_READ_TIMEOUT", "12.0"))
REQ_TIMEOUT         = (REQ_CONNECT_TIMEOUT, REQ_READ_TIMEOUT)

# Per-listing “walk” deadline (avoid hangs)
WALK_DEADLINE_SECS  = float(os.environ.get("RA_WALK_DEADLINE_SECS", "75"))
# Stop if we see this many consecutive “no date advance” pages
MAX_STAGNANT_STEPS  = int(os.environ.get("RA_MAX_STAGNANT_STEPS", "2"))

VERBOSE = bool(os.environ.get("RA_DISCOVER_VERBOSE"))

HOME_URL   = "https://www.racingaustralia.horse/home.aspx"
STATE_URLS = [
    "https://www.racingaustralia.horse/FreeFields/Calendar.aspx?State=NSW",
    "https://www.racingaustralia.horse/FreeFields/Calendar.aspx?State=VIC",
    "https://www.racingaustralia.horse/FreeFields/Calendar.aspx?State=QLD",
    "https://www.racingaustralia.horse/FreeFields/Calendar.aspx?State=WA",
    "https://www.racingaustralia.horse/FreeFields/Calendar.aspx?State=SA",
    "https://www.racingaustralia.horse/FreeFields/Calendar.aspx?State=TAS",
    "https://www.racingaustralia.horse/FreeFields/Calendar.aspx?State=ACT",
    "https://www.racingaustralia.horse/FreeFields/Calendar.aspx?State=NT",
]
# --- network hardening (drop-in) ---------------------------------------------
import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Make timeouts configurable; defaults are intentionally short.
RA_CONNECT_TIMEOUT = float(os.getenv("RA_CONNECT_TIMEOUT", "5"))
RA_READ_TIMEOUT    = float(os.getenv("RA_READ_TIMEOUT", "15"))
REQ_TIMEOUT = (RA_CONNECT_TIMEOUT, RA_READ_TIMEOUT)

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)

def _make_session() -> requests.Session:
    """
    Return a requests.Session with retry/backoff and sane defaults.
    Keep the function name/signature so the rest of ra_discover stays untouched.
    """
    s = requests.Session()

    # Retry idempotent GET/HEAD on transient failures and dropped connections.
    retry = Retry(
        total=3, connect=3, read=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("https://", adapter)
    s.mount("http://", adapter)

    # Default headers: close connections to avoid sticky/hung keep-alives.
    s.headers.update({
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-AU,en;q=0.9",
        "Connection": "close",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    })
    return s


def _fetch(sess: requests.Session, url: str, referer: str = None) -> str:
    """
    GET a page with strict timeouts and graceful failure.

    IMPORTANT: Keep the name/signature so callers (like _walk_listing) don't change.
    On any network error/timeout, return None so the walker can skip/stop instead of hanging.
    """
    headers = {}
    if referer:
        headers["Referer"] = referer

    try:
        r = sess.get(
            url,
            headers=headers,
            timeout=REQ_TIMEOUT,          # (connect_timeout, read_timeout)
            allow_redirects=True,
        )
        # If server returns a transient error, Retry handles it automatically.
        r.raise_for_status()
        return r.text
    except requests.exceptions.RequestException as e:
        # Optional: respect your RA_DISCOVER_VERBOSE logging if present.
        try:
            if os.getenv("RA_DISCOVER_VERBOSE"):
                print(f"  [get] {url} -> {type(e).__name__}: {e}; skip")
        except Exception:
            pass
        return None
# --- end network hardening ----------------------------------------------------

# ============================== Regex helpers ================================

# Liberal URL pick-up: find RaceProgram.aspx anywhere (href, onclick, data-*, text nodes)
_RE_ANY_PROGRAM_URL = re.compile(
    r'(?i)RaceProgram\.aspx\?[^<>"\'\s]*\bKey=([^&<>"\'\s]+)'
)

# Query param “Key” inside any URL
_RE_QUERY_KEY = re.compile(r'(?:^|[?&])Key=([^&]+)', re.I)

# Tuple patterns for fallback extraction (when link URL is not present)
_RE_TUPLE_URLENC = re.compile(
    r"([12]\d{3}[A-Za-z]{3}[0-3]\d)%2C(NSW|VIC|QLD|WA|SA|TAS|ACT|NT)%2C([^&<>'\"\s]+)",
    re.I
)
_RE_TUPLE_QUOTED = re.compile(
    r"'([12]\d{3}[A-Za-z]{3}[0-3]\d)'\s*,\s*'(NSW|VIC|QLD|WA|SA|TAS|ACT|NT)'\s*,\s*'([^']+?)'",
    re.I
)
_RE_TUPLE_PLAIN = re.compile(
    r"([12]\d{3}[A-Za-z]{3}[0-3]\d)\s*,\s*(NSW|VIC|QLD|WA|SA|TAS|ACT|NT)\s*,\s*([^\r\n<>'\"\s][^\r\n<>'\"]*?)",
    re.I
)

# Valid states
_VALID_STATES = {"NSW","VIC","QLD","WA","SA","TAS","ACT","NT"}

# Exclude Trials/Jumpouts (Picnics allowed)
_EXCLUDE_TAGS = re.compile(r'\b(trial|jump[-\s]?out|jumpout)\b', re.I)

# Date headings on listing pages (fallback when no program links found)
_MONTHS = {
    'jan':1,'january':1,
    'feb':2,'february':2,
    'mar':3,'march':3,
    'apr':4,'april':4,
    'may':5,
    'jun':6,'june':6,
    'jul':7,'july':7,
    'aug':8,'august':8,
    'sep':9,'sept':9,'september':9,
    'oct':10,'october':10,
    'nov':11,'november':11,
    'dec':12,'december':12,
}

_RE_DATE_WORDY = re.compile(
    r'(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s*,?\s*(\d{1,2})\s+([A-Za-z]{3,9})\s+(\d{4})',
    re.I
)
_RE_DATE_DD_MMM_YYYY = re.compile(r'(\d{1,2})\s+([A-Za-z]{3,9})\s+(\d{4})', re.I)
_RE_DATE_SLASH = re.compile(r'(\d{1,2})/(\d{1,2})/(\d{4})')

# ============================== Utilities ====================================

def _make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=2, connect=2, read=2, backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(['GET','POST'])
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=8)
    s.mount("https://", adapter)
    s.mount("http://",  adapter)
    s.headers.update({
        "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0 Safari/537.36"),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-AU,en;q=0.8",
        "Connection": "keep-alive",
    })
    return s

def _fetch(sess: requests.Session, url: str, referer: Optional[str] = None) -> str:
    headers = {}
    if referer:
        headers["Referer"] = referer
    try:
        r = sess.get(url, headers=headers, timeout=REQ_TIMEOUT)
        r.raise_for_status()
        return r.text
    except requests.RequestException as e:
        if VERBOSE:
            print(f"[fetch] FAIL {url} :: {e}")
        return ""

def _html_unescape(s: str) -> str:
    return re.sub(r'\s+', ' ', html.unescape(s or "")).strip()

def _norm_track(s: str) -> str:
    if not s:
        return s
    s = _html_unescape(s)
    s = re.sub(r'[\u200b<>#"]+', '', s)
    return s.strip()

def _parse_key_date(dstr: str) -> Optional[date]:
    try:
        return datetime.strptime(dstr.strip(), "%Y%b%d").date()
    except Exception:
        return None

def _normalize_key(triple: Tuple[str, str, str]) -> Optional[str]:
    d, st, track = triple
    st = st.upper().strip()
    if st not in _VALID_STATES:
        return None
    dt = _parse_key_date(d)
    if not dt:
        return None

    # Exclude trials/jumpouts (based on tail text)
    if _EXCLUDE_TAGS.search(track):
        return None

    track = _norm_track(track)
    if not track:
        return None

    return f"{dt.strftime('%Y%b%d')},{st},{track}"

# ============================== Extraction ===================================

def _extract_program_keys(html_text: str) -> List[str]:
    """
    Return raw Key strings (YYYYMonDD,STATE,TRACK) from the HTML by multiple strategies:
    1) Find any RaceProgram.aspx?Key=...
    2) Find URL-encoded tuples: YYYYMonDD%2CSTATE%2CTRACK
    3) Find quoted tuples: 'YYYYMonDD','STATE','TRACK'
    4) Find plain tuples:  YYYYMonDD,STATE,TRACK
    """
    if not html_text:
        return []

    keys: Set[str] = set()

    # Strategy 1: liberal URL scan
    for m in _RE_ANY_PROGRAM_URL.finditer(html_text):
        keys.add(up.unquote(m.group(1)))

    # Strategy 1b: href= fallback
    for href in re.findall(r'href\s*=\s*["\']([^"\']+)["\']', html_text, re.I):
        if "RaceProgram.aspx" in href and "Key=" in href:
            try:
                q = up.urlparse(up.urljoin(HOME_URL, href)).query
            except Exception:
                continue
            qm = _RE_QUERY_KEY.search(q or "")
            if qm:
                keys.add(up.unquote(qm.group(1)))

    # Strategy 2: URL-encoded tuple
    for m in _RE_TUPLE_URLENC.finditer(html_text):
        d, st, tr = m.group(1), m.group(2), up.unquote(m.group(3))
        nk = _normalize_key((d, st, tr))
        if nk:
            keys.add(nk)

    # Strategy 3: JS/onclick quoted tuple
    for m in _RE_TUPLE_QUOTED.finditer(html_text):
        d, st, tr = m.group(1), m.group(2), m.group(3)
        nk = _normalize_key((d, st, tr))
        if nk:
            keys.add(nk)

    # Strategy 4: plain textual tuple
    for m in _RE_TUPLE_PLAIN.finditer(html_text):
        d, st, tr = m.group(1), m.group(2), m.group(3)
        nk = _normalize_key((d, st, tr))
        if nk:
            keys.add(nk)

    # If we captured normalized values directly (2–4), they are already normalized.
    # But for strategy 1/1b, we added raw "YYYYMonDD,STATE,TRACK" values.
    normalized: Set[str] = set()
    for k in keys:
        if "," in k and len(k.split(",")) >= 3 and re.match(r'^\d{4}[A-Za-z]{3}\d{2}$', k.split(",")[0]):
            # looks normalized
            normalized.add(_normalize_key((k.split(",")[0], k.split(",")[1], ",".join(k.split(",")[2:]))))
        else:
            normalized.add(k)

    # Filter none
    normalized = {n for n in normalized if n}

    return sorted(normalized)

# ============================== Date sniffing ================================

def _safe_make_date(day: int, month: int, year: int) -> Optional[date]:
    try:
        return date(year, month, day)
    except Exception:
        return None

def _find_dates_in_html(html_text: str) -> List[date]:
    """Extract possible dates from headings and formatted strings."""
    out: List[date] = []

    for m in _RE_DATE_WORDY.finditer(html_text):
        dd, mon_s, yyyy = int(m.group(1)), m.group(2).lower(), int(m.group(3))
        month = _MONTHS.get(mon_s[:3], _MONTHS.get(mon_s, 0))
        d = _safe_make_date(dd, month, yyyy)
        if d:
            out.append(d)

    for m in _RE_DATE_DD_MMM_YYYY.finditer(html_text):
        dd, mon_s, yyyy = int(m.group(1)), m.group(2).lower(), int(m.group(3))
        month = _MONTHS.get(mon_s[:3], _MONTHS.get(mon_s, 0))
        d = _safe_make_date(dd, month, yyyy)
        if d:
            out.append(d)

    for m in _RE_DATE_SLASH.finditer(html_text):
        dd, mm, yyyy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        d = _safe_make_date(dd, mm, yyyy)
        if d:
            out.append(d)

    # Deduplicate & sort
    out = sorted({d for d in out})
    return out

# ============================== Pagination ===================================

def _add_or_replace_query(url: str, **params) -> str:
    pu = up.urlparse(url)
    q = dict(up.parse_qsl(pu.query, keep_blank_values=True))
    for k, v in params.items():
        if v is None:
            q.pop(k, None)
        else:
            q[k] = v
    new_q = up.urlencode(q, doseq=False, safe="+-:/")
    return up.urlunparse(pu._replace(query=new_q))

def _date_variants(dt: date) -> Dict[str, str]:
    return {
        "ddmmyyyy":       dt.strftime("%d/%m/%Y"),
        "yyyymmdd":       dt.strftime("%Y-%m-%d"),
        "ddMonyyyy":      dt.strftime("%d-%b-%Y"),
        "ddPlusMonyyyy":  dt.strftime("%d+%b+%Y"),
        "yyyymondd":      dt.strftime("%Y%b%d"),
    }

def _compute_next_candidates(base_url: str, next_date: date) -> List[str]:
    v = _date_variants(next_date)
    candidates = []
    param_names = [
        "d","date","Date","week","Week",
        "fromDate","FromDate","startDate","StartDate",
        "DateFrom","dateFrom","FromDateString","dateString",
        "Key"
    ]
    for pn in param_names:
        if pn.lower() == "key":
            candidates.append(_add_or_replace_query(base_url, **{pn: v["yyyymondd"]}))
        else:
            candidates.append(_add_or_replace_query(base_url, **{pn: v["ddmmyyyy"]}))
            candidates.append(_add_or_replace_query(base_url, **{pn: v["yyyymmdd"]}))
            candidates.append(_add_or_replace_query(base_url, **{pn: v["ddMonyyyy"]}))
            candidates.append(_add_or_replace_query(base_url, **{pn: v["ddPlusMonyyyy"]}))
    # dedupe preserving order
    seen, uniq = set(), []
    for u in candidates:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq

def _keys_and_earliest(html_text: str) -> Tuple[Set[str], Optional[date], str]:
    """Extract keys and an earliest date hint (from keys or headings)."""
    keys_list = _extract_program_keys(html_text)
    keys_set: Set[str] = set(keys_list)

    # Earliest by keys
    dts: List[date] = []
    for k in keys_set:
        dt = _parse_key_date(k.split(",", 1)[0])
        if dt:
            dts.append(dt)
    earliest_keys = min(dts) if dts else None

    # Fallback: sniff headings
    earliest_head = None
    if not earliest_keys:
        candidate_dates = _find_dates_in_html(html_text)
        earliest_head = candidate_dates[0] if candidate_dates else None

    source = "keys" if earliest_keys else ("head" if earliest_head else "none")
    return keys_set, (earliest_keys or earliest_head), source

def _walk_listing(url: str, start_date: date, end_date: date, max_hops: int = 40) -> Set[str]:
    label = "HOME" if url.endswith("home.aspx") else url
    if VERBOSE:
        print(f"[walk:{'HOME' if url.endswith('home.aspx') else url.split('=')[-1]}] {url}")

    sess = _make_session()
    html_text = _fetch(sess, url)
    if VERBOSE and html_text:
        print(f"  [get] {url} -> len={len(html_text)}")

    if not html_text:
        if VERBOSE:
            print("  [get] initial fetch failed; stop")
        return set()

    keys0, earliest, src = _keys_and_earliest(html_text)

    total: Set[str] = set()
    added0 = 0
    for k in keys0:
        dt = _parse_key_date(k.split(",", 1)[0])
        if dt and start_date <= dt <= end_date:
            total.add(k); added0 += 1

    if VERBOSE:
        print(f"  [keys] raw={len(keys0)} kept={added0} (week 0) src={src}")

    if not earliest:
        if VERBOSE:
            print("  [cursor] none; cannot page reliably; stop")
        return total

    deadline = time.monotonic() + WALK_DEADLINE_SECS
    stagnant = 0
    cursor = earliest

    for _ in range(max_hops):
        if time.monotonic() > deadline:
            if VERBOSE:
                print("  [next] deadline exceeded; stop")
            break

        next_cursor = cursor + timedelta(days=7)
        candidates = _compute_next_candidates(url, next_cursor)

        advanced = False
        for nxt_url in candidates:
            nxt_html = _fetch(sess, nxt_url, referer=url)
            if not nxt_html:
                break
            nxt_keys, nxt_earliest, nxt_src = _keys_and_earliest(nxt_html)
            if not nxt_earliest or nxt_earliest <= cursor:
                continue

            url = nxt_url
            html_text = nxt_html
            cursor = nxt_earliest
            advanced = True

            added_now = 0
            for k in nxt_keys:
                dt = _parse_key_date(k.split(",", 1)[0])
                if dt and start_date <= dt <= end_date:
                    if k not in total:
                        total.add(k); added_now += 1

            if VERBOSE:
                print(f"  [next] advanced -> {cursor.isoformat()} raw={len(nxt_keys)} +{added_now} kept src={nxt_src}")
            break

        if not advanced:
            stagnant += 1
            if VERBOSE:
                print("  [next] no advance; stagnant =", stagnant)
            if stagnant >= MAX_STAGNANT_STEPS:
                if VERBOSE:
                    print("  [next] stagnant limit; stop")
                break
        else:
            stagnant = 0

        if cursor and cursor > end_date:
            break

    return total

# =============================== Public API ==================================

def discover_meeting_keys(days: int, include_past: int = 0, debug: bool = False) -> Set[str]:
    global VERBOSE
    if debug:
        VERBOSE = True

    today = date.today()
    start_date = today - timedelta(days=max(0, int(include_past)))
    end_date   = today + timedelta(days=max(0, int(days)))

    total: Set[str] = set()

    # 1) HOME
    total |= _walk_listing(HOME_URL, start_date, end_date, max_hops=40)

    # 2) Each state
    for s_url in STATE_URLS:
        total |= _walk_listing(s_url, start_date, end_date, max_hops=40)

    if VERBOSE:
        by_day: Dict[str, int] = {}
        for k in total:
            d = k.split(",", 1)[0]
            by_day[d] = by_day.get(d, 0) + 1
        days_sorted = sorted(by_day.items(), key=lambda x: x[0])
        if days_sorted:
            head = days_sorted[:5]
            tail = days_sorted[-5:]
            print(f"[discover] window counts: {head} ... {tail}")
        print(f"[discover] TOTAL unique keys: {len(total)}")

    return total
