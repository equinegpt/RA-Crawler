# api/ra_discover.py
from __future__ import annotations

import os
import re
import time
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, parse_qs, unquote

import requests

HOME_URL  = "https://www.racingaustralia.horse/home.aspx"
STATE_URL = "https://www.racingaustralia.horse/FreeFields/Calendar.aspx?State={state}"
PROGRAM_URL = "https://www.racingaustralia.horse/FreeFields/RaceProgram.aspx"

STATES = ("NSW","VIC","QLD","WA","SA","TAS","ACT","NT")
ALLOWED_PATHS = {
    "/FreeFields/RaceProgram.aspx",
    "/FreeFields/Form.aspx",
    "/FreeFields/Nominations.aspx",
    "/FreeFields/Weights.aspx",
    "/FreeFields/Acceptances.aspx",
    "/FreeFields/RecentForm.aspx",
    "/FreeFields/AllForm.aspx",
    "/FreeFields/Gear.aspx",
    "/FreeFields/Scratchings.aspx",
    "/FreeFields/Results.aspx",
}
TRIAL_WORDS = ("trial","jumpout","jump out")

_VERBOSE = bool(int(os.getenv("RA_DISCOVER_VERBOSE","0")))
def _v(*a): 
    if _VERBOSE: print(*a, flush=True)

# -------- regex helpers --------
_RE_A = re.compile(r"<a\b([^>]*)>(.*?)</a>", re.I|re.S)
_RE_HREF = re.compile(r'href\s*=\s*([\'"])(.*?)\1', re.I)
_RE_INPUT = re.compile(r"<input\b([^>]*)>", re.I|re.S)
_RE_ATTR = re.compile(r'(id|name|value|title|aria-label|alt|type)\s*=\s*["\']([^"\']+)["\']', re.I)
_RE_TAGS = re.compile(r"<[^>]+>")
_RE_ANY_KEY = re.compile(r"/FreeFields/[^/?#]+\.aspx\?[^\"'<>]*\bKey=([A-Za-z0-9%\-_,@ +]+)", re.I)
_RE_POSTBACK = re.compile(r"__doPostBack\(\s*'([^']+)'\s*,\s*'([^']*)'\s*\)", re.I)
_RE_INPUT_NAME = re.compile(r'\bname\s*=\s*["\']([^"\']+)["\']', re.I)
_RE_INPUT_VALUE = re.compile(r'\bvalue\s*=\s*["\']([^"\']*)["\']', re.I)
_RE_INPUT_TYPE = re.compile(r'\btype\s*=\s*["\']([^"\']+)["\']', re.I)
_RE_HIDDEN_INPUT = re.compile(r'<input\b[^>]*type\s*=\s*["\']hidden["\'][^>]*>', re.I)
_RE_KEYDATE = re.compile(r"^(\d{4})([A-Za-z]{3})(\d{2}),")
_MONTHS = {"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,"JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}

# -------- date/key utils --------
def _today() -> date:
    return datetime.now().date()

def _key_to_date(key: str) -> Optional[date]:
    m = _RE_KEYDATE.match(key or "")
    if not m: return None
    y, mon3, dd = m.groups()
    mm = _MONTHS.get(mon3.upper())
    if not mm: return None
    try: return date(int(y), mm, int(dd))
    except: return None

def _within_window(key: str, lo: date, hi: date) -> bool:
    d = _key_to_date(key)
    return bool(d and lo <= d <= hi)

def _looks_meeting_key(key: str) -> bool:
    p = [x.strip() for x in (key or "").split(",")]
    return len(p) >= 3 and p[1].upper() in STATES

def _is_trialish(s: str) -> bool:
    s = (s or "").lower()
    return any(w in s for w in TRIAL_WORDS)

def _date_range(lo: date, hi: date) -> Iterable[date]:
    d = lo; one = timedelta(days=1)
    while d <= hi:
        yield d
        d += one

# -------- HTML parse helpers --------
def _textify(html: str) -> str:
    return _RE_TAGS.sub(" ", html or "").strip().lower()

def _extract_keys_any(base_url: str, html: str) -> Set[str]:
    keys: Set[str] = set()
    if not html: return keys

    for m in _RE_ANY_KEY.finditer(html):
        k = unquote(m.group(1)).replace("+"," ").strip()
        if _looks_meeting_key(k) and not _is_trialish(k): keys.add(k)

    for m in _RE_A.finditer(html):
        attrs, inner = m.group(1) or "", m.group(2) or ""
        hm = _RE_HREF.search(attrs); href = hm.group(2) if hm else ""
        if not href: continue
        abs_url = urljoin(base_url, href)
        p = urlparse(abs_url)
        if p.path not in ALLOWED_PATHS: continue
        q = parse_qs(p.query)
        kv = q.get("Key") or q.get("key")
        if not kv: continue
        k = unquote(kv[0]).replace("+"," ").strip()
        if _looks_meeting_key(k) and not _is_trialish(k): keys.add(k)
    return keys

def _collect_hidden_fields(html: str) -> Dict[str,str]:
    fields: Dict[str,str] = {}
    for m in _RE_HIDDEN_INPUT.finditer(html or ""):
        tag = m.group(0)
        nm = _RE_INPUT_NAME.search(tag)
        if not nm: continue
        name = nm.group(1)
        vm = _RE_INPUT_VALUE.search(tag); value = vm.group(1) if vm else ""
        fields[name] = value
    return fields

# -------- “try everything” next-page mechanics --------
class _Action:
    def __init__(self, kind: str, payload: Dict[str,str]): self.kind, self.payload = kind, payload
    def __repr__(self) -> str: return f"_Action({self.kind},{self.payload})"

def _candidate_actions(url: str, html: str) -> List[_Action]:
    cand: List[_Action] = []

    # anchors
    for m in _RE_A.finditer(html or ""):
        attrs, inner = m.group(1) or "", m.group(2) or ""
        txt = _textify(inner)
        hm = _RE_HREF.search(attrs); href = hm.group(2) if hm else ""
        if href and not href.lower().startswith("javascript:"):
            cand.append(_Action("href", {"href": urljoin(url, href)}))
        else:
            jm = re.search(r"__doPostBack\(\s*'([^']+)'\s*,\s*'([^']*)'\s*\)", href or "", re.I)
            if jm:
                cand.append(_Action("postback", {"__EVENTTARGET": jm.group(1), "__EVENTARGUMENT": jm.group(2) or ""}))
        if "next" in txt and "week" in txt:
            # keep these near the front by inserting again (de-dup later)
            if href and not href.lower().startswith("javascript:"):
                cand.insert(0, _Action("href", {"href": urljoin(url, href)}))

    # inputs
    for m in _RE_INPUT.finditer(html or ""):
        attrs = m.group(1) or ""
        tm = _RE_INPUT_TYPE.search(attrs); typ = (tm.group(1).lower() if tm else "")
        if typ not in ("submit","button","image"): continue
        nm = _RE_INPUT_NAME.search(attrs); name = nm.group(1) if nm else None
        if not name: continue
        vm = _RE_INPUT_VALUE.search(attrs); value = vm.group(1) if vm else ""
        blob = " ".join(v for _,v in _RE_ATTR.findall(attrs))
        if "next" in blob.lower() and "week" in blob.lower():
            cand.insert(0, _Action("submit", {"name":name,"value":value,"type":typ}))
        else:
            cand.append(_Action("submit", {"name":name,"value":value,"type":typ}))

    # stray postbacks in text
    for jm in _RE_POSTBACK.finditer(html or ""):
        cand.append(_Action("postback", {"__EVENTTARGET": jm.group(1), "__EVENTARGUMENT": jm.group(2) or ""}))

    # de-dup
    seen=set(); out: List[_Action]=[]
    for a in cand:
        key=(a.kind, tuple(sorted(a.payload.items())))
        if key not in seen: seen.add(key); out.append(a)
    return out

def _apply_action(s: requests.Session, url: str, html: str, action: _Action, hidden: Optional[Dict[str,str]]=None) -> Optional[str]:
    if action.kind == "href":
        href = action.payload.get("href"); 
        if not href: return None
        _v("      try href:", href)
        r = s.get(href, timeout=20); 
        return r.text if r.ok else None

    if action.kind == "submit":
        if hidden is None: hidden = _collect_hidden_fields(html)
        if not hidden: return None
        name = action.payload.get("name"); value = action.payload.get("value",""); typ = action.payload.get("type","")
        if not name: return None
        data = dict(hidden); data[name]=value
        if typ=="image": data[name+".x"]="1"; data[name+".y"]="1"
        _v(f"      try submit: name={name!r} type={typ}")
        r = s.post(url, data=data, timeout=20)
        return r.text if r.ok else None

    if action.kind == "postback":
        if hidden is None: hidden = _collect_hidden_fields(html)
        if not hidden: return None
        tgt = action.payload.get("__EVENTTARGET"); arg = action.payload.get("__EVENTARGUMENT","")
        if not tgt: return None
        data = dict(hidden); data["__EVENTTARGET"]=tgt; data["__EVENTARGUMENT"]=arg
        _v(f"      try postback: target={tgt!r}")
        r = s.post(url, data=data, timeout=20)
        return r.text if r.ok else None
    return None

def _max_key_date(keys: Set[str]) -> Optional[date]:
    ds=[d for d in (_key_to_date(k) for k in keys) if d]
    return max(ds) if ds else None

def _walk_calendar(start_url: str, label: str, days: int, include_past: int) -> Set[str]:
    lo = _today() - timedelta(days=include_past)
    hi = _today() + timedelta(days=days)
    max_hops = (days // 7) + 8

    keys: Set[str] = set()
    with requests.Session() as s:
        s.headers.update({"User-Agent":"Mozilla/5.0 (ra-autowalk/1.0)","Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"})
        r = s.get(start_url, timeout=25); r.raise_for_status()
        html = r.text or ""
        prev_sig=None
        advance: Optional[_Action]=None

        for hop in range(max_hops):
            _v(f"[walk:{label}] hop {hop} on {start_url}")
            page_keys = _extract_keys_any(start_url, html)
            kept = {k for k in page_keys if _within_window(k, lo, hi) and not _is_trialish(k)}
            keys |= kept
            _v(f"[walk:{label}]   [keys] +{len(kept)} (total {len(keys)})")

            sig = hash(html)
            if sig == prev_sig:
                _v(f"[walk:{label}]   [stop] unchanged; stopping")
                break
            prev_sig = sig

            mx = _max_key_date(page_keys)
            if mx and mx >= hi:
                _v(f"[walk:{label}]   [stop] reached window end ({mx}); stopping")
                break

            hidden = _collect_hidden_fields(html)

            # reuse prior action if we had one
            tried_any=False
            if advance:
                tried_any=True
                new_html = _apply_action(s, start_url, html, advance, hidden)
                if new_html and new_html != html:
                    old_mx = _max_key_date(page_keys) or date.min
                    new_mx = _max_key_date(_extract_keys_any(start_url, new_html)) or old_mx
                    if new_mx > old_mx:
                        html = new_html; time.sleep(0.25); continue
                advance=None  # fall through to discovery

            # discover a working action
            for cand in _candidate_actions(start_url, html):
                tried_any=True
                new_html = _apply_action(s, start_url, html, cand, hidden)
                if not new_html or new_html == html: continue
                old_mx = _max_key_date(page_keys) or date.min
                new_mx = _max_key_date(_extract_keys_any(start_url, new_html)) or old_mx
                if new_mx > old_mx:
                    advance = cand; html = new_html
                    _v(f"[walk:{label}]   [next] via {cand.kind} -> max {new_mx}")
                    time.sleep(0.25); break
            else:
                if not tried_any:
                    _v(f"[walk:{label}]   [stop] no next-week control found; stopping")
                else:
                    _v(f"[walk:{label}]   [stop] no usable next-week action; stopping")
                break

    return keys

# -------- tracks inventory --------
def _tracks_by_state_from_db() -> Dict[str, Set[str]]:
    try:
        from sqlalchemy import create_engine, text
        e = create_engine("sqlite:///./racing.db")
        with e.connect() as c:
            rows = c.execute(text("select distinct state, track from race_program where state is not null and track is not null")).fetchall()
        out: Dict[str, Set[str]] = {}
        for st, trk in rows:
            stU = (st or "").strip().upper()
            if stU in STATES and trk:
                out.setdefault(stU, set()).add(trk)
        return out
    except Exception:
        return {}

def _tracks_by_state_from_track_types() -> Dict[str, Set[str]]:
    try:
        from . import track_types as TT
    except Exception:
        return {}
    def _coerce(val) -> Set[str]:
        if isinstance(val, dict): return {str(k).strip() for k in val.keys()}
        if isinstance(val, (set, list, tuple)): return {str(x).strip() for x in val}
        return set()
    # common names
    for name in ("TRACKS_BY_STATE","ALL_TRACKS_BY_STATE","STATE_TRACKS"):
        if hasattr(TT, name) and isinstance(getattr(TT,name), dict):
            d = getattr(TT, name); out={}
            for st, v in d.items():
                stU = str(st).upper()
                if stU in STATES: 
                    s = _coerce(v)
                    if s: out[stU]=s
            if out: return out
    # fallback: search any dict keyed by state codes
    out={}
    for name in dir(TT):
        val = getattr(TT, name)
        if isinstance(val, dict) and any(str(k).upper() in STATES for k in val.keys()):
            for st, v in val.items():
                stU=str(st).upper()
                if stU in STATES:
                    out.setdefault(stU,set()).update(_coerce(v))
    return out

def _tracks_from_keys(keys: Set[str]) -> Dict[str, Set[str]]:
    out: Dict[str, Set[str]] = {}
    for k in keys:
        parts = [p.strip() for p in k.split(",")]
        if len(parts) >= 3:
            stU = parts[1].upper()
            if stU in STATES:
                out.setdefault(stU,set()).add(parts[2])
    return out

# -------- brute force probe by date --------
_RE_RACE1 = re.compile(r"race\s*&nbsp;*\s*1\b|race\s*[-: ]\s*1\b|>\s*race\s*1\s*<", re.I)
_RE_TRIAL = re.compile(r"barrier\s*trial|trial", re.I)

def _looks_like_real_program(html: str) -> bool:
    if not html: return False
    if _RE_TRIAL.search(html): return False
    return bool(_RE_RACE1.search(html))

def _probe_program(session: requests.Session, key: str) -> bool:
    try:
        r = session.get(PROGRAM_URL, params={"Key": key}, timeout=18)
        return bool(r.ok and _looks_like_real_program(r.text))
    except Exception:
        return False

def _bruteforce_missing(known: Set[str], days: int, include_past: int, debug: bool) -> Set[str]:
    lo = _today() - timedelta(days=include_past)
    hi = _today() + timedelta(days=days)
    # build track inventory from seeds, DB, and track_types
    tracks = _tracks_from_keys(known)
    dbt = _tracks_by_state_from_db()
    ttt = _tracks_by_state_from_track_types()
    for st, ts in dbt.items(): tracks.setdefault(st,set()).update(ts)
    for st, ts in ttt.items(): tracks.setdefault(st,set()).update(ts)
    total_tracks = sum(len(ts) for ts in tracks.values())
    if debug: _v(f"[bf] states with tracks: {sorted(k for k in tracks if tracks[k])} (total tracks ~{total_tracks})")

    # choose start date: if seeds already reach far, no need; else probe the gap
    seeded_max = _max_key_date(known) or lo
    start = max(seeded_max + timedelta(days=1), lo)
    if start > hi: 
        if debug: _v("[bf] nothing to probe; seeds already cover window")
        return set()

    found: Set[str] = set()
    with requests.Session() as s:
        s.headers.update({"User-Agent":"Mozilla/5.0 (ra-probe/1.0)","Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"})
        for d in _date_range(start, hi):
            for st in STATES:
                ts = tracks.get(st) or set()
                for trk in ts:
                    if _is_trialish(trk): continue
                    key = d.strftime("%Y%b%d") + f",{st},{trk}"
                    if key in known: continue
                    if _probe_program(s, key):
                        if not _is_trialish(key):
                            found.add(key)
            if debug: _v(f"[bf] {d.isoformat()} -> +{len(found)} (cum)")
    if debug: _v(f"[bf] discovered extra keys: {len(found)}")
    return found

# -------- main discovery --------
def _walk_all_sources(days: int, include_past: int, debug: bool) -> Set[str]:
    keys: Set[str] = set()
    # HOME: try to paginate; if not, we still get the first week
    keys |= _walk_calendar(HOME_URL, "HOME", days, include_past)
    # States: single pass, but walker will advance if a control exists
    for st in STATES:
        keys |= _walk_calendar(STATE_URL.format(state=st), st, days, include_past)
    if debug:
        from collections import defaultdict
        by_day = defaultdict(int)
        for k in keys: by_day[k.split(",",1)[0]] += 1
        b = sorted(by_day.items())
        if b: print("[discover] window counts:", b[:5], "...", b[-5:])
        print(f"[discover] seeds: {len(keys)}")
    return keys

def discover_meeting_keys(days: int = 30, include_past: int = 0, debug: bool = False) -> Set[str]:
    if debug: os.environ["RA_DISCOVER_VERBOSE"]="1"
    lo = _today() - timedelta(days=include_past)
    hi = _today() + timedelta(days=days)

    # 1) try calendars
    seeds = _walk_all_sources(days, include_past, debug)

    # 2) if seeds don’t cover far enough, brute-force the gap by date/state/track
    seeds_max = _max_key_date(seeds) or lo
    needs_gap_fill = seeds_max < hi - timedelta(days=3)  # if we’re short of the end, fill
    if needs_gap_fill:
        extra = _bruteforce_missing(seeds, days, include_past, debug)
        all_keys = seeds | extra
    else:
        all_keys = seeds

    # final filtering
    out = {k for k in all_keys if _within_window(k, lo, hi) and not _is_trialish(k)}
    if debug:
        from collections import defaultdict
        by_day = defaultdict(int)
        for k in out: by_day[k.split(",",1)[0]] += 1
        b = sorted(by_day.items())
        if b: print("[discover] window counts:", b[:5], "...", b[-5:])
        print(f"[discover] TOTAL unique keys: {len(out)}")
    return out

# ---- walk one calendar page with autopagination (best effort) ----
def _walk_calendar(start_url: str, label: str, days: int, include_past: int) -> Set[str]:
    lo = _today() - timedelta(days=include_past)
    hi = _today() + timedelta(days=days)
    max_hops = (days // 7) + 8

    keys: Set[str] = set()
    with requests.Session() as s:
        s.headers.update({"User-Agent":"Mozilla/5.0 (ra-autowalk/1.0)","Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"})
        r = s.get(start_url, timeout=25)
        if not r.ok: return keys
        html = r.text or ""
        prev_sig=None
        advance=None

        for hop in range(max_hops):
            _v(f"[walk:{label}] hop {hop} on {start_url}")
            page_keys = _extract_keys_any(start_url, html)
            kept = {k for k in page_keys if _within_window(k, lo, hi) and not _is_trialish(k)}
            keys |= kept
            _v(f"[walk:{label}]   [keys] +{len(kept)} (total {len(keys)})")

            sig = hash(html)
            if sig == prev_sig: _v(f"[walk:{label}]   [stop] unchanged; stopping"); break
            prev_sig = sig

            mx = _max_key_date(page_keys)
            if mx and mx >= hi: _v(f"[walk:{label}]   [stop] reached window end ({mx}); stopping"); break

            hidden = _collect_hidden_fields(html)
            # reuse
            if advance:
                nh = _apply_action(s, start_url, html, advance, hidden)
                if nh and nh != html:
                    old = _max_key_date(page_keys) or date.min
                    new = _max_key_date(_extract_keys_any(start_url, nh)) or old
                    if new > old: html = nh; time.sleep(0.25); continue
                advance=None

            # discover
            for cand in _candidate_actions(start_url, html):
                nh = _apply_action(s, start_url, html, cand, hidden)
                if not nh or nh == html: continue
                old = _max_key_date(page_keys) or date.min
                new = _max_key_date(_extract_keys_any(start_url, nh)) or old
                if new > old:
                    advance=cand; html=nh; _v(f"[walk:{label}]   [next] via {cand.kind} -> max {new}"); time.sleep(0.25); break
            else:
                _v(f"[walk:{label}]   [stop] no usable next-week action; stopping"); break
    return keys

# CLI
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Discover RA meeting keys (hybrid: calendar + date probe)")
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--include-past", type=int, default=0)
    ap.add_argument("--debug", action="store_true")
    ns = ap.parse_args()
    ks = sorted(discover_meeting_keys(days=ns.days, include_past=ns.include_past, debug=ns.debug))
    print(f"Discovered {len(ks)} meetings for next {ns.days} day(s).")
    for k in ks: print(k)
