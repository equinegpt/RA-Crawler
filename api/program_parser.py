# api/program_parser.py

import re
import html as _html
from typing import List, Dict, Optional, Tuple
from urllib.parse import unquote
import re
import re
from urllib.parse import urlparse, parse_qs, unquote

_MONTHS = {"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,
           "JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}

_WORD_NUM = {
    "ONE":"1","TWO":"2","THREE":"3","FOUR":"4","FIVE":"5","SIX":"6",
    "SEVEN":"7","EIGHT":"8","NINE":"9","TEN":"10","ELEVEN":"11","TWELVE":"12"
}

def _clean(s: str) -> str:
    # unescape entities, normalize odd spaces, strip tags, collapse whitespace
    s = _html.unescape(s)
    s = re.sub(r"[\u00A0\u202F\u2009\u200A]", " ", s)  # nbsp/thin/hair spaces -> normal
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _parse_key_from_url(url: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    # Key=2025Sep13,VIC,Flemington
    m = re.search(r"[?&]Key=([^&]+)", url)
    if not m:
        return None, None, None
    key = unquote(m.group(1))
    parts = key.split(",")
    if len(parts) != 3:
        return None, None, None
    ymondd, state, track = parts
    state = state.strip() or None
    track = track.strip() or None
    md = re.match(r"(\d{4})([A-Za-z]{3})(\d{2})$", ymondd.strip())
    if not md:
        return None, state, track
    y, mon3, d = md.groups()
    mon = _MONTHS.get(mon3.upper())
    date_iso = f"{y}-{mon}-{d}" if mon else None
    return date_iso, state, track

def _strip_time_prefix(s: str) -> str:
    return re.sub(r"^\s*\d{1,2}:\d{2}\s*[AP]M\s*", "", s, flags=re.I)

def _strip_trailing_metres(s: str) -> str:
    return re.sub(r"\s*\(\s*\d{3,4}\s*METRES?\s*\)\s*$", "", s, flags=re.I)

def _strip_leading_dash(s: str) -> str:
    return re.sub(r"^\s*[-\u2010-\u2015\u2212‒]+\s*", "", s)

def _normalize_title(raw: str) -> str:
    t = _strip_leading_dash(raw)
    t = _strip_time_prefix(t)
    t = _strip_trailing_metres(t)
    return t.strip()

def _to_int(s: Optional[str]) -> Optional[int]:
    if not s: return None
    try: return int(s.replace(",", "").strip())
    except Exception: return None

# ---------- strong header matcher (prevents dupes & misses) ----------
# Example it matches: "Race 10 - 5:25PM Ronald McDonald House ... (1700 METRES)"
_RE_HEADER_FULL = re.compile(
    r"Race\s*0*(?P<num>\d{1,2})\s*(?:[-\u2010-\u2015\u2212‒]\s*)?"
    r"(?:(?P<time>\d{1,2}:\d{2}\s*[AP]M)\s*)?"
    r"(?P<title>.*?)\(\s*(?P<metres>\d{3,4})\s*METRES?\s*\)",
    re.I | re.S
)

def _iter_blocks(text: str):
    """Yield (race_no:int, header_title:str, distance:int, block_text:str)."""
    heads = list(_RE_HEADER_FULL.finditer(text))
    for i, h in enumerate(heads):
        race_no = int(h.group("num"))
        title_raw = h.group("title") or ""
        distance = _to_int(h.group("metres"))
        start = h.end()
        end = heads[i+1].start() if i+1 < len(heads) else len(text)
        block = text[start:end].strip()
        yield race_no, title_raw, distance, block

# ---------- field regexes ----------
_RE_PRIZE_OF    = re.compile(r"\bOf\s*\$?\s*([\d,]{3,})", re.I)
_RE_PRIZE_TOTAL = re.compile(r"\b(?:Total\s+)?Prizemoney\b[^$]*\$\s*([\d,]{3,})", re.I)
_RE_PRIZE_ANY   = re.compile(r"\$\s*([\d,]{3,})")

_RE_HAS_SWP = re.compile(r"(?:SET\s*WEIGHTS\s*(?:&|AND|PLUS)\s*PENALTIES|SWP)", re.I)
_RE_HAS_SW  = re.compile(r"\bSET\s*WEIGHTS\b", re.I)
_RE_HAS_QLT = re.compile(r"\bQUALITY\b", re.I)
_RE_HAS_HCP = re.compile(r"\b(HANDICAP|HCP)\b", re.I)
_RE_HAS_WFA = re.compile(r"\bWFA\b|\bWEIGHT\s*FOR\s*AGE\b", re.I)

_RE_NOCLASS   = re.compile(r"\bNo\s+class\s+restriction(s)?\b", re.I)
_RE_BM        = re.compile(r"\bBM\s?(\d{2})\b", re.I)
_RE_BENCHMARK = re.compile(r"\bBENCHMARK\s*(\d{1,2})\b", re.I)
_RE_CLASSN    = re.compile(r"\bCLASS\s?(\d)\b", re.I)
_RE_GROUP     = re.compile(r"\bGROUP\s?(1|2|3)\b", re.I)
_RE_LISTED    = re.compile(r"\bLISTED\b", re.I)
_RE_MAIDEN    = re.compile(r"\bMAIDEN\b", re.I)

_RE_NO_AGE      = re.compile(r"\bNo\s+age\s+restriction(s)?\b", re.I)
_RE_AGE_YO      = re.compile(r"\b(\d{1,2})\s?YO\b", re.I)
_RE_AGE_WORD    = re.compile(r"\b(One|Two|Three|Four|Five|Six|Seven|Eight|Nine|Ten|Eleven|Twelve)\s*(?:-|\s)*Years?\s*(?:-|\s)*Old\b", re.I)
_RE_AGE_WORD_UP = re.compile(r"\b(One|Two|Three|Four|Five|Six|Seven|Eight|Nine|Ten|Eleven|Twelve)\s*(?:-|\s)*Years?\s*(?:-|\s)*Old\s*(?:and\s*Up(?:wards)?|&\s*Up)\b", re.I)
_RE_AGE_NUM_UP  = re.compile(r"\b(\d{1,2})\s*(?:-|\s)*Years?\s*(?:-|\s)*Old\s*(?:and\s*Up(?:wards)?|&\s*Up)\b", re.I)

_RE_NO_SEX      = re.compile(r"\bNo\s+sex\s+restriction(s)?\b", re.I)
_RE_SEX_FM_ABBR = re.compile(r"\bF\s*&\s*M\b|\bF&M\b", re.I)
_RE_SEX_CG_ABBR = re.compile(r"\bC\s*&\s*G\b|\bCG&E\b|\bC&G\b|\bC\s*G\s*&\s*E\b", re.I)
_RE_SEX_WORDS   = re.compile(r"\b(FILLIES(?:\s+AND\s+MARES)?|MARES|FILLIES|COLTS|GELDINGS|OPEN)\b", re.I)
_RE_SCHEME_BONUS = re.compile(
    r"\b((?:VOBIS|BOBS|QTIS|WESTSPEED|MAGIC MILLIONS)[^.:\n\r]*(?:BONUS|AVAILABLE)[^.\n\r]*)",
    re.I,
)
RE_NOMINATOR    = re.compile(r"\bNominator\s+Bonus[^.\n\r]*", re.I)

_RE_RTG          = re.compile(r'\bRTG\s*(\d+\+?)', re.I)                # "RTG 66+"
_RE_BASE_RATING  = re.compile(r'\bBase\s*Rating\s*(\d+)', re.I)         # "Base Rating 71"
_RE_BENCHMARK    = re.compile(r'\b(?:BM|Benchmark)\s*(\d+\+?)', re.I)   # "BM64" / "Benchmark 72"
_RE_CLASSNUM     = re.compile(r'\bClass\s*(\d+)\b', re.I)               # "Class 3"

def _parse_key_meta_from_url(url: str):
    """
    Fallback: extract (date_iso, state, track) from ?Key=YYYYMonDD,STATE,Track[,...]
    """
    try:
        p = urlparse(url or "")
        q = parse_qs(p.query)
        key = (q.get("Key") or q.get("key") or [""])[0]
        key = unquote(key)
        parts = [x.strip() for x in key.split(",")]
        if len(parts) < 3:
            return (None, None, None)
        dstr, state, track = parts[:3]
        m = re.match(r'^(\d{4})([A-Za-z]{3})(\d{2})$', dstr)
        date_iso = None
        if m:
            y, mon3, dd = m.groups()
            mm = _MONTHS.get(mon3.upper())
            if mm:
                date_iso = f"{y}-{mm:02d}-{int(dd):02d}"
        track = re.sub(r'\s+', ' ', track).strip()
        return (date_iso, state.upper(), track)
    except Exception:
        return (None, None, None)

def _derive_bm_or_class_from_text(txt: str):
    """
    Returns normalized class string like 'BM66+', 'BM71', or 'CL3' if present in text.
    Priority:
      Base Rating N -> BMN
      RTG N[+]      -> BMN[+]
      BM/Benchmark  -> BMN[+]
      Class N       -> CLN
    """
    t = (txt or "").strip()
    m = _RE_BASE_RATING.search(t)
    if m: return f"BM{m.group(1)}"
    m = _RE_RTG.search(t)
    if m: return f"BM{m.group(1)}"
    m = _RE_BENCHMARK.search(t)
    if m: return f"BM{m.group(1)}"
    m = _RE_CLASSNUM.search(t)
    if m: return f"CL{m.group(1)}"
    return None

# try to pick class near a particular race block in raw HTML (heuristic window after "Race {no}")
def _derive_class_near_race(html: str, race_no: int):
    if not html or not race_no:
        return None
    # find "Race 7", "Race&nbsp;7", "Race - 7", etc., then scan forward a bit
    pat = re.compile(rf'(?is)race\s*(?:&nbsp;|\s|[:-])*{race_no}\b')
    m = pat.search(html)
    if not m:
        return None
    segment = html[m.start(): m.start() + 2000]  # small window
    return _derive_bm_or_class_from_text(segment)

def _postprocess_rows(rows, url: str, html: str):
    """
    Adjust rows by:
      - filling class (BM/CL) from description or nearby HTML
      - filling date/state/track from ?Key=... when missing
    """
    for r in rows:
        # derive class from description first
        desc = r.get("description") or ""
        derived = _derive_bm_or_class_from_text(desc)

        # if still not found, try to scan near the race block in html
        if not derived:
            rn = r.get("race_no")
            try:
                rn = int(rn) if rn is not None else None
            except Exception:
                rn = None
            if rn:
                derived = _derive_class_near_race(html, rn)
        if derived:
            r["class"] = derived

        # meta fallback from Key
        if not (r.get("date") and r.get("state") and r.get("track")):
            d, s, t = _parse_key_meta_from_url(url)
            if not r.get("date") and d:  r["date"]  = d
            if not r.get("state") and s: r["state"] = s
            if not r.get("track") and t: r["track"] = t
    return rows

# ---------- pickers ----------
def _pick_prize_total(block: str) -> Optional[int]:
    m = _RE_PRIZE_OF.search(block) or _RE_PRIZE_TOTAL.search(block)
    if m: return _to_int(m.group(1))
    nums = [_to_int(x) for x in _RE_PRIZE_ANY.findall(block)]
    nums = [n for n in nums if n]
    return max(nums) if nums else None

def _pick_condition(block: str) -> Optional[str]:
    # precedence: SWP > SW > Quality > Hcp > WFA
    if _RE_HAS_SWP.search(block): return "SWP"
    if _RE_HAS_SW.search(block):  return "SW"
    if _RE_HAS_QLT.search(block): return "Quality"
    if _RE_HAS_HCP.search(block): return "Hcp"
    if _RE_HAS_WFA.search(block): return "WFA"
    return None

def _pick_class(block: str, title: str) -> Optional[str]:
    mg = _RE_GROUP.search(block)
    if mg: return "G" + mg.group(1)
    if _RE_LISTED.search(block): return "Listed"
    d = title.upper()
    m = _RE_BM.search(d) or _RE_BENCHMARK.search(d)
    if m: return f"BM{m.group(1)}"
    if _RE_MAIDEN.search(d): return "Maiden"
    m = _RE_CLASSN.search(d)
    if m: return f"CL{m.group(1)}"
    if " OPEN " in f" {d} " or _RE_NOCLASS.search(block):
        return "Open"
    return None

def _age_from_block(block: str) -> Optional[str]:
    if _RE_NO_AGE.search(block):
        return "No Restrictions"
    m = _RE_AGE_YO.search(block)
    if m:
        return m.group(1)
    m = _RE_AGE_NUM_UP.search(block)
    if m:
        return f"{m.group(1)}+"
    m = _RE_AGE_WORD_UP.search(block)
    if m:
        num = _WORD_NUM.get(m.group(1).upper())
        return f"{num}+" if num else None
    m = _RE_AGE_WORD.search(block)
    if m:
        return _WORD_NUM.get(m.group(1).upper())
    return None

def _sex_from_title(title: str) -> Optional[str]:
    t = f" {title.upper()} "
    if _RE_SEX_FM_ABBR.search(t) or " FILLIES AND MARES " in t:
        return "Fillies & Mares"
    if " FILLIES " in t:
        return "Fillies"
    if _RE_SEX_CG_ABBR.search(t) or " COLTS " in t or " GELDINGS " in t:
        return "Colts & Geldings"
    m = _RE_SEX_WORDS.search(t)
    if m:
        tok = m.group(1).upper()
        if tok.startswith("FILLIES AND MARES"): return "Fillies & Mares"
        if tok == "FILLIES": return "Fillies"
        if tok == "MARES":   return "Mares"
        if tok == "COLTS":   return "Colts & Geldings"
        if tok == "GELDINGS":return "Geldings"
        if tok == "OPEN":    return "Open"
    return None

def _sex_from_blob(block: str) -> Optional[str]:
    if _RE_NO_SEX.search(block): return "Open"
    if _RE_SEX_FM_ABBR.search(block) or re.search(r"FILLIES\s+AND\s+MARES", block, re.I):
        return "Fillies & Mares"
    if re.search(r"\bFILLIES\b", block, re.I):
        return "Fillies"
    if _RE_SEX_CG_ABBR.search(block) or re.search(r"\bCOLTS\b|\bGELDINGS\b", block, re.I):
        return "Colts & Geldings"
    m = _RE_SEX_WORDS.search(block)
    if m:
        tok = m.group(1).upper()
        if tok.startswith("FILLIES AND MARES"): return "Fillies & Mares"
        if tok == "FILLIES": return "Fillies"
        if tok == "MARES":   return "Mares"
        if tok == "COLTS":   return "Colts & Geldings"
        if tok == "GELDINGS":return "Geldings"
        if tok == "OPEN":    return "Open"
    return None

def _resolve_sex(title: str, block: str) -> str:
    s_blob  = _sex_from_blob(block)
    s_title = _sex_from_title(title)
    if s_blob and s_title and s_blob != s_title:
        return s_title
    return s_blob or s_title or "Open"

def _bonus_from_block(block: str) -> Optional[str]:
    """
    Return real bonus lines:
      - Scheme lines that mention BONUS/AVAILABLE (VOBIS/BOBS/QTIS/WESTSPEED/MAGIC MILLIONS ...)
      - 'Nominator Bonus ...' lines
    Explicitly ignore any HTML-attribute junk (alt/width/height/border/etc).
    """
    import re

    scheme_rx = re.compile(
        r"\b((?:VOBIS|BOBS|QTIS|WESTSPEED|MAGIC MILLIONS)[^.:\n\r]*(?:BONUS|AVAILABLE)[^.\n\r]*)",
        re.I,
    )
    nominator_rx = re.compile(r"\bNominator\s+Bonus[^.\n\r]*", re.I)

    # Anything with these tokens is junk (logo attributes etc.)
    _attr_junk = re.compile(r'\b(?:alt=|width=|height=|border=)\b', re.I)

    seen, out = set(), []
    for rx in (scheme_rx, nominator_rx):
        for m in rx.finditer(block):
            raw = m.group(0)
            if _attr_junk.search(raw):   # skip logo/HTML attribute noise
                continue
            txt = _clean(raw)
            key = txt.casefold()
            if key and key not in seen:
                seen.add(key)
                out.append(txt)

    return " | ".join(out) if out else None

_MONTHS = {"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,
           "JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}

# class/benchmark patterns
_RE_RTG          = re.compile(r'\bRTG\s*(\d+\+?)', re.I)                # e.g. "RTG 66+"
_RE_BASE_RATING  = re.compile(r'\bBase\s*Rating\s*(\d+)', re.I)         # e.g. "Base Rating 71"
_RE_BENCHMARK    = re.compile(r'\b(?:BM|Benchmark)\s*(\d+\+?)', re.I)   # e.g. "BM64" / "Benchmark 72"
_RE_CLASSNUM     = re.compile(r'\bClass\s*(\d+)\b', re.I)               # e.g. "Class 3"

def _parse_key_meta_from_url(url: str):
    """
    Fallback meta from Key param: 2025Sep27,NSW,Come-By-Chance[,Picnic...]
    Returns (date_iso, state, track) with track normalized.
    """
    try:
        p = urlparse(url or "")
        q = parse_qs(p.query)
        key = (q.get("Key") or q.get("key") or [""])[0]
        key = unquote(key)
        parts = [x.strip() for x in key.split(",")]
        if len(parts) < 3:
            return (None, None, None)

        dstr, state, track = parts[:3]
        m = re.match(r'^(\d{4})([A-Za-z]{3})(\d{2})$', dstr)
        date_iso = None
        if m:
            y, mon3, dd = m.groups()
            mm = _MONTHS.get(mon3.upper())
            if mm:
                date_iso = f"{y}-{mm:02d}-{int(dd):02d}"

        # normalize track a touch (RA uses mixed punctuation)
        track = re.sub(r'\s+', ' ', track).strip()
        # keep “Come-By-Chance” hyphens as-is
        return (date_iso, state.upper(), track)
    except Exception:
        return (None, None, None)

# ---------- main ----------
def parse_program(html: str, url: str) -> List[Dict]:
    date_iso, state, track = _parse_key_from_url(url)
    text = _clean(html)
    rows: List[Dict] = []

    for race_no, title_raw, distance, block in _iter_blocks(text):
        title = _normalize_title(title_raw)

        prize = _pick_prize_total(block)
        cond  = _pick_condition(block)
        race_class = _pick_class(block, title)
        age  = _age_from_block(block)
        sex  = _resolve_sex(title, block)
        bonus = _bonus_from_block(block)

        rows.append({
            "race_no": race_no,
            "date": date_iso,
            "state": state,
            "track": track,
            "type": None,  # harvester infers M/P/C by state+track
            "description": title or None,
            "prize": prize,
            "condition": cond,
            "class": race_class,
            "age": age,
            "sex": sex,
            "distance": distance,
            "bonus": bonus,
            "url": url,
        })

    rows = _postprocess_rows(rows, url, html)
    return rows 
