# api/class_backfill.py
from __future__ import annotations
import re
from typing import Optional

# --- Recognize Group / Listed grades ---
RE_GROUP = re.compile(r"\bGROUP\s*([123])\b", re.IGNORECASE)
RE_LISTED = re.compile(r"\bLISTED\b", re.IGNORECASE)

# --- Regular class patterns ---
RE_RATINGS_BAND = re.compile(
    r"\b(?:RATINGS?\s*(?:BAND)?|RB)\b[^0-9]*?(\d{1,3})\s*[-–]\s*(\d{1,3})",
    re.IGNORECASE,
)
RE_BENCHMARK = re.compile(r"\b(?:BENCHMARK|BM)\s*?(\d{2,3})\b", re.IGNORECASE)
RE_CLASS_N   = re.compile(r"\b(?:CLASS|CL)\s*?(\d{1,3})\b", re.IGNORECASE)
RE_MAIDEN    = re.compile(r"\bMAIDEN\b", re.IGNORECASE)

def _normalize_range(a: str, b: str) -> str:
    try:
        lo = int(a); hi = int(b)
    except Exception:
        return f"{a}-{b}".replace(" ", "")
    if lo > hi:
        lo, hi = hi, lo
    return f"{lo}-{hi}"

def normalize_class_label(label: Optional[str]) -> Optional[str]:
    if not label:
        return None
    s = str(label).strip()
    if not s:
        return None

    if RE_LISTED.fullmatch(s):
        return "Listed"
    m = RE_GROUP.fullmatch(s)
    if m:
        return f"Group {m.group(1)}"

    m = RE_BENCHMARK.fullmatch(s)
    if m:
        return f"BM{m.group(1)}"

    m = RE_CLASS_N.fullmatch(s)
    if m:
        return f"CL{m.group(1)}"

    if RE_MAIDEN.fullmatch(s):
        return "Maiden"

    if re.fullmatch(r"\d{1,3}-\d{1,3}", s):
        lo, hi = s.split("-")
        return _normalize_range(lo, hi)

    if s.lower() == "open":
        return "Open"

    return s

def infer_class_from_fields(
    description: Optional[str],
    bonus: Optional[str] = None,
    extra: Optional[str] = None,
    current: Optional[str] = None,
) -> Optional[str]:
    """
    Priority order:
      (A) If 'Group N' or 'Listed' appears in any supplied text → return that grade.
      (B) Else detect Ratings band / BMxx / CLn / Maiden from description.
      (C) Else keep current if it's explicit (BM/CL/Maiden/0-xx).
      (D) Else return 'Open' (no class restriction).
    """
    texts = " ".join(t for t in [description or "", bonus or "", extra or ""] if t).strip()

    # (A) Grades (Group / Listed)
    m = RE_GROUP.search(texts)
    if m:
        return f"Group {m.group(1)}"
    if RE_LISTED.search(texts):
        return "Listed"

    # (B) Explicit class restrictions
    if description:
        m = RE_RATINGS_BAND.search(description)
        if m:
            return _normalize_range(m.group(1), m.group(2))
        m = RE_BENCHMARK.search(description)
        if m:
            return f"BM{m.group(1)}"
        m = RE_CLASS_N.search(description)
        if m:
            return f"CL{m.group(1)}"
        if RE_MAIDEN.search(description):
            return "Maiden"

    # (C) Keep current if explicit
    cur = normalize_class_label(current)
    if cur and (
        cur.startswith("BM") or
        cur.startswith("CL") or
        cur == "Maiden" or
        re.fullmatch(r"\d{1,3}-\d{1,3}", cur or "")
    ):
        return cur

    # (D) Default: Open
    return "Open"

def infer_class_from_text(description: Optional[str], current: Optional[str] = None) -> Optional[str]:
    # Backwards-compatible wrapper (used by older code paths)
    return infer_class_from_fields(description, None, None, current)
