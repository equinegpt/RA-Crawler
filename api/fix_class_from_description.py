# api/fix_class_from_description.py
from __future__ import annotations
import re
import argparse
from typing import Optional, Tuple

from sqlalchemy import create_engine, text

# ---- Class inference ----
# We map a bunch of common RA phrasings into a normalized class string.
#
# Priority order:
#   1) Group/Listed (e.g., "Group 1", "Group 2", "Group 3", "Listed")
#   2) Rating bands like "0 - 64", "0-62", "62+", "66+"
#   3) Benchmark forms: "Benchmark 70", "BM70", "RTG 66+", "Ratings Band 0-55"
#   4) Class N: "Class 1", "CL2"
#   5) Maiden
#   6) Open (or leave as-is if better info isn’t present)

_RE_SP = re.compile(r"\s+")
_RE_GROUP = re.compile(r"\bGroup\s*([123])\b", re.I)
_RE_LISTED = re.compile(r"\bListed\b", re.I)

# rating band forms:
#  - "Rating 0 - 64", "Ratings Band 0-55", "0 - 68 Handicap"
_RE_BAND_0_TO_N = re.compile(r"\b(?:Rating|Ratings? Band)?\s*0\s*[-–]\s*(\d{2,3})\b", re.I)
#  - "66+" / "RTG 66+"
_RE_BAND_PLUS = re.compile(r"\b(\d{2,3})\s*\+\b", re.I)

# benchmark / ratings forms:
_RE_BM = re.compile(r"\b(?:Benchmark|BM)\s*(\d{2,3})\b", re.I)
_RE_RTG = re.compile(r"\bRTG\s*(\d{2,3})(\+)?\b", re.I)

# class forms:
_RE_CL = re.compile(r"\b(?:Class|CL)\s*(\d)\b", re.I)

# maiden:
_RE_MDN = re.compile(r"\bMaiden\b", re.I)

# “Open Handicap” cases – means no class restriction:
_RE_OPEN = re.compile(r"\bOpen\b", re.I)

def _norm(s: Optional[str]) -> str:
    return _RE_SP.sub(" ", (s or "")).strip()

def infer_class_from_text(*parts: Optional[str]) -> Optional[str]:
    """
    Return a normalized class string from any snippets we have (description, bonus line, etc.).
    Examples:
      - "Rating 0 - 64 Handicap"       -> "0-64"
      - "Ratings Band 0-55"            -> "0-55"
      - "RTG 66+" / "66+"              -> "66+"
      - "Benchmark 70" / "BM70"        -> "BM70"
      - "Class 1" / "CL1"              -> "CL1"
      - "Maiden Plate"                 -> "Maiden"
      - "Open Handicap"                -> "Open"
      - "Group 1" / "Listed"           -> "Group 1" / "Listed"
    """
    text_all = " ".join(_norm(p) for p in parts if p).strip()
    if not text_all:
        return None

    # 1) Group / Listed
    m = _RE_GROUP.search(text_all)
    if m:
        return f"Group {m.group(1)}"
    if _RE_LISTED.search(text_all):
        return "Listed"

    # 2) Rating band "0 - N"
    m = _RE_BAND_0_TO_N.search(text_all)
    if m:
        return f"0-{m.group(1)}"

    # 2b) N+ rating band
    m = _RE_BAND_PLUS.search(text_all)
    if m:
        return f"{m.group(1)}+"

    # 3) RTG / Benchmark
    m = _RE_RTG.search(text_all)
    if m:
        num, plus = m.group(1), m.group(2)
        return f"{num}+" if plus else f"BM{num}"
    m = _RE_BM.search(text_all)
    if m:
        return f"BM{m.group(1)}"

    # 4) Class N
    m = _RE_CL.search(text_all)
    if m:
        return f"CL{m.group(1)}"

    # 5) Maiden
    if _RE_MDN.search(text_all):
        return "Maiden"

    # 6) Open
    if _RE_OPEN.search(text_all):
        return "Open"

    return None


# ---- Backfill / repair runner ----

def fix_classes(db_url: str, dry_run: bool = False, limit: Optional[int] = None) -> Tuple[int, int]:
    """
    Scan race_program rows and correct the 'class' field when the description clearly indicates
    a better/more precise value (e.g., "0-62", "BM70", "Group 1", "Listed", "CL1", "Maiden", "Open").
    Returns: (scanned, updated)
    """
    eng = create_engine(db_url, future=True)
    scanned = 0
    updated = 0

    sel_sql = """
        SELECT id, description, class, bonus
        FROM race_program
        ORDER BY id
    """
    if limit:
        sel_sql += " LIMIT :limit"

    upd_sql = """
        UPDATE race_program
        SET class = :new_class
        WHERE id = :id
    """

    with eng.begin() as conn:
        rows = conn.execute(text(sel_sql), {"limit": limit} if limit else {}).mappings().fetchall()

    for r in rows:
        scanned += 1
        desc = r["description"] or ""
        curr = (r["class"] or "").strip()
        bonus = r.get("bonus") or ""

        inferred = infer_class_from_text(desc, curr, bonus)

        # Rules:
        #  - If inferred is a rating band or BM/CL/Group/Listed/Maiden/Open we trust it over a mismatched existing.
        #  - If inferred is None, skip.
        #  - If inferred == current, skip.
        if not inferred:
            continue
        if inferred == curr:
            continue

        # Only update if inferred looks "more correct" than curr:
        # e.g., desc says "0 - 62", current says "BM80" -> update to "0-62"
        should_update = False

        # If we detected Group/Listed, always prefer it.
        if inferred.startswith("Group") or inferred == "Listed":
            should_update = True

        # If we detected an explicit band or benchmark/class/maiden/open that conflicts, prefer inferred.
        band_like = bool(re.match(r"^\d{2,3}\+$|^0-\d{2,3}$", inferred))
        bm_like   = bool(re.match(r"^BM\d{2,3}$", inferred))
        cl_like   = bool(re.match(r"^CL\d$", inferred))

        if band_like or bm_like or cl_like or inferred in ("Maiden", "Open"):
            if inferred != curr:
                should_update = True

        if should_update:
            if not dry_run:
                with eng.begin() as conn:
                    conn.execute(text(upd_sql), {"new_class": inferred, "id": r["id"]})
            updated += 1

    return scanned, updated


def main():
    ap = argparse.ArgumentParser(description="Backfill/repair class from description text.")
    ap.add_argument("--url", default="sqlite:///./racing.db", help="SQLAlchemy DB URL")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    scanned, updated = fix_classes(args.url, dry_run=args.dry_run, limit=args.limit)
    print(f"[class-fix] scanned={scanned} updated={updated} dry_run={args.dry_run}")


if __name__ == "__main__":
    main()
