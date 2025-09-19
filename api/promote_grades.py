# api/promote_grades.py
from __future__ import annotations

"""
Promote Group/Listed grades into the `class` field by scanning each Program page.

Usage:
  .venv/bin/python -m api.promote_grades --url "sqlite:///$PWD/racing.db" --dry-run
  .venv/bin/python -m api.promote_grades --url "sqlite:///$PWD/racing.db"
  .venv/bin/python -m api.promote_grades --url "sqlite:///$PWD/racing.db" --key "2025Sep20,VIC,Caulfield"
"""

import os
import re
import time
import urllib.parse as up
from typing import Dict, List, Optional, Tuple

import requests
from sqlalchemy import create_engine, text

# -------- HTTP config --------
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; grade-promoter/1.0)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
}
TIMEOUT = 15

# -------- Patterns --------
RE_GROUP = re.compile(r"\bGROUP\s*([123])\b", re.IGNORECASE)
RE_LISTED = re.compile(r"\bLISTED\b", re.IGNORECASE)


def get_db_url() -> str:
    return os.getenv("DATABASE_URL") or f"sqlite:///{os.getcwd()}/racing.db"


def fetch(url: str, referer: Optional[str] = None) -> str:
    headers = dict(HEADERS)
    if referer:
        headers["Referer"] = referer
    r = requests.get(url, headers=headers, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text


def key_from_url(url: str) -> Optional[str]:
    """Extract the ?Key=... value from a Program URL."""
    q = up.urlparse(url).query
    params = up.parse_qs(q)
    vals = params.get("Key")
    if not vals:
        return None
    return vals[0]


def load_meeting_html_cache(keys: List[str]) -> Dict[str, str]:
    """
    Fetch each Program page once and cache HTML by meeting key.
    """
    base = "https://www.racingaustralia.horse/FreeFields/RaceProgram.aspx?Key="
    cache: Dict[str, str] = {}
    for k in keys:
        try:
            url = base + up.quote(k, safe="")
            html = fetch(url)
            cache[k] = html
            time.sleep(0.35)  # polite pause
        except Exception as e:
            print(f"[promote] WARN: failed to fetch {k}: {e}")
    return cache


def detect_grade_near(html: str, race_name: str, context: int = 1200) -> Optional[str]:
    """
    Find 'Group N' or 'Listed' within a context window around the race name.
    """
    if not race_name:
        return None
    pat = re.compile(re.escape(race_name), re.IGNORECASE)
    m = pat.search(html)
    if not m:
        return None
    start = max(0, m.start() - context)
    end = min(len(html), m.end() + context)
    window = html[start:end]

    mg = RE_GROUP.search(window)
    if mg:
        return f"Group {mg.group(1)}"
    if RE_LISTED.search(window):
        return "Listed"
    return None


def promote_grades(
    db_url: str,
    only_key: Optional[str] = None,
    dry_run: bool = False,
) -> Tuple[int, int]:
    """
    For rows with class NULL/Open, try to detect Group/Listed from the Program HTML.
    Returns: (scanned_rows, updated_rows)
    """
    eng = create_engine(db_url, future=True)

    # 1) Pull candidate rows
    with eng.begin() as conn:
        sql = """
            SELECT id, race_no, description, class, url
            FROM race_program
            WHERE (class IS NULL OR class = 'Open')
              AND url LIKE '%RaceProgram.aspx?Key=%'
        """
        params = {}
        if only_key:
            sql += " AND url LIKE :kpat"
            params["kpat"] = f"%{only_key}%"

        res = conn.execute(text(sql), params).fetchall()
        if not res:
            print("[promote] No candidates to scan.")
            return (0, 0)

        # Normalize rows to plain dicts (avoid attribute access to 'class' keyword)
        rows = [dict(r._mapping) for r in res]

        # Group rows by meeting key
        by_key: Dict[str, List[dict]] = {}
        for r in rows:
            key = key_from_url(r["url"])
            if not key:
                continue
            by_key.setdefault(key, []).append(r)

    keys = sorted(by_key.keys())
    total_rows = sum(len(v) for v in by_key.values())
    print(f"[promote] Scanning {total_rows} rows across {len(keys)} meeting(s).")

    # 2) Fetch each meeting page once
    html_cache = load_meeting_html_cache(keys if not only_key else [only_key])

    # 3) Detect grade near each race name and update
    updated = 0
    scanned = 0
    with create_engine(db_url, future=True).begin() as conn:
        for k in keys:
            html = html_cache.get(k)
            if not html:
                continue
            for r in by_key[k]:
                scanned += 1
                grade = detect_grade_near(html, r["description"])
                if not grade or r.get("class") == grade:
                    continue
                print(f"[promote] id={r['id']} R{r['race_no']} {r['description']} → {grade}")
                if not dry_run:
                    conn.execute(
                        text("UPDATE race_program SET class = :cls WHERE id = :id"),
                        {"cls": grade, "id": r["id"]},
                    )
                    updated += 1

    return (scanned, updated)


def main():
    import argparse

    ap = argparse.ArgumentParser(
        description="Promote Group/Listed grades into class by scanning Program HTML."
    )
    ap.add_argument(
        "--url",
        dest="db_url",
        default=get_db_url(),
        help="DATABASE_URL (default: env or ./racing.db)",
    )
    ap.add_argument(
        "--key",
        dest="only_key",
        default=None,
        help="Limit to a single meeting key (e.g. '2025Sep20,VIC,Caulfield')",
    )
    ap.add_argument("--dry-run", action="store_true", help="Do not write changes")
    args = ap.parse_args()

    print(f"[promote] Using DATABASE_URL: {args.db_url}")
    scanned, updated = promote_grades(args.db_url, only_key=args.only_key, dry_run=args.dry_run)
    print(f"[promote] Scanned: {scanned}, Updated: {updated}, Dry-run: {args.dry_run}")


if __name__ == "__main__":
    main()
