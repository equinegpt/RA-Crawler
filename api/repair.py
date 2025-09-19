# api/repair.py
from __future__ import annotations

import argparse
import os
import re
import sys
import html
from typing import Dict, List, Optional, Set, Tuple, Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Optional imports from your codebase; we fall back gracefully if not present
try:
    from .class_backfill import infer_class_from_text  # type: ignore
except Exception:
    # Fallback implementation (keep this consistent with your project logic)
    RE_RATINGS_BAND = re.compile(
        r"\b(?:RATINGS?\s*(?:BAND)?|RB)\b[^0-9]*?(\d{1,3})\s*[-–]\s*(\d{1,3})",
        re.IGNORECASE,
    )
    RE_BENCHMARK = re.compile(r"\b(?:BENCHMARK|BM)\s*(\d{2,3})\b", re.IGNORECASE)
    RE_CLASS_N = re.compile(r"\b(?:CLASS|CL)\s*(\d{1,3})\b", re.IGNORECASE)
    RE_MAIDEN = re.compile(r"\bMAIDEN\b", re.IGNORECASE)

    def _normalize_range(a: str, b: str) -> str:
        try:
            lo = int(a)
            hi = int(b)
        except Exception:
            return f"{a}-{b}".replace(" ", "")
        if lo > hi:
            lo, hi = hi, lo
        return f"{lo}-{hi}"

    def infer_class_from_text(description: Optional[str], _current: Optional[str]) -> Optional[str]:
        desc = (description or "").strip()
        if not desc:
            return None
        m = RE_RATINGS_BAND.search(desc)
        if m:
            return _normalize_range(m.group(1), m.group(2))
        m = RE_BENCHMARK.search(desc)
        if m:
            return f"BM{m.group(1)}"
        m = RE_CLASS_N.search(desc)
        if m:
            return f"CL{m.group(1)}"
        if RE_MAIDEN.search(desc):
            return "Maiden"
        return None


# ------------------------------------------------------------------------------
# Engine / DB helpers
# ------------------------------------------------------------------------------

def _resolve_engine(url_arg: Optional[str]) -> Engine:
    if url_arg:
        return create_engine(url_arg, future=True)

    env_url = os.getenv("DATABASE_URL")
    if env_url:
        return create_engine(env_url, future=True)

    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    default_sqlite = f"sqlite:///{os.path.join(repo_root, 'racing.db')}"
    return create_engine(default_sqlite, future=True)


def _has_table(conn, name: str) -> bool:
    row = conn.execute(
        text("SELECT name FROM sqlite_master WHERE type='table' AND name=:n"),
        {"n": name},
    ).fetchone()
    return bool(row)


def _list_tables(conn):
    rows = conn.execute(
        text("SELECT name FROM sqlite_master WHERE type='table' ORDER BY 1")
    ).fetchall()
    return [r[0] for r in rows]


def _extract_key_from_url(url: str) -> Optional[str]:
    # URL example: ... RaceProgram.aspx?Key=2025Sep19,QLD,Townsville
    m = re.search(r"[?&]Key=([^&]+)", url)
    return m.group(1) if m else None


# ------------------------------------------------------------------------------
# Strict class recalculation (each row from its own description only)
# ------------------------------------------------------------------------------

def recalc_classes(
    engine: Engine,
    state: Optional[str] = None,
    key_filter: Optional[str] = None,
    dry_run: bool = False,
) -> int:
    where = ["1=1"]
    params: Dict[str, object] = {}
    if state:
        where.append("state = :st")
        params["st"] = state
    if key_filter:
        where.append("url LIKE :uk")
        params["uk"] = f"%Key={key_filter}%"

    sel_sql = f"""
        SELECT id, description, class
        FROM race_program
        WHERE {' AND '.join(where)}
        ORDER BY id
    """

    updated = 0
    with engine.begin() as conn:
        if not _has_table(conn, "race_program"):
            print("[repair] ERROR: table 'race_program' not found.")
            return 0

        rows = conn.execute(text(sel_sql), params).fetchall()
        for rid, desc, cur_cls in rows:
            want = infer_class_from_text(desc, None)  # ignore current; derive from description only
            cur = (cur_cls or "").strip() or None
            if want != cur:
                updated += 1
                if dry_run:
                    print(f"[repair] would update id={rid}: class {cur!r} -> {want!r}")
                else:
                    conn.execute(
                        text("UPDATE race_program SET class=:c WHERE id=:i"),
                        {"c": want, "i": rid},
                    )
    print(f"[repair] recalc_classes updated: {updated}")
    return updated


# ------------------------------------------------------------------------------
# Resync a meeting by key
#   - harvest fresh rows (robust to different ra_harvest return types)
#   - upsert them
#   - optionally delete rows for that key that are no longer present
# ------------------------------------------------------------------------------

_MONTHS = {
    "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
    "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
    "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
}

def _date_from_key(key: str) -> Optional[str]:
    # 2025Sep19,STATE,Track
    m = re.match(r"^(\d{4})([A-Za-z]{3})(\d{2})", key)
    if not m:
        return None
    y, mon_str, d = m.group(1), m.group(2).title(), m.group(3)
    mm = _MONTHS.get(mon_str)
    if not mm:
        return None
    return f"{y}-{mm}-{d}"

def _state_from_key(key: str) -> Optional[str]:
    parts = key.split(",")
    if len(parts) >= 2:
        return parts[1]
    return None

def _track_from_key(key: str) -> Optional[str]:
    parts = key.split(",")
    if len(parts) >= 3:
        # Keys you pass us already look unescaped (e.g. "Aquis Park Gold Coast")
        # but if they were URL-encoded, unescape them:
        return html.unescape(parts[2]).replace("%20", " ")
    return None

def _unique_row_key(row: dict) -> Tuple[str, str, str, int]:
    return (
        str(row.get("date") or ""),
        str(row.get("state") or ""),
        str(row.get("track") or ""),
        int(row.get("race_no") or 0),
    )

def _fetch_db_rows_for_key(conn, meeting_key: str) -> List[dict]:
    rows = conn.execute(
        text(
            """
        SELECT id, race_no, date, state, track, description, prize, condition, class, age, sex, distance_m, bonus, url
        FROM race_program
        WHERE url LIKE :uk
        ORDER BY race_no
        """
        ),
        {"uk": f"%Key={meeting_key}%"},
    ).mappings().all()
    return [dict(r) for r in rows]

def _upsert_row(conn, row: dict) -> int:
    sel = conn.execute(
        text(
            """
        SELECT id FROM race_program
        WHERE date=:d AND state=:s AND track=:t AND race_no=:n
        """
        ),
        {
            "d": row.get("date"),
            "s": row.get("state"),
            "t": row.get("track"),
            "n": row.get("race_no"),
        },
    ).fetchone()

    if sel:
        rid = int(sel[0])
        conn.execute(
            text(
                """
            UPDATE race_program
            SET description=:description,
                prize=:prize,
                condition=:condition,
                class=:class,
                age=:age,
                sex=:sex,
                distance_m=:distance_m,
                bonus=:bonus,
                url=:url
            WHERE id=:id
            """
            ),
            {**row, "id": rid},
        )
        return rid
    else:
        res = conn.execute(
            text(
                """
            INSERT INTO race_program
            (race_no, date, state, track, description, prize, condition, class, age, sex, distance_m, bonus, url)
            VALUES
            (:race_no, :date, :state, :track, :description, :prize, :condition, :class, :age, :sex, :distance_m, :bonus, :url)
            """
            ),
            row,
        )
        return int(res.lastrowid)

def _harvest_rows_direct(meeting_key: str) -> List[Dict[str, Any]]:
    """
    Fallback: fetch + parse + normalize rows without depending on ra_harvest's return type.
    """
    import requests
    try:
        from .program_parser import parse_program  # type: ignore
    except Exception:
        print("[repair] ERROR: cannot import api.program_parser.parse_program", file=sys.stderr)
        return []

    base = "https://www.racingaustralia.horse/FreeFields/RaceProgram.aspx?Key="
    url = base + meeting_key
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
    resp.raise_for_status()
    html_text = resp.text

    rows = parse_program(html_text, url)  # expect a list of dict-like rows
    # Normalize the essentials:
    d = _date_from_key(meeting_key)
    st = _state_from_key(meeting_key)
    tr = _track_from_key(meeting_key)
    out: List[Dict[str, Any]] = []
    for r in rows:
        rd = dict(r)
        rd.setdefault("race_no", r.get("race_no"))
        rd["date"] = d
        rd["state"] = st
        rd["track"] = tr
        rd["url"] = url
        # class: STRICT from description only
        rd["class"] = infer_class_from_text(rd.get("description"), None)
        out.append(rd)
    return out

def _get_fresh_rows_for_key(meeting_key: str, force: bool) -> List[Dict[str, Any]]:
    """
    Try to use ra_harvest.harvest_program_from_key; if it returns something
    unexpected (e.g., counts), fall back to direct fetch+parse.
    """
    try:
        from .ra_harvest import harvest_program_from_key  # type: ignore
    except Exception:
        # No ra_harvest or signature mismatch — fallback
        return _harvest_rows_direct(meeting_key)

    try:
        fresh = harvest_program_from_key(meeting_key, force=force, debug=False)
    except TypeError:
        # Signature mismatch — fallback
        return _harvest_rows_direct(meeting_key)

    # If harvester returned a list of dicts -> perfect
    if isinstance(fresh, list) and (not fresh or isinstance(fresh[0], dict)):
        return fresh  # we'll still re-derive class below

    # If it returned a tuple (e.g., (saved, updated)) or anything else, fallback.
    return _harvest_rows_direct(meeting_key)

def resync_meeting_by_key(
    engine: Engine,
    meeting_key: str,
    force: bool = False,
    purge_missing: bool = True,
    dry_run: bool = False,
) -> Dict[str, int]:
    """
    Harvest a meeting by key -> upsert into race_program -> optionally delete
    rows for that key that are not present anymore.
    """
    fresh_rows = _get_fresh_rows_for_key(meeting_key, force=force)

    # Normalize/override class STRICTLY from description for safety:
    cleaned: List[dict] = []
    for r in fresh_rows:
        rr = dict(r)
        rr["class"] = infer_class_from_text(rr.get("description"), None)
        # Make sure required identity fields exist (date/state/track/race_no/url)
        rr.setdefault("date", _date_from_key(meeting_key))
        rr.setdefault("state", _state_from_key(meeting_key))
        rr.setdefault("track", _track_from_key(meeting_key))
        rr.setdefault("url", f"https://www.racingaustralia.horse/FreeFields/RaceProgram.aspx?Key={meeting_key}")
        if rr.get("race_no") is None:
            # If parser didn't set race_no, try to parse from description prefix like "Race 7" (defensive)
            m = re.search(r"\b[Rr]ace\s+(\d{1,2})\b", rr.get("description") or "")
            if m:
                rr["race_no"] = int(m.group(1))
        cleaned.append(rr)

    keep_keys: Set[Tuple[str, str, str, int]] = set(
        _unique_row_key(r) for r in cleaned if r.get("race_no") is not None
    )

    changed = 0
    deleted = 0

    with engine.begin() as conn:
        if not _has_table(conn, "race_program"):
            print("[repair] ERROR: table 'race_program' not found.")
            return {"changed": 0, "deleted": 0, "fresh": 0}

        existing = _fetch_db_rows_for_key(conn, meeting_key)

        # upsert all fresh rows
        for row in cleaned:
            if row.get("race_no") is None:
                continue
            if dry_run:
                print(
                    f"[repair] would upsert race_no={row.get('race_no')} "
                    f"{row.get('state')}/{row.get('track')} {row.get('date')}"
                )
            else:
                _upsert_row(conn, row)
            changed += 1

        if purge_missing and existing:
            for old in existing:
                k = _unique_row_key(old)
                if k not in keep_keys:
                    deleted += 1
                    if dry_run:
                        print(
                            f"[repair] would DELETE id={old['id']} race_no={old['race_no']} (missing from fresh)"
                        )
                    else:
                        conn.execute(text("DELETE FROM race_program WHERE id=:i"), {"i": old["id"]})

    return {"changed": changed, "deleted": deleted, "fresh": len(cleaned)}


# ------------------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------------------

def main() -> None:
    p = argparse.ArgumentParser(description="Repair tools for race_program data.")
    p.add_argument("--url", help="SQLAlchemy DB URL (overrides env/DATABASE_URL).")
    p.add_argument("--dry-run", action="store_true", help="Print actions, don’t modify DB.")

    # Actions:
    p.add_argument(
        "--recalc-classes",
        action="store_true",
        help="Recalculate class for rows strictly from description.",
    )
    p.add_argument("--state", help="Optional filter for --recalc-classes (e.g., QLD).")
    p.add_argument(
        "--key",
        help="Optional filter for --recalc-classes: meeting key like 2025Sep19,QLD,Townsville",
    )

    p.add_argument(
        "--resync-key",
        help="Harvest a meeting by Key and purge rows not present in fresh data.",
    )
    p.add_argument("--force", action="store_true", help="Force fetch on resync.")
    p.add_argument("--no-purge", action="store_true", help="Do not delete missing rows on resync.")

    args = p.parse_args()
    eng = _resolve_engine(args.url)

    print(f"[repair] Using DATABASE_URL: {eng.url}")
    with eng.connect() as conn:
        print(f"[repair] Found tables: { _list_tables(conn) }")

    did_something = False

    if args.recalc_classes:
        did_something = True
        recalc_classes(eng, state=args.state, key_filter=args.key, dry_run=args.dry_run)

    if args.resync_key:
        did_something = True
        stats = resync_meeting_by_key(
            eng,
            meeting_key=args.resync_key,
            force=args.force,
            purge_missing=not args.no_purge,
            dry_run=args.dry_run,
        )
        print(f"[repair] resync {args.resync_key}: {stats}")

    if not did_something:
        print("[repair] No action. Use --recalc-classes and/or --resync-key.")


if __name__ == "__main__":
    main()
