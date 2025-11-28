from __future__ import annotations

import argparse
import os
import sys
import re
from datetime import date, datetime, timedelta
from typing import Dict, List, Tuple, Optional

import httpx
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from zoneinfo import ZoneInfo

# Punting Form meetingslist endpoint
PF_MEETINGS_URL = os.getenv(
    "PF_MEETINGS_URL",
    "https://api.puntingform.com.au/v2/form/meetingslist",
)
PF_API_KEY_ENV = "PF_API_KEY"

# Which states we care about for mapping RA → PF
AUS_STATES = {"NSW", "VIC", "QLD", "SA", "WA", "TAS", "NT", "ACT"}


# --------- Helpers: DB + dates ----------


def _normalise_track_name(raw: str) -> str:
    """
    Normalise track names so PF + RA strings match better.

    Examples:
      "Mt Gambier"      → "mount gambier"
      "Mount Gambier"   → "mount gambier"
      " MOUNT  GAMBIER" → "mount gambier"
    """
    if not raw:
        return ""

    s = raw.strip().lower()
    # Collapse multiple spaces
    s = re.sub(r"\s+", " ", s)

    # Normalise common abbreviations / variants at the *start* of the name.
    # You can extend this over time as you find more mismatches.
    replacements = [
        ("mt ", "mount "),
        ("mt. ", "mount "),
        ("st ", "saint "),
        ("st. ", "saint "),
    ]

    for short, full in replacements:
        if s.startswith(short):
            s = full + s[len(short):]
            break

    return s


def _get_engine(url: Optional[str]) -> Engine:
    db_url = url or os.getenv("DATABASE_URL")
    if not db_url:
        raise SystemExit("[meeting_ids] DATABASE_URL not set and no --url given")
    return create_engine(db_url, future=True)


def _as_date(d) -> date:
    """
    Normalise DB 'date' field into a datetime.date.
    Handles DATE, DATETIME, and ISO strings "YYYY-MM-DD".
    """
    if isinstance(d, date) and not isinstance(d, datetime):
        return d
    if isinstance(d, datetime):
        return d.date()
    s = str(d).split("T", 1)[0]
    return date.fromisoformat(s)


def _today_melb() -> date:
    return datetime.now(ZoneInfo("Australia/Melbourne")).date()


# --------- Track-name canonicalisation ----------


def canonical_track_name(raw: str) -> str:
    """
    Aggressively strip sponsors + fluff so RA and PF
    track names converge to the same canonical token.

    This sits *on top of* _normalise_track_name, so things like
    "Mt Gambier" vs "Mount Gambier" are already aligned before we
    strip sponsors / fluff.

    Examples:
      "Canterbury Park"                → "canterbury"
      "Canterbury"                     → "canterbury"
      "bet365 Park Kyneton"            → "kyneton"
      "Kyneton"                        → "kyneton"
      "Thomas Farms RC Murray Bridge"  → "murray bridge"
      "Murray Bridge GH"               → "murray bridge"
      "Sportsbet-Ballarat"             → "ballarat"
      "Sportsbet Port Lincoln"         → "port lincoln"
      "Aquis Park Gold Coast"          → "gold coast"
      "Aquis Park Gold Coast Poly"     → "gold coast poly"
      "Ladbrokes Geelong"              → "geelong"
      "Southside Pakenham"             → "pakenham"
    """
    if not raw:
        return ""

    # First pass: handle Mt/Mount, St/Saint, spacing, lowercase
    s = _normalise_track_name(raw)
    if not s:
        return ""

    # Normalise separators
    s = re.sub(r"[-,/]", " ", s)

    # Strip "sponsor-ish" words (both RA & PF sides)
    sponsors = [
        "sportsbet",
        "ladbrokes",
        "bet365",
        "picklebet",
        "thomas farms",
        "aquis park",
        "aquis",
        "tabtouch",
        "tab ",
    ]
    for sp in sponsors:
        s = s.replace(sp, "")

    # Generic fluff words
    junk_words = [
        "rc",
        "racecourse",
        "raceway",
        "race club",
        "race club inc",
        "race club incorporated",
        "park",
        "gh",
    ]
    for jw in junk_words:
        # Middle of string
        s = s.replace(f" {jw} ", " ")
        # End of string
        if s.endswith(f" {jw}"):
            s = s[: -len(f" {jw}")]
        # Start of string
        if s.startswith(f"{jw} "):
            s = s[len(f"{jw} ") :]

    # Collapse whitespace
    s = " ".join(s.split())

    # 🔁 Special-case Southside variants so RA + PF align
    # RA: "Southside Pakenham" / "Southside Cranbourne"
    # PF: "Pakenham" / "Cranbourne"
    if s.startswith("southside cranbourne"):
        return "cranbourne"
    if s.startswith("southside pakenham"):
        return "pakenham"

    return s


# --------- PF API fetch ----------


def _format_pf_meeting_date(d: date) -> str:
    """
    Punting Form expects e.g. '19 Nov 2025' (no leading zero on the day).
    """
    # Avoid %-d portability issues: strip leading zero manually.
    day = str(d.day)
    return f"{day} {d.strftime('%b %Y')}"


def _fetch_pf_meetings_for_date(d: date, debug: bool = False) -> Dict[Tuple[str, str], str]:
    api_key = os.getenv(PF_API_KEY_ENV)
    if not api_key:
        raise SystemExit(f"[meeting_ids] {PF_API_KEY_ENV} env var is required")

    params = {
        "apiKey": api_key,
        "meetingDate": _format_pf_meeting_date(d),
    }

    if debug:
        print(f"[meeting_ids] Fetching PF meetings for {d} as '{params['meetingDate']}'")

    with httpx.Client(timeout=20.0) as client:
        resp = client.get(PF_MEETINGS_URL, params=params)
        resp.raise_for_status()
        data = resp.json()

    payload = data.get("payLoad") or []

    pf_map: Dict[Tuple[str, str], str] = {}
    for m in payload:
        track = m.get("track") or {}
        state = (track.get("state") or "").upper()
        if state not in AUS_STATES:
            continue
        name = track.get("name") or ""
        meeting_id = m.get("meetingId")
        if not meeting_id:
            continue

        key = (state, canonical_track_name(name))
        pf_map[key] = meeting_id

    if debug:
        labels = [f"{s}:{n}" for (s, n) in pf_map.keys()]
        print(f"[meeting_ids] PF {d.isoformat()} returned {len(pf_map)} meetings → {labels}")

    return pf_map


# --------- Core backfill ----------


def backfill(
    url: Optional[str] = None,
    dry_run: bool = False,
    limit: Optional[int] = None,
    debug: bool = False,
    past_days: int = 2,
    future_days: int = 3,
) -> Tuple[int, int]:
    """
    Backfill meeting_id on race_program using PF meetingslist.

    - Only touches rows where meeting_id IS NULL / blank.
    - Restricts to [today - past_days, today + future_days] in AU/Melbourne.
    - Matching is by (state, canonical_track_name(track)).

    Returns: (meetings_updated, rows_updated)
    """
    eng = _get_engine(url)

    # 1) Find candidate (date,state,track) combos with missing meeting_id.
    with eng.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT date, state, track
                FROM race_program
                WHERE meeting_id IS NULL OR TRIM(COALESCE(meeting_id, '')) = ''
                GROUP BY date, state, track
                ORDER BY date, state, track
                """
            )
        ).fetchall()

    today = _today_melb()
    min_d = today - timedelta(days=past_days)
    max_d = today + timedelta(days=future_days)

    candidates: List[Tuple[date, str, str]] = []
    for d_raw, state, track in rows:
        d = _as_date(d_raw)
        # Only care about a window around "now" so old historical rows
        # don't keep bothering PF forever.
        if not (min_d <= d <= max_d):
            continue
        candidates.append((d, (state or "").upper(), track or ""))

    if not candidates:
        if debug:
            print("[meeting_ids] No candidate meetings in the configured window")
        return (0, 0)

    # Optional global limit (distinct (date,state,track) tuples)
    if limit is not None and len(candidates) > limit:
        candidates = candidates[:limit]

    # Group by date so we call PF once per day.
    by_date: Dict[date, List[Tuple[str, str]]] = {}
    for d, state, track in candidates:
        by_date.setdefault(d, []).append((state, track))

    all_dates = sorted(by_date.keys())
    if debug:
        print(
            f"[meeting_ids] candidate meetings: {len(candidates)} across {len(all_dates)} "
            f"date(s): {[d.isoformat() for d in all_dates]}"
        )

    meetings_updated = 0
    rows_updated = 0

    with eng.begin() as conn:
        for d in all_dates:
            day_meetings = by_date[d]
            if debug:
                print(f"[meeting_ids] Processing date={d} meetings={len(day_meetings)}")

            pf_map = _fetch_pf_meetings_for_date(d, debug=debug)
            if not pf_map:
                if debug:
                    print(f"[meeting_ids] No PF meetings map built for {d}, skipping.")
                continue

            for state, track in day_meetings:
                canon = canonical_track_name(track)
                key = (state, canon)
                meeting_id = pf_map.get(key)

                if not meeting_id:
                    if debug:
                        print(
                            f"[meeting_ids] WARN: no PF match for {d} {state} "
                            f"'{track}' (canon='{canon}')"
                        )
                    continue

                if dry_run:
                    # Count rows that *would* be updated
                    res = conn.execute(
                        text(
                            """
                            SELECT COUNT(*) FROM race_program
                            WHERE date = :d AND state = :s AND track = :t
                            """
                        ),
                        {"d": d, "s": state, "t": track},
                    )
                    count = int(res.scalar() or 0)
                else:
                    res = conn.execute(
                        text(
                            """
                            UPDATE race_program
                            SET meeting_id = :mid
                            WHERE date = :d AND state = :s AND track = :t
                            """
                        ),
                        {"mid": meeting_id, "d": d, "s": state, "t": track},
                    )
                    count = res.rowcount or 0

                if count > 0:
                    meetings_updated += 1
                    rows_updated += count
                    if debug:
                        print(
                            f"[meeting_ids] Set meeting_id={meeting_id} "
                            f"for {count} row(s) on {d} {state} '{track}'"
                        )

    if debug:
        print(
            f"[meeting_ids] DONE meetings_updated={meetings_updated}, "
            f"rows_updated={rows_updated}"
        )
    return meetings_updated, rows_updated


# --------- CLI entrypoint ----------


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill meeting_id using Punting Form meetingslist")
    ap.add_argument(
        "--url",
        dest="url",
        help="DB URL; if omitted, uses DATABASE_URL env var",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write to DB; just report counts",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of (date,state,track) combos to process",
    )
    ap.add_argument(
        "--debug",
        action="store_true",
        help="Verbose logging",
    )
    ap.add_argument(
        "--past-days",
        type=int,
        default=2,
        help="How many days *before* today (Melbourne) to consider",
    )
    ap.add_argument(
        "--future-days",
        type=int,
        default=3,
        help="How many days *after* today (Melbourne) to consider",
    )

    ns = ap.parse_args()

    meetings_updated, rows_updated = backfill(
        url=ns.url,
        dry_run=ns.dry_run,
        limit=ns.limit,
        debug=ns.debug,
        past_days=ns.past_days,
        future_days=ns.future_days,
    )

    print(
        f"[meeting_ids] meetings_updated={meetings_updated}, "
        f"rows_updated={rows_updated}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
