# api/backfill_meeting_ids.py
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from itertools import groupby
from typing import Dict, List, Optional, Tuple

import httpx
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore


PF_MEETINGS_URL = "https://api.puntingform.com.au/v2/form/meetingslist"


# Words we treat as “sponsor/noise” in track names
SPONSOR_WORDS = {
    "sportsbet",
    "ladbrokes",
    "bet365",
    "tab",
    "nbn",
    "picklebet",
    "racecourse",
    "raceclub",
    "raceway",
    "race",
    "rc",
    "park",       # Canterbury Park → canterbury
    "course",
    "track",
}


@dataclass
class RaMeetingKey:
    date_iso: str
    state: str
    track: str


# ----------------- Name helpers -----------------


def _iso_to_pf_date(value) -> str:
    """
    Convert 'YYYY-MM-DD' or date/datetime → 'D MMM YYYY'.
    E.g. 2025-11-20 -> '20 Nov 2025'
    """
    if isinstance(value, datetime):
        d = value.date()
    elif isinstance(value, date):
        d = value
    elif isinstance(value, str):
        s = value.strip()
        # race_program.date is normalized to 'YYYY-MM-DD'
        d = datetime.strptime(s, "%Y-%m-%d").date()
    else:
        raise TypeError(f"Unsupported date type: {type(value)!r}")
    return f"{d.day} {d.strftime('%b %Y')}"


def _canonical_track_name(name: Optional[str]) -> str:
    """
    Lowercase, strip sponsor words, remove punctuation, normalize whitespace.

    Examples:
      'Sportsbet-Ballarat'      -> 'ballarat'
      'bet365 Park Kyneton'     -> 'kyneton'
      'Ladbrokes Cannon Park'   -> 'cannon'
      'Canterbury Park'         -> 'canterbury'
    """
    if not name:
        return ""
    import re

    s = name.lower().strip()
    s = s.replace("’", "'")

    # split on non-alnum
    tokens = [t for t in re.split(r"[^a-z0-9]+", s) if t]
    filtered: List[str] = []
    for t in tokens:
        if t in SPONSOR_WORDS:
            continue
        filtered.append(t)
    return " ".join(filtered)


# ----------------- PF client -----------------


def _fetch_pf_meetings_for_date(
    meeting_date_iso: str,
    api_key: str,
    debug: bool = False,
):
    """Call PF /meetingslist for a single date."""
    pf_date = _iso_to_pf_date(meeting_date_iso)
    params = {
        "apiKey": api_key,
        "meetingDate": pf_date,
    }
    if debug:
        print(f"[meeting_ids] Fetching PF meetings for {meeting_date_iso} as '{pf_date}'")

    with httpx.Client(timeout=15) as client:
        resp = client.get(PF_MEETINGS_URL, params=params)
        resp.raise_for_status()
        data = resp.json()

    if data.get("statusCode") != 200:
        if debug:
            print(
                f"[meeting_ids] PF non-200 statusCode={data.get('statusCode')} "
                f"for {meeting_date_iso}"
            )
        return []

    payload = data.get("payLoad") or []
    if debug:
        names = [
            f"{m.get('track', {}).get('state')}:{m.get('track', {}).get('name')}"
            for m in payload
        ]
        print(
            f"[meeting_ids] PF {meeting_date_iso} returned "
            f"{len(payload)} meetings → {names}"
        )
    return payload


def _build_pf_maps(pf_payload) -> Tuple[
    Dict[Tuple[str, str], dict],
    Dict[Tuple[str, str], dict]
]:
    """
    Build two maps:
      1) exact_map: (STATE, lower(name)) → meeting
      2) canon_map: (STATE, canonical(name)) → meeting
    """
    exact_map: Dict[Tuple[str, str], dict] = {}
    canon_map: Dict[Tuple[str, str], dict] = {}

    for m in pf_payload:
        t = m.get("track") or {}
        state = (t.get("state") or "").strip().upper()
        name_raw = (t.get("name") or "").strip()
        if not state or not name_raw:
            continue

        exact_key = (state, name_raw.lower())
        canon_key = (state, _canonical_track_name(name_raw))

        exact_map[exact_key] = m
        if canon_key[1]:
            canon_map[canon_key] = m

    return exact_map, canon_map


def _match_pf_meeting(
    state: str,
    track: str,
    exact_map: Dict[Tuple[str, str], dict],
    canon_map: Dict[Tuple[str, str], dict],
    debug: bool = False,
    date_iso: Optional[str] = None,
) -> Optional[dict]:
    """
    Match RA (state, track) against PF meetings:

      1) Exact (STATE, lower(track)) match
      2) Exact (STATE, canonical(track)) match
      3) Same-state fuzzy: overlap of canonical tokens
    """
    state = state.strip().upper()
    raw = (track or "").strip()
    raw_l = raw.lower()
    canon = _canonical_track_name(raw)

    # 1) exact name
    key_exact = (state, raw_l)
    if key_exact in exact_map:
        if debug:
            print(f"[meeting_ids] exact match {date_iso} {state} '{track}'")
        return exact_map[key_exact]

    # 2) canonical
    key_canon = (state, canon)
    if key_canon in canon_map:
        if debug:
            print(f"[meeting_ids] canonical match {date_iso} {state} '{track}' → '{canon}'")
        return canon_map[key_canon]

    # 3) fuzzy within same state
    tokens = set(canon.split())
    if not tokens:
        return None

    best: Optional[Tuple[int, dict]] = None
    for (st, c_name), mtg in canon_map.items():
        if st != state:
            continue
        c_tokens = set(c_name.split())
        score = len(tokens & c_tokens)
        if score <= 0:
            continue
        if best is None or score > best[0]:
            best = (score, mtg)

    if best is not None and best[0] > 0:
        if debug:
            t = best[1].get("track", {})
            print(
                f"[meeting_ids] fuzzy match {date_iso} {state} '{track}' "
                f"→ PF '{t.get('name')}' (score={best[0]})"
            )
        return best[1]

    if debug:
        print(
            f"[meeting_ids] WARN: no PF match for {date_iso} {state} "
            f"'{track}' (canon='{canon}')"
        )
    return None


# ----------------- main backfill -----------------


def backfill(
    db_url: Optional[str] = None,
    limit: Optional[int] = None,
    dry_run: bool = False,
    debug: bool = False,
) -> Tuple[int, int]:
    """
    Backfill meeting_id in race_program from PF meetingslist.

    Defaults to date range: today (Melbourne) → today + 3 days.

    Returns (meetings_updated, rows_updated).
    """
    api_key = os.getenv("PF_API_KEY")
    if not api_key:
        raise SystemExit("PF_API_KEY environment variable is required")

    if db_url is None:
        db_url = os.getenv("DATABASE_URL") or "sqlite:///./data/racing.db"

    eng: Engine = create_engine(db_url, future=True)

    today_melb = datetime.now(ZoneInfo("Australia/Melbourne")).date()
    start_date = today_melb
    end_date = today_melb + timedelta(days=3)

    sel = text(
        """
        SELECT date, state, track
        FROM race_program
        WHERE (meeting_id IS NULL OR TRIM(COALESCE(meeting_id, '')) = '')
          AND date >= :start_date
          AND date <= :end_date
        GROUP BY date, state, track
        ORDER BY date, state, track
        """
    )

    meetings: List[RaMeetingKey] = []
    with eng.connect() as conn:
        rows = conn.execute(
            sel,
            {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            },
        ).fetchall()
        for r in rows:
            meetings.append(
                RaMeetingKey(
                    date_iso=str(r[0]),
                    state=str(r[1]).strip().upper(),
                    track=str(r[2]),
                )
            )

    if limit is not None and len(meetings) > limit:
        meetings = meetings[:limit]

    if not meetings:
        if debug:
            print("[meeting_ids] No candidate meetings found.")
        return (0, 0)

    unique_dates = sorted({m.date_iso for m in meetings})
    if debug:
        print(
            f"[meeting_ids] candidate meetings: {len(meetings)} "
            f"across {len(unique_dates)} date(s): {unique_dates}"
        )

    meetings_updated = 0
    rows_updated = 0

    with eng.begin() as conn:
        for date_iso, group in groupby(meetings, key=lambda m: m.date_iso):
            group_list = list(group)
            if debug:
                print(f"[meeting_ids] Processing date={date_iso} meetings={len(group_list)}")

            pf_payload = _fetch_pf_meetings_for_date(date_iso, api_key, debug=debug)
            if not pf_payload:
                if debug:
                    print(f"[meeting_ids] No PF meetings returned for {date_iso}, skipping.")
                continue

            exact_map, canon_map = _build_pf_maps(pf_payload)

            for m in group_list:
                pf_mtg = _match_pf_meeting(
                    m.state,
                    m.track,
                    exact_map,
                    canon_map,
                    debug=debug,
                    date_iso=date_iso,
                )
                if not pf_mtg:
                    continue

                meeting_id = (pf_mtg.get("meetingId") or "").strip()
                if not meeting_id:
                    if debug:
                        print(
                            f"[meeting_ids] WARN: PF meeting missing meetingId "
                            f"for {date_iso} {m.state} '{m.track}'"
                        )
                    continue

                if debug:
                    t = pf_mtg.get("track") or {}
                    print(
                        f"[meeting_ids] MATCH {date_iso} {m.state} '{m.track}' "
                        f"← PF '{t.get('name')}' meetingId={meeting_id}"
                    )

                if dry_run:
                    continue

                upd = text(
                    """
                    UPDATE race_program
                    SET meeting_id = :meeting_id
                    WHERE date = :date
                      AND state = :state
                      AND track = :track
                    """
                )
                res = conn.execute(
                    upd,
                    {
                        "meeting_id": meeting_id,
                        "date": m.date_iso,
                        "state": m.state,
                        "track": m.track,
                    },
                )
                if res.rowcount and res.rowcount > 0:
                    rows_updated += res.rowcount
                    meetings_updated += 1

    if debug:
        print(
            f"[meeting_ids] DONE meetings_updated={meetings_updated}, "
            f"rows_updated={rows_updated}"
        )
    return meetings_updated, rows_updated


# ----------------- CLI -----------------


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Backfill meeting_id from Punting Form meetingslist."
    )
    parser.add_argument(
        "--url",
        dest="url",
        default=None,
        help="Database URL (defaults to env DATABASE_URL or sqlite)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of RA meetings to process",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write changes to DB",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Verbose logging",
    )
    args = parser.parse_args()

    meetings_updated, rows_updated = backfill(
        db_url=args.url,
        limit=args.limit,
        dry_run=args.dry_run,
        debug=args.debug,
    )
    print(f"[meeting_ids] meetings_updated={meetings_updated}, rows_updated={rows_updated}")


if __name__ == "__main__":
    main()
