# api/backfill_meeting_ids.py
from __future__ import annotations

import argparse
import os
import sys
import re
from datetime import datetime
from typing import Dict, List, Tuple, Optional

import requests
from sqlalchemy import create_engine, text

PF_URL = "https://api.puntingform.com.au/v2/form/meetingslist"
PF_API_KEY_ENV = "PF_API_KEY"
REQ_TIMEOUT = 20


# ---------- helpers ----------

def _iso_to_pf_date(iso_date: str) -> str:
    """
    '2025-11-19' -> '19 Nov 2025'
    PF says 'D MMM YYYY'; it happily accepts a leading zero, but we strip it anyway.
    """
    dt = datetime.strptime(iso_date, "%Y-%m-%d")
    s = dt.strftime("%d %b %Y")  # '19 Nov 2025' or '09 Nov 2025'
    if s[0] == "0":
        s = s[1:]
    return s


def _fetch_pf_meetings_for_date(iso_date: str, api_key: str) -> List[dict]:
    """
    Call PF /meetingslist for one date. Returns payLoad list or [] on any non-200 / weird status.
    """
    meeting_date_str = _iso_to_pf_date(iso_date)
    params = {
        "apiKey": api_key,
        "meetingDate": meeting_date_str,
    }
    try:
        r = requests.get(PF_URL, params=params, timeout=REQ_TIMEOUT)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"[meeting_ids] ERROR fetching PF meetings for {iso_date}: {e}", file=sys.stderr)
        return []

    if data.get("statusCode") != 200 or data.get("status") not in (0, "0", None):
        print(f"[meeting_ids] PF non-OK status for {iso_date}: {data.get('statusCode')} / {data.get('status')}",
              file=sys.stderr)
        return []

    payload = data.get("payLoad") or []
    if not isinstance(payload, list):
        return []

    return payload


# Normalise track names so RA + PF line up despite sponsors / branding.
_SPONSOR_WORDS = {
    "sportsbet",
    "ladbrokes",
    "bet365",
    "picklebet",
    "aquis",
    "thomas",
    "farms",
    "tab",
    "southside",
}

_GENERIC_WORDS = {
    "racecourse",
    "raceclub",
    "race",
    "club",
    "rc",
    "park",
    "poly",
    "picnic",
}


def _normalize_track_name(name: str) -> str:
    """
    Aggressive but safe-ish normaliser:

    - lowercases
    - strips punctuation to spaces
    - drops sponsor words ('sportsbet', 'ladbrokes', 'aquis', 'bet365', 'picklebet', 'southside', 'tab', etc.)
    - drops generic noise ('park', 'rc', 'racecourse', 'poly', 'picnic', etc.)
    - collapses whitespace
    """
    if not name:
        return ""

    s = name.lower()

    # punctuation → space
    s = re.sub(r"[^\w\s]", " ", s)

    tokens = []
    for tok in s.split():
        if tok in _SPONSOR_WORDS:
            continue
        if tok in _GENERIC_WORDS:
            continue
        tokens.append(tok)

    if not tokens:
        # fall back to basic cleaned string so we don't end up with empty key
        s = re.sub(r"\s+", " ", s).strip()
        return s

    return " ".join(tokens)


def _build_pf_meeting_map(pf_meetings: List[dict]) -> Dict[Tuple[str, str], str]:
    """
    Build {(STATE, normalized_track_name) -> meetingId} map from PF payload.
    """
    out: Dict[Tuple[str, str], str] = {}
    for m in pf_meetings:
        track = m.get("track") or {}
        name = track.get("name") or ""
        state = (track.get("state") or "").upper()
        meeting_id = m.get("meetingId")
        if not name or not state or not meeting_id:
            continue
        key = (state, _normalize_track_name(name))
        out.setdefault(key, str(meeting_id))
    return out


# ---------- main backfill ----------

def backfill(
    db_url: Optional[str] = None,
    dry_run: bool = False,
    limit: Optional[int] = None,
) -> Tuple[int, int]:
    """
    Backfill race_program.meeting_id using PF /meetingslist.

    Returns (meetings_updated, rows_updated).
    Works with both SQLite and Postgres — no PRAGMA usage.
    """
    api_key = os.getenv(PF_API_KEY_ENV)
    if not api_key:
        raise RuntimeError(f"{PF_API_KEY_ENV} env var is required for backfill_meeting_ids")

    if not db_url:
        db_url = os.getenv("DATABASE_URL", "sqlite:///./data/racing.db")

    eng = create_engine(db_url, future=True)

    # 1) find distinct (date, state, track) with empty / NULL meeting_id
    sel = text("""
        SELECT date, state, track
        FROM race_program
        WHERE meeting_id IS NULL OR TRIM(COALESCE(meeting_id, '')) = ''
        GROUP BY date, state, track
        ORDER BY date, state, track
    """)

    with eng.connect() as conn:
        rows = conn.execute(sel).fetchall()

    if limit is not None and limit > 0:
        rows = rows[:limit]

    # group by date so we only hit PF once per day
    date_groups: Dict[str, List[Tuple[str, str]]] = {}
    for d, s, t in rows:
        if not d or not s or not t:
            continue
        date_groups.setdefault(d, []).append((s, t))

    print(f"[meeting_ids] candidate meetings: {len(rows)} across {len(date_groups)} date(s)")

    meetings_updated = 0
    rows_updated = 0

    for i, (d, items) in enumerate(sorted(date_groups.items()), start=1):
        print(f"[meeting_ids] ({i}/{len(date_groups)}) date={d} meetings={len(items)}")

        pf_meetings = _fetch_pf_meetings_for_date(d, api_key)
        if not pf_meetings:
            print(f"[meeting_ids] No PF meetings map built for {d}, skipping.")
            continue

        pf_map = _build_pf_meeting_map(pf_meetings)

        if not pf_map:
            print(f"[meeting_ids] Empty PF map for {d}, skipping.")
            continue

        # apply updates
        with eng.begin() as conn:
            for state, track in items:
                key = (state.upper(), _normalize_track_name(track))
                mid = pf_map.get(key)
                if not mid:
                    # no confident match; leave NULL
                    continue

                if dry_run:
                    # count but don't actually write
                    meetings_updated += 1
                    # assume ~8 races/meeting; we won't know exact rowcount without hitting DB
                    continue

                res = conn.execute(
                    text("""
                        UPDATE race_program
                        SET meeting_id = :mid
                        WHERE date = :d AND state = :s AND track = :t
                    """),
                    {"mid": mid, "d": d, "s": state, "t": track},
                )
                if res.rowcount and res.rowcount > 0:
                    meetings_updated += 1
                    rows_updated += res.rowcount

        print(f"[meeting_ids] date={d} → meetings_updated so far={meetings_updated}, rows={rows_updated}")

    print(f"[meeting_ids] DONE meetings_updated={meetings_updated}, rows_updated={rows_updated}")
    return meetings_updated, rows_updated


# ---------- CLI ----------

def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill race_program.meeting_id from Punting Form meetingslist.")
    ap.add_argument("--url", help="DATABASE_URL (defaults to env DATABASE_URL or sqlite:///./data/racing.db)")
    ap.add_argument("--dry-run", action="store_true", help="Do not write; just log what would change.")
    ap.add_argument("--limit", type=int, help="Limit number of (date,state,track) meetings to process.")
    args = ap.parse_args()

    try:
        meetings_updated, rows_updated = backfill(
            db_url=args.url,
            dry_run=args.dry_run,
            limit=args.limit,
        )
        if args.dry_run:
            print(f"[meeting_ids] (dry-run) would update {meetings_updated} meetings, {rows_updated} rows.")
    except Exception as e:
        print(f"[meeting_ids] ERROR: {e}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
