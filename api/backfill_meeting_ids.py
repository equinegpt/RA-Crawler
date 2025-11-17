from __future__ import annotations

import os
import sys
import time
from datetime import datetime, date
from typing import Dict, Tuple, Optional, List

import requests
from sqlalchemy import create_engine, text

REQ_TIMEOUT = 20
PF_MEETINGS_URL = "https://api.puntingform.com.au/v2/form/meetingslist"
PF_API_KEY_ENV = "PF_API_KEY"


# ---------------------- Helpers ----------------------


def _iso_to_pf_date(value) -> str:
    """
    Accepts either:
      - 'YYYY-MM-DD' string
      - datetime.date
      - datetime.datetime

    Returns PF-style 'D MMM YYYY' (e.g. '19 Nov 2025').
    """
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, date):
        dt = datetime.combine(value, datetime.min.time())
    else:
        s = str(value).strip()
        # defensive: take first 10 chars 'YYYY-MM-DD'
        dt = datetime.strptime(s[:10], "%Y-%m-%d")

    s = dt.strftime("%d %b %Y")  # '09 Nov 2025'
    if s[0] == "0":
        s = s[1:]
    return s


_BRAND_TOKENS = {
    "sportsbet",
    "ladbrokes",
    "bet365",
    "picklebet",
    "aquis",
    "southside",
    "tab",
    "thomas",
}
_GENERIC_TOKENS = {
    "park",
    "poly",
    "picnic",
    "racecourse",
    "rc",
    "course",
}


def _canonical_track_name(name: str) -> str:
    """
    Normalise track names so that e.g.
      'bet365 Park Kyneton'      -> 'kyneton'
      'Southside Pakenham'       -> 'pakenham'
      'Aquis Park Gold Coast'    -> 'gold coast'
      'Canterbury Park'          -> 'canterbury'
      'Doomben'                  -> 'doomben'
      'Kilcoy'                   -> 'kilcoy'
    """
    if not name:
        return ""

    s = name.lower()
    # normalise punctuation to spaces
    for ch in ",-&/":
        s = s.replace(ch, " ")
    parts = [p for p in s.split() if p]

    cleaned: List[str] = []
    for i, tok in enumerate(parts):
        # strip sponsors if they appear as the first word
        if i == 0 and tok in _BRAND_TOKENS:
            continue
        # drop generic words like 'park', 'poly', 'picnic', 'rc'
        if tok in _GENERIC_TOKENS:
            continue
        cleaned.append(tok)

    if not cleaned:
        cleaned = parts

    return " ".join(cleaned)


def _fetch_pf_map_for_date(pf_date: str, api_key: str) -> Dict[Tuple[str, str], str]:
    """
    Call Punting Form meetingslist for a given date and build a map:
        (STATE, canonical_track_name) -> meetingId
    """
    params = {
        "apiKey": api_key,
        "meetingDate": pf_date,
    }
    try:
        r = requests.get(PF_MEETINGS_URL, params=params, timeout=REQ_TIMEOUT)
        r.raise_for_status()
    except Exception as e:
        print(f"[meeting_ids] ERROR calling PF for {pf_date}: {e}", file=sys.stderr)
        return {}

    try:
        data = r.json()
    except Exception as e:
        print(f"[meeting_ids] ERROR decoding JSON for {pf_date}: {e}", file=sys.stderr)
        return {}

    if data.get("statusCode") != 200 or not data.get("payLoad"):
        # PF sometimes has no meetings yet for a future date
        return {}

    out: Dict[Tuple[str, str], str] = {}
    for m in data["payLoad"]:
        track = m.get("track") or {}
        state = (track.get("state") or "").upper()
        raw_name = track.get("name") or ""
        meeting_id = m.get("meetingId")
        if not state or not raw_name or not meeting_id:
            continue
        canon = _canonical_track_name(raw_name)
        if not canon:
            continue
        key = (state, canon)
        out[key] = str(meeting_id)

    return out


def _match_pf_meeting(state: str, track: str, pf_map: Dict[Tuple[str, str], str]) -> Optional[str]:
    """
    Try to find the PF meetingId for a given RA (state, track) using:
      1) Exact canonical match
      2) Token-overlap fuzzy match within the same state
    """
    st = (state or "").upper()
    canon_ra = _canonical_track_name(track)
    if not canon_ra:
        return None

    # 1) Exact canonical match
    key = (st, canon_ra)
    mid = pf_map.get(key)
    if mid:
        return mid

    # 2) Fuzzy: largest token overlap in the same state
    tokens_ra = set(canon_ra.split())
    best_mid: Optional[str] = None
    best_score = 0

    for (pf_state, canon_pf), mid in pf_map.items():
        if pf_state != st:
            continue
        tokens_pf = set(canon_pf.split())
        overlap = len(tokens_ra & tokens_pf)
        if overlap > best_score:
            best_score = overlap
            best_mid = mid

    # Require at least 1 token in common to accept
    if best_score >= 1:
        return best_mid

    return None


# ---------------------- Core backfill ----------------------


def backfill(
    url: Optional[str] = None,
    dry_run: bool = False,
    limit: Optional[int] = None,
) -> Tuple[int, int]:
    """
    Backfill race_program.meeting_id using Punting Form meetingslist.

    Returns (meetings_updated, rows_updated).
    """
    db_url = url or os.getenv("DATABASE_URL", "sqlite:///./racing.db")
    api_key = os.getenv(PF_API_KEY_ENV)
    if not api_key:
        raise RuntimeError(f"{PF_API_KEY_ENV} env var is required for backfill_meeting_ids")

    eng = create_engine(db_url, future=True)

    sel = text(
        """
        SELECT date, state, track, COUNT(*) AS races
        FROM race_program
        WHERE meeting_id IS NULL OR TRIM(COALESCE(meeting_id, '')) = ''
        GROUP BY date, state, track
        ORDER BY date, state, track
        """
    )

    with eng.connect() as conn:
        meetings = conn.execute(sel).fetchall()

    if limit is not None:
        meetings = meetings[:limit]

    if not meetings:
        print("[meeting_ids] No candidate meetings to update.")
        return 0, 0

    # Group meetings by date so we only call PF once per calendar day
    by_date: Dict[date | str, List[Tuple[str, str, int]]] = {}
    for row in meetings:
        d, st, trk, races = row
        by_date.setdefault(d, []).append((st, trk, races))

    total_meetings_updated = 0
    total_rows_updated = 0

    dates_sorted = sorted(by_date.keys(), key=lambda x: str(x))

    print(f"[meeting_ids] candidate meetings: {len(meetings)} across {len(dates_sorted)} date(s)")

    for idx, d in enumerate(dates_sorted, start=1):
        items = by_date[d]
        pf_date_str = _iso_to_pf_date(d)

        print(f"[meeting_ids] ({idx}/{len(dates_sorted)}) date={d} meetings={len(items)}")

        pf_map = _fetch_pf_map_for_date(pf_date_str, api_key)
        if not pf_map:
            print(f"[meeting_ids] No PF meetings map built for {d}, skipping.")
            continue

        # For each (state, track) on this date, try to match and update
        updates_for_date = 0
        rows_for_date = 0

        with eng.begin() as tx:
            for st, trk, races in items:
                mid = _match_pf_meeting(st, trk, pf_map)
                if not mid:
                    # Helpful but not too spammy
                    print(
                        f"[meeting_ids] WARN: no PF match for {d} {st} {trk!r}; "
                        f"canon={_canonical_track_name(trk)!r}"
                    )
                    continue

                if dry_run:
                    updates_for_date += 1
                    rows_for_date += races
                    continue

                upd = text(
                    """
                    UPDATE race_program
                    SET meeting_id = :mid
                    WHERE date = :d AND state = :s AND track = :t
                      AND (meeting_id IS NULL OR TRIM(COALESCE(meeting_id,'')) = '')
                    """
                )
                res = tx.execute(
                    upd,
                    {"mid": str(mid), "d": d, "s": st, "t": trk},
                )
                rc = res.rowcount or 0
                if rc > 0:
                    updates_for_date += 1
                    rows_for_date += rc

        total_meetings_updated += updates_for_date
        total_rows_updated += rows_for_date
        print(
            f"[meeting_ids] date={d} → meetings_updated so far={total_meetings_updated}, "
            f"rows={total_rows_updated}"
        )

        # Be nice to PF
        time.sleep(0.25)

    print(
        f"[meeting_ids] DONE meetings_updated={total_meetings_updated}, "
        f"rows_updated={total_rows_updated}"
    )
    return total_meetings_updated, total_rows_updated


# ---------------------- CLI entrypoint ----------------------


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser(
        description="Backfill race_program.meeting_id from Punting Form meetingslist."
    )
    ap.add_argument(
        "--url",
        help="DATABASE_URL (defaults to env DATABASE_URL or sqlite:///./racing.db)",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write; just report.",
    )
    ap.add_argument(
        "--limit",
        type=int,
        help="Only process this many meetings (grouped rows).",
    )
    args = ap.parse_args()

    meetings_updated, rows_updated = backfill(
        url=args.url,
        dry_run=args.dry_run,
        limit=args.limit,
    )

    if args.dry_run:
        print(
            f"[meeting_ids] (dry-run) would update {meetings_updated} meetings "
            f"({rows_updated} race rows)."
        )
    else:
        print(
            f"[meeting_ids] done: updated {meetings_updated} meetings "
            f"({rows_updated} race rows)."
        )

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
