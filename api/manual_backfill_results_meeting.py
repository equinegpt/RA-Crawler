# api/manual_backfill_results_meeting.py
from __future__ import annotations

import argparse
from datetime import datetime, date
from typing import Any, Dict

from api.ra_results_crawler import RAResultsCrawler, SessionLocal


def _parse_key(key: str) -> tuple[date, str, str]:
    """
    Parse an RA Key like '2025Nov26,NSW,Wyong' -> (date(2025-11-26), 'NSW', 'Wyong').
    """
    parts = key.split(",", 2)
    if len(parts) != 3:
        raise SystemExit(f"[manual_backfill_results_meeting] Invalid Key format: {key!r}")
    date_part, state, track = parts
    try:
        d = datetime.strptime(date_part, "%Y%b%d").date()
    except ValueError as e:
        raise SystemExit(f"[manual_backfill_results_meeting] Invalid date part in Key: {date_part!r}") from e
    return d, state, track


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Backfill RA results for a single meeting (one RA Key or date/state/track)."
    )
    ap.add_argument(
        "--key",
        help="RA Key like '2025Nov26,NSW,Wyong'. If provided, overrides --date/--state/--track.",
    )
    ap.add_argument(
        "--date",
        dest="date_str",
        help="Meeting date in YYYY-MM-DD (used if --key is not given)",
    )
    ap.add_argument(
        "--state",
        help="State code, e.g. NSW/VIC/QLD (used if --key is not given)",
    )
    ap.add_argument(
        "--track",
        help="Track name exactly as RA uses in the Key, e.g. 'Wyong' (used if --key is not given)",
    )

    ns = ap.parse_args()

    if ns.key:
        meeting_date, state, track = _parse_key(ns.key)
    else:
        if not (ns.date_str and ns.state and ns.track):
            ap.error("Provide either --key OR all of --date, --state, --track")
        meeting_date = date.fromisoformat(ns.date_str)
        state = ns.state
        track = ns.track

        # Build the RA Key from parts (so behaviour is identical to --key)
        key_date = meeting_date.strftime("%Y%b%d")  # e.g. 2025Nov26
        ns.key = f"{key_date},{state},{track}"

    # Build the exact Results.aspx URL
    url = f"https://www.racingaustralia.horse/FreeFields/Results.aspx?Key={ns.key}"
    print(
        f"[manual_backfill_results_meeting] Backfilling results for "
        f"{meeting_date} {state} {track} via\n  {url}"
    )

    crawler = RAResultsCrawler()
    session = SessionLocal()
    try:
        meeting_row: Dict[str, Any] = {
            "state": state,
            "track": track,
            "url": url,  # _build_meeting_results_url() will just return this as-is
        }

        crawler._fetch_meeting_results(session, meeting_date, meeting_row)
        session.commit()
    finally:
        session.close()

    print("[manual_backfill_results_meeting] Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
