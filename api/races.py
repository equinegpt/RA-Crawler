# api/races.py
from __future__ import annotations

from typing import List, Any, Dict, Tuple
from datetime import date, datetime
import os

from fastapi import APIRouter, HTTPException
from sqlalchemy import text

import httpx

from .db import get_engine
from .backfill_meeting_ids import canonical_track_name

races_router = APIRouter()


def _fetch_pf_meetings_for_date(target_date: date) -> List[Dict[str, Any]]:
    """
    Fetch PF meetings for a specific date.

    Expected PF shape (example):

      [
        {
          "track": {
            "name": "Beaumont",
            "trackId": "1138",
            "location": "P",
            "state": "NSW",
            ...
          },
          "meetingId": "235996",
          ...
        },
        ...
      ]

    Adjust PF_BASE_URL / path if your PF API is slightly different.
    """
    base = os.getenv("PF_RESULTS_BASE_URL", "").rstrip("/")
    if not base:
        raise RuntimeError("PF_RESULTS_BASE_URL is not set in environment")

    # You may need to tweak this path/query to match your PF service
    url = f"{base}/meetings"
    params = {"date": target_date.isoformat()}

    with httpx.Client(timeout=15.0) as client:
        resp = client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()

    if isinstance(data, dict) and "meetings" in data:
        meetings = data["meetings"]
    else:
        meetings = data

    if not isinstance(meetings, list):
        raise ValueError("Unexpected PF /meetings response; expected list or {'meetings': [...]}")

    return meetings


def _build_pf_meeting_lookup(pf_meetings: List[Dict[str, Any]]) -> Dict[Tuple[str, str, str], str]:
    """
    Build a lookup:

        (state, location, canonical_track_name) → pf_meeting_id
    """
    lookup: Dict[Tuple[str, str, str], str] = {}

    for m in pf_meetings:
        track = m.get("track") or {}
        if not isinstance(track, dict):
            continue

        name = track.get("name")
        state = track.get("state")
        location = track.get("location")  # "M" / "P" / "C"

        pf_meeting_id = m.get("meetingId") or m.get("meeting_id")
        if not (name and state and location and pf_meeting_id):
            continue

        key = (str(state), str(location), canonical_track_name(name))
        lookup[key] = str(pf_meeting_id)

    return lookup


def _sync_pf_meeting_ids_for_date(target_date: date) -> Dict[str, Any]:
    """
    For a given date, read RA race_program rows and PF meetings,
    then update race_program.meeting_id with the correct PF meetingId
    where it's currently NULL.

    Matching key:
      (state, type/location, norm_track_name_with_overrides)
    """
    eng = get_engine()

    # 1) Fetch PF meetings + build lookup
    pf_meetings = _fetch_pf_meetings_for_date(target_date)
    pf_lookup = _build_pf_meeting_lookup(pf_meetings)

    # 2) Fetch distinct (track, state, type) for this date with NULL meeting_id
    with eng.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT DISTINCT
                    track,
                    state,
                    type
                FROM race_program
                WHERE date = :d
                  AND meeting_id IS NULL
                """
            ),
            {"d": target_date},
        ).mappings().all()

        updated_meetings: List[Dict[str, Any]] = []
        missing_meetings: List[Dict[str, Any]] = []

        for r in rows:
            track = r["track"]
            state = r["state"]
            mtype = (r["type"] or "").strip().upper()  # "M"/"P"/"C" from RA

            # Map RA type → PF location code
            if mtype in {"M", "METRO", "METROPOLITAN"}:
                loc = "M"
            elif mtype in {"P", "PROV", "PROVINCIAL"}:
                loc = "P"
            else:
                loc = "C"

            # Use canonical_track_name for consistent matching with backfill_meeting_ids
            key = (str(state), loc, canonical_track_name(track))

            pf_meeting_id = pf_lookup.get(key)

            if not pf_meeting_id:
                missing_meetings.append(
                    {
                        "track": track,
                        "state": state,
                        "type": mtype,
                        "pf_lookup_key": key,
                    }
                )
                continue

            # 3) Update all rows for (date, track, state, type) with this PF meetingId
            res = conn.execute(
                text(
                    """
                    UPDATE race_program
                    SET meeting_id = :mid
                    WHERE date = :d
                      AND track = :track
                      AND state = :state
                      AND type = :type
                      AND meeting_id IS NULL
                    """
                ),
                {
                    "mid": pf_meeting_id,
                    "d": target_date,
                    "track": track,
                    "state": state,
                    "type": r["type"],
                },
            )

            updated_meetings.append(
                {
                    "track": track,
                    "state": state,
                    "type": mtype,
                    "pf_meeting_id": pf_meeting_id,
                    "rows_updated": res.rowcount,
                }
            )

    return {
        "date": target_date.isoformat(),
        "updated": updated_meetings,
        "missing": missing_meetings,
    }


# ---------------------------------------------------
# Public endpoints
# ---------------------------------------------------


@races_router.get("/races")
def list_races() -> List[Dict[str, Any]]:
    """
    Return all races from race_program.

    NOTE:
    - meeting_id (DB column) is exposed as meetingId (camelCase) in JSON.
    - date is returned as "YYYY-MM-DD" string.
    """
    eng = get_engine()

    # Check if race_time column exists (migration may not have run yet)
    has_race_time = False
    with eng.connect() as c:
        try:
            if eng.dialect.name == "postgresql":
                result = c.execute(text(
                    "SELECT 1 FROM information_schema.columns "
                    "WHERE table_name = 'race_program' AND column_name = 'race_time'"
                ))
                has_race_time = result.fetchone() is not None
            else:
                # SQLite
                result = c.execute(text("PRAGMA table_info(race_program)"))
                cols = {row[1] for row in result.fetchall()}
                has_race_time = "race_time" in cols
        except Exception:
            has_race_time = False

    # Build query based on available columns
    if has_race_time:
        query = """
            SELECT
                id, race_no, date, state, meeting_id, track, type,
                description, prize, condition, class, age, sex,
                distance_m, bonus, url, race_time
            FROM race_program
            ORDER BY date, state, track, race_no, id
        """
    else:
        query = """
            SELECT
                id, race_no, date, state, meeting_id, track, type,
                description, prize, condition, class, age, sex,
                distance_m, bonus, url
            FROM race_program
            ORDER BY date, state, track, race_no, id
        """

    with eng.connect() as c:
        rows = c.execute(text(query)).mappings().all()

    out: List[Dict[str, Any]] = []
    for r in rows:
        dt = r["date"]
        if hasattr(dt, "isoformat"):
            date_str = dt.isoformat()
        else:
            date_str = str(dt) if dt is not None else None

        out.append(
            {
                "id": r["id"],
                "race_no": r["race_no"],
                "date": date_str,
                "state": r["state"],
                "meetingId": r["meeting_id"],  # 👈 PF meetingId, once synced
                "track": r["track"],
                "type": r["type"],
                "description": r["description"],
                "prize": r["prize"],
                "condition": r["condition"],
                "class": r["class"],
                "age": r["age"],
                "sex": r["sex"],
                "distance_m": r["distance_m"],
                "bonus": r["bonus"],
                "url": r["url"],
                "raceTime": r.get("race_time") if has_race_time else None,
            }
        )

    return out


@races_router.get("/races/debug-db")
def debug_db() -> dict:
    """
    Debug endpoint: shows which DB the API is actually hitting,
    and what meeting_id looks like for the known problematic meetings.
    """
    eng = get_engine()
    engine_url = str(eng.url)
    env_url = os.getenv("DATABASE_URL")

    with eng.connect() as c:
        min_date = c.execute(text("SELECT MIN(date) FROM race_program")).scalar()
        max_date = c.execute(text("SELECT MAX(date) FROM race_program")).scalar()

        sample_rows = c.execute(
            text(
                """
                SELECT id, date, state, track, meeting_id
                FROM race_program
                WHERE date IN ('2025-11-18','2025-11-19','2025-11-20')
                  AND track IN (
                    'bet365 Park Kyneton',
                    'Canterbury Park',
                    'Doomben',
                    'Kilcoy',
                    'Thomas Farms RC Murray Bridge',
                    'Newcastle',
                    'Belmont'
                  )
                ORDER BY date, state, track, id
                """
            )
        ).mappings().all()

    def _date_str(d):
        if hasattr(d, "isoformat"):
            return d.isoformat()
        return str(d) if d is not None else None

    sample = [
        {
            "id": r["id"],
            "date": _date_str(r["date"]),
            "state": r["state"],
            "track": r["track"],
            "meeting_id": r["meeting_id"],
        }
        for r in sample_rows
    ]

    return {
        "engine_url": engine_url,
        "env_DATABASE_URL": env_url,
        "min_date": _date_str(min_date),
        "max_date": _date_str(max_date),
        "sample": sample,
    }


@races_router.post("/admin/sync-pf-meeting-ids")
def admin_sync_pf_meeting_ids(date_str: str) -> Dict[str, Any]:
    """
    Admin endpoint to backfill PF meetingId → race_program.meeting_id for a given date.

    Usage example (shell):

        curl -X POST 'https://<ra-crawler>/admin/sync-pf-meeting-ids' \
          -H 'Content-Type: application/json' \
          -d '"2025-12-16"'

    (Note the raw JSON string body: "YYYY-MM-DD")

    This will:
      - fetch PF meetings for that date,
      - match them to RA races by (state, type/location, track name with overrides),
      - and update race_program.meeting_id where it is currently NULL.
    """
    try:
        target_date = date.fromisoformat(date_str)
    except ValueError:
        raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD")

    try:
        result = _sync_pf_meeting_ids_for_date(target_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error syncing PF meeting IDs: {e}")

    return result
