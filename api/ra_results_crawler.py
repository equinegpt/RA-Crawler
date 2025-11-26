# api/ra_results_crawler.py
from __future__ import annotations

from datetime import date
from typing import Iterable, Mapping, Any, List, Optional

import re
import requests
from bs4 import BeautifulSoup
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from .db import get_engine
from .models import RAResult

# Create our own SessionLocal based on the existing engine helper
_engine = get_engine()
SessionLocal = sessionmaker(bind=_engine, autoflush=False, autocommit=False)

# Matches lines like: "Race 1 - 2:45PM BOXING DAY TICKETS ON SALE NOW ..."
RACE_HEADING_RE = re.compile(r"^Race\s+(\d+)\s+-")


class RAResultsCrawler:
    """
    Crawler that fetches official Racing Australia results for all meetings
    on a given date and stores them into the ra_results table.

    This is kept separate from the existing "races" crawler so we can run it
    at different times (e.g. 6pm and 11pm).
    """

    def fetch_for_date(self, meeting_date: date) -> None:
        """Top-level entry point for cron: fetch + upsert results for a date."""
        session: Session = SessionLocal()
        try:
            meetings = list(self._load_meetings_for_date(session, meeting_date))
            if not meetings:
                print(f"[RAResultsCrawler] No meetings found for {meeting_date}")
                return

            print(
                f"[RAResultsCrawler] Fetching results for {meeting_date} – "
                f"{len(meetings)} meeting(s)"
            )

            for m in meetings:
                try:
                    self._fetch_meeting_results(session, meeting_date, m)
                except Exception as exc:
                    # Don't let one meeting kill the entire run
                    print(
                        f"[RAResultsCrawler] ERROR meeting={m.get('state')}/"
                        f"{m.get('track')}: {exc}"
                    )

            session.commit()
        finally:
            session.close()

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------

    def _load_meetings_for_date(
        self, db: Session, meeting_date: date
    ) -> Iterable[Mapping[str, Any]]:
        """
        Load distinct meetings from race_program for a given date.

        We use raw SQL here to avoid any mismatch between the ORM type and
        the actual DB column type. The DDL uses DATE in Postgres.
        """
        stmt = text(
            """
            SELECT DISTINCT state, track, url
            FROM race_program
            WHERE date = :meeting_date
              AND state IS NOT NULL
              AND track IS NOT NULL
            ORDER BY state, track
            """
        )

        rows = db.execute(stmt, {"meeting_date": meeting_date}).mappings().all()
        return rows

    def _fetch_meeting_results(
        self,
        db: Session,
        meeting_date: date,
        meeting_row: Mapping[str, Any],
    ) -> None:
        state = meeting_row["state"]
        track = meeting_row["track"]
        url = self._build_meeting_results_url(meeting_row)

        if not url:
            print(
                f"[RAResultsCrawler] Skipping meeting {meeting_date} {state} {track}: "
                f"no results URL"
            )
            return

        print(f"[RAResultsCrawler] Fetching results from {url}")

        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        html = resp.text

        results = self._parse_meeting_results_html(html, meeting_date, state, track)
        print(
            f"[RAResultsCrawler] Parsed {len(results)} runner result(s) for "
            f"{meeting_date} {state} {track}"
        )

        for rr in results:
            self._upsert_result(db, rr)

    def _build_meeting_results_url(self, meeting_row: Mapping[str, Any]) -> Optional[str]:
        """
        Build the Racing Australia results URL for a given meeting.

        We start from the race_program.url column, which currently points
        at RaceProgram.aspx, and map it to the corresponding Results.aspx
        URL for the same Key.
        """
        url = meeting_row.get("url")
        if not url:
            return None

        # Normal RA pattern:
        #   RaceProgram.aspx?Key=2025Nov25,NSW,Ballina
        # → Results.aspx?Key=2025Nov25,NSW,Ballina
        if "RaceProgram.aspx" in url:
            return url.replace("RaceProgram.aspx", "Results.aspx")

        # Fallback: if it's already a Results URL or some other variant, just use it.
        return url

    def _parse_meeting_results_html(
        self,
        html: str,
        meeting_date: date,
        state: str,
        track: str,
    ) -> List[RAResult]:
        """
        Parse a single RA meeting results page into RAResult objects.

        RA Results.aspx structure is effectively plain text with markers like:

          Race 1 - 2:45PM BOXING DAY ...
          Colour Finish No. Horse Trainer Jockey Margin Bar. Weight Penalty Starting Price
          【Image】 1 4 CANSORT ... 3 57.5kg $2.25F

        We:
        - Track the current "Race N -" heading.
        - For subsequent lines that contain an "Image" token and numbers,
          treat them as runner rows.
        - Lines with no '$' get stored as scratched/emergencies.
        """
        soup = BeautifulSoup(html, "lxml")
        text_content = soup.get_text("\n", strip=True)
        lines = [ln.strip() for ln in text_content.splitlines() if ln.strip()]

        results: List[RAResult] = []
        current_race_no: Optional[int] = None

        for line in lines:
            # Detect "Race N - ..." headings
            m_heading = RACE_HEADING_RE.match(line)
            if m_heading:
                current_race_no = int(m_heading.group(1))
                continue

            # Ignore anything before we see a Race heading
            if current_race_no is None:
                continue

            # Skip obvious non-runner lines
            if "Colour Finish No. Horse Trainer Jockey Margin Bar. Weight Penalty Starting Price" in line:
                continue
            if line.startswith("Official Comments:"):
                continue
            if line.startswith("Total Number of starters"):
                continue

            tokens = line.split()
            if not tokens:
                continue

            # Runner lines always have something that includes "Image"
            if not any("Image" in t for t in tokens):
                continue

            # Identify numeric tokens like "1", "4", "11e"
            numeric_tokens = [t for t in tokens if re.fullmatch(r"\d{1,2}e?", t)]
            if not numeric_tokens:
                continue  # nothing numeric to work with

            # Find starting price token like "$2.25F", "$6", "$2F"
            sp_token: Optional[str] = None
            for t in reversed(tokens):
                if t.startswith("$"):
                    sp_token = t
                    break

            def int_prefix(tok: str) -> Optional[int]:
                m = re.match(r"(\d+)", tok)
                if not m:
                    return None
                try:
                    return int(m.group(1))
                except ValueError:
                    return None

            def extract_name(from_tab_token: str) -> str:
                try:
                    idx = tokens.index(from_tab_token)
                except ValueError:
                    return ""
                name_parts: List[str] = []
                for t in tokens[idx + 1 :]:
                    if t in {"Image", "Image:"}:
                        continue
                    if t in {"BOBS", "Bonus", "Scheme", "Silver"}:
                        break
                    if t.startswith("$"):
                        break
                    # Cut off at margin/weight-like tokens
                    if re.search(r"\d", t) and ("L" in t or "kg" in t):
                        break
                    name_parts.append(t)
                    # Most names are short; avoid swallowing trainer/jockey
                    if len(name_parts) >= 4:
                        break
                return " ".join(name_parts).strip()

            # --------------------------------------------------------------
            # Case 1: emergencies / non-starters – no SP token
            # --------------------------------------------------------------
            if sp_token is None:
                tab_token = numeric_tokens[0]
                horse_number = int_prefix(tab_token)
                if horse_number is None:
                    continue

                horse_name = extract_name(tab_token) or f"Horse {horse_number}"

                results.append(
                    RAResult(
                        meeting_date=meeting_date,
                        state=state,
                        track=track,
                        race_no=current_race_no,
                        horse_number=horse_number,
                        horse_name=horse_name,
                        finishing_pos=None,
                        is_scratched=True,
                        margin_lens=None,
                        starting_price=None,
                    )
                )
                continue

            # --------------------------------------------------------------
            # Case 2: normal starters – need finish pos + TAB + SP
            # --------------------------------------------------------------
            if len(numeric_tokens) < 2:
                # need at least [finish, TAB]
                continue

            fin_token, tab_token = numeric_tokens[0], numeric_tokens[1]
            finishing_pos = int_prefix(fin_token)
            horse_number = int_prefix(tab_token)
            if finishing_pos is None or horse_number is None:
                continue

            # Parse SP float from "$2.25F" / "$6"
            starting_price: Optional[float] = None
            stripped_sp = sp_token.lstrip("$")
            m_sp = re.search(r"\d+(?:\.\d+)?", stripped_sp)
            if m_sp:
                try:
                    starting_price = float(m_sp.group(0))
                except ValueError:
                    starting_price = None

            horse_name = extract_name(tab_token) or f"Horse {horse_number}"

            results.append(
                RAResult(
                    meeting_date=meeting_date,
                    state=state,
                    track=track,
                    race_no=current_race_no,
                    horse_number=horse_number,
                    horse_name=horse_name,
                    finishing_pos=finishing_pos,
                    is_scratched=False,
                    margin_lens=None,      # can be extended later
                    starting_price=starting_price,
                )
            )

        return results

    def _upsert_result(self, db: Session, rr: RAResult) -> None:
        """
        Insert or update a single RAResult row, keyed on
        (meeting_date, state, track, race_no, horse_number).
        """
        existing: Optional[RAResult] = (
            db.query(RAResult)
            .filter(
                RAResult.meeting_date == rr.meeting_date,
                RAResult.state == rr.state,
                RAResult.track == rr.track,
                RAResult.race_no == rr.race_no,
                RAResult.horse_number == rr.horse_number,
            )
            .one_or_none()
        )

        if existing:
            existing.horse_name = rr.horse_name
            existing.finishing_pos = rr.finishing_pos
            existing.is_scratched = rr.is_scratched
            existing.margin_lens = rr.margin_lens
            existing.starting_price = rr.starting_price
        else:
            db.add(rr)
