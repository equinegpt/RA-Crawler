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

        We parse HTML tables whose header row contains the typical
        "Colour / Finish / No. / Horse / Trainer / Jockey / ... / Starting Price"
        columns, e.g.:

            Colour Finish No. Horse Trainer Jockey Margin Bar. Weight Penalty Starting Price

        For each such table we:
        - Infer the race number from the nearest preceding "Race N -" text.
        - For each body row, extract finish, horse number, horse name, margin and SP.

        Scratched/emergency runners appear as rows with no '$' starting price;
        we mark those as is_scratched=True and finishing_pos=None.
        """
        soup = BeautifulSoup(html, "lxml")
        results: List[RAResult] = []

        for table in soup.find_all("table"):
            # --- 1) Find the header row for a results table ---
            header_row = None
            for tr in table.find_all("tr"):
                header_text = " ".join(
                    cell.get_text(" ", strip=True)
                    for cell in tr.find_all(["th", "td"])
                )
                if (
                    "Finish" in header_text
                    and "No" in header_text
                    and "Horse" in header_text
                    and "Trainer" in header_text
                    and "Jockey" in header_text
                    and "Starting Price" in header_text
                ):
                    header_row = tr
                    break

            if header_row is None:
                continue  # not a results table

            # --- 2) Try to find the "Race X -" text immediately before this table ---
            race_no = 0
            race_text_node = table.find_previous(string=re.compile(r"Race\s+(\d+)\s+-"))
            if race_text_node:
                m_race = re.search(r"Race\s+(\d+)\s+-", race_text_node)
                if m_race:
                    race_no = int(m_race.group(1))

            # Helper to parse int prefix (handles "11e" → 11)
            def int_prefix(token: str) -> Optional[int]:
                m = re.match(r"(\d+)", token)
                if not m:
                    return None
                try:
                    return int(m.group(1))
                except ValueError:
                    return None

            # --- 3) All rows after the header row are runners ---
            for row in header_row.find_next_siblings("tr"):
                cells = row.find_all("td")
                if len(cells) < 4:
                    continue

                # Squash all text; skip empty / junk rows.
                row_text = " ".join(
                    c.get_text(" ", strip=True) for c in cells
                ).strip()
                if not row_text:
                    continue

                # Finish & TAB columns
                finish_text = cells[1].get_text(strip=True)
                no_text = cells[2].get_text(strip=True)

                # If neither finish nor number present, probably not a runner row.
                if not finish_text and not no_text:
                    continue

                # Starting price is typically the last column.
                sp_text = cells[-1].get_text(strip=True)
                has_price = sp_text.startswith("$")

                # Scratched/emergency rows have no '$' SP.
                is_scratched = not has_price

                # Horse number from the "No." column
                horse_number = int_prefix(no_text)
                if horse_number is None:
                    continue

                # Horse name from the "Horse" column.
                horse_cell = cells[3]
                horse_raw = horse_cell.get_text(" ", strip=True)

                # Many horses will have an extra "Image: BOBS Silver Bonus Scheme" piece;
                # if so, trim everything from "Image:" onwards.
                img_idx = horse_raw.find("Image:")
                if img_idx != -1:
                    horse_raw = horse_raw[:img_idx].strip()

                horse_name = horse_raw or f"Horse {horse_number}"

                # Finishing position: only for starters
                finishing_pos: Optional[int] = None
                if not is_scratched:
                    finishing_pos = int_prefix(finish_text)

                # Margin (optional) – based on the column order "Margin Bar. Weight ..."
                margin_lens: Optional[float] = None
                if not is_scratched and len(cells) > 6:
                    margin_text = cells[6].get_text(strip=True)
                    if margin_text:
                        m_margin = re.search(r"([\d\.]+)", margin_text)
                        if m_margin:
                            try:
                                margin_lens = float(m_margin.group(1))
                            except ValueError:
                                margin_lens = None

                # Starting price (optional)
                starting_price: Optional[float] = None
                if has_price:
                    stripped_sp = sp_text.lstrip("$")
                    m_sp = re.search(r"([\d\.]+)", stripped_sp)
                    if m_sp:
                        try:
                            starting_price = float(m_sp.group(1))
                        except ValueError:
                            starting_price = None

                rr = RAResult(
                    meeting_date=meeting_date,
                    state=state,
                    track=track,
                    race_no=race_no,
                    horse_number=horse_number,
                    horse_name=horse_name,
                    finishing_pos=finishing_pos,
                    is_scratched=is_scratched,
                    margin_lens=margin_lens,
                    starting_price=starting_price,
                )
                results.append(rr)

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
