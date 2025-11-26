# api/ra_results_crawler.py
from __future__ import annotations

from datetime import date
from typing import Iterable, Mapping, Any, List

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
                        f"[RAResultsCrawler] ERROR meeting={m.get('state')}/{m.get('track')}: {exc}"
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

    def _build_meeting_results_url(self, meeting_row: Mapping[str, Any]) -> str | None:
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

        We:
        - Find each table whose header contains the typical "Colour / Finish / Horse" fields.
        - Infer the race number from the closest preceding "Race X -" text.
        - For each runner row, extract finish, horse number, horse name, margin, and SP.
        """
        import re

        soup = BeautifulSoup(html, "lxml")
        results: list[RAResult] = []

        # Iterate over all tables and pick the ones that look like "results" tables.
        for table in soup.find_all("table"):
            header_cell = table.find(
                lambda tag: tag.name in ("th", "td")
                and "Colour" in tag.get_text()
                and "Finish" in tag.get_text()
                and "Horse" in tag.get_text()
            )
            if not header_cell:
                continue

            # Try to find the "Race X -" text immediately before this table.
            race_no = 0
            race_text_node = table.find_previous(
                string=re.compile(r"Race\s+(\d+)\s+-")
            )
            if race_text_node:
                m = re.search(r"Race\s+(\d+)\s+-", race_text_node)
                if m:
                    race_no = int(m.group(1))

            header_row = header_cell.find_parent("tr")
            if header_row is None:
                continue

            # All rows after the header row are runners (until the table ends).
            for row in header_row.find_next_siblings("tr"):
                cells = row.find_all("td")
                if not cells:
                    continue

                # Squash all text; skip empty / junk rows.
                row_text = " ".join(c.get_text(" ", strip=True) for c in cells).strip()
                if not row_text:
                    continue

                # We expect at least: Colour | Finish | No | Horse | ...
                if len(cells) < 4:
                    continue

                # --- Finish position / scratched detection ---
                finish_text = cells[1].get_text(strip=True)
                no_text = cells[2].get_text(strip=True)

                # If neither finish nor number present, probably not a runner row.
                if not finish_text and not no_text:
                    continue

                fin_upper = finish_text.upper()
                is_scratched = fin_upper.startswith("SCR") or fin_upper in ("", "-", "0")

                finishing_pos: int | None = None
                if not is_scratched:
                    m_fin = re.search(r"\d+", finish_text)
                    if m_fin:
                        finishing_pos = int(m_fin.group(0))
                    else:
                        # If we can't parse a number at all, skip this row.
                        continue

                # --- Horse number (strip trailing 'e' for emergencies like "11e") ---
                m_no = re.search(r"\d+", no_text)
                if not m_no:
                    continue
                horse_number = int(m_no.group(0))

                # --- Horse name ---
                horse_cell = cells[3]
                horse_raw = horse_cell.get_text(" ", strip=True)

                # Many horses will have an extra "Image: BOBS Silver Bonus Scheme" piece;
                # if so, trim everything from "Image:" onwards.
                img_idx = horse_raw.find("Image:")
                if img_idx != -1:
                    horse_raw = horse_raw[:img_idx].strip()

                horse_name = horse_raw

                # --- Margin (optional) ---
                margin = None
                # Based on the column order "Margin Bar. Weight Penalty Starting Price"
                # margin is typically the first of those numeric columns.
                if len(cells) > 6:
                    margin_text = cells[6].get_text(strip=True)
                    if margin_text:
                        m_margin = re.search(r"([\d\.]+)", margin_text)
                        if m_margin:
                            try:
                                margin = float(m_margin.group(1))
                            except ValueError:
                                margin = None

                # --- Starting price (optional) ---
                sp = None
                sp_text = ""
                if len(cells) > 10:
                    sp_text = cells[10].get_text(strip=True)
                else:
                    # Fallback: use last cell
                    sp_text = cells[-1].get_text(strip=True)

                if sp_text:
                    # Strip '$' and trailing 'F', pick first number.
                    stripped = sp_text.replace("$", "")
                    m_sp = re.search(r"([\d\.]+)", stripped)
                    if m_sp:
                        try:
                            sp = float(m_sp.group(1))
                        except ValueError:
                            sp = None

                rr = RAResult(
                    meeting_date=meeting_date,
                    state=state,
                    track=track,
                    race_no=race_no,
                    horse_number=horse_number,
                    horse_name=horse_name,
                    finishing_pos=finishing_pos,
                    is_scratched=is_scratched,
                    margin_lens=margin,
                    starting_price=sp,
                )
                results.append(rr)

        return results

    def _upsert_result(self, db: Session, rr: RAResult) -> None:
        """
        Insert or update a single RAResult row, keyed on
        (meeting_date, state, track, race_no, horse_number).
        """
        existing: RAResult | None = (
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
