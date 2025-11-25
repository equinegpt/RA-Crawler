# api/ra_results_crawler.py
from __future__ import annotations

from datetime import date
from typing import Iterable, Mapping, Any, List

import requests
from bs4 import BeautifulSoup
from sqlalchemy import text
from sqlalchemy.orm import Session

from .db import SessionLocal
from .models import RAResult


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

        Right now we just start from the race_program.url column and let you
        tweak it if RA has a distinct "results" URL vs "fields".

        TODO: If RA uses a separate route for results, adjust this function
        to transform the field URL into the corresponding results URL.
        """
        url = meeting_row.get("url")
        if not url:
            return None

        # Example (if you need to tweak):
        # return url.replace("FreeFields/DisplayMeeting.aspx", "FreeFields/Results.aspx")

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

        This is the one place you MUST customise based on the actual
        Racing Australia HTML structure.

        For now, this raises NotImplementedError so it fails loudly instead
        of silently doing nothing.
        """
        soup = BeautifulSoup(html, "lxml")

        # TODO: Inspect the RA results HTML and wire selectors here.
        # Rough sketch of what you might end up with:
        #
        # results: list[RAResult] = []
        # for race_block in soup.select("table.race-results"):
        #     race_no = int( ... )
        #     for row in race_block.select("tbody tr"):
        #         horse_number = ...
        #         horse_name = ...
        #         finishing_pos = ...
        #         is_scratched = ...
        #         margin_lens = ...
        #         starting_price = ...
        #
        #         results.append(
        #             RAResult(
        #                 meeting_date=meeting_date,
        #                 state=state,
        #                 track=track,
        #                 race_no=race_no,
        #                 horse_number=horse_number,
        #                 horse_name=horse_name,
        #                 finishing_pos=finishing_pos,
        #                 is_scratched=is_scratched,
        #                 margin_lens=margin_lens,
        #                 starting_price=starting_price,
        #             )
        #         )
        #
        # return results

        raise NotImplementedError(
            "TODO: implement _parse_meeting_results_html() based on RA HTML structure"
        )

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
