# api/racenet_odds.py
"""
Live fixed-odds sweeps from Racenet form-guide pages (SSR "Best Odds").

WHY (2026-07-24 incident): SkyNet/PF's gateway feed served 4 of 6 meetings
with zero attached TAB prices all morning — whole meetings vanished from
Tips/BoD/Lanes. PF's standard v2 API exposes no live market prices, TAB's
own API tarpits non-browser clients, and racing.com hydrates client-side.
Racenet race pages SERVER-SIDE RENDER a per-runner "Best Odds" cell
(best fixed price across Ladbrokes/Sportsbet/Ubet/Pointsbet/etc), fetched
through the same Scrape.do stack as racenet_dividends.

BASIS NOTE: this is BEST fixed odds, not TAB specifically — it reads
slightly ABOVE a TAB price (best >= any single book). Downstream gates
tag plays with price_basis="racenet_best" so band cohorts stay honest.

Cadence (user, 2026-07-25): sweeps at 08:15, 12:00 and pre-race — driven
by Pi timers hitting POST /sweep-racenet-odds. ~1 index + ~50-60 race
pages per full sweep.

Rows land in racenet_odds; GET /odds-latest?date= serves the freshest
price per (track, race, tab).
"""
from __future__ import annotations

import re
import time
from datetime import date
from typing import Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from .scraper_proxy import scraper_get
from .db import get_engine

_engine = get_engine()
SessionLocal = sessionmaker(bind=_engine, autoflush=False, autocommit=False)

BASE = "https://www.racenet.com.au"
INDEX = f"{BASE}/form-guide"

# international meetings carry a country suffix in the slug (ankara-tr,
# newbury-uk ...). We keep AU (no suffix) + NZ.
_FOREIGN = re.compile(
    r"-(tr|uk|fr|jp|hk|sgp|us|za|de|ie|ire|ca|arg|cl|usa|swe|nor)-?\d*$")
# NOTE: some internationals carry NO suffix (La Zarzuela, Hipodromo
# Presidente Remon — ~64 wasted pages on 2026-07-24). Proper fix is a
# cross-check against the day's PF meetings; suffix list is the cheap
# 90% for now. NZ (-nz) is deliberately KEPT (Waverley etc are carded).

_DDL = """
CREATE TABLE IF NOT EXISTS racenet_odds (
    id BIGSERIAL PRIMARY KEY,
    meeting_date DATE NOT NULL,
    track TEXT NOT NULL,
    race_no INT NOT NULL,
    tab_no INT NOT NULL,
    horse TEXT,
    book TEXT,
    price REAL NOT NULL,
    swept_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_racenet_odds_lookup
    ON racenet_odds (meeting_date, track, race_no, tab_no, swept_at DESC);
"""


def _ensure_table(session) -> None:
    for stmt in _DDL.strip().split(";"):
        if stmt.strip():
            session.execute(text(stmt))
    session.commit()


def discover_race_paths(meeting_date: date) -> List[str]:
    """Race-page paths for the date from the form-guide index (Racenet's
    own hrefs — no slug guessing, which is what soft-404'd meeting URLs)."""
    resp = scraper_get(INDEX, timeout=60)
    if resp is None or resp.status_code != 200:
        raise RuntimeError(f"index -> HTTP {getattr(resp, 'status_code', '?')}")
    ymd = meeting_date.strftime("%Y%m%d")
    paths = sorted({m.group(0) for m in re.finditer(
        r"/form-guide/horse-racing/[a-z0-9\-]+-" + ymd +
        r"/[a-z0-9\-]*race-\d+[a-z0-9\-/]*", resp.text)})
    keep = []
    for p in paths:
        slug = p.split("/")[3]                     # e.g. ipswich-20260724
        track_slug = slug[: -(len(ymd) + 1)]
        if _FOREIGN.search(track_slug):
            continue
        keep.append(p)
    return keep


def parse_race_page(html: str) -> Dict[int, tuple]:
    """{tab_no: (horse, book, price)} from the SSR Best Odds cells."""
    out: Dict[int, tuple] = {}
    for block in re.split(r"event-selection-row-container", html)[1:]:
        nm = re.search(
            r"horseracing-selection-details-name[^>]*>\s*(\d+)\.\s*([^<]+?)\s*<",
            block)
        if not nm:
            continue
        tab = int(nm.group(1))
        if tab in out:
            continue                                # mobile/desktop dup rows
        horse = nm.group(2).strip()
        for m in re.finditer(r"/bet/([a-z0-9]+)/\d+", block):
            seg = block[m.end(): m.end() + 600]
            px = re.search(r">\s*\$?(\d{1,3}\.\d{2})\s*<", seg)
            if px:
                price = float(px.group(1))
                if 1.01 <= price <= 501.0:
                    out[tab] = (horse, m.group(1), price)
                    break
    return out


def _track_from_path(path: str, ymd: str) -> str:
    slug = path.split("/")[3]
    return slug[: -(len(ymd) + 1)].replace("-", " ").title()


def sweep(meeting_date: date, paths: Optional[List[str]] = None,
          detail: Optional[list] = None) -> int:
    """Fetch + parse race pages, insert one row per runner. Returns rows."""
    ymd = meeting_date.strftime("%Y%m%d")
    if paths is None:
        paths = discover_race_paths(meeting_date)
    session = SessionLocal()
    total = 0
    try:
        _ensure_table(session)
        for p in paths:
            track = _track_from_path(p, ymd)
            rn = re.search(r"race-(\d+)", p.rsplit("/", 2)[-2] if p.endswith("/overview") else p)
            race_no = int(rn.group(1)) if rn else 0
            try:
                resp = scraper_get(BASE + p, timeout=60)
                if resp is None or resp.status_code != 200:
                    raise RuntimeError(f"HTTP {getattr(resp, 'status_code', '?')}")
                runners = parse_race_page(resp.text)
                for tab, (horse, book, price) in runners.items():
                    session.execute(text("""
                        INSERT INTO racenet_odds
                            (meeting_date, track, race_no, tab_no, horse,
                             book, price)
                        VALUES (:d, :t, :r, :n, :h, :b, :p)"""),
                        {"d": meeting_date.isoformat(), "t": track,
                         "r": race_no, "n": tab, "h": horse,
                         "b": book, "p": price})
                session.commit()
                total += len(runners)
                if detail is not None:
                    detail.append({"path": p, "runners": len(runners)})
            except Exception as e:
                session.rollback()
                print(f"[racenet-odds] {p} FAILED: {e}")
                if detail is not None:
                    detail.append({"path": p, "error": f"{type(e).__name__}: {e}"})
            time.sleep(0.3)
    finally:
        session.close()
    print(f"[racenet-odds] sweep {meeting_date}: {total} runner prices "
          f"from {len(paths)} race pages")
    return total


def latest_odds(meeting_date: date) -> List[Dict]:
    """Freshest price per (track, race, tab) for the date."""
    session = SessionLocal()
    try:
        _ensure_table(session)
        rows = session.execute(text("""
            SELECT DISTINCT ON (track, race_no, tab_no)
                   track, race_no, tab_no, horse, book, price,
                   swept_at
            FROM racenet_odds
            WHERE meeting_date = :d
            ORDER BY track, race_no, tab_no, swept_at DESC"""),
            {"d": meeting_date.isoformat()}).mappings().all()
        return [dict(r) for r in rows]
    finally:
        session.close()
