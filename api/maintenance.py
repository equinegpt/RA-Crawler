# api/maintenance.py
from __future__ import annotations

import argparse
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from sqlalchemy import create_engine, text, bindparam
from sqlalchemy.engine import Engine


# ----------------------------- Engine resolution ----------------------------- #

def _resolve_engine(url_arg: Optional[str]) -> Engine:
    """
    Resolve a SQLAlchemy engine from:
      1) --url argument
      2) DATABASE_URL env var
      3) api.db.engine or api.db.get_engine()
      4) fallback sqlite:///<repo>/racing.db
    """
    if url_arg:
        return create_engine(url_arg, future=True)

    env_url = os.getenv("DATABASE_URL")
    if env_url:
        return create_engine(env_url, future=True)

    # Try to import your project's engine first
    try:
        from .db import engine as _engine  # type: ignore
        if _engine:
            return _engine
    except Exception:
        pass

    try:
        from .db import get_engine as _get  # type: ignore
        return _get()
    except Exception:
        pass

    # Fallback: local sqlite file at repo root
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    default_sqlite = f"sqlite:///{os.path.join(repo_root, 'racing.db')}"
    return create_engine(default_sqlite, future=True)


def _has_table(conn, name: str) -> bool:
    row = conn.execute(text(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=:n"
    ), {"n": name}).fetchone()
    return bool(row)


def _list_tables(conn) -> List[str]:
    rows = conn.execute(text(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY 1"
    )).fetchall()
    return [r[0] for r in rows]


def _has_index(conn, table: str, index_name: str) -> bool:
    rows = conn.execute(text(f"PRAGMA index_list({table})")).fetchall()
    for r in rows:
        # (seq, name, unique, origin, partial)
        if r[1] == index_name:
            return True
    return False


# ------------------------------ Backfill dates ------------------------------- #

DATE_KEY_RE = re.compile(r"Key=([0-9]{4}[A-Za-z]{3}[0-9]{2}),")

def _parse_date_from_url(url: str) -> Optional[str]:
    """
    Parse YYYYMonDD from ...Key=YYYYMonDD,... in URL and return YYYY-MM-DD.
    """
    m = DATE_KEY_RE.search(url or "")
    if not m:
        return None
    raw = m.group(1)  # e.g. 2025Sep20
    y = raw[:4]
    mon = raw[4:7].lower()
    d = raw[7:9]
    months = {
        "jan":"01","feb":"02","mar":"03","apr":"04","may":"05","jun":"06",
        "jul":"07","aug":"08","sep":"09","oct":"10","nov":"11","dec":"12"
    }
    if mon not in months:
        return None
    return f"{y}-{months[mon]}-{d}"


def backfill_race_dates(engine: Engine, dry_run: bool = False) -> int:
    """
    Set date (YYYY-MM-DD) in race_program when NULL, parsed from the URL's Key.
    Returns number of rows updated.
    """
    with engine.begin() as conn:
        if not _has_table(conn, "race_program"):
            print("[maintenance] ERROR: table 'race_program' not found.")
            return 0

        # Check if 'date' column exists
        cols = [r[1] for r in conn.execute(text("PRAGMA table_info(race_program)")).fetchall()]
        if "date" not in cols:
            print("[maintenance] ERROR: column 'date' missing in race_program.")
            return 0

        total_null = conn.execute(text("SELECT COUNT(*) FROM race_program WHERE date IS NULL")).scalar_one()
        print(f"[maintenance] Rows with NULL date in race_program: {total_null}")

        sel = text("SELECT id, url FROM race_program WHERE date IS NULL")
        rows = conn.execute(sel).fetchall()

        updates = []
        for rid, url in rows:
            dt = _parse_date_from_url(url or "")
            if dt:
                updates.append((rid, dt))

        print(f"[maintenance] Parsed valid dates for {len(updates)} rows")
        if not updates:
            return 0

        if dry_run:
            print("[maintenance] DRY RUN: would update these ids:", [rid for rid, _ in updates[:50]], "...")
            return len(updates)

        for rid, dt in updates:
            conn.execute(text("UPDATE race_program SET date=:d WHERE id=:i"), {"d": dt, "i": rid})
        print(f"[maintenance] Updated rows: {len(updates)}")
        return len(updates)


# ------------------------------ Deduplicate ---------------------------------- #

@dataclass
class RaceRow:
    id: int
    date: Optional[str]
    state: Optional[str]
    track: Optional[str]
    race_no: Optional[int]
    url: Optional[str]
    description: Optional[str]
    klass: Optional[str]
    type: Optional[str]
    prize: Optional[int]
    condition: Optional[str]
    age: Optional[str]
    sex: Optional[str]
    distance_m: Optional[int]
    bonus: Optional[str]


def _fetch_all_rows_for_dedupe(conn) -> List[RaceRow]:
    # Introspect columns to be portable
    cols = [r[1] for r in conn.execute(text("PRAGMA table_info(race_program)")).fetchall()]
    # Build SELECT only for columns that exist
    wanted = [
        "id","date","state","track","race_no","url",
        "description","class","type","prize","condition","age","sex","distance_m","bonus",
    ]
    select_cols = ", ".join([c for c in wanted if c in cols])
    rows = conn.execute(text(f"SELECT {select_cols} FROM race_program")).mappings().all()

    def get(m,k):
        return m[k] if k in m else None

    out: List[RaceRow] = []
    for m in rows:
        out.append(RaceRow(
            id=int(get(m,"id")),
            date=get(m,"date"),
            state=get(m,"state"),
            track=get(m,"track"),
            race_no=int(get(m,"race_no")) if get(m,"race_no") is not None else None,
            url=get(m,"url"),
            description=get(m,"description"),
            klass=get(m,"class"),
            type=get(m,"type"),
            prize=int(get(m,"prize")) if get(m,"prize") is not None else None,
            condition=get(m,"condition"),
            age=get(m,"age"),
            sex=get(m,"sex"),
            distance_m=int(get(m,"distance_m")) if get(m,"distance_m") is not None else None,
            bonus=get(m,"bonus"),
        ))
    return out


def _identity_key(r: RaceRow) -> Optional[Tuple]:
    """
    Identity of a race: (date, state, track, race_no, url)
    If any of these are missing, we can't safely dedupe that row.
    """
    if not (r.date and r.state and r.track and r.race_no and r.url):
        return None
    return (r.date, r.state, r.track, r.race_no, r.url)


def _score_row(r: RaceRow) -> int:
    """
    Heuristic score for the canonical row among duplicates.
    We try to keep the row whose 'class' matches the description pattern.
    """
    score = 0
    desc = (r.description or "").lower()
    klass = (r.klass or "").upper().replace(" ", "")

    if "maiden" in desc:
        # Prefer 'MAIDEN' over BM/CL
        if klass == "MAIDEN":
            score += 100
        if klass.startswith("BM"):
            score -= 20
        if klass.startswith("CL"):
            score -= 10

    # Benchmark (BM##)
    if ("benchmark" in desc) or re.search(r"\bbm\s*\d{2}\b", desc):
        if klass.startswith("BM"):
            score += 80

    # Class N (CLN)
    m = re.search(r"\bclass\s*(\d+)\b", desc)
    if m:
        want = f"CL{m.group(1)}"
        if klass == want:
            score += 70
        elif klass.startswith("CL"):
            score += 30

    # Prefer rows with non-null type/prize/distance (more info)
    if r.type: score += 5
    if r.distance_m: score += 3
    if r.prize: score += 2

    # Mild preference for lower id (older ingest) if still tied
    score += max(0, 1000 - (r.id % 1000))  # keeps determinism in ties
    return score


def _merge_winner_fields(winner: RaceRow, others: Iterable[RaceRow]) -> Dict[str, Any]:
    """
    Prepare a dict of fields to update on 'winner' from 'others' when winner fields are NULL/empty
    but other duplicates have a non-null value.
    """
    fields = ["type","prize","condition","klass","age","sex","distance_m","bonus","description"]
    db_fields = {"klass": "class"}

    updates: Dict[str, Any] = {}
    def val(x): return x if x not in ("", None) else None

    for f in fields:
        wv = getattr(winner, f)
        if val(wv) is not None:
            continue
        for o in others:
            ov = getattr(o, f)
            if val(ov) is not None:
                col = db_fields.get(f, f)
                updates[col] = ov
                break
    return updates


def dedupe_race_program(engine: Engine, dry_run: bool = False, add_unique_index: bool = True) -> Dict[str,int]:
    """
    Deduplicate race_program on (date, state, track, race_no, url).
    - Keep ONE canonical row per identity using heuristics.
    - Merge non-null fields into the winner if missing.
    - Delete the rest.
    - Optionally add a UNIQUE index to prevent re-introduction of dupes.
    Returns stats.
    """
    stats = {"groups": 0, "affected_groups": 0, "deleted": 0, "updated": 0}
    with engine.begin() as conn:
        if not _has_table(conn, "race_program"):
            print("[maintenance] ERROR: table 'race_program' not found.")
            return stats

        rows = _fetch_all_rows_for_dedupe(conn)
        buckets: Dict[Tuple, List[RaceRow]] = defaultdict(list)
        for r in rows:
            k = _identity_key(r)
            if k:
                buckets[k].append(r)

        stats["groups"] = len(buckets)
        to_delete: List[int] = []
        updates: List[Tuple[int, Dict[str, Any]]] = []

        for key, group in buckets.items():
            if len(group) <= 1:
                continue
            stats["affected_groups"] += 1

            # Pick winner
            winner = max(group, key=_score_row)
            others = [g for g in group if g.id != winner.id]

            # Merge missing fields on winner (if any)
            upd = _merge_winner_fields(winner, others)
            if upd:
                updates.append((winner.id, upd))

            # Delete others
            to_delete.extend([o.id for o in others])

        print(f"[maintenance] Duplicate groups: {stats['affected_groups']}")
        if dry_run:
            if to_delete:
                print(f"[maintenance] DRY RUN: would DELETE ids: {to_delete[:50]}{' ...' if len(to_delete)>50 else ''}")
            if updates:
                preview = [(i, list(upd.keys())) for i, upd in updates[:50]]
                print(f"[maintenance] DRY RUN: would UPDATE ids/cols: {preview}{' ...' if len(updates)>50 else ''}")
            return stats

        # Apply updates first
        for rid, upd in updates:
            sets = ", ".join(f"{k} = :{k}" for k in upd.keys())
            params = dict(upd)
            params["id"] = rid
            conn.execute(text(f"UPDATE race_program SET {sets} WHERE id=:id"), params)
            stats["updated"] += 1

        # Then delete losers (SQLAlchemy 2.x: use expanding bindparam)
        if to_delete:
            del_stmt = (
                text("DELETE FROM race_program WHERE id IN :ids")
                .bindparams(bindparam("ids", expanding=True))
            )
            for i in range(0, len(to_delete), 500):
                batch = to_delete[i:i+500]
                conn.execute(del_stmt, {"ids": batch})
            stats["deleted"] = len(to_delete)

        print(f"[maintenance] Deleted {stats['deleted']} rows; Updated {stats['updated']} winner rows.")

        # Add UNIQUE index to prevent future duplicates (optional)
        if add_unique_index:
            idx = "ux_race_program_ident"
            if not _has_index(conn, "race_program", idx):
                try:
                    conn.execute(text(
                        "CREATE UNIQUE INDEX IF NOT EXISTS ux_race_program_ident "
                        "ON race_program(date, state, track, race_no, url)"
                    ))
                    print("[maintenance] Added unique index ux_race_program_ident(date,state,track,race_no,url)")
                except Exception as e:
                    print(f"[maintenance] NOTE: could not add unique index yet: {e}")

    return stats


# --------------------------------- CLI --------------------------------------- #

def main() -> None:
    p = argparse.ArgumentParser(description="Maintenance utilities for race_program.")
    p.add_argument("--url", help="SQLAlchemy DB URL (overrides env/DATABASE_URL).")
    p.add_argument("--dry-run", action="store_true", help="Don’t modify data; just print actions.")
    p.add_argument("--backfill-dates", action="store_true", help="Backfill NULL dates from URL Key=YYYYMonDD.")
    p.add_argument("--dedupe", action="store_true", help="Deduplicate race_program and keep best record per race.")
    p.add_argument("--no-index", action="store_true", help="Skip adding unique index after dedupe.")
    args = p.parse_args()

    eng = _resolve_engine(args.url)
    print(f"[maintenance] Using DATABASE_URL: {eng.url}")

    with eng.connect() as conn:
        print(f"[maintenance] Found tables: { _list_tables(conn) }")

    # Default behavior: if no flags given, run both
    run_backfill = args.backfill_dates or (not args.backfill_dates and not args.dedupe)
    run_dedupe = args.dedupe or (not args.backfill_dates and not args.dedupe)

    if run_backfill:
        backfill_race_dates(eng, dry_run=args.dry_run)
    if run_dedupe:
        dedupe_race_program(eng, dry_run=args.dry_run, add_unique_index=not args.no_index)


if __name__ == "__main__":
    main()
