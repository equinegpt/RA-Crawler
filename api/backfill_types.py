# api/backfill_types.py
from __future__ import annotations

import argparse
import os
from typing import Optional, Tuple

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Uses your track_types module (drop-in provided earlier)
from .track_types import get_track_type


def get_engine_from_url(url: Optional[str]) -> Engine:
    if not url:
        # default to a local sqlite file named racing.db in repo root
        url = f"sqlite:///{os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'racing.db'))}"
    eng = create_engine(url, future=True)
    print(f"[backfill-types] Using DATABASE_URL: {eng.url}")
    return eng


def table_exists(eng: Engine, name: str) -> bool:
    with eng.connect() as conn:
        rs = conn.execute(text(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=:n"
        ), {"n": name}).fetchone()
        return rs is not None


def column_exists(eng: Engine, table: str, column: str) -> bool:
    with eng.connect() as conn:
        rs = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
        cols = {r[1] for r in rs}  # (cid, name, type, notnull, dflt_value, pk)
        return column in cols


def backfill_types(eng: Engine, only_null: bool = True, dry_run: bool = False, chunk: int = 500) -> Tuple[int, int]:
    """
    Returns (scanned, updated)
    """
    if not table_exists(eng, "race_program"):
        raise SystemExit("[backfill-types] ERROR: table 'race_program' not found.")

    if not column_exists(eng, "race_program", "type"):
        raise SystemExit("[backfill-types] ERROR: column 'type' not found in 'race_program'.")

    where = "type IS NULL OR TRIM(type) = ''" if only_null else "1=1"

    # Count candidates
    with eng.connect() as conn:
        total = conn.execute(text(f"SELECT COUNT(*) FROM race_program WHERE {where}")).scalar_one()
    print(f"[backfill-types] Candidates to compute: {total}")

    scanned = 0
    updated = 0

    offset = 0
    while True:
        with eng.connect() as conn:
            rows = conn.execute(
                text(f"""
                    SELECT id, state, track, type
                    FROM race_program
                    WHERE {where}
                    ORDER BY id
                    LIMIT :lim OFFSET :off
                """),
                {"lim": chunk, "off": offset},
            ).fetchall()

        if not rows:
            break

        to_update = []
        for (rid, state, track, typ) in rows:
            scanned += 1
            new_type = get_track_type(state or "", track or "")
            if new_type and new_type != (typ or "").strip():
                to_update.append((rid, new_type))

        if to_update:
            if dry_run:
                print(f"[backfill-types] (dry-run) would update {len(to_update)} rows in this batch")
            else:
                with eng.begin() as tx:
                    tx.execute(
                        text("UPDATE race_program SET type = :t WHERE id = :id"),
                        [{"id": rid, "t": t} for (rid, t) in to_update],
                    )
                updated += len(to_update)
                print(f"[backfill-types] Updated {len(to_update)} rows in this batch")

        offset += chunk

    return scanned, updated


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill race_program.type (M/P/C) from track/state.")
    ap.add_argument("--url", default=os.getenv("DATABASE_URL"), help="SQLAlchemy DB URL. Defaults to $DATABASE_URL or local sqlite file.")
    ap.add_argument("--all", action="store_true", help="Recompute type for ALL rows (not just NULL/blank).")
    ap.add_argument("--dry-run", action="store_true", help="Do not write changes.")
    ap.add_argument("--chunk", type=int, default=500, help="Batch size (default 500).")
    args = ap.parse_args()

    eng = get_engine_from_url(args.url)
    scanned, updated = backfill_types(eng, only_null=not args.all, dry_run=args.dry_run, chunk=args.chunk)
    print(f"[backfill-types] Done. Scanned={scanned}, Updated={updated}, only_null={not args.all}, dry_run={args.dry_run}")


if __name__ == "__main__":
    main()
