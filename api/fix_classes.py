# api/fix_classes.py
from __future__ import annotations

import argparse
import re
from typing import Optional, Tuple

from sqlalchemy import create_engine, text

# Patterns we care about:
#   "Rating 0 - 64 Handicap"
#   "Ratings Band 0 - 55 Handicap"
#   Allow spaces / en-dash / hyphen variants and optional word after.
RE_RATINGS_BAND = re.compile(
    r"""(?ix)
        \b
        ratings?\s*band\s*0\s*[-–]\s*(\d{2})
        \b
    """
)
RE_RATING_RANGE = re.compile(
    r"""(?ix)
        \b
        rating\s*0\s*[-–]\s*(\d{2})
        \b
    """
)

def compute_class_from_description(desc: str, current: Optional[str]) -> Optional[str]:
    """
    Return the corrected class for 'Rating ...' races.
    If no change needed, return None.
    """
    if not desc:
        return None

    m = RE_RATINGS_BAND.search(desc)
    if not m:
        m = RE_RATING_RANGE.search(desc)
    if not m:
        return None

    upper_limit = m.group(1)  # e.g., "64", "68", "55"
    desired = f"0-{upper_limit}"

    # If it’s already the desired, no change
    if (current or "").strip().upper() == desired.upper():
        return None

    # If it’s a wrong BM… like BM64/BM70, we replace it with the rating band
    return desired


def main():
    ap = argparse.ArgumentParser(description="Fix class for 'Rating 0-XX' / 'Ratings Band 0-XX' races.")
    ap.add_argument("--url", required=True, help="SQLAlchemy DB URL, e.g. sqlite:////abs/path/to/racing.db")
    ap.add_argument("--dry-run", action="store_true", help="Show changes without writing")
    ap.add_argument("--limit", type=int, default=None, help="Optional limit of rows to scan")
    args = ap.parse_args()

    eng = create_engine(args.url, future=True)
    print(f"[fix-classes] Using DATABASE_URL: {args.url}")

    # Filter to likely candidates; we’ll confirm with regex in Python
    base_sql = """
        SELECT id, description, class
        FROM race_program
        WHERE
          description IS NOT NULL
          AND (
               UPPER(description) LIKE 'RATING %'
            OR UPPER(description) LIKE 'RATINGS BAND %'
          )
    """
    if args.limit:
        base_sql += " LIMIT :limit"

    with eng.connect() as c:
        rows = c.execute(text(base_sql), {"limit": args.limit} if args.limit else {}).fetchall()

    print(f"[fix-classes] Candidate rows: {len(rows)}")

    updates = []
    for r in rows:
        rid, desc, curr = r[0], r[1] or "", r[2]
        new_class = compute_class_from_description(desc, curr)
        if new_class:
            updates.append({"id": rid, "new_class": new_class})

    print(f"[fix-classes] Will update {len(updates)} rows" + (" (dry-run)" if args.dry_run else ""))

    if not args.dry_run and updates:
        with eng.begin() as c:
            # Chunk updates to avoid SQLite param limits
            for i in range(0, len(updates), 500):
                chunk = updates[i : i + 500]
                c.execute(
                    text("UPDATE race_program SET class = :new_class WHERE id = :id"),
                    chunk,
                )

        print("[fix-classes] Updates committed.")
    elif args.dry_run:
        # Show a few examples for confidence
        for ex in updates[:10]:
            print(f"  id={ex['id']} -> class='{ex['new_class']}' (dry-run)")
        print("[fix-classes] Dry-run complete.")


if __name__ == "__main__":
    main()
