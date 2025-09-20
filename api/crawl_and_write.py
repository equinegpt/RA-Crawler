# api/crawl_and_write.py
from __future__ import annotations
import argparse
from typing import Dict, Any, Iterable, List

from sqlalchemy import text

from .db import get_engine, ensure_schema
from .ra_discover import discover_meeting_keys
from .ra_harvest import harvest_program_from_key

FIELDS = ["race_no","date","state","track","type","description",
          "prize","condition","class","age","sex","distance_m","bonus","url"]

def _norm_row(r: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    for k in FIELDS:
        out[k] = r.get(k)
    return out

def save_rows(rows: Iterable[Dict[str, Any]]) -> int:
    """
    Safe 'upsert': delete by (url, race_no) then insert fresh.
    This avoids duplicate rows without needing unique indexes right now.
    """
    rows = list(rows)
    if not rows:
        return 0
    eng = get_engine()
    n = 0
    with eng.begin() as c:
        for r in rows:
            rr = _norm_row(r)
            c.execute(text("DELETE FROM race_program WHERE url=:url AND race_no=:race_no"),
                      {"url": rr["url"], "race_no": rr["race_no"]})
            c.execute(text("""
                INSERT INTO race_program
                (race_no,date,state,track,type,description,prize,condition,class,age,sex,distance_m,bonus,url)
                VALUES
                (:race_no,:date,:state,:track,:type,:description,:prize,:condition,:class,:age,:sex,:distance_m,:bonus,:url)
            """), rr)
            n += 1
    return n

def run(days: int, include_past: int, limit_keys: int | None) -> None:
    ensure_schema()
    keys = sorted(discover_meeting_keys(days=days, include_past=include_past, debug=False))
    if limit_keys:
        keys = keys[:limit_keys]
    print(f"[crawl_and_write] discovered {len(keys)} meeting keys")

    total = 0
    for i, k in enumerate(keys, 1):
        try:
            rows = harvest_program_from_key(k, force=True, debug=False)
            saved = save_rows(rows)
            total += saved
            print(f"[{i}/{len(keys)}] {k} -> saved {saved}")
        except Exception as e:
            print(f"[{i}/{len(keys)}] {k} -> ERROR {e}")
    print(f"[crawl_and_write] TOTAL saved: {total}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--include-past", type=int, default=2)
    ap.add_argument("--limit-keys", type=int, default=None, help="debug: process only first N keys")
    ns = ap.parse_args()
    run(ns.days, ns.include_past, ns.limit_keys)

if __name__ == "__main__":
    main()
