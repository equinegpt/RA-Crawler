# api/crawl_calendar.py
from __future__ import annotations

import argparse
from typing import Any, Iterable, Tuple

from .ra_discover import discover_meeting_keys

# We try to import both names so we can fall back gracefully if needed.
try:
    from .ra_harvest import harvest_program  # type: ignore
except Exception as _e:  # pragma: no cover
    harvest_program = None  # type: ignore

try:
    from .ra_harvest import harvest_program_from_key  # type: ignore
except Exception:
    harvest_program_from_key = None  # type: ignore


def _coerce_result(ret: Any) -> Tuple[int, int]:
    """
    Normalize various possible return shapes from ra_harvest into (saved, updated).

    Accepted shapes:
      - (saved, updated)
      - (saved, updated, *extras)
      - [rows...] -> (len(rows), 0)
      - int -> (int, 0)
      - None -> (0, 0)
    """
    if ret is None:
        return (0, 0)

    # Tuple-like (saved, updated, ...)
    if isinstance(ret, tuple):
        if len(ret) >= 2:
            s, u = ret[0], ret[1]
            try:
                return (int(s), int(u))
            except Exception:
                # best effort
                return (int(s) if isinstance(s, int) else 0, int(u) if isinstance(u, int) else 0)
        elif len(ret) == 1:
            s = ret[0]
            return (int(s) if isinstance(s, int) else 0, 0)
        else:
            return (0, 0)

    # List/iterable of rows -> count as saved
    if isinstance(ret, list):
        return (len(ret), 0)

    # Some code paths return an integer
    if isinstance(ret, int):
        return (ret, 0)

    # Unknown shape; ignore but don't crash
    return (0, 0)


def crawl_next(days: int, *, force: bool = False, include_past: int = 0, debug: bool = False) -> None:
    keys = discover_meeting_keys(days=days, include_past=include_past, debug=debug)
    print(f"Discovered {len(keys)} meetings for next {days} day(s).")

    total_saved = 0
    total_updated = 0
    failures = []

    for k in sorted(keys):
        try:
            # Primary call: many versions of ra_harvest expose harvest_program(key, force=?, debug=?)
            ret = None
            if callable(harvest_program):
                try:
                    # Try with keywords first (newer impl)
                    ret = harvest_program(k, force=force, debug=debug)  # type: ignore[misc]
                except TypeError:
                    # Fallback to positional only (older impl)
                    try:
                        ret = harvest_program(k, force)  # type: ignore[misc]
                    except TypeError:
                        ret = harvest_program(k)  # type: ignore[misc]
            # Fallback: some repos only expose harvest_program_from_key
            if ret is None and callable(harvest_program_from_key):
                try:
                    ret = harvest_program_from_key(k, force=force, debug=debug)  # type: ignore[misc]
                except TypeError:
                    try:
                        ret = harvest_program_from_key(k, force)  # type: ignore[misc]
                    except TypeError:
                        ret = harvest_program_from_key(k)  # type: ignore[misc]

            s, u = _coerce_result(ret)
            total_saved += s
            total_updated += u
            if debug:
                print(f"[crawl] {k}: saved={s} updated={u}")

        except KeyboardInterrupt:
            raise
        except Exception as e:
            failures.append((k, f"{type(e).__name__}: {e}"))
            if debug:
                print(f"[crawl:ERR] {k}: {e}")

    print(f"TOTAL saved {total_saved}, updated {total_updated}")
    if failures and debug:
        print(f"[crawl] Failures ({len(failures)}):")
        for k, msg in failures:
            print("  -", k, "->", msg)


def _main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--include-past", type=int, default=0)
    p.add_argument("--force", action="store_true")
    p.add_argument("--debug", action="store_true")
    ns = p.parse_args()
    crawl_next(ns.days, force=ns.force, include_past=ns.include_past, debug=ns.debug)


if __name__ == "__main__":
    _main()
