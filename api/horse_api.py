# api/horse_api.py
"""
Horse search, profile, breeding analytics, and track stats API.

These endpoints query the racing-db tables (horses, race_results, races, tracks,
trainers, jockeys) which live in the same Postgres database.

If the racing-db tables are in a SEPARATE database, set RACING_DB_URL env var.
"""
from __future__ import annotations

import os
import time
from typing import Optional

from fastapi import APIRouter, Query
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .db import get_engine as get_default_engine

router = APIRouter(prefix="/api", tags=["horses"])

# Use separate DB connection if RACING_DB_URL is set, otherwise same DB
_racing_engine: Engine | None = None

def _get_racing_engine() -> Engine:
    global _racing_engine
    if _racing_engine is None:
        url = os.getenv("RACING_DB_URL")
        if url:
            _racing_engine = create_engine(url, pool_pre_ping=True)
        else:
            _racing_engine = get_default_engine()
    return _racing_engine


def _rows_to_dicts(result, keys: list[str]) -> list[dict]:
    return [dict(zip(keys, row)) for row in result]


# ============================================================================
# SIMPLE IN-MEMORY CACHE
# ============================================================================
# Heavy breeding/track queries take 30-50s. Cache results for 1 hour.

_cache: dict[str, tuple[float, list]] = {}
CACHE_TTL = 3600  # 1 hour

def _cached(key: str, fn, ttl: int = CACHE_TTL):
    """Return cached result if fresh, otherwise compute and cache."""
    now = time.time()
    if key in _cache:
        ts, data = _cache[key]
        if now - ts < ttl:
            return data
    data = fn()
    _cache[key] = (now, data)
    return data


# ============================================================================
# HORSE SEARCH
# ============================================================================

@router.get("/search/horses")
def search_horses(q: str = "", limit: int = Query(20, le=50)):
    if len(q) < 2:
        return []
    eng = _get_racing_engine()
    with eng.connect() as db:
        result = db.execute(text("""
            SELECT h.horse_code, h.name, h.sire_name, h.dam_name, h.sex, h.colour,
                   COALESCE(stats.wins, 0) as career_wins,
                   COALESCE(stats.prizemoney, 0) as career_prizemoney
            FROM horses h
            LEFT JOIN LATERAL (
                SELECT SUM(CASE WHEN rr.position = 1 THEN 1 ELSE 0 END) as wins,
                       SUM(rr.prizemoney_won) as prizemoney
                FROM race_results rr
                WHERE rr.horse_id = h.horse_id AND rr.is_trial = FALSE
            ) stats ON true
            WHERE h.name ILIKE :q
            ORDER BY stats.prizemoney DESC NULLS LAST
            LIMIT :limit
        """), {"q": f"%{q}%", "limit": limit})
        return _rows_to_dicts(result, [
            'horse_code', 'name', 'sire_name', 'dam_name', 'sex', 'colour',
            'career_wins', 'career_prizemoney'
        ])


@router.get("/search/sires")
def search_sires(q: str = "", limit: int = Query(20, le=50)):
    if len(q) < 2:
        return []
    eng = _get_racing_engine()
    with eng.connect() as db:
        result = db.execute(text("""
            SELECT sire_name, COUNT(*) as progeny
            FROM horses
            WHERE sire_name ILIKE :q
            GROUP BY sire_name
            ORDER BY progeny DESC
            LIMIT :limit
        """), {"q": f"%{q}%", "limit": limit})
        return [{"name": r[0], "progeny": r[1]} for r in result]


# ============================================================================
# HORSE PROFILE
# ============================================================================

@router.get("/horse/{horse_code}")
def horse_profile(horse_code: str, form_limit: int = Query(50, le=100)):
    eng = _get_racing_engine()
    with eng.connect() as db:
        row = db.execute(text("""
            SELECT h.horse_code, h.name, h.sex, h.colour, h.dob,
                   h.sire_name, h.dam_name, h.sire_of_dam,
                   h.sire_code, h.dam_code, h.country,
                   t.name as trainer_name
            FROM horses h
            LEFT JOIN trainers t ON h.trainer_id = t.trainer_id
            WHERE h.horse_code = :code
        """), {"code": horse_code}).fetchone()
        if not row:
            return {"error": "Horse not found"}

        keys = ['horse_code', 'name', 'sex', 'colour', 'dob',
                'sire_name', 'dam_name', 'sire_of_dam',
                'sire_code', 'dam_code', 'country', 'trainer_name']
        horse = dict(zip(keys, row))
        # Convert date to string
        if horse.get("dob"):
            horse["dob"] = str(horse["dob"])

        hid_row = db.execute(text(
            "SELECT horse_id FROM horses WHERE horse_code = :code"
        ), {"code": horse_code}).fetchone()
        hid = hid_row[0]

        # Career stats (calculated, not scraped)
        s = db.execute(text("""
            SELECT COUNT(*), SUM(CASE WHEN rr.position=1 THEN 1 ELSE 0 END),
                   SUM(CASE WHEN rr.position=2 THEN 1 ELSE 0 END),
                   SUM(CASE WHEN rr.position=3 THEN 1 ELSE 0 END),
                   COALESCE(SUM(rr.prizemoney_won),0), MAX(rr.handicap_rating)
            FROM race_results rr WHERE rr.horse_id=:hid AND rr.is_trial=FALSE
        """), {"hid": hid}).fetchone()
        horse.update({
            "career_starts": s[0] or 0, "career_wins": s[1] or 0,
            "career_seconds": s[2] or 0, "career_thirds": s[3] or 0,
            "career_prizemoney": float(s[4] or 0), "best_rating": s[5],
        })

        # Form
        form_rows = db.execute(text("""
            SELECT ra.race_date, t.name, t.state, ra.distance, ra.race_class,
                   ra.track_condition, rr.position, rr.margin, rr.barrier, rr.weight,
                   rr.handicap_rating, j.name, rr.race_time, rr.last_600m,
                   rr.odds_closing, rr.prizemoney_won, ra.field_size, rr.is_trial
            FROM race_results rr
            JOIN races ra ON rr.race_id=ra.race_id
            JOIN tracks t ON ra.track_id=t.track_id
            LEFT JOIN jockeys j ON rr.jockey_id=j.jockey_id
            WHERE rr.horse_id=:hid AND rr.is_trial=FALSE
            ORDER BY ra.race_date DESC LIMIT :lim
        """), {"hid": hid, "lim": form_limit})
        fkeys = ['race_date','track','state','distance','race_class',
                 'track_condition','position','margin','barrier','weight',
                 'handicap_rating','jockey','race_time','last_600m',
                 'odds_closing','prizemoney_won','field_size','is_trial']
        form = []
        for r in form_rows:
            d = dict(zip(fkeys, r))
            if d.get("race_date"):
                d["race_date"] = str(d["race_date"])
            form.append(d)

        # Distance stats
        dist = db.execute(text("""
            SELECT CASE WHEN ra.distance<1200 THEN 'Sprint'
                        WHEN ra.distance<1600 THEN 'Mile'
                        WHEN ra.distance<2000 THEN 'Middle'
                        ELSE 'Staying' END,
                   COUNT(*), SUM(CASE WHEN rr.position=1 THEN 1 ELSE 0 END),
                   SUM(CASE WHEN rr.position<=3 THEN 1 ELSE 0 END)
            FROM race_results rr JOIN races ra ON rr.race_id=ra.race_id
            WHERE rr.horse_id=:hid AND rr.is_trial=FALSE
            GROUP BY 1 ORDER BY MIN(ra.distance)
        """), {"hid": hid})
        distance_stats = _rows_to_dicts(dist, ['category','runs','wins','places'])

        # Condition stats
        cond = db.execute(text("""
            SELECT ra.track_condition, COUNT(*),
                   SUM(CASE WHEN rr.position=1 THEN 1 ELSE 0 END),
                   SUM(CASE WHEN rr.position<=3 THEN 1 ELSE 0 END)
            FROM race_results rr JOIN races ra ON rr.race_id=ra.race_id
            WHERE rr.horse_id=:hid AND ra.track_condition IS NOT NULL AND rr.is_trial=FALSE
            GROUP BY ra.track_condition ORDER BY COUNT(*) DESC
        """), {"hid": hid})
        condition_stats = _rows_to_dicts(cond, ['condition','runs','wins','places'])

        # Track stats
        trk = db.execute(text("""
            SELECT t.name, COUNT(*),
                   SUM(CASE WHEN rr.position=1 THEN 1 ELSE 0 END),
                   SUM(CASE WHEN rr.position<=3 THEN 1 ELSE 0 END),
                   ROUND(AVG(rr.last_600m)::numeric,2)
            FROM race_results rr JOIN races ra ON rr.race_id=ra.race_id
            JOIN tracks t ON ra.track_id=t.track_id
            WHERE rr.horse_id=:hid AND rr.is_trial=FALSE
            GROUP BY t.name ORDER BY COUNT(*) DESC
        """), {"hid": hid})
        track_stats = _rows_to_dicts(trk, ['track','runs','wins','places','avg_600'])

    return {
        "horse": horse, "form": form,
        "distance_stats": distance_stats,
        "condition_stats": condition_stats,
        "track_stats": track_stats,
    }


# ============================================================================
# BREEDING ANALYTICS
# ============================================================================

@router.get("/breeding/sire-leaderboard")
def sire_leaderboard(
    sort: str = "prizemoney", limit: int = 50, min_runners: int = 10
):
    cache_key = f"sire_lb_{sort}_{limit}_{min_runners}"
    def _query():
        return _sire_leaderboard_query(sort, limit, min_runners)
    return _cached(cache_key, _query)

def _sire_leaderboard_query(sort, limit, min_runners):
    """Query the materialised view — instant indexed lookup."""
    order_map = {
        "prizemoney": "prizemoney DESC NULLS LAST",
        "winners": "winners DESC",
        "win_pct": "win_pct DESC NULLS LAST",
        "sw": "stakes_winners DESC",
        "rating": "avg_rating DESC NULLS LAST",
        "runners": "runners DESC",
        "roi": "prize_per_start DESC NULLS LAST",
    }
    order = order_map.get(sort, "prizemoney DESC NULLS LAST")
    eng = _get_racing_engine()
    with eng.connect() as db:
        result = db.execute(text(f"""
            SELECT name, runners, winners, starts, wins, places,
                   winners_to_runners, win_pct, place_pct,
                   prizemoney, prize_per_runner, prize_per_start,
                   avg_rating, peak_rating,
                   stakes_runners, stakes_winners, stakes_placegetters
            FROM mv_sire_leaderboard
            WHERE runners >= :min_runners
            ORDER BY {order}
            LIMIT :limit
        """), {"limit": limit, "min_runners": min_runners})

        return [{
            "name": r[0], "runners": r[1], "winners": r[2],
            "starts": r[3], "wins": r[4], "places": r[5],
            "winners_to_runners": float(r[6] or 0), "win_pct": float(r[7] or 0),
            "place_pct": float(r[8] or 0), "prizemoney": float(r[9] or 0),
            "prize_per_runner": float(r[10] or 0), "prize_per_start": float(r[11] or 0),
            "avg_rating": float(r[12] or 0), "peak_rating": r[13],
            "stakes_runners": r[14], "stakes_winners": r[15],
        } for r in result]


@router.get("/breeding/broodmare-sire-leaderboard")
def broodmare_sire_leaderboard(
    sort: str = "prizemoney", limit: int = 50, min_runners: int = 3
):
    return _cached(f"bms_{sort}_{limit}_{min_runners}",
                   lambda: _broodmare_sire_query(sort, limit, min_runners))

def _broodmare_sire_query(sort, limit, min_runners):
    """Query the materialised view."""
    order_map = {
        "prizemoney": "prizemoney DESC NULLS LAST",
        "winners": "winners DESC",
        "win_pct": "win_pct DESC NULLS LAST",
        "runners": "runners DESC",
    }
    order = order_map.get(sort, "prizemoney DESC NULLS LAST")
    eng = _get_racing_engine()
    with eng.connect() as db:
        result = db.execute(text(f"""
            SELECT name, runners, winners, starts, wins, win_pct,
                   prizemoney, avg_rating, stakes_winners
            FROM mv_broodmare_sire_leaderboard
            WHERE runners >= :min_runners
            ORDER BY {order}
            LIMIT :limit
        """), {"limit": limit, "min_runners": min_runners})

        return [{
            "name": r[0], "runners": r[1], "winners": r[2],
            "starts": r[3], "wins": r[4], "win_pct": float(r[5] or 0),
            "prizemoney": float(r[6] or 0), "avg_rating": float(r[7] or 0),
            "stakes_winners": r[8],
        } for r in result]


@router.get("/breeding/distance-dna")
def distance_dna(limit: int = 30, min_runners: int = 15):
    return _cached(f"dna_{limit}_{min_runners}",
                   lambda: _distance_dna_query(limit, min_runners))

def _distance_dna_query(limit, min_runners):
    """Query the materialised view."""
    eng = _get_racing_engine()
    with eng.connect() as db:
        result = db.execute(text("""
            SELECT sire_name, total_runners,
                   sprint_runs, sprint_wins, sprint_pct,
                   mile_runs, mile_wins, mile_pct,
                   middle_runs, middle_wins, middle_pct,
                   staying_runs, staying_wins, staying_pct
            FROM mv_breeding_distance_dna
            WHERE total_runners >= :min_runners
            ORDER BY total_runners DESC
            LIMIT :limit
        """), {"limit": limit, "min_runners": min_runners})

        return [{
            "sire_name": r[0], "total_runners": r[1],
            "sprint_runs": r[2], "sprint_wins": r[3], "sprint_pct": float(r[4] or 0),
            "mile_runs": r[5], "mile_wins": r[6], "mile_pct": float(r[7] or 0),
            "middle_runs": r[8], "middle_wins": r[9], "middle_pct": float(r[10] or 0),
            "staying_runs": r[11], "staying_wins": r[12], "staying_pct": float(r[13] or 0),
        } for r in result]


@router.get("/breeding/nicks")
def nicks(min_runners: int = 2, limit: int = 50):
    return _cached(f"nicks_{min_runners}_{limit}",
                   lambda: _nicks_query(min_runners, limit))

def _nicks_query(min_runners, limit):
    """Query the materialised view."""
    eng = _get_racing_engine()
    with eng.connect() as db:
        result = db.execute(text("""
            SELECT sire_name, broodmare_sire, runners, wins, starts,
                   win_pct, prizemoney, avg_rating
            FROM mv_breeding_nicks
            WHERE runners >= :min_runners
            ORDER BY prizemoney DESC NULLS LAST
            LIMIT :limit
        """), {"min_runners": min_runners, "limit": limit})

        return [{
            "sire_name": r[0], "broodmare_sire": r[1], "runners": r[2],
            "winners": r[2],  # in materialised view, runners == winners count contextually
            "starts": r[4], "wins": r[3],
            "win_pct": float(r[5] or 0), "prizemoney": float(r[6] or 0),
            "avg_rating": float(r[7]) if r[7] else None,
        } for r in result]


@router.get("/breeding/sectionals")
def sectionals(limit: int = 30, min_runs: int = 50):
    return _cached(f"sect_{limit}_{min_runs}",
                   lambda: _sectionals_query(limit, min_runs))

def _sectionals_query(limit, min_runs):
    """Query the materialised view."""
    eng = _get_racing_engine()
    with eng.connect() as db:
        result = db.execute(text("""
            SELECT sire_name, total_runs, avg_last_600m,
                   avg_position_800m, avg_position_400m,
                   avg_position_gain, wins
            FROM mv_breeding_sectionals
            WHERE total_runs >= :min_runs
            ORDER BY avg_last_600m ASC NULLS LAST
            LIMIT :limit
        """), {"min_runs": min_runs, "limit": limit})

        return [{
            "sire_name": r[0], "total_runs": r[1],
            "avg_last_600m": float(r[2]) if r[2] else None,
            "avg_position_800m": float(r[3]) if r[3] else None,
            "avg_position_400m": float(r[4]) if r[4] else None,
            "avg_position_gain": float(r[5]) if r[5] else None,
            "wins": r[6],
        } for r in result]


@router.get("/breeding/class-ceiling")
def class_ceiling(limit: int = 30, min_runners: int = 15):
    return _cached(f"ceiling_{limit}_{min_runners}",
                   lambda: _class_ceiling_query(limit, min_runners))

def _class_ceiling_query(limit, min_runners):
    """Query the materialised view."""
    eng = _get_racing_engine()
    with eng.connect() as db:
        result = db.execute(text("""
            SELECT sire_name, runners,
                   g1_runners, g1_winners,
                   g2_runners, g2_winners,
                   g3_runners, g3_winners,
                   listed_runners, listed_winners,
                   stakes_reach_pct
            FROM mv_breeding_class_ceiling
            WHERE runners >= :min_runners
            ORDER BY stakes_reach_pct DESC NULLS LAST
            LIMIT :limit
        """), {"min_runners": min_runners, "limit": limit})

        return [{
            "sire_name": r[0], "runners": r[1],
            "g1_runners": r[2], "g1_winners": r[3],
            "g2_runners": r[4], "g2_winners": r[5],
            "g3_runners": r[6], "g3_winners": r[7],
            "listed_runners": r[8], "listed_winners": r[9],
            "stakes_reach_pct": float(r[10] or 0),
        } for r in result]


# ============================================================================
# TRACK STATS
# ============================================================================

@router.get("/tracks")
def track_stats(state: Optional[str] = None):
    return _cached(f"tracks_{state}",
                   lambda: _track_stats_query(state))

def _track_stats_query(state):
    eng = _get_racing_engine()
    with eng.connect() as db:
        sql = """
            SELECT t.name, t.state,
                COUNT(DISTINCT ra.race_id) as total_races,
                COUNT(DISTINCT rr.horse_id) as total_horses,
                ROUND(AVG(ra.field_size)::numeric, 1) as avg_field_size
            FROM tracks t
            JOIN races ra ON ra.track_id = t.track_id
            JOIN race_results rr ON rr.race_id = ra.race_id
            WHERE rr.is_trial = FALSE
        """
        params = {}
        if state:
            sql += " AND t.state = :state"
            params["state"] = state
        sql += " GROUP BY t.name, t.state ORDER BY total_races DESC"

        result = db.execute(text(sql), params)
        return [{
            "name": r[0], "state": r[1],
            "total_races": r[2], "total_horses": r[3],
            "avg_field_size": float(r[4]) if r[4] else None,
        } for r in result]
