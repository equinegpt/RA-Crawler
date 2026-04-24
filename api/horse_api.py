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
    order_map = {
        "prizemoney": "total_prizemoney DESC NULLS LAST",
        "winners": "winners DESC",
        "win_pct": "win_pct DESC NULLS LAST",
        "sw": "stakes_winners DESC",
        "rating": "avg_best_rating DESC NULLS LAST",
        "runners": "runners DESC",
        "roi": "prize_per_start DESC NULLS LAST",
    }
    order = order_map.get(sort, "total_prizemoney DESC NULLS LAST")
    eng = _get_racing_engine()
    with eng.connect() as db:
        result = db.execute(text(f"""
            WITH sire_horses AS (
                SELECT DISTINCT ON (h.name) h.horse_id, h.sire_name
                FROM horses h WHERE h.sire_name IS NOT NULL
                ORDER BY h.name, h.last_run_date DESC NULLS LAST
            ),
            horse_stats AS (
                SELECT sh.sire_name,
                    COUNT(*) FILTER (WHERE rr.is_trial=FALSE) as starts,
                    SUM(CASE WHEN rr.position=1 AND rr.is_trial=FALSE THEN 1 ELSE 0 END) as wins,
                    SUM(CASE WHEN rr.position<=3 AND rr.is_trial=FALSE THEN 1 ELSE 0 END) as places,
                    COALESCE(SUM(rr.prizemoney_won) FILTER (WHERE rr.is_trial=FALSE),0) as prizemoney,
                    MAX(rr.handicap_rating) FILTER (WHERE rr.is_trial=FALSE) as best_rating,
                    BOOL_OR(ra.race_class ILIKE 'Group%%' OR ra.race_class ILIKE 'Listed%%') FILTER (WHERE rr.is_trial=FALSE) as has_stakes_run,
                    BOOL_OR(rr.position=1 AND (ra.race_class ILIKE 'Group%%' OR ra.race_class ILIKE 'Listed%%')) FILTER (WHERE rr.is_trial=FALSE) as has_stakes_win
                FROM sire_horses sh
                LEFT JOIN race_results rr ON rr.horse_id=sh.horse_id
                LEFT JOIN races ra ON rr.race_id=ra.race_id
                GROUP BY sh.sire_name, sh.horse_id
            )
            SELECT sire_name, COUNT(*) as runners,
                SUM(CASE WHEN wins>0 THEN 1 ELSE 0 END) as winners,
                SUM(starts) as total_starts, SUM(wins) as total_wins, SUM(places) as total_places,
                ROUND(100.0*SUM(CASE WHEN wins>0 THEN 1 ELSE 0 END)/COUNT(*),1) as winners_to_runners,
                ROUND(100.0*SUM(wins)/NULLIF(SUM(starts),0),1) as win_pct,
                ROUND(100.0*SUM(places)/NULLIF(SUM(starts),0),1) as place_pct,
                SUM(prizemoney) as total_prizemoney,
                ROUND(SUM(prizemoney)/NULLIF(COUNT(*),0)) as prize_per_runner,
                ROUND(SUM(prizemoney)/NULLIF(SUM(starts),0)) as prize_per_start,
                ROUND(AVG(best_rating)::numeric,1) as avg_best_rating,
                MAX(best_rating) as peak_rating,
                SUM(CASE WHEN has_stakes_run THEN 1 ELSE 0 END) as stakes_runners,
                SUM(CASE WHEN has_stakes_win THEN 1 ELSE 0 END) as stakes_winners
            FROM horse_stats
            GROUP BY sire_name HAVING COUNT(*)>=:min_runners
            ORDER BY {order} LIMIT :limit
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
    order_map = {
        "prizemoney": "total_prizemoney DESC NULLS LAST",
        "winners": "winners DESC", "win_pct": "win_pct DESC NULLS LAST",
        "runners": "runners DESC",
    }
    order = order_map.get(sort, "total_prizemoney DESC NULLS LAST")
    eng = _get_racing_engine()
    with eng.connect() as db:
        result = db.execute(text(f"""
            WITH bms_horses AS (
                SELECT DISTINCT ON (h.name) h.horse_id, h.sire_of_dam as bms_name
                FROM horses h WHERE h.sire_of_dam IS NOT NULL AND h.sire_of_dam != ''
                ORDER BY h.name, h.last_run_date DESC NULLS LAST
            ),
            horse_stats AS (
                SELECT bh.bms_name,
                    COUNT(*) FILTER (WHERE rr.is_trial=FALSE) as starts,
                    SUM(CASE WHEN rr.position=1 AND rr.is_trial=FALSE THEN 1 ELSE 0 END) as wins,
                    COALESCE(SUM(rr.prizemoney_won) FILTER (WHERE rr.is_trial=FALSE),0) as prizemoney,
                    MAX(rr.handicap_rating) FILTER (WHERE rr.is_trial=FALSE) as best_rating,
                    BOOL_OR(rr.position=1 AND (ra.race_class ILIKE 'Group%%' OR ra.race_class ILIKE 'Listed%%')) FILTER (WHERE rr.is_trial=FALSE) as has_stakes_win
                FROM bms_horses bh
                LEFT JOIN race_results rr ON rr.horse_id=bh.horse_id
                LEFT JOIN races ra ON rr.race_id=ra.race_id
                GROUP BY bh.bms_name, bh.horse_id
            )
            SELECT bms_name, COUNT(*) as runners,
                SUM(CASE WHEN wins>0 THEN 1 ELSE 0 END) as winners,
                SUM(starts), SUM(wins),
                ROUND(100.0*SUM(wins)/NULLIF(SUM(starts),0),1) as win_pct,
                SUM(prizemoney) as total_prizemoney,
                ROUND(AVG(best_rating)::numeric,1),
                SUM(CASE WHEN has_stakes_win THEN 1 ELSE 0 END) as stakes_winners
            FROM horse_stats
            GROUP BY bms_name HAVING COUNT(*)>=:min_runners
            ORDER BY {order} LIMIT :limit
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
    eng = _get_racing_engine()
    with eng.connect() as db:
        result = db.execute(text("""
            WITH sire_dist AS (
                SELECT h.sire_name,
                    CASE WHEN ra.distance<1200 THEN 'sprint'
                         WHEN ra.distance<1600 THEN 'mile'
                         WHEN ra.distance<2000 THEN 'middle'
                         ELSE 'staying' END as band,
                    COUNT(*) as runs,
                    SUM(CASE WHEN rr.position=1 THEN 1 ELSE 0 END) as wins
                FROM horses h
                JOIN race_results rr ON rr.horse_id=h.horse_id
                JOIN races ra ON rr.race_id=ra.race_id
                WHERE h.sire_name IS NOT NULL AND rr.is_trial=FALSE
                GROUP BY h.sire_name, band
            )
            SELECT sire_name,
                SUM(runs) as total_runs,
                SUM(runs) FILTER (WHERE band='sprint') as sprint_runs,
                SUM(wins) FILTER (WHERE band='sprint') as sprint_wins,
                ROUND(100.0*SUM(wins) FILTER (WHERE band='sprint')/NULLIF(SUM(runs) FILTER (WHERE band='sprint'),0),1) as sprint_pct,
                SUM(runs) FILTER (WHERE band='mile') as mile_runs,
                SUM(wins) FILTER (WHERE band='mile') as mile_wins,
                ROUND(100.0*SUM(wins) FILTER (WHERE band='mile')/NULLIF(SUM(runs) FILTER (WHERE band='mile'),0),1) as mile_pct,
                SUM(runs) FILTER (WHERE band='middle') as middle_runs,
                SUM(wins) FILTER (WHERE band='middle') as middle_wins,
                ROUND(100.0*SUM(wins) FILTER (WHERE band='middle')/NULLIF(SUM(runs) FILTER (WHERE band='middle'),0),1) as middle_pct,
                SUM(runs) FILTER (WHERE band='staying') as staying_runs,
                SUM(wins) FILTER (WHERE band='staying') as staying_wins,
                ROUND(100.0*SUM(wins) FILTER (WHERE band='staying')/NULLIF(SUM(runs) FILTER (WHERE band='staying'),0),1) as staying_pct
            FROM sire_dist
            GROUP BY sire_name
            HAVING SUM(runs)>=(SELECT COUNT(DISTINCT h2.horse_id) FROM horses h2 WHERE h2.sire_name=sire_dist.sire_name)*:min_factor
            ORDER BY SUM(runs) DESC LIMIT :limit
        """), {"limit": limit, "min_factor": min_runners})

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
    eng = _get_racing_engine()
    with eng.connect() as db:
        result = db.execute(text("""
            WITH nick_stats AS (
                SELECT h.sire_name, h.sire_of_dam as broodmare_sire,
                    COUNT(DISTINCT h.horse_id) as runners,
                    SUM(CASE WHEN rr.position=1 AND rr.is_trial=FALSE THEN 1 ELSE 0 END) as wins,
                    COUNT(*) FILTER (WHERE rr.is_trial=FALSE) as starts,
                    COALESCE(SUM(rr.prizemoney_won) FILTER (WHERE rr.is_trial=FALSE),0) as prizemoney,
                    ROUND(AVG(rr.handicap_rating) FILTER (WHERE rr.is_trial=FALSE)::numeric,1) as avg_rating
                FROM horses h
                JOIN race_results rr ON rr.horse_id=h.horse_id
                WHERE h.sire_name IS NOT NULL AND h.sire_of_dam IS NOT NULL AND h.sire_of_dam != ''
                GROUP BY h.sire_name, h.sire_of_dam
            )
            SELECT sire_name, broodmare_sire, runners,
                SUM(CASE WHEN wins>0 THEN 1 ELSE 0 END)::int as winners,
                starts, wins,
                ROUND(100.0*wins/NULLIF(starts,0),1) as win_pct,
                prizemoney, avg_rating
            FROM nick_stats
            WHERE runners>=:min_runners
            GROUP BY sire_name, broodmare_sire, runners, starts, wins, prizemoney, avg_rating
            ORDER BY prizemoney DESC LIMIT :limit
        """), {"min_runners": min_runners})

        return [{
            "sire_name": r[0], "broodmare_sire": r[1], "runners": r[2],
            "winners": r[3], "starts": r[4], "wins": r[5],
            "win_pct": float(r[6] or 0), "prizemoney": float(r[7] or 0),
            "avg_rating": float(r[8]) if r[8] else None,
        } for r in result]


@router.get("/breeding/sectionals")
def sectionals(limit: int = 30, min_runs: int = 50):
    return _cached(f"sect_{limit}_{min_runs}",
                   lambda: _sectionals_query(limit, min_runs))

def _sectionals_query(limit, min_runs):
    eng = _get_racing_engine()
    with eng.connect() as db:
        result = db.execute(text("""
            SELECT h.sire_name,
                COUNT(*) as total_runs,
                ROUND(AVG(rr.last_600m)::numeric,2) as avg_last_600m,
                ROUND(AVG(rr.position_800m)::numeric,1),
                ROUND(AVG(rr.position_400m)::numeric,1),
                ROUND(AVG(rr.position_800m - rr.position_400m)::numeric,1) as avg_position_gain,
                SUM(CASE WHEN rr.position=1 THEN 1 ELSE 0 END) as wins
            FROM horses h
            JOIN race_results rr ON rr.horse_id=h.horse_id
            WHERE h.sire_name IS NOT NULL AND rr.is_trial=FALSE AND rr.last_600m IS NOT NULL
            GROUP BY h.sire_name HAVING COUNT(*)>=:min_runs
            ORDER BY AVG(rr.last_600m) ASC LIMIT :limit
        """), {"min_runs": min_runs})

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
    eng = _get_racing_engine()
    with eng.connect() as db:
        result = db.execute(text("""
            WITH horse_stakes AS (
                SELECT h.sire_name, h.horse_id,
                    BOOL_OR(ra.race_class ILIKE 'Group 1%%') as g1_runner,
                    BOOL_OR(rr.position=1 AND ra.race_class ILIKE 'Group 1%%') as g1_winner,
                    BOOL_OR(ra.race_class ILIKE 'Group 2%%') as g2_runner,
                    BOOL_OR(rr.position=1 AND ra.race_class ILIKE 'Group 2%%') as g2_winner,
                    BOOL_OR(ra.race_class ILIKE 'Group 3%%') as g3_runner,
                    BOOL_OR(rr.position=1 AND ra.race_class ILIKE 'Group 3%%') as g3_winner,
                    BOOL_OR(ra.race_class ILIKE 'Listed%%') as listed_runner,
                    BOOL_OR(rr.position=1 AND ra.race_class ILIKE 'Listed%%') as listed_winner
                FROM horses h
                JOIN race_results rr ON rr.horse_id=h.horse_id AND rr.is_trial=FALSE
                JOIN races ra ON rr.race_id=ra.race_id
                WHERE h.sire_name IS NOT NULL
                GROUP BY h.sire_name, h.horse_id
            )
            SELECT sire_name, COUNT(*) as runners,
                SUM(CASE WHEN g1_runner THEN 1 ELSE 0 END),
                SUM(CASE WHEN g1_winner THEN 1 ELSE 0 END),
                SUM(CASE WHEN g2_runner THEN 1 ELSE 0 END),
                SUM(CASE WHEN g2_winner THEN 1 ELSE 0 END),
                SUM(CASE WHEN g3_runner THEN 1 ELSE 0 END),
                SUM(CASE WHEN g3_winner THEN 1 ELSE 0 END),
                SUM(CASE WHEN listed_runner THEN 1 ELSE 0 END),
                SUM(CASE WHEN listed_winner THEN 1 ELSE 0 END),
                ROUND(100.0*SUM(CASE WHEN g1_runner OR g2_runner OR g3_runner OR listed_runner THEN 1 ELSE 0 END)/COUNT(*),1) as stakes_reach_pct
            FROM horse_stakes
            GROUP BY sire_name HAVING COUNT(*)>=:min_runners
            ORDER BY stakes_reach_pct DESC NULLS LAST LIMIT :limit
        """), {"min_runners": min_runners})

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
