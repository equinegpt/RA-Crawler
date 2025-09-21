# api/races.py
from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from .db import get_engine

races_router = APIRouter()

def _row_to_dict(row):
    # row is a Mapping (mappings().all()); treat missing keys safely
    d = dict(row)
    # Ensure distance_m is present in JSON as whatever is in DB (int or None)
    return {
        "id": d.get("id"),
        "race_no": d.get("race_no"),
        "date": d.get("date"),
        "state": d.get("state"),
        "track": d.get("track"),
        "type": d.get("type"),
        "description": d.get("description"),
        "prize": d.get("prize"),
        "condition": d.get("condition"),
        "class": d.get("class"),
        "age": d.get("age"),
        "sex": d.get("sex"),
        "distance_m": d.get("distance_m"),   # <- keep as-is
        "bonus": d.get("bonus"),
        "url": d.get("url"),
    }

@races_router.get("/races")
def list_races(
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    sql = """
        SELECT
            id, race_no, date, state, track, type, description, prize,
            condition, class, age, sex, distance_m, bonus, url
        FROM race_program
        ORDER BY 
            CASE WHEN date IS NULL THEN 1 ELSE 0 END,
            date, state, track, race_no
        LIMIT :limit OFFSET :offset
    """
    params = {"limit": limit, "offset": offset}
    eng = get_engine()
    with eng.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
    return [_row_to_dict(r) for r in rows]
