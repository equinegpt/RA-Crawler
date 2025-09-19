from __future__ import annotations
from typing import Optional
from datetime import date, datetime
from sqlalchemy import Integer, String, Date, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column
from .db import Base
# api/models.py
from sqlalchemy import Column, Integer, String, Text
from .db import Base  # use your existing Base

class RaceProgram(Base):
    __tablename__ = "race_program"

    id = Column(Integer, primary_key=True, index=True)
    race_no = Column(Integer, nullable=True)

    # NEW: map the column you added via ALTER TABLE (stored as TEXT 'YYYY-MM-DD')
    date = Column(String, nullable=True)

    state = Column(String, nullable=True)
    track = Column(String, nullable=True)
    type = Column(String, nullable=True)
    description = Column(Text, nullable=True)
    prize = Column(Integer, nullable=True)
    condition = Column(String, nullable=True)

    # 'class' is a reserved word; map it as class_
    class_ = Column("class", String, nullable=True)

    age = Column(String, nullable=True)
    sex = Column(String, nullable=True)
    distance_m = Column(Integer, nullable=True)
    bonus = Column(Text, nullable=True)
    url = Column(Text, nullable=True, index=True)
