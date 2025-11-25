from __future__ import annotations

from typing import Optional
from datetime import date, datetime

from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    Date,
    DateTime,
    Boolean,
    Numeric,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from .db import Base


class RaceProgram(Base):
    __tablename__ = "race_program"

    id = Column(Integer, primary_key=True, index=True)
    race_no = Column(Integer, nullable=True)

    # Stored as TEXT "YYYY-MM-DD" to match the existing table
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


# --- NEW: Racing Australia results model -------------------------------------


class RAResult(Base):
    __tablename__ = "ra_results"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)

    # Using a proper DATE here – API can still send/receive "YYYY-MM-DD"
    meeting_date = Column(Date, nullable=False, index=True)

    state = Column(String(3), nullable=False, index=True)   # "VIC", "NSW", etc.
    track = Column(String, nullable=False, index=True)      # Normalised track name

    race_no = Column(Integer, nullable=False)               # 1..10
    horse_number = Column(Integer, nullable=False)          # TAB / saddle number
    horse_name = Column(String, nullable=False)

    # Results info
    finishing_pos = Column(Integer, nullable=True)          # 1,2,3,...; None if scratched
    is_scratched = Column(Boolean, nullable=False, server_default="false")

    margin_lens = Column(Numeric(5, 2), nullable=True)      # e.g. 1.25
    starting_price = Column(Numeric(8, 2), nullable=True)   # optional if we can grab it

    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint(
            "meeting_date",
            "state",
            "track",
            "race_no",
            "horse_number",
            name="uq_ra_results_runner",
        ),
    )
