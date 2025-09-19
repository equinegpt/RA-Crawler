# api/db.py
from __future__ import annotations
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from .settings import settings

class Base(DeclarativeBase):
    pass

engine = create_engine(settings.DATABASE_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

def get_session():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_db() -> None:
    # Import models here so they register with Base before create_all
    from . import models  # noqa: F401
    Base.metadata.create_all(bind=engine)
