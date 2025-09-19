# api/settings.py
from __future__ import annotations
import os
from dataclasses import dataclass
from pathlib import Path

try:
    from dotenv import load_dotenv  # optional
    load_dotenv()
except Exception:
    pass

ROOT_DIR = Path(__file__).resolve().parent.parent

@dataclass(frozen=True)
class Settings:
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///./racing.db")
    RA_BASE_URL: str = os.getenv("RA_BASE_URL", "https://www.racingaustralia.horse")
    USER_AGENT: str = os.getenv(
        "USER_AGENT",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36 EquineGPT/1.0"
    )
    REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "20"))
    REQUEST_DELAY_MS: int = int(os.getenv("REQUEST_DELAY_MS", "250"))

settings = Settings()
