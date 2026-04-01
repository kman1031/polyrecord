"""
Shared utilities: time helpers, reconnection logic, logging setup.
"""

import asyncio
import logging
import time
from datetime import datetime, timezone, timedelta
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from config import (
    DATA_DIR,
    RECONNECT_BASE_DELAY,
    RECONNECT_MAX_DELAY,
    RECONNECT_MULTIPLIER,
)


# ── Time helpers ─────────────────────────────────────────────


def utc_now_ms() -> int:
    """Current UTC time in milliseconds."""
    return int(time.time() * 1000)


def utc_now() -> datetime:
    """Current UTC datetime."""
    return datetime.now(timezone.utc)


def today_utc() -> str:
    """Today's date string in UTC, e.g. '2026-04-02'."""
    return utc_now().strftime("%Y-%m-%d")


def current_hour_start_ms() -> int:
    """Start of the current UTC hour in milliseconds."""
    now = utc_now()
    hour_start = now.replace(minute=0, second=0, microsecond=0)
    return int(hour_start.timestamp() * 1000)


def next_hour_start_ms() -> int:
    """Start of the next UTC hour in milliseconds."""
    now = utc_now()
    next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    return int(next_hour.timestamp() * 1000)


def seconds_until_next_day() -> float:
    """Seconds until UTC midnight."""
    now = utc_now()
    midnight = (now + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return (midnight - now).total_seconds()


def seconds_until_next_hour() -> float:
    """Seconds until the next UTC hour."""
    now = utc_now()
    next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    return (next_hour - now).total_seconds()


def ms_to_iso(ts_ms: int) -> str:
    """Convert millisecond timestamp to ISO 8601 string."""
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()


# ── Reconnection helper ─────────────────────────────────────


class Reconnector:
    """Exponential backoff reconnection helper."""

    def __init__(
        self,
        base: float = RECONNECT_BASE_DELAY,
        cap: float = RECONNECT_MAX_DELAY,
        multiplier: float = RECONNECT_MULTIPLIER,
    ):
        self._base = base
        self._cap = cap
        self._multiplier = multiplier
        self._attempt = 0

    def reset(self):
        """Call after a successful connection."""
        self._attempt = 0

    async def wait(self):
        """Wait with exponential backoff. Returns the delay used."""
        delay = min(self._base * (self._multiplier**self._attempt), self._cap)
        self._attempt += 1
        await asyncio.sleep(delay)
        return delay

    @property
    def attempt(self) -> int:
        return self._attempt


# ── Logging setup ────────────────────────────────────────────


def setup_logging(
    name: str = "polyrecord", level: int = logging.INFO
) -> logging.Logger:
    """
    Configure logging: stdout + daily rotating file.
    Log files go to DATA_DIR/logs/.
    """
    log_dir = DATA_DIR / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Prevent duplicate handlers on repeated calls
    if logger.handlers:
        return logger

    fmt = logging.Formatter(
        "%(asctime)s.%(msecs)03d [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fmt.converter = time.gmtime  # UTC timestamps in logs

    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # File handler – daily rotation, keep 30 days
    fh = TimedRotatingFileHandler(
        log_dir / f"{name}.log",
        when="midnight",
        interval=1,
        backupCount=30,
        utc=True,
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger
