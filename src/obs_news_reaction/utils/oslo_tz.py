"""Oslo timezone helpers."""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

OSLO_TZ = ZoneInfo("Europe/Oslo")


def to_utc(dt: datetime) -> datetime:
    """Convert any datetime to UTC."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=OSLO_TZ)
    return dt.astimezone(timezone.utc)


def to_oslo(dt: datetime) -> datetime:
    """Convert any datetime to Oslo time."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(OSLO_TZ)


def is_trading_hours(dt: datetime) -> bool:
    """Check if a datetime falls within Oslo Bors trading hours (09:00-16:20 CET/CEST)."""
    oslo_dt = to_oslo(dt)
    if oslo_dt.weekday() >= 5:  # Saturday/Sunday
        return False
    t = oslo_dt.time()
    from obs_news_reaction.config import (
        OSLO_OPEN_HOUR, OSLO_OPEN_MINUTE,
        OSLO_CLOSE_HOUR, OSLO_CLOSE_MINUTE,
    )
    from datetime import time
    market_open = time(OSLO_OPEN_HOUR, OSLO_OPEN_MINUTE)
    market_close = time(OSLO_CLOSE_HOUR, OSLO_CLOSE_MINUTE)
    return market_open <= t <= market_close
