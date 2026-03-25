"""Tests for Oslo timezone utilities."""

from datetime import datetime, timezone, time
from zoneinfo import ZoneInfo

from obs_news_reaction.utils.oslo_tz import to_utc, to_oslo, is_trading_hours, OSLO_TZ


def test_to_utc_from_oslo():
    dt = datetime(2025, 6, 15, 10, 0, tzinfo=OSLO_TZ)  # CEST = UTC+2
    utc = to_utc(dt)
    assert utc.tzinfo == timezone.utc
    assert utc.hour == 8


def test_to_utc_naive_assumes_oslo():
    dt = datetime(2025, 1, 15, 10, 0)  # CET = UTC+1
    utc = to_utc(dt)
    assert utc.hour == 9


def test_to_oslo_from_utc():
    dt = datetime(2025, 6, 15, 8, 0, tzinfo=timezone.utc)
    oslo = to_oslo(dt)
    assert oslo.hour == 10


def test_is_trading_hours_weekday():
    # Tuesday 10:00 Oslo = trading hours
    dt = datetime(2025, 6, 17, 10, 0, tzinfo=OSLO_TZ)
    assert is_trading_hours(dt) is True


def test_is_trading_hours_before_open():
    dt = datetime(2025, 6, 17, 8, 30, tzinfo=OSLO_TZ)
    assert is_trading_hours(dt) is False


def test_is_trading_hours_after_close():
    dt = datetime(2025, 6, 17, 16, 30, tzinfo=OSLO_TZ)
    assert is_trading_hours(dt) is False


def test_is_trading_hours_weekend():
    # Saturday
    dt = datetime(2025, 6, 14, 10, 0, tzinfo=OSLO_TZ)
    assert is_trading_hours(dt) is False
