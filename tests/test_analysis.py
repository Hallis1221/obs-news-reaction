"""Tests for analysis engine with synthetic data."""

import pytest

from obs_news_reaction.models import PriceBar, Announcement
from obs_news_reaction.analysis.engine import (
    compute_returns,
    compute_abnormal_return,
    find_reaction_time,
)
from datetime import datetime, timezone


def _bar(close: float, timestamp: str = "2025-01-01T10:00:00+00:00", **kw) -> PriceBar:
    defaults = dict(
        id=0, ticker="TEST.OL", timestamp=timestamp, interval="1m",
        open=close, high=close + 1, low=close - 1, close=close,
        volume=1000, fetched_at="2025-01-01",
    )
    defaults.update(kw)
    return PriceBar(**defaults)


def test_compute_returns_basic():
    bars = [_bar(100), _bar(105), _bar(110)]
    rets = compute_returns(bars)
    assert len(rets) == 2
    assert abs(rets[0] - 0.05) < 1e-9
    assert abs(rets[1] - (110 / 105 - 1)) < 1e-9


def test_compute_returns_empty():
    assert compute_returns([]) == []
    assert compute_returns([_bar(100)]) == []


def test_compute_abnormal_return_with_benchmark():
    stock = [_bar(100), _bar(110)]  # 10% return
    bench = [_bar(100), _bar(102)]  # 2% return
    ar = compute_abnormal_return(stock, bench)
    assert ar is not None
    assert abs(ar - 0.08) < 1e-9


def test_compute_abnormal_return_no_benchmark():
    stock = [_bar(100), _bar(110)]  # 10% return
    ar = compute_abnormal_return(stock, [])
    assert ar is not None
    assert abs(ar - 0.10) < 1e-9


def test_compute_abnormal_return_insufficient_bars():
    assert compute_abnormal_return([], []) is None
    assert compute_abnormal_return([_bar(100)], []) is None


def test_find_reaction_time_detects_spike():
    event_time = datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc)
    bars = [
        _bar(100, "2025-01-01T09:59:00+00:00"),
        _bar(100.1, "2025-01-01T10:01:00+00:00"),
        _bar(100.2, "2025-01-01T10:02:00+00:00"),
        _bar(115, "2025-01-01T10:03:00+00:00"),  # Big spike
    ]
    # pre_mean=0, pre_std=0.01, threshold=2*0.01=0.02
    rt = find_reaction_time(bars, event_time, pre_mean=0.0, pre_std=0.01, threshold_sigma=2.0)
    assert rt is not None
    assert rt == 180  # 3 minutes = 180 seconds


def test_find_reaction_time_no_spike():
    event_time = datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc)
    bars = [
        _bar(100, "2025-01-01T09:59:00+00:00"),
        _bar(100.01, "2025-01-01T10:01:00+00:00"),
        _bar(100.02, "2025-01-01T10:02:00+00:00"),
    ]
    rt = find_reaction_time(bars, event_time, pre_mean=0.0, pre_std=0.01, threshold_sigma=2.0)
    assert rt is None


def test_find_reaction_time_zero_std():
    event_time = datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc)
    bars = [_bar(100, "2025-01-01T10:01:00+00:00")]
    assert find_reaction_time(bars, event_time, 0.0, 0.0, 2.0) is None
