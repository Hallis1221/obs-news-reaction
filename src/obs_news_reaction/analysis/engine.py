"""Event study analysis — computes abnormal returns and reaction times."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import numpy as np

from obs_news_reaction.config import (
    EVENT_WINDOWS,
    MIN_BARS_FOR_ANALYSIS,
    PRE_EVENT_LOOKBACK_MINUTES,
    REACTION_THRESHOLD_SIGMA,
)
from obs_news_reaction.db.operations import (
    get_announcements,
    get_price_bars,
    insert_event_result,
    get_event_results,
)
from obs_news_reaction.models import Announcement, PriceBar
from obs_news_reaction.prices.fetcher import best_available_interval

log = logging.getLogger(__name__)


def compute_returns(bars: list[PriceBar]) -> list[float]:
    """Compute simple returns from a series of price bars."""
    if len(bars) < 2:
        return []
    closes = [b.close for b in bars]
    return [(closes[i] / closes[i - 1]) - 1.0 for i in range(1, len(closes))]


def compute_abnormal_return(
    stock_bars: list[PriceBar], benchmark_bars: list[PriceBar],
) -> float | None:
    """Compute abnormal return = stock return - benchmark return over matching period."""
    if not stock_bars or len(stock_bars) < 2:
        return None
    stock_ret = (stock_bars[-1].close / stock_bars[0].close) - 1.0
    if benchmark_bars and len(benchmark_bars) >= 2:
        bench_ret = (benchmark_bars[-1].close / benchmark_bars[0].close) - 1.0
    else:
        bench_ret = 0.0
    return stock_ret - bench_ret


def find_reaction_time(
    bars: list[PriceBar], event_time: datetime,
    pre_mean: float, pre_std: float, threshold_sigma: float,
) -> int | None:
    """Find seconds until first bar exceeding threshold_sigma from pre-event mean."""
    if pre_std == 0:
        return None
    threshold = threshold_sigma * pre_std

    for bar in bars:
        bar_dt = datetime.fromisoformat(bar.timestamp)
        if bar_dt.tzinfo is None:
            bar_dt = bar_dt.replace(tzinfo=timezone.utc)
        if bar_dt <= event_time:
            continue
        ret = (bar.close / bars[0].close) - 1.0 if bars[0].close else 0
        if abs(ret - pre_mean) > threshold:
            delta = bar_dt - event_time
            return int(delta.total_seconds())
    return None


def _assess_quality(n_bars: int, has_benchmark: bool) -> str:
    if n_bars >= 20 and has_benchmark:
        return "good"
    if n_bars >= MIN_BARS_FOR_ANALYSIS:
        return "fair"
    return "poor"


def analyze_announcement(announcement: Announcement) -> list[dict]:
    """Run event study for all windows on a single announcement. Returns results."""
    pub_dt = datetime.fromisoformat(announcement.published_at)
    if pub_dt.tzinfo is None:
        pub_dt = pub_dt.replace(tzinfo=timezone.utc)

    age_days = (datetime.now(timezone.utc) - pub_dt).total_seconds() / 86400
    interval = best_available_interval(age_days)

    # Already analyzed?
    existing = get_event_results(announcement_id=announcement.id)
    existing_windows = {e.window_name for e in existing}

    results = []
    for window_name, pre_min, post_min in EVENT_WINDOWS:
        if window_name in existing_windows:
            continue

        win_start = pub_dt + timedelta(minutes=pre_min)
        win_end = pub_dt + timedelta(minutes=post_min)

        # Fetch stock bars for the window
        stock_bars = get_price_bars(
            ticker=announcement.ticker + ".OL" if not announcement.ticker.endswith(".OL") else announcement.ticker,
            interval=interval,
            start=win_start.isoformat(),
            end=win_end.isoformat(),
        )

        if len(stock_bars) < MIN_BARS_FOR_ANALYSIS:
            log.info(f"  {window_name}: insufficient bars ({len(stock_bars)})")
            continue

        # Pre-event stats for reaction time
        lookback_start = pub_dt - timedelta(minutes=PRE_EVENT_LOOKBACK_MINUTES)
        pre_bars = get_price_bars(
            ticker=stock_bars[0].ticker,
            interval=interval,
            start=lookback_start.isoformat(),
            end=pub_dt.isoformat(),
        )
        pre_returns = compute_returns(pre_bars)
        pre_mean = float(np.mean(pre_returns)) if pre_returns else 0.0
        pre_std = float(np.std(pre_returns)) if pre_returns else 0.0

        # Abnormal return (no benchmark bars for now — would need separate fetch)
        ar = compute_abnormal_return(stock_bars, [])
        car = ar  # Single-window CAR equals AR

        # Reaction time
        reaction_s = find_reaction_time(
            stock_bars, pub_dt, pre_mean, pre_std, REACTION_THRESHOLD_SIGMA,
        )

        quality = _assess_quality(len(stock_bars), False)

        insert_event_result(
            announcement_id=announcement.id,
            ticker=announcement.ticker,
            window_name=window_name,
            abnormal_return=ar,
            cumulative_ar=car,
            reaction_time_seconds=reaction_s,
            pre_event_mean=pre_mean,
            pre_event_std=pre_std,
            benchmark_return=None,
            data_quality=quality,
        )

        results.append({
            "window": window_name,
            "abnormal_return": ar,
            "reaction_time_s": reaction_s,
            "quality": quality,
            "n_bars": len(stock_bars),
        })
        log.info(f"  {window_name}: AR={ar:.4f}, reaction={reaction_s}s, quality={quality}")

    return results


def analyze_all(since: str | None = None) -> int:
    """Analyze all announcements (optionally since a date). Returns count analyzed."""
    announcements = get_announcements(since=since)
    total = 0
    for ann in announcements:
        log.info(f"Analyzing {ann.ticker} [{ann.published_at}]: {ann.title[:50]}")
        results = analyze_announcement(ann)
        if results:
            total += 1
    log.info(f"Analyzed {total} announcements")
    return total
