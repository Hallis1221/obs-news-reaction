"""yfinance wrapper with tiered intervals and caching."""

from __future__ import annotations

import logging
import time as _time
from datetime import datetime, timedelta, timezone

import pandas as pd
import yfinance as yf

from obs_news_reaction.config import (
    BENCHMARK_TICKER,
    BENCHMARK_FALLBACK,
    YFINANCE_RATE_LIMIT,
)
from obs_news_reaction.db.operations import (
    insert_price_bars,
    get_price_bar_range,
    log_fetch_start,
    log_fetch_end,
)

log = logging.getLogger(__name__)


def _ol_ticker(ticker: str) -> str:
    t = ticker.strip().upper()
    if not t.endswith(".OL"):
        t += ".OL"
    return t


def best_available_interval(event_age_days: float) -> str:
    if event_age_days <= 7:
        return "1m"
    elif event_age_days <= 60:
        return "5m"
    return "1d"


def fetch_prices(
    ticker: str, start: datetime, end: datetime, interval: str = "1m",
) -> list[dict]:
    ol_ticker = _ol_ticker(ticker)
    log.info(f"Fetching {ol_ticker} {interval}: {start.date()} to {end.date()}")

    try:
        data = yf.download(
            ol_ticker, start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"),
            interval=interval, progress=False, auto_adjust=True,
        )
    except Exception as e:
        log.error(f"yfinance failed for {ol_ticker}: {e}")
        return []

    if data.empty:
        log.warning(f"No data for {ol_ticker} ({interval})")
        return []

    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(0)

    bars = []
    for ts, row in data.iterrows():
        ts_dt = pd.Timestamp(ts)
        if ts_dt.tzinfo is None:
            ts_dt = ts_dt.tz_localize("UTC")
        else:
            ts_dt = ts_dt.tz_convert("UTC")
        bars.append({
            "ticker": ol_ticker,
            "timestamp": ts_dt.isoformat(),
            "interval": interval,
            "open": float(row["Open"]),
            "high": float(row["High"]),
            "low": float(row["Low"]),
            "close": float(row["Close"]),
            "volume": int(row.get("Volume", 0)),
        })
    log.info(f"Got {len(bars)} bars for {ol_ticker}")
    return bars


def backfill_prices_for_ticker(ticker: str) -> int:
    ol_ticker = _ol_ticker(ticker)
    total = 0
    now = datetime.now(timezone.utc)
    fetch_id = log_fetch_start("yfinance_price", ol_ticker)
    error = None

    try:
        # 1m bars: last 7 days
        _, mx = get_price_bar_range(ol_ticker, "1m")
        if mx is None or datetime.fromisoformat(mx) < now - timedelta(hours=1):
            bars = fetch_prices(ticker, now - timedelta(days=7), now, "1m")
            total += insert_price_bars(bars)
            _time.sleep(YFINANCE_RATE_LIMIT)

        # 5m bars: last 60 days
        _, mx = get_price_bar_range(ol_ticker, "5m")
        if mx is None or datetime.fromisoformat(mx) < now - timedelta(hours=1):
            bars = fetch_prices(ticker, now - timedelta(days=60), now, "5m")
            total += insert_price_bars(bars)
            _time.sleep(YFINANCE_RATE_LIMIT)

        # 1d bars: last 2 years
        _, mx = get_price_bar_range(ol_ticker, "1d")
        if mx is None or datetime.fromisoformat(mx) < now - timedelta(days=1):
            bars = fetch_prices(ticker, now - timedelta(days=730), now, "1d")
            total += insert_price_bars(bars)

    except Exception as e:
        error = str(e)
        log.error(f"Backfill failed for {ol_ticker}: {e}")
    finally:
        log_fetch_end(fetch_id, records_fetched=total, error_message=error)
    return total


def fetch_benchmark(start: datetime, end: datetime, interval: str = "1m") -> list[dict]:
    bars = fetch_prices(BENCHMARK_TICKER.replace(".OL", ""), start, end, interval)
    if not bars:
        bars = fetch_prices(BENCHMARK_FALLBACK.replace(".OL", ""), start, end, interval)
    return bars


def backfill_benchmark() -> int:
    total = 0
    for bm in [BENCHMARK_TICKER, BENCHMARK_FALLBACK]:
        n = backfill_prices_for_ticker(bm.replace(".OL", ""))
        total += n
        if n > 0:
            break
        _time.sleep(YFINANCE_RATE_LIMIT)
    return total
