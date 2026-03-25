"""Stock metadata fetcher using yfinance."""

from __future__ import annotations

import logging
import time as _time

import yfinance as yf

from obs_news_reaction.config import MCAP_BUCKETS, VOLUME_BUCKETS, YFINANCE_RATE_LIMIT
from obs_news_reaction.db.operations import (
    upsert_stock_meta,
    get_known_message_ids,
    get_announcements,
    log_fetch_start,
    log_fetch_end,
)

log = logging.getLogger(__name__)


def _classify_bucket(value: float | None, buckets: dict[str, tuple[float, float]]) -> str | None:
    if value is None:
        return None
    for name, (lo, hi) in buckets.items():
        if lo <= value < hi:
            return name
    return None


def _ol_ticker(ticker: str) -> str:
    t = ticker.strip().upper()
    if not t.endswith(".OL"):
        t += ".OL"
    return t


def fetch_stock_meta(ticker: str) -> dict | None:
    """Fetch metadata for a single ticker from yfinance."""
    ol = _ol_ticker(ticker)
    log.info(f"Fetching metadata for {ol}")

    try:
        info = yf.Ticker(ol).info
    except Exception as e:
        log.error(f"yfinance info failed for {ol}: {e}")
        return None

    if not info or info.get("regularMarketPrice") is None:
        log.warning(f"No info for {ol}")
        return None

    market_cap = info.get("marketCap")
    avg_volume = info.get("averageDailyVolume10Day") or info.get("averageVolume")

    meta = {
        "ticker": ol,
        "company_name": info.get("longName") or info.get("shortName") or ol,
        "market_cap": float(market_cap) if market_cap else None,
        "avg_daily_volume": float(avg_volume) if avg_volume else None,
        "sector": info.get("sector"),
        "industry": info.get("industry"),
        "market_cap_bucket": _classify_bucket(market_cap, MCAP_BUCKETS),
        "volume_bucket": _classify_bucket(avg_volume, VOLUME_BUCKETS),
    }
    return meta


def fetch_and_store_meta(ticker: str) -> bool:
    """Fetch and upsert metadata for a single ticker. Returns True on success."""
    meta = fetch_stock_meta(ticker)
    if meta is None:
        return False
    upsert_stock_meta(**meta)
    log.info(f"Stored meta for {meta['ticker']}: {meta['company_name']} ({meta['market_cap_bucket']})")
    return True


def backfill_all_meta() -> int:
    """Fetch metadata for all tickers in the announcements table."""
    anns = get_announcements()
    tickers = sorted({a.ticker for a in anns})
    log.info(f"Backfilling metadata for {len(tickers)} tickers")

    fetch_id = log_fetch_start("yfinance_meta")
    count = 0
    error = None

    try:
        for ticker in tickers:
            ok = fetch_and_store_meta(ticker)
            if ok:
                count += 1
            _time.sleep(YFINANCE_RATE_LIMIT)
    except Exception as e:
        error = str(e)
        log.error(f"Meta backfill failed: {e}")
    finally:
        log_fetch_end(fetch_id, records_fetched=count, error_message=error)

    log.info(f"Backfilled metadata for {count}/{len(tickers)} tickers")
    return count
