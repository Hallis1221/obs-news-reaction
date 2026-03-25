"""Data models: Announcement, PriceBar, StockMeta, EventResult."""

from dataclasses import dataclass


@dataclass
class Announcement:
    id: int
    message_id: str
    ticker: str
    published_at: str  # ISO 8601 UTC
    category: str
    title: str
    url: str
    fetched_at: str
    issuer_name: str | None = None


@dataclass
class PriceBar:
    id: int
    ticker: str
    timestamp: str  # ISO 8601 UTC
    interval: str  # "1m", "5m", "1d"
    open: float
    high: float
    low: float
    close: float
    volume: int
    fetched_at: str


@dataclass
class StockMeta:
    id: int
    ticker: str
    company_name: str
    market_cap: float | None
    avg_daily_volume: float | None
    sector: str | None
    industry: str | None
    updated_at: str
    market_cap_bucket: str | None = None
    volume_bucket: str | None = None


@dataclass
class EventResult:
    id: int
    announcement_id: int
    ticker: str
    window_name: str
    abnormal_return: float | None
    cumulative_ar: float | None
    reaction_time_seconds: int | None
    pre_event_mean: float | None
    pre_event_std: float | None
    benchmark_return: float | None
    computed_at: str
    data_quality: str | None = None
