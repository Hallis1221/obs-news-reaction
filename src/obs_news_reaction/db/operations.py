"""CRUD operations for all tables."""

from __future__ import annotations

import sqlite3
from typing import TypeVar, Type

from obs_news_reaction.db.schema import get_connection
from obs_news_reaction.models import Announcement, PriceBar, StockMeta, EventResult

T = TypeVar("T")


def _row_to(cls: Type[T], row: sqlite3.Row) -> T:
    return cls(**dict(row))


def _get_conn(conn: sqlite3.Connection | None) -> tuple[sqlite3.Connection, bool]:
    if conn is not None:
        return conn, False
    return get_connection(), True


# --- Announcements ---

def insert_announcement(
    message_id: str, ticker: str, published_at: str, category: str,
    title: str, url: str, issuer_name: str | None = None,
    conn: sqlite3.Connection | None = None,
) -> int | None:
    c, close = _get_conn(conn)
    try:
        cur = c.execute(
            """INSERT OR IGNORE INTO announcements
               (message_id, ticker, published_at, category, title, url, issuer_name)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (message_id, ticker, published_at, category, title, url, issuer_name),
        )
        c.commit()
        return cur.lastrowid if cur.rowcount > 0 else None
    finally:
        if close:
            c.close()


def get_announcement_by_message_id(
    message_id: str, conn: sqlite3.Connection | None = None,
) -> Announcement | None:
    c, close = _get_conn(conn)
    try:
        row = c.execute(
            "SELECT * FROM announcements WHERE message_id = ?", (message_id,)
        ).fetchone()
        return _row_to(Announcement, row) if row else None
    finally:
        if close:
            c.close()


def get_known_message_ids(conn: sqlite3.Connection | None = None) -> set[str]:
    c, close = _get_conn(conn)
    try:
        rows = c.execute("SELECT message_id FROM announcements").fetchall()
        return {row["message_id"] for row in rows}
    finally:
        if close:
            c.close()


def get_announcements(
    ticker: str | None = None, since: str | None = None,
    category: str | None = None, limit: int | None = None,
    conn: sqlite3.Connection | None = None,
) -> list[Announcement]:
    c, close = _get_conn(conn)
    try:
        query = "SELECT * FROM announcements WHERE 1=1"
        params: list = []
        if ticker:
            query += " AND ticker = ?"
            params.append(ticker)
        if since:
            query += " AND published_at >= ?"
            params.append(since)
        if category:
            query += " AND category = ?"
            params.append(category)
        query += " ORDER BY published_at DESC"
        if limit:
            query += " LIMIT ?"
            params.append(limit)
        rows = c.execute(query, params).fetchall()
        return [_row_to(Announcement, r) for r in rows]
    finally:
        if close:
            c.close()


# --- Price Bars ---

def insert_price_bars(bars: list[dict], conn: sqlite3.Connection | None = None) -> int:
    if not bars:
        return 0
    c, close = _get_conn(conn)
    try:
        cur = c.executemany(
            """INSERT OR IGNORE INTO price_bars
               (ticker, timestamp, interval, open, high, low, close, volume)
               VALUES (:ticker, :timestamp, :interval, :open, :high, :low, :close, :volume)""",
            bars,
        )
        c.commit()
        return cur.rowcount
    finally:
        if close:
            c.close()


def get_price_bars(
    ticker: str, interval: str | None = None,
    start: str | None = None, end: str | None = None,
    conn: sqlite3.Connection | None = None,
) -> list[PriceBar]:
    c, close = _get_conn(conn)
    try:
        query = "SELECT * FROM price_bars WHERE ticker = ?"
        params: list = [ticker]
        if interval:
            query += " AND interval = ?"
            params.append(interval)
        if start:
            query += " AND timestamp >= ?"
            params.append(start)
        if end:
            query += " AND timestamp <= ?"
            params.append(end)
        query += " ORDER BY timestamp ASC"
        rows = c.execute(query, params).fetchall()
        return [_row_to(PriceBar, r) for r in rows]
    finally:
        if close:
            c.close()


def get_price_bar_range(
    ticker: str, interval: str, conn: sqlite3.Connection | None = None,
) -> tuple[str | None, str | None]:
    c, close = _get_conn(conn)
    try:
        row = c.execute(
            "SELECT MIN(timestamp) as mn, MAX(timestamp) as mx "
            "FROM price_bars WHERE ticker = ? AND interval = ?",
            (ticker, interval),
        ).fetchone()
        return (row["mn"], row["mx"]) if row and row["mn"] else (None, None)
    finally:
        if close:
            c.close()


# --- Stock Meta ---

def upsert_stock_meta(
    ticker: str, company_name: str, market_cap: float | None = None,
    avg_daily_volume: float | None = None, sector: str | None = None,
    industry: str | None = None, market_cap_bucket: str | None = None,
    volume_bucket: str | None = None, conn: sqlite3.Connection | None = None,
) -> int:
    c, close = _get_conn(conn)
    try:
        cur = c.execute(
            """INSERT INTO stock_meta
               (ticker, company_name, market_cap, avg_daily_volume, sector, industry,
                market_cap_bucket, volume_bucket, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
               ON CONFLICT(ticker) DO UPDATE SET
                 company_name=excluded.company_name, market_cap=excluded.market_cap,
                 avg_daily_volume=excluded.avg_daily_volume, sector=excluded.sector,
                 industry=excluded.industry, market_cap_bucket=excluded.market_cap_bucket,
                 volume_bucket=excluded.volume_bucket, updated_at=datetime('now')""",
            (ticker, company_name, market_cap, avg_daily_volume, sector, industry,
             market_cap_bucket, volume_bucket),
        )
        c.commit()
        return cur.lastrowid
    finally:
        if close:
            c.close()


def get_stock_meta(
    ticker: str, conn: sqlite3.Connection | None = None,
) -> StockMeta | None:
    c, close = _get_conn(conn)
    try:
        row = c.execute(
            "SELECT * FROM stock_meta WHERE ticker = ?", (ticker,)
        ).fetchone()
        return _row_to(StockMeta, row) if row else None
    finally:
        if close:
            c.close()


def get_all_stock_meta(conn: sqlite3.Connection | None = None) -> list[StockMeta]:
    c, close = _get_conn(conn)
    try:
        rows = c.execute("SELECT * FROM stock_meta ORDER BY ticker").fetchall()
        return [_row_to(StockMeta, r) for r in rows]
    finally:
        if close:
            c.close()


# --- Event Results ---

def insert_event_result(
    announcement_id: int, ticker: str, window_name: str,
    abnormal_return: float | None = None, cumulative_ar: float | None = None,
    reaction_time_seconds: int | None = None, pre_event_mean: float | None = None,
    pre_event_std: float | None = None, benchmark_return: float | None = None,
    data_quality: str | None = None, conn: sqlite3.Connection | None = None,
) -> int | None:
    c, close = _get_conn(conn)
    try:
        cur = c.execute(
            """INSERT OR REPLACE INTO event_results
               (announcement_id, ticker, window_name, abnormal_return, cumulative_ar,
                reaction_time_seconds, pre_event_mean, pre_event_std, benchmark_return,
                data_quality)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (announcement_id, ticker, window_name, abnormal_return, cumulative_ar,
             reaction_time_seconds, pre_event_mean, pre_event_std, benchmark_return,
             data_quality),
        )
        c.commit()
        return cur.lastrowid
    finally:
        if close:
            c.close()


def get_event_results(
    announcement_id: int | None = None, ticker: str | None = None,
    window_name: str | None = None, conn: sqlite3.Connection | None = None,
) -> list[EventResult]:
    c, close = _get_conn(conn)
    try:
        query = "SELECT * FROM event_results WHERE 1=1"
        params: list = []
        if announcement_id is not None:
            query += " AND announcement_id = ?"
            params.append(announcement_id)
        if ticker:
            query += " AND ticker = ?"
            params.append(ticker)
        if window_name:
            query += " AND window_name = ?"
            params.append(window_name)
        query += " ORDER BY announcement_id, window_name"
        rows = c.execute(query, params).fetchall()
        return [_row_to(EventResult, r) for r in rows]
    finally:
        if close:
            c.close()


# --- Fetch Log ---

def log_fetch_start(
    source: str, ticker: str | None = None,
    conn: sqlite3.Connection | None = None,
) -> int:
    c, close = _get_conn(conn)
    try:
        cur = c.execute(
            "INSERT INTO fetch_log (source, ticker) VALUES (?, ?)", (source, ticker),
        )
        c.commit()
        return cur.lastrowid
    finally:
        if close:
            c.close()


def log_fetch_end(
    log_id: int, records_fetched: int = 0, error_message: str | None = None,
    conn: sqlite3.Connection | None = None,
) -> None:
    c, close = _get_conn(conn)
    try:
        c.execute(
            """UPDATE fetch_log
               SET completed_at = datetime('now'), records_fetched = ?, error_message = ?
               WHERE id = ?""",
            (records_fetched, error_message, log_id),
        )
        c.commit()
    finally:
        if close:
            c.close()


def get_last_fetch(
    source: str, ticker: str | None = None,
    conn: sqlite3.Connection | None = None,
) -> dict | None:
    c, close = _get_conn(conn)
    try:
        query = "SELECT * FROM fetch_log WHERE source = ? AND completed_at IS NOT NULL"
        params: list = [source]
        if ticker:
            query += " AND ticker = ?"
            params.append(ticker)
        query += " ORDER BY completed_at DESC LIMIT 1"
        row = c.execute(query, params).fetchone()
        return dict(row) if row else None
    finally:
        if close:
            c.close()


def get_db_stats(conn: sqlite3.Connection | None = None) -> dict:
    c, close = _get_conn(conn)
    try:
        stats = {}
        for table in ["announcements", "price_bars", "stock_meta", "event_results"]:
            row = c.execute(f"SELECT COUNT(*) as cnt FROM {table}").fetchone()
            stats[f"{table}_count"] = row["cnt"]
        row = c.execute("SELECT COUNT(DISTINCT ticker) as cnt FROM announcements").fetchone()
        stats["distinct_tickers"] = row["cnt"]
        for source in ["newsweb", "yfinance_price", "yfinance_meta"]:
            entry = get_last_fetch(source, conn=c)
            stats[f"last_{source}_fetch"] = entry["completed_at"] if entry else "never"
        row = c.execute(
            "SELECT MIN(published_at) as earliest, MAX(published_at) as latest FROM announcements"
        ).fetchone()
        stats["earliest_announcement"] = row["earliest"] or "N/A"
        stats["latest_announcement"] = row["latest"] or "N/A"
        return stats
    finally:
        if close:
            c.close()
