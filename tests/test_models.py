"""Tests for data models and DB operations."""

import sqlite3
import tempfile
from pathlib import Path

import pytest

from obs_news_reaction.db.schema import init_db, get_connection
from obs_news_reaction.db.operations import (
    insert_announcement,
    get_announcement_by_message_id,
    get_known_message_ids,
    insert_price_bars,
    get_price_bars,
    upsert_stock_meta,
    get_stock_meta,
    get_db_stats,
)
from obs_news_reaction.models import Announcement, PriceBar, StockMeta


@pytest.fixture
def db_path(tmp_path):
    path = tmp_path / "test.db"
    init_db(path)
    return path


@pytest.fixture
def conn(db_path):
    c = get_connection(db_path)
    yield c
    c.close()


def test_insert_and_get_announcement(conn):
    row_id = insert_announcement(
        message_id="MSG001", ticker="EQNR", published_at="2025-01-01T10:00:00+00:00",
        category="INSIDER", title="CEO buys shares", url="https://example.com/1",
        conn=conn,
    )
    assert row_id is not None
    ann = get_announcement_by_message_id("MSG001", conn=conn)
    assert ann is not None
    assert ann.ticker == "EQNR"
    assert ann.category == "INSIDER"


def test_duplicate_announcement_ignored(conn):
    insert_announcement(
        message_id="MSG002", ticker="DNB", published_at="2025-01-01T10:00:00+00:00",
        category="OTHER", title="Test", url="https://example.com/2", conn=conn,
    )
    dup = insert_announcement(
        message_id="MSG002", ticker="DNB", published_at="2025-01-01T10:00:00+00:00",
        category="OTHER", title="Test", url="https://example.com/2", conn=conn,
    )
    assert dup is None


def test_known_message_ids(conn):
    insert_announcement(
        message_id="A1", ticker="NHY", published_at="2025-01-01T10:00:00+00:00",
        category="OTHER", title="T", url="u", conn=conn,
    )
    insert_announcement(
        message_id="A2", ticker="NHY", published_at="2025-01-02T10:00:00+00:00",
        category="OTHER", title="T", url="u", conn=conn,
    )
    ids = get_known_message_ids(conn=conn)
    assert ids == {"A1", "A2"}


def test_insert_and_get_price_bars(conn):
    bars = [
        {"ticker": "EQNR.OL", "timestamp": "2025-01-01T10:00:00+00:00",
         "interval": "1m", "open": 100.0, "high": 101.0, "low": 99.5,
         "close": 100.5, "volume": 1000},
        {"ticker": "EQNR.OL", "timestamp": "2025-01-01T10:01:00+00:00",
         "interval": "1m", "open": 100.5, "high": 102.0, "low": 100.0,
         "close": 101.5, "volume": 1500},
    ]
    n = insert_price_bars(bars, conn=conn)
    assert n == 2
    result = get_price_bars("EQNR.OL", interval="1m", conn=conn)
    assert len(result) == 2
    assert result[0].open == 100.0


def test_upsert_stock_meta(conn):
    upsert_stock_meta(
        ticker="EQNR.OL", company_name="Equinor ASA", market_cap=5e11,
        sector="Energy", conn=conn,
    )
    meta = get_stock_meta("EQNR.OL", conn=conn)
    assert meta is not None
    assert meta.company_name == "Equinor ASA"

    # Update
    upsert_stock_meta(
        ticker="EQNR.OL", company_name="Equinor ASA", market_cap=6e11,
        sector="Energy", conn=conn,
    )
    meta2 = get_stock_meta("EQNR.OL", conn=conn)
    assert meta2.market_cap == 6e11


def test_db_stats(conn):
    insert_announcement(
        message_id="S1", ticker="TEL", published_at="2025-03-01T10:00:00+00:00",
        category="OTHER", title="T", url="u", conn=conn,
    )
    stats = get_db_stats(conn=conn)
    assert stats["announcements_count"] == 1
    assert stats["distinct_tickers"] == 1
