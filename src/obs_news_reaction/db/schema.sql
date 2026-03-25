CREATE TABLE IF NOT EXISTS announcements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id TEXT UNIQUE NOT NULL,
    ticker TEXT NOT NULL,
    published_at TEXT NOT NULL,
    category TEXT NOT NULL,
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    issuer_name TEXT,
    fetched_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS price_bars (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    interval TEXT NOT NULL CHECK(interval IN ('1m', '5m', '1d')),
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume INTEGER NOT NULL,
    fetched_at TEXT DEFAULT (datetime('now')),
    UNIQUE(ticker, timestamp, interval)
);

CREATE TABLE IF NOT EXISTS stock_meta (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT UNIQUE NOT NULL,
    company_name TEXT NOT NULL,
    market_cap REAL,
    avg_daily_volume REAL,
    sector TEXT,
    industry TEXT,
    market_cap_bucket TEXT,
    volume_bucket TEXT,
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS event_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    announcement_id INTEGER NOT NULL REFERENCES announcements(id),
    ticker TEXT NOT NULL,
    window_name TEXT NOT NULL,
    abnormal_return REAL,
    cumulative_ar REAL,
    reaction_time_seconds INTEGER,
    pre_event_mean REAL,
    pre_event_std REAL,
    benchmark_return REAL,
    data_quality TEXT,
    computed_at TEXT DEFAULT (datetime('now')),
    UNIQUE(announcement_id, window_name)
);

CREATE TABLE IF NOT EXISTS fetch_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL CHECK(source IN ('newsweb', 'yfinance_price', 'yfinance_meta')),
    ticker TEXT,
    started_at TEXT DEFAULT (datetime('now')),
    completed_at TEXT,
    records_fetched INTEGER DEFAULT 0,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_announcements_ticker ON announcements(ticker, published_at);
CREATE INDEX IF NOT EXISTS idx_announcements_published ON announcements(published_at);
CREATE INDEX IF NOT EXISTS idx_price_bars_ticker ON price_bars(ticker, timestamp, interval);
CREATE INDEX IF NOT EXISTS idx_event_results_announcement ON event_results(announcement_id);
CREATE INDEX IF NOT EXISTS idx_stock_meta_bucket ON stock_meta(market_cap_bucket, volume_bucket);
