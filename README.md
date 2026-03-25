# obs-news-reaction

**Oslo Bors News Reaction Timer** - measures stock price reaction speed to Oslo Stock Exchange announcements and identifies exploitable alpha signals.

## Findings

### Current Status: No Confirmed Alpha

Independent validation using 60-day 1-minute data (31 insider trades, obs-react project) shows
**insider trade (PDMR) notifications do NOT generate positive returns** after Nordnet transaction costs:

| Position Size | Mean Net Return | Win Rate |
|--------------|----------------|----------|
| 15,000 NOK | -2.38% | 3% |
| 50,000 NOK | -1.35% | 32% |

Our initial daily-bar backtest (9 trades, Sharpe 6.64) was a **small-sample artifact** dominated
by 2 outlier trades. Key issues:

1. **Daily bars mask intraday timing** — entry/exit prices are imprecise
2. **No buy/sell distinction** — going long on insider SELLS destroys returns
3. **79 NOK minimum commission** (Nordnet) kills alpha on positions under 50k NOK
4. **Small samples** (9 trades) produce unreliable Sharpe ratios

### What We Learned

- Oslo Børs announcement-day returns are ~1.7% higher than normal days (gross)
- After realistic Nordnet costs (0.049% + 79 NOK min), this edge disappears for most strategies
- Insider trade notifications are NOT a reliable signal without parsing the full message body
- The gap-fade strategy (buying large dips) showed promise but had only 3-4 trades

### Open Questions

- Can parsing PDMR message bodies to isolate genuine **insider buys** (not exercises/sells) recover alpha?
- Does the signal work on larger-cap, more liquid stocks where costs are lower?
- Is there alpha in the **speed** of reaction (first 5 minutes) that daily bars can't capture?

> This project is a research tool, not trading advice. Use at your own risk.

## Quick Start

```bash
pip install -e ".[dev]"

# Scrape latest announcements
obs-nr scrape

# Backfill historical announcements (3 months)
obs-nr backfill-announcements --months 3

# Backfill prices for a ticker
obs-nr backfill EQNR

# Run the full pipeline
obs-nr run

# Check trading signals
obs-nr signals

# Run backtest
obs-nr backtest

# Run historical study
obs-nr study
```

## Architecture

```
src/obs_news_reaction/
  cli.py              # Click CLI with 15+ commands
  config.py           # Configuration and constants
  models.py           # Dataclasses: Announcement, PriceBar, StockMeta, EventResult
  signals.py          # Signal scoring and alerting
  export.py           # CSV/JSON export
  db/
    schema.py         # SQLite setup and migrations
    schema.sql        # Table definitions
    operations.py     # CRUD operations
  news/
    scraper.py        # NewsWeb scraper (API + Playwright DOM)
    poller.py         # Continuous polling loop
    historical.py     # Historical date-range scraper
  prices/
    fetcher.py        # yfinance price fetcher with tiered intervals
    meta.py           # Stock metadata fetcher
  analysis/
    engine.py         # Event study analysis
    historical.py     # Historical reaction study
    backtest.py       # Backtesting framework (6 strategies)
  viz/
    charts.py         # matplotlib visualizations
  utils/
    oslo_tz.py        # Oslo timezone helpers
```

## Data Sources

- **Announcements**: [NewsWeb (Oslo Bors)](https://newsweb.oslobors.no) via Playwright
- **Prices**: [Yahoo Finance](https://finance.yahoo.com) via yfinance (1m, 5m, 1d intervals)
- **Storage**: SQLite (WAL mode, ~30k+ price bars)

## CLI Commands

| Command | Description |
|---------|-------------|
| `obs-nr status` | Database statistics |
| `obs-nr scrape` | Scrape latest announcements |
| `obs-nr poll` | Continuous polling |
| `obs-nr backfill <ticker>` | Backfill price data |
| `obs-nr backfill-benchmark` | Backfill OSEBX/OBX benchmark |
| `obs-nr backfill-announcements` | Historical announcement backfill |
| `obs-nr fetch-meta <ticker>` | Fetch stock metadata |
| `obs-nr analyze` | Run event study analysis |
| `obs-nr signals` | Scan for trading signals |
| `obs-nr backtest` | Run all backtest strategies |
| `obs-nr study` | Historical reaction study |
| `obs-nr results` | Show event results |
| `obs-nr announcements` | List announcements |
| `obs-nr category-stats` | Stats by category |
| `obs-nr export-results <file>` | Export to CSV/JSON |
| `obs-nr plot-reaction <id>` | Plot price reaction |
| `obs-nr plot-summary` | Generate summary charts |
| `obs-nr run` | Full pipeline |

## License

MIT
