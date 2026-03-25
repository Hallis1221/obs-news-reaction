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

### Buy/Sell Split (full body parsing, 66 messages)

All 66 PDMR message bodies parsed: **32 BUY, 16 SELL, 18 UNKNOWN**.

| Action | N (with price data) | Mean 24h | Median | Win% |
|--------|-------------------|---------|--------|------|
| **Insider BUYS** | 11 | **+5.11%** | 0.00% | 45% |
| Insider SELLS | 8 | +2.02% | +1.43% | 88% |
| **Spread** | — | **+3.09%** | — | — |

**Key insight**: Insider buys are **fat-tailed**, not consistent. Median is 0% but outliers are huge (CODE +23.4%, ACED +22.0%, AFISH +10.4%). Win rate is only 45%.

### Short-Side Signals (strongest findings)

| Strategy | Trades | Mean Net | Win% | Sharpe |
|----------|--------|---------|------|--------|
| **SHORT: Inside Info** | 3 | **+10.80%** | 67% | 8.97 |
| **SHORT: GM Notices** | 6 | **+4.21%** | 67% | 7.78 |
| Gap fade (long) | 8 | +5.26% | 75% | 12.37 |

- **Inside Information** (private placements, dilution): stock drops -9.5% avg
- **General Meeting Notices**: 0% positive rate — always negative (-3.01% mean)

### Enriched Subcategory Analysis (124 announcements)

| Subcategory | N | Mean | Win% |
|-------------|---|------|------|
| INSIDER: Unclassified Trade | 13 | +3.70% | 69% |
| REG: Financial Report | 1 | +10.95% | 100% |
| PR: Contract Award | 4 | +0.72% | 75% |
| REG: GM Notice | 6 | **-3.01%** | **0%** |
| BUYBACK: Share Repurchase | 8 | -0.46% | 38% |
| EX DATE | 3 | -2.53% | 0% |
| INSIDE INFORMATION | 3 | **-9.54%** | 33% |

### Validated: No Category-Based Edge (1m data)

Independent 1-minute validation (obs-react, 60-day dataset):
- **GM Notices**: 20 events → 8 positive, 12 negative, mean +0.08%. **No edge.** Our "always negative" was small-sample bias (6 trades).
- **Insider trades (all)**: -1.35% to -2.38% mean net. **No edge.**
- **Mean-reversion on >1% overreactions**: This is the only surviving strategy — it works across categories because it's a **microstructure effect**, not a news-type effect.

### Mean-Reversion Parameter Sweep (the surviving edge)

Full sweep across 54 configurations (6 thresholds × 3 directions × 3 hold periods):

| Strategy | Trades | Net@50k | Win% | Sharpe |
|----------|--------|---------|------|--------|
| **Buy >3% gap-down, hold 1d** | 15 | **+5.16%** | **80%** | **9.83** |
| Buy >5% gap-down, intraday | 5 | +6.76% | 100% | 13.73 |
| Fade both >3%, intraday | 37 | +3.34% | 76% | 5.17 |
| Buy >2% gap-down, hold 1d | 24 | +2.52% | 62% | 4.88 |
| Buy >1% gap-down, hold 1d | 39 | +2.11% | 62% | 4.63 |

**Key pattern**: Buying gap-downs works at every threshold. Shorting gap-ups only works intraday — fails overnight. The asymmetry: negative overreactions correct, positive ones stick.

**Results strengthen with more data** (75k bars vs initial 35k).

### Conclusion

The edge is **not in the announcement category** but in the **magnitude of price reaction**:

1. **Category-based signals fail** 1m validation (insider trades, GM notices, inside info — all noise)
2. **Mean-reversion on gap-downs is the only surviving edge** — microstructure overreaction → correction
3. **Practical strategy**: Buy Oslo Børs stocks that gap down >3% on announcement days, hold 1 day
4. **Survives Nordnet costs** at 50k+ NOK positions (+5.16% net, Sharpe 9.83)

### Remaining Questions

- Does this hold with 1-minute entry timing? (awaiting obs-react validation)
- Is the 80% win rate stable out-of-sample?
- Can we improve by filtering to liquid stocks only?
- How does borrow cost affect the intraday short-side component?

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
