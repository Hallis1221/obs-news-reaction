"""CLI entry point for obs-news-reaction."""

from __future__ import annotations

import logging
import sys

import click
from tabulate import tabulate

from obs_news_reaction.db.schema import init_db


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level, format="%(asctime)s %(name)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )


@click.group()
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging")
def cli(verbose: bool) -> None:
    """Oslo Bors News Reaction Timer."""
    _setup_logging(verbose)
    init_db()


@cli.command()
def status() -> None:
    """Show database statistics."""
    from obs_news_reaction.db.operations import get_db_stats
    stats = get_db_stats()
    rows = [[k, v] for k, v in stats.items()]
    click.echo(tabulate(rows, headers=["Metric", "Value"], tablefmt="simple"))


@cli.command()
@click.option("--pages", default=3, help="Max pages to scrape")
def scrape(pages: int) -> None:
    """Scrape latest announcements from NewsWeb."""
    from obs_news_reaction.news.poller import poll_once
    n = poll_once()
    click.echo(f"Inserted {n} new announcements")


@cli.command()
def poll() -> None:
    """Continuously poll NewsWeb for new announcements."""
    from obs_news_reaction.news.poller import poll_loop
    poll_loop()


@cli.command()
@click.argument("ticker")
def backfill(ticker: str) -> None:
    """Backfill price data for a ticker."""
    from obs_news_reaction.prices.fetcher import backfill_prices_for_ticker
    n = backfill_prices_for_ticker(ticker)
    click.echo(f"Inserted {n} price bars for {ticker}")


@cli.command("backfill-benchmark")
def backfill_benchmark() -> None:
    """Backfill benchmark index data."""
    from obs_news_reaction.prices.fetcher import backfill_benchmark as _bf
    n = _bf()
    click.echo(f"Inserted {n} benchmark bars")


@cli.command()
@click.option("--since", default=None, help="Analyze announcements since (ISO date)")
@click.option("--category", default=None, help="Filter by category (e.g. INSIDER, FINANCIAL)")
@click.option("--ticker", default=None, help="Filter by ticker")
def analyze(since: str | None, category: str | None, ticker: str | None) -> None:
    """Run event study analysis on announcements."""
    from obs_news_reaction.analysis.engine import analyze_all
    n = analyze_all(since=since, category=category, ticker=ticker)
    click.echo(f"Analyzed {n} announcements")


@cli.command("category-stats")
@click.option("--window", default="[-5m,+5m]", help="Event window to summarize")
def category_stats_cmd(window: str) -> None:
    """Show aggregate stats per announcement category."""
    from obs_news_reaction.analysis.engine import category_stats
    stats = category_stats(window_name=window)
    if not stats:
        click.echo("No category stats (run analyze first)")
        return
    rows = []
    for cat, s in stats.items():
        rows.append([
            cat, s["count"],
            f"{s['mean_ar']:.4f}" if s["mean_ar"] is not None else "N/A",
            f"{s['median_ar']:.4f}" if s["median_ar"] is not None else "N/A",
            f"{s['mean_reaction_s']:.0f}s" if s["mean_reaction_s"] is not None else "N/A",
        ])
    click.echo(tabulate(
        rows,
        headers=["Category", "Count", "Mean AR", "Median AR", "Mean React."],
        tablefmt="simple",
    ))


@cli.command("study")
def study_cmd() -> None:
    """Run historical reaction study and print findings."""
    from obs_news_reaction.analysis.historical import run_historical_study, print_study
    results = run_historical_study()
    click.echo(print_study(results))


@cli.command("backtest")
def backtest_cmd() -> None:
    """Run all backtesting strategies and compare results."""
    from obs_news_reaction.analysis.backtest import run_all_strategies
    click.echo(run_all_strategies())


@cli.command()
@click.option("--ticker", default=None, help="Filter by ticker")
@click.option("--window", default=None, help="Filter by window name")
def results(ticker: str | None, window: str | None) -> None:
    """Show event study results."""
    from obs_news_reaction.db.operations import get_event_results
    evts = get_event_results(ticker=ticker, window_name=window)
    if not evts:
        click.echo("No results found")
        return
    rows = []
    for e in evts:
        rows.append([
            e.ticker, e.window_name,
            f"{e.abnormal_return:.4f}" if e.abnormal_return is not None else "N/A",
            f"{e.reaction_time_seconds}s" if e.reaction_time_seconds is not None else "N/A",
            e.data_quality or "N/A",
        ])
    click.echo(tabulate(
        rows,
        headers=["Ticker", "Window", "Abn. Return", "Reaction", "Quality"],
        tablefmt="simple",
    ))


@cli.command()
@click.option("--ticker", default=None, help="Filter by ticker")
@click.option("--limit", default=20, help="Max announcements to show")
def announcements(ticker: str | None, limit: int) -> None:
    """List stored announcements."""
    from obs_news_reaction.db.operations import get_announcements
    anns = get_announcements(ticker=ticker, limit=limit)
    if not anns:
        click.echo("No announcements found")
        return
    rows = []
    for a in anns:
        rows.append([a.ticker, a.published_at[:16], a.category, a.title[:60]])
    click.echo(tabulate(
        rows, headers=["Ticker", "Published", "Category", "Title"], tablefmt="simple",
    ))


@cli.command("fetch-meta")
@click.argument("ticker", required=False)
@click.option("--all", "fetch_all", is_flag=True, help="Fetch meta for all known tickers")
def fetch_meta(ticker: str | None, fetch_all: bool) -> None:
    """Fetch stock metadata from yfinance."""
    from obs_news_reaction.prices.meta import fetch_and_store_meta, backfill_all_meta
    if fetch_all:
        n = backfill_all_meta()
        click.echo(f"Fetched metadata for {n} tickers")
    elif ticker:
        ok = fetch_and_store_meta(ticker)
        click.echo(f"{'Success' if ok else 'Failed'}: {ticker}")
    else:
        click.echo("Provide a ticker or use --all")


@cli.command("list-meta")
def list_meta() -> None:
    """List all stored stock metadata."""
    from obs_news_reaction.db.operations import get_all_stock_meta
    metas = get_all_stock_meta()
    if not metas:
        click.echo("No metadata stored")
        return
    rows = []
    for m in metas:
        mcap = f"{m.market_cap/1e9:.1f}B" if m.market_cap else "N/A"
        rows.append([m.ticker, m.company_name[:30], mcap, m.market_cap_bucket or "N/A",
                      m.sector or "N/A"])
    click.echo(tabulate(
        rows, headers=["Ticker", "Company", "MCap", "Bucket", "Sector"], tablefmt="simple",
    ))


@cli.command("plot-reaction")
@click.argument("announcement_id", type=int)
def plot_reaction_cmd(announcement_id: int) -> None:
    """Plot price reaction around an announcement."""
    from obs_news_reaction.viz.charts import plot_reaction
    path = plot_reaction(announcement_id)
    click.echo(f"Saved: {path}")


@cli.command("plot-summary")
def plot_summary_cmd() -> None:
    """Generate all summary charts."""
    from obs_news_reaction.viz.charts import plot_summary
    paths = plot_summary()
    for p in paths:
        click.echo(f"Saved: {p}")
    if not paths:
        click.echo("No plots generated (need event results first)")


@cli.command("export-announcements")
@click.argument("output")
@click.option("--format", "fmt", type=click.Choice(["csv", "json"]), default="csv")
@click.option("--ticker", default=None)
@click.option("--since", default=None)
def export_announcements_cmd(output: str, fmt: str, ticker: str | None, since: str | None) -> None:
    """Export announcements to CSV or JSON."""
    from pathlib import Path
    from obs_news_reaction.export import export_announcements_csv, export_announcements_json
    p = Path(output)
    if fmt == "json":
        n = export_announcements_json(p, ticker=ticker, since=since)
    else:
        n = export_announcements_csv(p, ticker=ticker, since=since)
    click.echo(f"Exported {n} announcements to {p}")


@cli.command("export-results")
@click.argument("output")
@click.option("--format", "fmt", type=click.Choice(["csv", "json"]), default="csv")
@click.option("--ticker", default=None)
@click.option("--window", default=None)
def export_results_cmd(output: str, fmt: str, ticker: str | None, window: str | None) -> None:
    """Export event results to CSV or JSON."""
    from pathlib import Path
    from obs_news_reaction.export import export_results_csv, export_results_json
    p = Path(output)
    if fmt == "json":
        n = export_results_json(p, ticker=ticker, window_name=window)
    else:
        n = export_results_csv(p, ticker=ticker, window_name=window)
    click.echo(f"Exported {n} results to {p}")


@cli.command()
@click.option("--pages", "scrape_pages", default=3, help="Max scrape pages")
@click.option("--skip-plots", is_flag=True, help="Skip plot generation")
def run(scrape_pages: int, skip_plots: bool) -> None:
    """Full pipeline: scrape → backfill prices → fetch meta → analyze → plot."""
    import time as _time

    from obs_news_reaction.news.poller import poll_once
    from obs_news_reaction.prices.fetcher import (
        backfill_prices_for_ticker,
        backfill_benchmark,
    )
    from obs_news_reaction.prices.meta import backfill_all_meta
    from obs_news_reaction.analysis.engine import analyze_all
    from obs_news_reaction.db.operations import get_announcements

    click.echo("=== Step 1/5: Scraping announcements ===")
    n_ann = poll_once()
    click.echo(f"  {n_ann} new announcements")

    click.echo("=== Step 2/5: Backfilling prices ===")
    anns = get_announcements()
    tickers = sorted({a.ticker for a in anns})
    total_bars = 0
    for ticker in tickers:
        bars = backfill_prices_for_ticker(ticker)
        total_bars += bars
        if bars:
            click.echo(f"  {ticker}: {bars} bars")
        _time.sleep(0.5)
    click.echo(f"  Total: {total_bars} bars across {len(tickers)} tickers")

    click.echo("=== Step 3/5: Backfilling benchmark ===")
    n_bench = backfill_benchmark()
    click.echo(f"  {n_bench} benchmark bars")

    click.echo("=== Step 4/5: Fetching metadata ===")
    n_meta = backfill_all_meta()
    click.echo(f"  Metadata for {n_meta} tickers")

    click.echo("=== Step 5/5: Running analysis ===")
    n_analyzed = analyze_all()
    click.echo(f"  Analyzed {n_analyzed} announcements")

    if not skip_plots:
        click.echo("=== Generating plots ===")
        from obs_news_reaction.viz.charts import plot_summary
        paths = plot_summary()
        for p in paths:
            click.echo(f"  Saved: {p}")

    click.echo("=== Pipeline complete ===")


if __name__ == "__main__":
    cli()
