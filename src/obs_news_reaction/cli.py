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
def analyze(since: str | None) -> None:
    """Run event study analysis on announcements."""
    from obs_news_reaction.analysis.engine import analyze_all
    n = analyze_all(since=since)
    click.echo(f"Analyzed {n} announcements")


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


if __name__ == "__main__":
    cli()
