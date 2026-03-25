"""Visualization: price reaction charts, AR distributions, reaction histograms."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from obs_news_reaction.config import EVENT_WINDOWS
from obs_news_reaction.db.operations import (
    get_announcement_by_message_id,
    get_announcements,
    get_price_bars,
    get_event_results,
    get_all_stock_meta,
)
from obs_news_reaction.models import Announcement, EventResult

log = logging.getLogger(__name__)

PLOT_DIR = Path("data/plots")


def _ensure_plot_dir() -> Path:
    PLOT_DIR.mkdir(parents=True, exist_ok=True)
    return PLOT_DIR


def plot_reaction(announcement_id: int, output: Path | None = None) -> Path:
    """Plot price reaction around an announcement."""
    from obs_news_reaction.db.operations import get_announcements
    import sqlite3
    from obs_news_reaction.db.schema import get_connection

    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT * FROM announcements WHERE id = ?", (announcement_id,)
        ).fetchone()
    finally:
        conn.close()

    if not row:
        raise ValueError(f"Announcement {announcement_id} not found")

    ticker = row["ticker"]
    pub_str = row["published_at"]
    title = row["title"]

    pub_dt = datetime.fromisoformat(pub_str)
    if pub_dt.tzinfo is None:
        pub_dt = pub_dt.replace(tzinfo=timezone.utc)

    # Get price bars: 1 hour before to 2 hours after
    ol_ticker = ticker if ticker.endswith(".OL") else ticker + ".OL"
    start = pub_dt - timedelta(hours=1)
    end = pub_dt + timedelta(hours=2)

    bars = get_price_bars(ol_ticker, interval="1m", start=start.isoformat(), end=end.isoformat())
    if not bars:
        bars = get_price_bars(ol_ticker, interval="5m", start=start.isoformat(), end=end.isoformat())
    if not bars:
        raise ValueError(f"No price bars for {ol_ticker} around {pub_str}")

    times = [datetime.fromisoformat(b.timestamp) for b in bars]
    closes = [b.close for b in bars]

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(times, closes, "b-", linewidth=1.5)
    ax.axvline(x=pub_dt, color="r", linestyle="--", linewidth=2, label="Announcement")
    ax.set_title(f"{ticker}: {title[:60]}", fontsize=12)
    ax.set_xlabel("Time (UTC)")
    ax.set_ylabel("Price (NOK)")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.autofmt_xdate()

    out = output or _ensure_plot_dir() / f"reaction_{announcement_id}.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    log.info(f"Saved reaction plot: {out}")
    return out


def plot_ar_distribution(window_name: str = "[-5m,+5m]", output: Path | None = None) -> Path:
    """Plot distribution of abnormal returns for a given window."""
    results = get_event_results(window_name=window_name)
    ars = [r.abnormal_return for r in results if r.abnormal_return is not None]

    if not ars:
        raise ValueError(f"No results for window {window_name}")

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.hist(ars, bins=30, edgecolor="black", alpha=0.7, color="steelblue")
    ax.axvline(x=0, color="r", linestyle="--", linewidth=1.5, label="Zero")
    mean_ar = np.mean(ars)
    ax.axvline(x=mean_ar, color="g", linestyle="-", linewidth=1.5, label=f"Mean: {mean_ar:.4f}")
    ax.set_title(f"Abnormal Return Distribution — {window_name} (n={len(ars)})")
    ax.set_xlabel("Abnormal Return")
    ax.set_ylabel("Frequency")
    ax.legend()
    ax.grid(True, alpha=0.3)

    out = output or _ensure_plot_dir() / f"ar_dist_{window_name.replace('[','').replace(']','').replace(',','_')}.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    log.info(f"Saved AR distribution: {out}")
    return out


def plot_reaction_time_histogram(window_name: str = "[0,+1h]", output: Path | None = None) -> Path:
    """Plot histogram of reaction times."""
    results = get_event_results(window_name=window_name)
    times = [r.reaction_time_seconds for r in results if r.reaction_time_seconds is not None]

    if not times:
        raise ValueError(f"No reaction times for window {window_name}")

    # Convert to minutes for readability
    times_min = [t / 60.0 for t in times]

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.hist(times_min, bins=20, edgecolor="black", alpha=0.7, color="coral")
    median = np.median(times_min)
    ax.axvline(x=median, color="b", linestyle="--", linewidth=1.5, label=f"Median: {median:.1f}m")
    ax.set_title(f"Reaction Time Distribution — {window_name} (n={len(times)})")
    ax.set_xlabel("Reaction Time (minutes)")
    ax.set_ylabel("Frequency")
    ax.legend()
    ax.grid(True, alpha=0.3)

    out = output or _ensure_plot_dir() / f"reaction_hist_{window_name.replace('[','').replace(']','').replace(',','_')}.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    log.info(f"Saved reaction time histogram: {out}")
    return out


def plot_category_heatmap(output: Path | None = None) -> Path:
    """Plot heatmap of mean AR by category and window."""
    all_results = []
    for window_name, _, _ in EVENT_WINDOWS:
        results = get_event_results(window_name=window_name)
        for r in results:
            if r.abnormal_return is not None:
                # Look up category from announcement
                all_results.append((r, window_name))

    if not all_results:
        raise ValueError("No event results for heatmap")

    # Build category -> window -> [AR] mapping
    from collections import defaultdict
    cat_window_ars: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))

    # We need announcement categories - fetch them
    from obs_news_reaction.db.schema import get_connection
    conn = get_connection()
    try:
        ann_cache: dict[int, str] = {}
        for r, wn in all_results:
            if r.announcement_id not in ann_cache:
                row = conn.execute(
                    "SELECT category FROM announcements WHERE id = ?", (r.announcement_id,)
                ).fetchone()
                ann_cache[r.announcement_id] = row["category"] if row else "UNKNOWN"
            cat = ann_cache[r.announcement_id]
            cat_window_ars[cat][wn].append(r.abnormal_return)
    finally:
        conn.close()

    categories = sorted(cat_window_ars.keys())
    windows = [w for w, _, _ in EVENT_WINDOWS]

    data = np.zeros((len(categories), len(windows)))
    for i, cat in enumerate(categories):
        for j, win in enumerate(windows):
            vals = cat_window_ars[cat].get(win, [])
            data[i, j] = np.mean(vals) if vals else 0.0

    fig, ax = plt.subplots(figsize=(12, max(4, len(categories) * 0.6)))
    im = ax.imshow(data, aspect="auto", cmap="RdYlGn")
    ax.set_xticks(range(len(windows)))
    ax.set_xticklabels(windows, rotation=45, ha="right")
    ax.set_yticks(range(len(categories)))
    ax.set_yticklabels(categories)
    ax.set_title("Mean Abnormal Return by Category & Window")
    fig.colorbar(im, label="Mean AR")

    # Annotate cells
    for i in range(len(categories)):
        for j in range(len(windows)):
            ax.text(j, i, f"{data[i,j]:.3f}", ha="center", va="center", fontsize=8)

    out = output or _ensure_plot_dir() / "category_heatmap.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    log.info(f"Saved category heatmap: {out}")
    return out


def plot_summary(output_dir: Path | None = None) -> list[Path]:
    """Generate all summary plots. Returns list of paths."""
    plots = []
    try:
        plots.append(plot_ar_distribution())
    except ValueError as e:
        log.warning(f"Skipping AR distribution: {e}")
    try:
        plots.append(plot_reaction_time_histogram())
    except ValueError as e:
        log.warning(f"Skipping reaction histogram: {e}")
    try:
        plots.append(plot_category_heatmap())
    except ValueError as e:
        log.warning(f"Skipping category heatmap: {e}")
    return plots
