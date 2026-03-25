"""Mean-reversion strategy on announcement-day overreactions.

The only strategy confirmed by both daily and 1m data: fading large
price moves on announcement days. This is a microstructure effect
(overreaction → correction) not a news-category effect.

Tests multiple thresholds, hold periods, and directions.
"""

from __future__ import annotations

import logging
import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta

from obs_news_reaction.analysis.backtest import (
    Trade, StrategyResult, _compile_results, _get_daily_bars,
    compute_round_trip_cost_pct, POSITION_SIZES_NOK,
)
from obs_news_reaction.db.schema import get_connection

log = logging.getLogger(__name__)


def strategy_fade_overreaction(
    threshold_pct: float = 1.0,
    direction: str = "both",  # "down" = buy dips, "up" = short spikes, "both"
    hold_days: int = 1,
    position_nok: float = 50_000,
) -> StrategyResult:
    """Fade announcement-day overreactions.

    Entry: when open-to-prev-close gap exceeds threshold.
    - Gap down > threshold → buy at open, sell at close (mean reversion up)
    - Gap up > threshold → short at open, cover at close (mean reversion down)
    """
    cost_pct = compute_round_trip_cost_pct(position_nok)
    conn = get_connection()
    try:
        anns = conn.execute(
            "SELECT * FROM announcements ORDER BY published_at"
        ).fetchall()

        # Deduplicate: one trade per ticker per day
        seen_ticker_dates: set[str] = set()
        trades = []

        for ann in anns:
            ticker = ann["ticker"]
            ol_ticker = ticker + ".OL" if not ticker.endswith(".OL") else ticker
            ann_date = ann["published_at"][:10]

            key = f"{ticker}_{ann_date}"
            if key in seen_ticker_dates:
                continue

            bars = _get_daily_bars(conn, ol_ticker)
            if len(bars) < 3:
                continue

            date_idx = {b["timestamp"][:10]: i for i, b in enumerate(bars)}
            entry_idx = None
            dt = datetime.fromisoformat(ann_date)
            for offset in range(-1, 4):
                candidate = (dt + timedelta(days=offset)).strftime("%Y-%m-%d")
                if candidate in date_idx:
                    entry_idx = date_idx[candidate]
                    break

            if entry_idx is None or entry_idx < 1:
                continue

            prev_close = bars[entry_idx - 1]["close"]
            day_open = bars[entry_idx]["open"]
            if prev_close <= 0 or day_open <= 0:
                continue

            gap_pct = (day_open / prev_close - 1) * 100

            exit_idx = min(entry_idx + hold_days, len(bars) - 1)
            if exit_idx <= entry_idx:
                exit_idx = entry_idx
            exit_price = bars[exit_idx]["close"]

            # Gap down → buy (long)
            if direction in ("down", "both") and gap_pct < -threshold_pct:
                gross_ret = (exit_price / day_open - 1) * 100
                net_ret = gross_ret - cost_pct
                trades.append(Trade(
                    ticker=ticker,
                    entry_date=bars[entry_idx]["timestamp"][:10],
                    exit_date=bars[exit_idx]["timestamp"][:10],
                    entry_price=day_open,
                    exit_price=exit_price,
                    gross_return_pct=gross_ret,
                    net_return_pct=net_ret,
                    category=f"FADE_DOWN gap={gap_pct:+.1f}%",
                    hold_days=hold_days,
                ))
                seen_ticker_dates.add(key)

            # Gap up → short
            elif direction in ("up", "both") and gap_pct > threshold_pct:
                gross_ret = (day_open / exit_price - 1) * 100  # short profit
                net_ret = gross_ret - cost_pct
                trades.append(Trade(
                    ticker=ticker,
                    entry_date=bars[entry_idx]["timestamp"][:10],
                    exit_date=bars[exit_idx]["timestamp"][:10],
                    entry_price=day_open,
                    exit_price=exit_price,
                    gross_return_pct=gross_ret,
                    net_return_pct=net_ret,
                    category=f"FADE_UP gap={gap_pct:+.1f}%",
                    hold_days=hold_days,
                ))
                seen_ticker_dates.add(key)

        name = f"Fade {direction} >{threshold_pct}% (hold={hold_days}d, pos={position_nok/1000:.0f}k)"
        return _compile_results(name, trades)
    finally:
        conn.close()


def run_parameter_sweep() -> str:
    """Sweep across thresholds, directions, hold periods, and position sizes."""
    lines = []
    lines.append("=" * 90)
    lines.append("MEAN-REVERSION PARAMETER SWEEP")
    lines.append("=" * 90)
    lines.append("")

    # Sweep thresholds and directions
    lines.append(f"{'Threshold':>10s} {'Direction':>8s} {'Hold':>5s} {'Trades':>6s} {'Gross':>7s} {'Net50k':>7s} {'Net100k':>8s} {'Win%':>5s} {'Sharpe':>7s}")
    lines.append("-" * 80)

    best_sharpe = -999
    best_config = ""

    for threshold in [0.5, 1.0, 1.5, 2.0, 3.0, 5.0]:
        for direction in ["down", "up", "both"]:
            for hold in [0, 1, 2]:
                # Use 50k as default
                result = strategy_fade_overreaction(
                    threshold_pct=threshold, direction=direction,
                    hold_days=hold, position_nok=50_000,
                )
                if not result.trades:
                    continue

                # Also compute at 100k
                result_100k = strategy_fade_overreaction(
                    threshold_pct=threshold, direction=direction,
                    hold_days=hold, position_nok=100_000,
                )
                net_100k = result_100k.avg_net_pct if result_100k.trades else 0

                hold_str = "intra" if hold == 0 else f"{hold}d"
                lines.append(
                    f"{threshold:>9.1f}% {direction:>8s} {hold_str:>5s} "
                    f"{len(result.trades):6d} {result.avg_gross_pct:+6.2f}% "
                    f"{result.avg_net_pct:+6.2f}% {net_100k:+7.2f}% "
                    f"{result.win_rate:4.0f}% {result.sharpe_approx:+6.2f}"
                )

                if result.sharpe_approx > best_sharpe and len(result.trades) >= 5:
                    best_sharpe = result.sharpe_approx
                    best_config = f"threshold={threshold}%, dir={direction}, hold={hold_str}, n={len(result.trades)}"

    lines.append("")
    lines.append(f"Best config (>= 5 trades): {best_config} — Sharpe {best_sharpe:+.2f}")
    lines.append("")

    # Detailed breakdown of best config
    if best_config:
        # Parse best config and show trades
        lines.append("=" * 90)
        lines.append("BEST STRATEGY TRADES")
        lines.append("=" * 90)

    lines.append("")
    lines.append("=" * 90)
    return "\n".join(lines)
