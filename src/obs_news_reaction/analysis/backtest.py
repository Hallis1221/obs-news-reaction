"""Backtesting framework for Oslo Bors announcement-based strategies.

Uses historical daily bars to simulate what would have happened if we
traded on announcement signals. The key question: is the +1.7%
announcement-day excess return exploitable after transaction costs?
"""

from __future__ import annotations

import logging
import statistics
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from obs_news_reaction.db.schema import get_connection

log = logging.getLogger(__name__)

# Oslo Bors costs (conservative estimates)
SPREAD_BPS = 30  # 30 bps for mid/small-cap Oslo stocks
COMMISSION_BPS = 10  # broker commission
SLIPPAGE_BPS = 10  # market impact
TOTAL_COST_BPS = SPREAD_BPS + COMMISSION_BPS + SLIPPAGE_BPS  # 50 bps round-trip
COST_PCT = TOTAL_COST_BPS / 100  # 0.50%


@dataclass
class Trade:
    ticker: str
    entry_date: str
    exit_date: str
    entry_price: float
    exit_price: float
    gross_return_pct: float
    net_return_pct: float
    category: str
    hold_days: int


@dataclass
class StrategyResult:
    name: str
    trades: list[Trade] = field(default_factory=list)
    total_gross_pct: float = 0
    total_net_pct: float = 0
    win_rate: float = 0
    avg_gross_pct: float = 0
    avg_net_pct: float = 0
    max_drawdown_pct: float = 0
    sharpe_approx: float = 0
    category_breakdown: dict = field(default_factory=dict)


def _get_daily_bars(conn, ticker: str) -> list[dict]:
    """Get all daily bars for a ticker, ordered by date."""
    rows = conn.execute(
        """SELECT timestamp, open, high, low, close, volume
           FROM price_bars WHERE ticker = ? AND interval = '1d'
           ORDER BY timestamp ASC""",
        (ticker,),
    ).fetchall()
    return [dict(r) for r in rows]


def strategy_buy_on_announcement(hold_days: int = 1, categories: list[str] | None = None) -> StrategyResult:
    """Backtest: buy at open on announcement day, sell after hold_days.

    If categories is specified, only trade on those announcement categories.
    """
    conn = get_connection()
    try:
        anns = conn.execute(
            "SELECT * FROM announcements ORDER BY published_at"
        ).fetchall()

        trades = []
        for ann in anns:
            if categories and ann["category"] not in categories:
                continue

            ticker = ann["ticker"]
            ol_ticker = ticker + ".OL" if not ticker.endswith(".OL") else ticker
            ann_date = ann["published_at"][:10]

            bars = _get_daily_bars(conn, ol_ticker)
            if len(bars) < hold_days + 2:
                continue

            # Build date→index map
            date_idx = {}
            for i, b in enumerate(bars):
                date_idx[b["timestamp"][:10]] = i

            # Find entry bar (announcement day or next trading day)
            entry_idx = None
            dt = datetime.fromisoformat(ann_date)
            for offset in range(-1, 4):
                candidate = (dt + timedelta(days=offset)).strftime("%Y-%m-%d")
                if candidate in date_idx:
                    entry_idx = date_idx[candidate]
                    break

            if entry_idx is None:
                continue

            # Exit bar: hold_days later, or last available bar
            exit_idx = min(entry_idx + hold_days, len(bars) - 1)
            if exit_idx <= entry_idx and entry_idx > 0:
                # Use prev close → entry close as the trade
                exit_idx = entry_idx

            entry_bar = bars[entry_idx]
            exit_bar = bars[exit_idx]

            # Entry at open of announcement day (or prev close if same bar)
            entry_price = entry_bar["open"]
            exit_price = exit_bar["close"]

            if entry_price <= 0:
                continue

            gross_ret = (exit_price / entry_price - 1) * 100
            net_ret = gross_ret - COST_PCT

            trades.append(Trade(
                ticker=ticker,
                entry_date=entry_bar["timestamp"][:10],
                exit_date=exit_bar["timestamp"][:10],
                entry_price=entry_price,
                exit_price=exit_price,
                gross_return_pct=gross_ret,
                net_return_pct=net_ret,
                category=ann["category"],
                hold_days=hold_days,
            ))

        return _compile_results(f"Buy-on-Announcement (hold={hold_days}d)", trades)
    finally:
        conn.close()


def strategy_buy_insider_trades(hold_days: int = 1) -> StrategyResult:
    """Backtest: only buy when there's a mandatory insider trade notification."""
    insider_cats = [
        "MANDATORY NOTIFICATION OF TRADE PRIMARY INSIDERS",
        "MANDATORY NOTIFICATION OF TRADE BY PRIMARY INSIDERS",
    ]
    return strategy_buy_on_announcement(hold_days=hold_days, categories=insider_cats)


def strategy_buy_inside_info(hold_days: int = 1) -> StrategyResult:
    """Backtest: buy on inside information disclosures."""
    return strategy_buy_on_announcement(
        hold_days=hold_days, categories=["INSIDE INFORMATION"]
    )


def strategy_gap_fade(threshold_pct: float = 2.0) -> StrategyResult:
    """Backtest: fade large gaps on announcement days.

    If stock gaps up > threshold on announcement day, sell short (or skip).
    If stock gaps down > threshold, buy expecting mean reversion.
    """
    conn = get_connection()
    try:
        anns = conn.execute(
            "SELECT * FROM announcements ORDER BY published_at"
        ).fetchall()

        trades = []
        for ann in anns:
            ticker = ann["ticker"]
            ol_ticker = ticker + ".OL" if not ticker.endswith(".OL") else ticker
            ann_date = ann["published_at"][:10]

            bars = _get_daily_bars(conn, ol_ticker)
            if len(bars) < 3:
                continue

            date_idx = {}
            for i, b in enumerate(bars):
                date_idx[b["timestamp"][:10]] = i

            # Find announcement day
            dt = datetime.fromisoformat(ann_date)
            entry_idx = None
            for offset in range(-1, 4):
                candidate = (dt + timedelta(days=offset)).strftime("%Y-%m-%d")
                if candidate in date_idx:
                    entry_idx = date_idx[candidate]
                    break

            if entry_idx is None or entry_idx < 1:
                continue

            prev_close = bars[entry_idx - 1]["close"]
            day_open = bars[entry_idx]["open"]
            if prev_close <= 0:
                continue

            gap_pct = (day_open / prev_close - 1) * 100

            # Only trade large gap-downs (buy the dip)
            if gap_pct < -threshold_pct:
                entry_price = day_open
                exit_price = bars[entry_idx]["close"]  # sell at close same day
                gross_ret = (exit_price / entry_price - 1) * 100
                net_ret = gross_ret - COST_PCT

                trades.append(Trade(
                    ticker=ticker,
                    entry_date=bars[entry_idx]["timestamp"][:10],
                    exit_date=bars[entry_idx]["timestamp"][:10],
                    entry_price=entry_price,
                    exit_price=exit_price,
                    gross_return_pct=gross_ret,
                    net_return_pct=net_ret,
                    category=ann["category"],
                    hold_days=0,
                ))

        return _compile_results("Gap-Fade (buy large dips)", trades)
    finally:
        conn.close()


def _compile_results(name: str, trades: list[Trade]) -> StrategyResult:
    """Compile trades into a StrategyResult."""
    result = StrategyResult(name=name, trades=trades)

    if not trades:
        return result

    gross_rets = [t.gross_return_pct for t in trades]
    net_rets = [t.net_return_pct for t in trades]

    result.total_gross_pct = sum(gross_rets)
    result.total_net_pct = sum(net_rets)
    result.avg_gross_pct = statistics.mean(gross_rets)
    result.avg_net_pct = statistics.mean(net_rets)
    result.win_rate = sum(1 for r in net_rets if r > 0) / len(net_rets) * 100

    # Max drawdown (cumulative)
    cumulative = 0
    peak = 0
    max_dd = 0
    for r in net_rets:
        cumulative += r
        if cumulative > peak:
            peak = cumulative
        dd = peak - cumulative
        if dd > max_dd:
            max_dd = dd
    result.max_drawdown_pct = max_dd

    # Sharpe approximation (annualized)
    if len(net_rets) > 1:
        avg = statistics.mean(net_rets)
        std = statistics.stdev(net_rets)
        if std > 0:
            # Assume ~250 trades/year
            result.sharpe_approx = (avg / std) * (250 ** 0.5)

    # Category breakdown
    by_cat: dict[str, list[float]] = defaultdict(list)
    for t in trades:
        by_cat[t.category].append(t.net_return_pct)
    for cat, rets in by_cat.items():
        result.category_breakdown[cat] = {
            "n": len(rets),
            "mean_net_pct": statistics.mean(rets),
            "total_net_pct": sum(rets),
            "win_rate": sum(1 for r in rets if r > 0) / len(rets) * 100,
        }

    return result


def print_backtest(result: StrategyResult) -> str:
    """Format backtest results as a readable report."""
    lines = []
    lines.append(f"{'=' * 60}")
    lines.append(f"BACKTEST: {result.name}")
    lines.append(f"{'=' * 60}")

    if not result.trades:
        lines.append("No trades generated.")
        return "\n".join(lines)

    lines.append(f"Trades:         {len(result.trades)}")
    lines.append(f"Win rate:       {result.win_rate:.1f}%")
    lines.append(f"Avg gross:      {result.avg_gross_pct:+.3f}% per trade")
    lines.append(f"Avg net:        {result.avg_net_pct:+.3f}% per trade (after {COST_PCT:.2f}% costs)")
    lines.append(f"Total gross:    {result.total_gross_pct:+.2f}%")
    lines.append(f"Total net:      {result.total_net_pct:+.2f}%")
    lines.append(f"Max drawdown:   {result.max_drawdown_pct:.2f}%")
    lines.append(f"Sharpe (approx):{result.sharpe_approx:+.2f}")
    lines.append("")

    # Category breakdown
    if result.category_breakdown:
        lines.append("By category:")
        for cat, stats in sorted(result.category_breakdown.items(), key=lambda x: x[1]["mean_net_pct"], reverse=True):
            lines.append(f"  {cat[:45]:45s}  n={stats['n']:3d}  net={stats['mean_net_pct']:+.3f}%  win={stats['win_rate']:.0f}%")
    lines.append("")

    # Top/bottom trades
    sorted_trades = sorted(result.trades, key=lambda t: t.net_return_pct, reverse=True)
    lines.append("Top 5 trades:")
    for t in sorted_trades[:5]:
        lines.append(f"  {t.ticker:8s} {t.entry_date}  {t.net_return_pct:+.2f}%  ({t.category[:30]})")
    lines.append("Bottom 5 trades:")
    for t in sorted_trades[-5:]:
        lines.append(f"  {t.ticker:8s} {t.entry_date}  {t.net_return_pct:+.2f}%  ({t.category[:30]})")

    lines.append(f"{'=' * 60}")
    return "\n".join(lines)


def run_all_strategies() -> str:
    """Run all strategies and return combined report."""
    lines = []

    strategies = [
        ("All announcements, 1-day hold", lambda: strategy_buy_on_announcement(hold_days=1)),
        ("All announcements, 3-day hold", lambda: strategy_buy_on_announcement(hold_days=3)),
        ("Insider trades only, 1-day hold", lambda: strategy_buy_insider_trades(hold_days=1)),
        ("Insider trades only, 3-day hold", lambda: strategy_buy_insider_trades(hold_days=3)),
        ("Inside information, 1-day hold", lambda: strategy_buy_inside_info(hold_days=1)),
        ("Gap fade (buy dips > 2%)", lambda: strategy_gap_fade(threshold_pct=2.0)),
    ]

    for name, fn in strategies:
        result = fn()
        lines.append(print_backtest(result))
        lines.append("")

    # Summary comparison
    lines.append("=" * 60)
    lines.append("STRATEGY COMPARISON")
    lines.append("=" * 60)
    lines.append(f"{'Strategy':45s} {'Trades':>6s} {'Avg Net':>8s} {'Win%':>5s} {'Sharpe':>7s}")
    lines.append("-" * 75)

    for name, fn in strategies:
        result = fn()
        if result.trades:
            lines.append(
                f"{name:45s} {len(result.trades):6d} {result.avg_net_pct:+7.3f}% {result.win_rate:4.0f}% {result.sharpe_approx:+6.2f}"
            )
        else:
            lines.append(f"{name:45s}      0     N/A   N/A    N/A")

    return "\n".join(lines)
