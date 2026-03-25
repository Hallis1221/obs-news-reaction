"""Signal detection and scoring for Oslo Bors announcements.

Scores each announcement by expected alpha based on backtest findings:
- PDMR (insider trade) notifications: HIGH signal (+4.6% avg)
- Non-regulatory press releases: MEDIUM signal (+2.7% avg)
- Major shareholding changes: MEDIUM signal (+2.5% avg)
- Annual reports / ex-dates: NEGATIVE signal (avoid)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from obs_news_reaction.db.operations import get_announcements, get_stock_meta
from obs_news_reaction.models import Announcement

log = logging.getLogger(__name__)

# Liquidity thresholds
MIN_AVG_DAILY_VOLUME = 50_000  # minimum 50k shares/day avg
MIN_AVG_DAILY_VALUE_NOK = 500_000  # minimum 500k NOK/day traded (volume * price)

# Signal scores based on backtest results (avg net return per category)
CATEGORY_SCORES: dict[str, float] = {
    "MANDATORY NOTIFICATION OF TRADE PRIMARY INSIDERS": 4.60,
    "MANDATORY NOTIFICATION OF TRADE BY PRIMARY INSIDERS": 4.60,
    "NON-REGULATORY PRESS RELEASES": 2.69,
    "MAJOR SHAREHOLDING NOTIFICATIONS": 2.53,
    "INSIDE INFORMATION": -1.06,  # market prices this in efficiently
    "ACQUISITION OR DISPOSAL OF THE ISSUER'S OWN SHARES": -0.13,
    "ADDITIONAL REGULATED INFORMATION REQUIRED TO BE DISCLOSED UNDER THE LAWS OF A MEMBER STATE": 0.29,
    "ANNUAL FINANCIAL AND AUDIT REPORTS": -0.90,
    "HALF YEARLY FINANCIAL REPORTS AND AUDIT REPORTS / LIMITED REVIEWS": -0.50,
    "EX DATE": -1.35,
}

# Minimum score to consider a signal actionable
SIGNAL_THRESHOLD = 1.0


@dataclass
class Signal:
    announcement: Announcement
    score: float  # expected net return %
    category_label: str
    action: str  # "BUY", "AVOID", "NEUTRAL"
    reasoning: str
    liquid: bool = True  # passes liquidity filter
    avg_daily_value_nok: float | None = None  # estimated daily traded value


def _check_liquidity(ticker: str) -> tuple[bool, float | None]:
    """Check if a ticker has sufficient liquidity for trading.

    Uses recent daily bars to estimate average daily volume * price.
    Returns (is_liquid, avg_daily_value_nok).
    """
    from obs_news_reaction.db.schema import get_connection
    conn = get_connection()
    try:
        ol_ticker = ticker + ".OL" if not ticker.endswith(".OL") else ticker
        rows = conn.execute(
            """SELECT close, volume FROM price_bars
               WHERE ticker = ? AND interval = '1d'
               ORDER BY timestamp DESC LIMIT 20""",
            (ol_ticker,),
        ).fetchall()

        if not rows:
            # No price data — check stock_meta
            meta = get_stock_meta(ol_ticker)
            if meta and meta.avg_daily_volume:
                return meta.avg_daily_volume >= MIN_AVG_DAILY_VOLUME, None
            return True, None  # no data, assume liquid (don't penalize)

        # Average daily value = avg(close * volume)
        values = [r["close"] * r["volume"] for r in rows if r["volume"] > 0]
        if not values:
            return True, None

        avg_value = sum(values) / len(values)
        is_liquid = avg_value >= MIN_AVG_DAILY_VALUE_NOK
        return is_liquid, avg_value
    finally:
        conn.close()


def score_announcement(ann: Announcement) -> Signal:
    """Score a single announcement for trading signal strength."""
    # Match category (partial match since categories can be truncated)
    score = 0.0
    matched_cat = "UNKNOWN"
    for cat, cat_score in CATEGORY_SCORES.items():
        if ann.category.startswith(cat[:30]) or cat.startswith(ann.category[:30]):
            score = cat_score
            matched_cat = cat
            break

    # Determine action
    if score >= SIGNAL_THRESHOLD:
        action = "BUY"
        reasoning = f"Category '{matched_cat[:40]}' has {score:+.2f}% avg net return"
    elif score <= -SIGNAL_THRESHOLD:
        action = "AVOID"
        reasoning = f"Category '{matched_cat[:40]}' has {score:+.2f}% avg net return — avoid"
    else:
        action = "NEUTRAL"
        reasoning = f"Category '{matched_cat[:40]}' has low expected return ({score:+.2f}%)"

    # Refine insider trade signals using BUY/SELL/EXERCISE classification
    title_lower = ann.title.lower()
    if ann.category.startswith("MANDATORY NOTIFICATION"):
        from obs_news_reaction.analysis.insider import classify_insider_trade, InsiderAction
        ic = classify_insider_trade(ann.ticker, ann.title)
        if ic.action == InsiderAction.BUY:
            score = 6.0  # genuine insider buys are the strongest signal
            action = "BUY"
            reasoning = f"INSIDER BUY detected ({ic.matched_pattern}) — strongest alpha signal"
        elif ic.action == InsiderAction.SELL:
            score = -2.0
            action = "AVOID"
            reasoning = f"Insider SELL ({ic.matched_pattern}) — negative signal"
        elif ic.action == InsiderAction.EXERCISE:
            score = 2.0  # weaker than genuine buys
            action = "BUY"
            reasoning = f"Insider option exercise ({ic.matched_pattern}) — moderate signal"
        elif ic.action == InsiderAction.ALLOCATION:
            score = 0.5  # routine
            action = "NEUTRAL"
            reasoning = f"Routine share allocation ({ic.matched_pattern}) — weak signal"
        else:
            # Unclassified PDMR — use base category score
            score = max(score, 4.0)
            action = "BUY"
            reasoning = "Insider trade (unclassified direction) — historically +4.6% avg"

    # Penalty for routine/admin announcements
    if any(kw in title_lower for kw in ["annual report", "årsrapport", "financial calendar",
                                         "generalforsamling", "annual general meeting"]):
        score = min(score, -0.5)
        action = "AVOID"
        reasoning = "Routine administrative announcement — no trading edge"

    return Signal(
        announcement=ann,
        score=score,
        category_label=matched_cat[:50],
        action=action,
        reasoning=reasoning,
    )


def scan_for_signals(
    since: str | None = None, min_score: float = SIGNAL_THRESHOLD,
    require_liquid: bool = False,
) -> list[Signal]:
    """Scan recent announcements and return actionable signals.

    Args:
        since: ISO date to filter announcements from
        min_score: Minimum absolute score to include
        require_liquid: If True, exclude illiquid stocks from BUY signals
    """
    anns = get_announcements(since=since)
    signals = []
    liquidity_cache: dict[str, tuple[bool, float | None]] = {}

    for ann in anns:
        sig = score_announcement(ann)
        if abs(sig.score) >= min_score:
            # Check liquidity
            if ann.ticker not in liquidity_cache:
                liquidity_cache[ann.ticker] = _check_liquidity(ann.ticker)
            is_liquid, avg_val = liquidity_cache[ann.ticker]
            sig.liquid = is_liquid
            sig.avg_daily_value_nok = avg_val

            if require_liquid and not is_liquid and sig.action == "BUY":
                continue  # skip illiquid BUY signals
            signals.append(sig)

    # Sort by score descending (best signals first)
    signals.sort(key=lambda s: s.score, reverse=True)
    return signals


def print_signals(signals: list[Signal]) -> str:
    """Format signals as a readable alert report."""
    if not signals:
        return "No actionable signals found."

    lines = []
    lines.append("=" * 70)
    lines.append("OSLO BØRS SIGNAL ALERTS")
    lines.append("=" * 70)
    lines.append("")

    buy_signals = [s for s in signals if s.action == "BUY"]
    avoid_signals = [s for s in signals if s.action == "AVOID"]

    liquid_buys = [s for s in buy_signals if s.liquid]
    illiquid_buys = [s for s in buy_signals if not s.liquid]

    if liquid_buys:
        lines.append(f">>> BUY SIGNALS — LIQUID ({len(liquid_buys)}) <<<")
        lines.append("")
        for s in liquid_buys:
            val_str = f" vol={s.avg_daily_value_nok/1e6:.1f}M" if s.avg_daily_value_nok else ""
            lines.append(f"  [{s.score:+.1f}%] {s.announcement.ticker:8s} {s.announcement.published_at[:16]}{val_str}")
            lines.append(f"         {s.announcement.title[:65]}")
            lines.append(f"         {s.reasoning}")
            lines.append("")

    if illiquid_buys:
        lines.append(f">>> BUY SIGNALS — ILLIQUID ({len(illiquid_buys)}) <<<")
        lines.append("(Caution: low daily volume, high market impact risk)")
        lines.append("")
        for s in illiquid_buys:
            val_str = f" vol={s.avg_daily_value_nok/1e3:.0f}k" if s.avg_daily_value_nok else " vol=N/A"
            lines.append(f"  [{s.score:+.1f}%] {s.announcement.ticker:8s} {s.announcement.published_at[:16]}{val_str}")
            lines.append(f"         {s.announcement.title[:65]}")
            lines.append("")

    if avoid_signals:
        lines.append(f">>> AVOID ({len(avoid_signals)}) <<<")
        lines.append("")
        for s in avoid_signals:
            lines.append(f"  [{s.score:+.1f}%] {s.announcement.ticker:8s} {s.announcement.published_at[:16]}")
            lines.append(f"         {s.announcement.title[:65]}")
            lines.append("")

    lines.append("=" * 70)
    return "\n".join(lines)
