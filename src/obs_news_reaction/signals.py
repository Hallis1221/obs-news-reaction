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


def scan_for_signals(since: str | None = None, min_score: float = SIGNAL_THRESHOLD) -> list[Signal]:
    """Scan recent announcements and return actionable signals.

    Args:
        since: ISO date to filter announcements from
        min_score: Minimum absolute score to include
    """
    anns = get_announcements(since=since)
    signals = []

    for ann in anns:
        sig = score_announcement(ann)
        if abs(sig.score) >= min_score:
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

    if buy_signals:
        lines.append(f">>> BUY SIGNALS ({len(buy_signals)}) <<<")
        lines.append("")
        for s in buy_signals:
            lines.append(f"  [{s.score:+.1f}%] {s.announcement.ticker:8s} {s.announcement.published_at[:16]}")
            lines.append(f"         {s.announcement.title[:65]}")
            lines.append(f"         {s.reasoning}")
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
