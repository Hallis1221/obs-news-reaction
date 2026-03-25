"""Classify insider trade notifications as BUY, SELL, or EXERCISE.

Parses PDMR announcement titles to determine the nature of the trade.
Insider BUYS are the strongest alpha signal; option exercises and
routine share plan allocations are weaker.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum


class InsiderAction(Enum):
    BUY = "BUY"
    SELL = "SELL"
    EXERCISE = "EXERCISE"  # stock option exercise
    ALLOCATION = "ALLOCATION"  # share plan allocation, bonus shares
    UNKNOWN = "UNKNOWN"


# Keywords indicating a genuine purchase
BUY_KEYWORDS = [
    "purchase", "purchases", "buys", "buy", "acquisition",
    "kjøp", "kjøper", "erverv",
    "subscribe", "subscription",
    "chairman.*buys", "board.*buys", "ceo.*buys",
    "insider.*purchase", "insider.*buy",
]

# Keywords indicating a sale
SELL_KEYWORDS = [
    "sale of shares", "sells", "sold", "disposal",
    "salg", "solgt",
    "divestment",
]

# Keywords indicating option exercise (weaker signal)
EXERCISE_KEYWORDS = [
    "exercise of.*option", "option.*exercise", "exercise of share option",
    "exercised", "stock option",
    "utøvelse av.*opsjon", "opsjoner",
    "ltip", "share option plan",
]

# Keywords indicating routine allocation (weakest signal)
ALLOCATION_KEYWORDS = [
    "allocation", "allotment", "share allocation",
    "share purchase program", "employee share",
    "incentive", "long-term incentive", "bonus share",
    "award of", "granted",
    "tildeling",
]


@dataclass
class InsiderClassification:
    action: InsiderAction
    confidence: float  # 0-1
    matched_pattern: str
    ticker: str
    title: str


def classify_insider_trade(ticker: str, title: str) -> InsiderClassification:
    """Classify an insider trade announcement by parsing the title."""
    title_lower = title.lower()

    # Check exercises first (common and distinctive)
    for kw in EXERCISE_KEYWORDS:
        if re.search(kw, title_lower):
            # Check if it's exercise + sale (weaker)
            has_sale = any(re.search(sk, title_lower) for sk in SELL_KEYWORDS)
            if has_sale:
                return InsiderClassification(
                    action=InsiderAction.SELL,
                    confidence=0.7,
                    matched_pattern=f"exercise+sale: {kw}",
                    ticker=ticker, title=title,
                )
            return InsiderClassification(
                action=InsiderAction.EXERCISE,
                confidence=0.8,
                matched_pattern=f"exercise: {kw}",
                ticker=ticker, title=title,
            )

    # Check allocations
    for kw in ALLOCATION_KEYWORDS:
        if re.search(kw, title_lower):
            return InsiderClassification(
                action=InsiderAction.ALLOCATION,
                confidence=0.8,
                matched_pattern=f"allocation: {kw}",
                ticker=ticker, title=title,
            )

    # Check genuine buys
    for kw in BUY_KEYWORDS:
        if re.search(kw, title_lower):
            return InsiderClassification(
                action=InsiderAction.BUY,
                confidence=0.9,
                matched_pattern=f"buy: {kw}",
                ticker=ticker, title=title,
            )

    # Check sells
    for kw in SELL_KEYWORDS:
        if re.search(kw, title_lower):
            return InsiderClassification(
                action=InsiderAction.SELL,
                confidence=0.8,
                matched_pattern=f"sell: {kw}",
                ticker=ticker, title=title,
            )

    # Default: unknown — generic "mandatory notification of trade"
    return InsiderClassification(
        action=InsiderAction.UNKNOWN,
        confidence=0.3,
        matched_pattern="no specific keywords matched",
        ticker=ticker, title=title,
    )


def classify_all_insider_trades() -> dict[str, list[InsiderClassification]]:
    """Classify all insider trade announcements in the database.

    Returns dict grouped by action type.
    """
    from obs_news_reaction.db.schema import get_connection
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT ticker, title FROM announcements
            WHERE category LIKE '%MANDATORY NOTIFICATION%'
            ORDER BY published_at DESC
        """).fetchall()
    finally:
        conn.close()

    results: dict[str, list[InsiderClassification]] = {
        "BUY": [], "SELL": [], "EXERCISE": [], "ALLOCATION": [], "UNKNOWN": [],
    }
    for row in rows:
        c = classify_insider_trade(row["ticker"], row["title"])
        results[c.action.value].append(c)

    return results


def print_insider_analysis() -> str:
    """Format insider trade classification as a report."""
    results = classify_all_insider_trades()
    lines = []
    lines.append("=" * 70)
    lines.append("INSIDER TRADE CLASSIFICATION")
    lines.append("=" * 70)
    lines.append("")

    total = sum(len(v) for v in results.values())
    lines.append(f"Total insider notifications: {total}")
    lines.append("")

    for action in ["BUY", "SELL", "EXERCISE", "ALLOCATION", "UNKNOWN"]:
        items = results[action]
        label = {
            "BUY": "GENUINE BUYS (strongest signal)",
            "SELL": "SALES (weak/negative signal)",
            "EXERCISE": "OPTION EXERCISES (moderate signal)",
            "ALLOCATION": "SHARE ALLOCATIONS (routine, weak)",
            "UNKNOWN": "UNCLASSIFIED",
        }[action]
        lines.append(f"--- {label}: {len(items)} ---")
        for c in items[:10]:  # show up to 10 per category
            lines.append(f"  {c.ticker:8s} [{c.confidence:.0%}] {c.title[:55]}")
            lines.append(f"           pattern: {c.matched_pattern}")
        if len(items) > 10:
            lines.append(f"  ... and {len(items) - 10} more")
        lines.append("")

    lines.append("=" * 70)
    return "\n".join(lines)
