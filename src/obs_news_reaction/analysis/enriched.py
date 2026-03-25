"""Enriched categorization of all announcements with price moves.

Breaks broad categories into actionable subcategories and adds
size/role metadata for insider trades and buybacks.
"""

from __future__ import annotations

import re
import statistics
from collections import defaultdict
from dataclasses import dataclass

from obs_news_reaction.db.schema import get_connection


@dataclass
class EnrichedAnnouncement:
    ticker: str
    published: str
    title: str
    raw_category: str
    subcategory: str
    return_pct: float
    close_before: float
    close_after: float
    # Insider trade enrichment
    insider_role: str | None = None
    insider_action: str | None = None  # BUY/SELL
    trade_value_nok: float | None = None
    shares_traded: int | None = None
    # Buyback enrichment
    buyback_type: str | None = None


def _subcategorize_press_release(title: str) -> str:
    t = title.lower()
    if any(kw in t for kw in ["contract", "awarded", "award", "order for", "receives order"]):
        return "PR: Contract/Order Award"
    if any(kw in t for kw in ["trading update", "quarterly", "q1 ", "q2 ", "q3 ", "q4 ",
                               "quarter report", "results presentation", "investor update"]):
        return "PR: Trading Update / Results"
    if any(kw in t for kw in ["new leader", "ny leder", "ceo", "appointed", "steps down",
                               "succession", "resignation"]):
        return "PR: Management Change"
    if any(kw in t for kw in ["acquisition", "acquire", "divestment", "merger", "takeover"]):
        return "PR: M&A / Divestiture"
    if any(kw in t for kw in ["conversion", "konvertering", "license"]):
        return "PR: License/Conversion"
    if any(kw in t for kw in ["payment", "coupon", "bond"]):
        return "PR: Bond/Coupon Payment"
    if any(kw in t for kw in ["general meeting", "generalforsamling", "agm", "egm", "summons"]):
        return "PR: General Meeting"
    if any(kw in t for kw in ["share", "aksje", "capital increase", "private placement"]):
        return "PR: Share/Capital Action"
    return "PR: Other"


def _subcategorize_regulated(title: str) -> str:
    t = title.lower()
    if any(kw in t for kw in ["exercise of share option", "exercise of ltip",
                               "share options", "capital increase", "new share capital",
                               "issuance of shares"]):
        return "REG: Share Option Exercise / Issuance"
    if any(kw in t for kw in ["financial calendar", "finansiell kalender"]):
        return "REG: Financial Calendar"
    if any(kw in t for kw in ["general meeting", "generalforsamling", "agm", "egm"]):
        return "REG: General Meeting Notice"
    if any(kw in t for kw in ["quarter report", "fourth quarter", "årsrapport", "annual report",
                               "q4 ", "q1 ", "q2 ", "q3 "]):
        return "REG: Financial Report"
    if any(kw in t for kw in ["obligasjon", "bond", "utstedelse"]):
        return "REG: Bond Issuance"
    if any(kw in t for kw in ["first day of trading", "listing", "admission"]):
        return "REG: New Listing"
    if any(kw in t for kw in ["ceo", "steps down", "appointed", "interim"]):
        return "REG: Management Change"
    if any(kw in t for kw in ["ex date", "eks utbytte", "ex-dividend", "dividend"]):
        return "REG: Ex-Dividend / Dividend"
    if any(kw in t for kw in ["bookbuilding", "placement", "private placement"]):
        return "REG: Capital Raise"
    if any(kw in t for kw in ["managers' transactions", "manager"]):
        return "REG: Manager Transactions (non-PDMR)"
    if any(kw in t for kw in ["rating", "credit"]):
        return "REG: Credit Rating"
    return "REG: Other"


def _subcategorize_insider(title: str) -> str:
    t = title.lower()
    if any(kw in t for kw in ["exercise of", "option", "ltip", "opsjon", "utøvelse"]):
        return "INSIDER: Option Exercise"
    if any(kw in t for kw in ["allocation", "incentive", "award", "tildeling"]):
        return "INSIDER: Share Plan Allocation"
    if any(kw in t for kw in ["purchase", "buys", "acquired", "kjøp", "erverv", "subscription"]):
        return "INSIDER: Confirmed Buy"
    if any(kw in t for kw in ["sale", "sold", "sells", "disposal", "salg"]):
        return "INSIDER: Confirmed Sell"
    return "INSIDER: Unclassified Trade"


def _parse_insider_role(title: str) -> str | None:
    t = title.lower()
    if "ceo" in t: return "CEO"
    if "cfo" in t: return "CFO"
    if "coo" in t: return "COO"
    if "cto" in t: return "CTO"
    if "chair" in t: return "Chair"
    if "director" in t: return "Director"
    if "board" in t: return "Board"
    if re.search(r"primary insider|primærinnsider|prim.rinnsider", t): return "Primary Insider"
    if re.search(r"close associate|nærstående|n.rst.ende", t): return "Close Associate"
    return None


def _parse_trade_size(title: str) -> tuple[int | None, float | None]:
    """Try to extract shares and value from title."""
    shares = None
    value = None
    # Shares pattern
    m = re.search(r"(\d[\d,. ]*)\s*(?:shares|aksjer)", title, re.IGNORECASE)
    if m:
        try:
            shares = int(float(m.group(1).replace(",", "").replace(" ", "")))
        except ValueError:
            pass
    # Value pattern
    m = re.search(r"(?:NOK|USD|EUR)\s*([\d,. ]+)", title)
    if m:
        try:
            value = float(m.group(1).replace(",", "").replace(" ", ""))
        except ValueError:
            pass
    return shares, value


def _subcategorize_buyback(title: str) -> str:
    t = title.lower()
    if any(kw in t for kw in ["status", "programme", "program"]):
        return "BUYBACK: Programme Status"
    if any(kw in t for kw in ["repurchase", "share repurchase", "buy-back", "buyback"]):
        return "BUYBACK: Share Repurchase"
    if any(kw in t for kw in ["conveyance", "treasury", "incentive"]):
        return "BUYBACK: Treasury / Incentive Transfer"
    return "BUYBACK: Other"


def run_enriched_analysis() -> list[EnrichedAnnouncement]:
    """Run enriched categorization on all announcements with price data."""
    conn = get_connection()
    try:
        rows = conn.execute('''
            SELECT
                a.id, a.ticker, a.published_at, a.category, a.title,
                pb_before.close as close_before,
                pb_after.close as close_after
            FROM announcements a
            INNER JOIN price_bars pb_after ON pb_after.ticker = a.ticker || '.OL'
                AND pb_after.interval = '1d'
                AND DATE(pb_after.timestamp) >= DATE(a.published_at, '-1 day')
                AND DATE(pb_after.timestamp) <= DATE(a.published_at, '+1 day')
            INNER JOIN price_bars pb_before ON pb_before.ticker = a.ticker || '.OL'
                AND pb_before.interval = '1d'
                AND DATE(pb_before.timestamp) >= DATE(a.published_at, '-4 days')
                AND DATE(pb_before.timestamp) < DATE(pb_after.timestamp)
            WHERE pb_before.close > 0
            GROUP BY a.id
            HAVING pb_after.timestamp = MAX(pb_after.timestamp)
               AND pb_before.timestamp = MAX(pb_before.timestamp)
            ORDER BY a.published_at DESC
        ''').fetchall()
    finally:
        conn.close()

    results = []
    for r in rows:
        cat = r["category"]
        title = r["title"]
        ret = (r["close_after"] / r["close_before"] - 1) * 100

        # Subcategorize
        if "MANDATORY NOTIFICATION" in cat:
            subcat = _subcategorize_insider(title)
            role = _parse_insider_role(title)
            shares, value = _parse_trade_size(title)
            # Determine buy/sell from subcategory
            if "Confirmed Buy" in subcat:
                action = "BUY"
            elif "Confirmed Sell" in subcat:
                action = "SELL"
            elif "Option Exercise" in subcat:
                action = "EXERCISE"
            else:
                action = "UNKNOWN"
            results.append(EnrichedAnnouncement(
                ticker=r["ticker"], published=r["published_at"][:16],
                title=title, raw_category=cat, subcategory=subcat,
                return_pct=ret, close_before=r["close_before"],
                close_after=r["close_after"],
                insider_role=role, insider_action=action,
                trade_value_nok=value, shares_traded=shares,
            ))
        elif "NON-REGULATORY PRESS" in cat:
            subcat = _subcategorize_press_release(title)
            results.append(EnrichedAnnouncement(
                ticker=r["ticker"], published=r["published_at"][:16],
                title=title, raw_category=cat, subcategory=subcat,
                return_pct=ret, close_before=r["close_before"],
                close_after=r["close_after"],
            ))
        elif "ADDITIONAL REGULATED" in cat:
            subcat = _subcategorize_regulated(title)
            results.append(EnrichedAnnouncement(
                ticker=r["ticker"], published=r["published_at"][:16],
                title=title, raw_category=cat, subcategory=subcat,
                return_pct=ret, close_before=r["close_before"],
                close_after=r["close_after"],
            ))
        elif "ACQUISITION OR DISPOSAL" in cat:
            subcat = _subcategorize_buyback(title)
            results.append(EnrichedAnnouncement(
                ticker=r["ticker"], published=r["published_at"][:16],
                title=title, raw_category=cat, subcategory=subcat,
                return_pct=ret, close_before=r["close_before"],
                close_after=r["close_after"],
                buyback_type=subcat,
            ))
        else:
            # Keep other categories as-is
            results.append(EnrichedAnnouncement(
                ticker=r["ticker"], published=r["published_at"][:16],
                title=title, raw_category=cat, subcategory=cat[:50],
                return_pct=ret, close_before=r["close_before"],
                close_after=r["close_after"],
            ))

    return results


def print_enriched_analysis(results: list[EnrichedAnnouncement]) -> str:
    """Format enriched analysis as a detailed report."""
    lines = []
    lines.append("=" * 110)
    lines.append("ENRICHED ANNOUNCEMENT ANALYSIS — 24h PRICE MOVES")
    lines.append("=" * 110)
    lines.append("")

    # Group by subcategory
    by_sub: dict[str, list[EnrichedAnnouncement]] = defaultdict(list)
    for r in results:
        by_sub[r.subcategory].append(r)

    # Sort categories: INSIDER first, then PR, REG, BUYBACK, others
    def sort_key(cat: str) -> tuple:
        prefix_order = {"INSIDER": 0, "PR": 1, "REG": 2, "BUYBACK": 3}
        prefix = cat.split(":")[0] if ":" in cat else cat
        return (prefix_order.get(prefix, 9), cat)

    # Summary table first
    lines.append(f"{'Subcategory':<50s} {'N':>3s} {'Mean':>7s} {'Med':>7s} {'Win%':>5s} {'Best':>7s} {'Worst':>7s}")
    lines.append("-" * 95)

    for subcat in sorted(by_sub.keys(), key=sort_key):
        items = by_sub[subcat]
        rets = [i.return_pct for i in items]
        mean = statistics.mean(rets)
        med = statistics.median(rets)
        pos = sum(1 for r in rets if r > 0)
        wr = pos / len(rets) * 100
        best = max(rets)
        worst = min(rets)
        lines.append(
            f"{subcat:<50s} {len(items):3d} {mean:+6.2f}% {med:+6.2f}% {wr:4.0f}% {best:+6.2f}% {worst:+6.2f}%"
        )
    lines.append("")

    # Detailed sections
    for subcat in sorted(by_sub.keys(), key=sort_key):
        items = by_sub[subcat]
        rets = [i.return_pct for i in items]
        mean = statistics.mean(rets)

        lines.append(f"--- {subcat} (n={len(items)}, mean={mean:+.2f}%) ---")

        sorted_items = sorted(items, key=lambda x: x.return_pct, reverse=True)
        for i in sorted_items:
            extra = ""
            if i.insider_role:
                extra += f" [{i.insider_role}]"
            if i.insider_action and i.insider_action != "UNKNOWN":
                extra += f" {i.insider_action}"
            if i.trade_value_nok:
                if i.trade_value_nok >= 1e6:
                    extra += f" {i.trade_value_nok/1e6:.1f}M NOK"
                else:
                    extra += f" {i.trade_value_nok/1e3:.0f}k NOK"
            if i.shares_traded:
                extra += f" ({i.shares_traded:,} shares)"

            lines.append(
                f"  {i.ticker:8s} {i.return_pct:+7.2f}%  {i.published}  {i.title[:50]}{extra}"
            )
        lines.append("")

    lines.append("=" * 110)
    return "\n".join(lines)
