"""Historical reaction study using daily bars.

Analyzes the relationship between Oslo Bors announcements and stock
price movements on the announcement day vs prior days, to detect
whether there is exploitable alpha (delayed reaction, pre-announcement
drift, or post-announcement momentum).
"""

from __future__ import annotations

import logging
import statistics
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from obs_news_reaction.db.schema import get_connection

log = logging.getLogger(__name__)


def _daily_returns(conn, ticker: str, days: int = 60) -> list[dict]:
    """Get daily returns for a ticker over the last N days."""
    rows = conn.execute(
        """SELECT timestamp, open, close, volume
           FROM price_bars
           WHERE ticker = ? AND interval = '1d'
           ORDER BY timestamp DESC
           LIMIT ?""",
        (ticker, days),
    ).fetchall()
    rows = list(reversed(rows))  # oldest first
    returns = []
    for i in range(1, len(rows)):
        prev_close = rows[i - 1]["close"]
        if prev_close == 0:
            continue
        ret = (rows[i]["close"] / prev_close - 1) * 100
        returns.append({
            "date": rows[i]["timestamp"][:10],
            "return_pct": ret,
            "close": rows[i]["close"],
            "volume": rows[i]["volume"],
            "open": rows[i]["open"],
            "gap_pct": (rows[i]["open"] / prev_close - 1) * 100,
        })
    return returns


def _match_ann_date(ann_date: str, bar_dates: set[str]) -> str | None:
    """Match an announcement date to the closest bar date (same day, previous, or next trading day)."""
    if ann_date in bar_dates:
        return ann_date
    from datetime import datetime as _dt, timedelta
    dt = _dt.fromisoformat(ann_date)
    # Try nearby days (backward first since bar data may lag)
    for offset in [-1, 1, -2, 2, -3, 3]:
        candidate = (dt + timedelta(days=offset)).strftime("%Y-%m-%d")
        if candidate in bar_dates:
            return candidate
    return None


def run_historical_study() -> dict:
    """Run a comprehensive historical reaction study.

    Returns a dict with findings about:
    - Day-of-announcement returns vs normal days
    - Pre-announcement drift (day before)
    - Post-announcement momentum (day after)
    - Category-specific patterns
    - Gap analysis (open vs prev close on announcement day)
    """
    conn = get_connection()
    try:
        # Get all announcements with their dates
        anns = conn.execute(
            """SELECT a.id, a.ticker, a.published_at, a.category, a.title
               FROM announcements a
               ORDER BY a.published_at"""
        ).fetchall()

        if not anns:
            return {"error": "No announcements in database"}

        # Group announcements by ticker and date
        ann_dates: dict[str, set[str]] = defaultdict(set)
        ann_categories: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
        for a in anns:
            date_str = a["published_at"][:10]
            ticker = a["ticker"]
            ann_dates[ticker].add(date_str)
            ann_categories[ticker][date_str].append(a["category"])

        # For each ticker, compare announcement-day returns to normal-day returns
        all_ann_day_returns = []
        all_normal_day_returns = []
        all_pre_ann_returns = []
        all_post_ann_returns = []
        all_gap_on_ann = []
        all_gap_normal = []
        category_returns: dict[str, list[float]] = defaultdict(list)
        category_gaps: dict[str, list[float]] = defaultdict(list)
        ticker_results = {}

        for ticker in ann_dates:
            ol_ticker = ticker + ".OL" if not ticker.endswith(".OL") else ticker
            returns = _daily_returns(conn, ol_ticker, days=500)
            if len(returns) < 10:
                continue

            bar_date_set = {r["date"] for r in returns}
            # Map announcement dates to actual bar dates
            matched_dates = set()
            for ad in ann_dates[ticker]:
                md = _match_ann_date(ad, bar_date_set)
                if md:
                    matched_dates.add(md)

            ann_rets = []
            normal_rets = []
            pre_rets = []
            post_rets = []
            ann_gaps = []
            normal_gaps = []

            for i, r in enumerate(returns):
                if r["date"] in matched_dates:
                    ann_rets.append(r["return_pct"])
                    ann_gaps.append(r["gap_pct"])
                    # Pre-announcement (day before)
                    if i > 0:
                        pre_rets.append(returns[i - 1]["return_pct"])
                    # Post-announcement (day after)
                    if i < len(returns) - 1:
                        post_rets.append(returns[i + 1]["return_pct"])
                    # Category tracking — check both original and matched dates
                    cats = ann_categories[ticker].get(r["date"], [])
                    if not cats:
                        # Try to find original ann date that mapped to this bar date
                        for ad in ann_dates[ticker]:
                            if _match_ann_date(ad, {r["date"]}) == r["date"]:
                                cats = ann_categories[ticker].get(ad, [])
                                break
                    for cat in cats:
                        category_returns[cat].append(r["return_pct"])
                        category_gaps[cat].append(r["gap_pct"])
                else:
                    normal_rets.append(r["return_pct"])
                    normal_gaps.append(r["gap_pct"])

            if ann_rets:
                ticker_results[ticker] = {
                    "ann_day_mean": statistics.mean(ann_rets),
                    "normal_day_mean": statistics.mean(normal_rets) if normal_rets else 0,
                    "ann_day_count": len(ann_rets),
                    "ann_day_gap_mean": statistics.mean(ann_gaps) if ann_gaps else 0,
                }
                all_ann_day_returns.extend(ann_rets)
                all_normal_day_returns.extend(normal_rets)
                all_pre_ann_returns.extend(pre_rets)
                all_post_ann_returns.extend(post_rets)
                all_gap_on_ann.extend(ann_gaps)
                all_gap_normal.extend(normal_gaps)

        # Compute aggregate stats
        def _stats(data: list[float]) -> dict:
            if not data:
                return {"n": 0, "mean": None, "median": None, "std": None}
            return {
                "n": len(data),
                "mean": statistics.mean(data),
                "median": statistics.median(data),
                "std": statistics.stdev(data) if len(data) > 1 else 0,
            }

        results = {
            "tickers_analyzed": len(ticker_results),
            "total_announcements": len(anns),
            "announcement_day_returns": _stats(all_ann_day_returns),
            "normal_day_returns": _stats(all_normal_day_returns),
            "pre_announcement_returns": _stats(all_pre_ann_returns),
            "post_announcement_returns": _stats(all_post_ann_returns),
            "announcement_day_gaps": _stats(all_gap_on_ann),
            "normal_day_gaps": _stats(all_gap_normal),
            "category_analysis": {},
            "ticker_highlights": [],
            "alpha_signals": [],
        }

        # Category breakdown
        for cat, rets in sorted(category_returns.items()):
            gaps = category_gaps.get(cat, [])
            results["category_analysis"][cat] = {
                "returns": _stats(rets),
                "gaps": _stats(gaps),
            }

        # Find tickers with largest announcement-day vs normal-day spread
        for ticker, tr in sorted(ticker_results.items(), key=lambda x: abs(x[1]["ann_day_mean"] - x[1]["normal_day_mean"]), reverse=True)[:10]:
            spread = tr["ann_day_mean"] - tr["normal_day_mean"]
            results["ticker_highlights"].append({
                "ticker": ticker,
                "ann_day_mean_pct": round(tr["ann_day_mean"], 3),
                "normal_day_mean_pct": round(tr["normal_day_mean"], 3),
                "spread_pct": round(spread, 3),
                "ann_count": tr["ann_day_count"],
                "ann_day_gap_pct": round(tr["ann_day_gap_mean"], 3),
            })

        # Alpha signals
        ann_stats = results["announcement_day_returns"]
        norm_stats = results["normal_day_returns"]
        if ann_stats["mean"] is not None and norm_stats["mean"] is not None:
            mean_diff = ann_stats["mean"] - norm_stats["mean"]
            results["alpha_signals"].append({
                "signal": "announcement_day_excess",
                "value_pct": round(mean_diff, 4),
                "description": f"Announcement days return {mean_diff:+.4f}% more than normal days on average",
            })

        pre_stats = results["pre_announcement_returns"]
        if pre_stats["mean"] is not None and norm_stats["mean"] is not None:
            pre_diff = pre_stats["mean"] - norm_stats["mean"]
            results["alpha_signals"].append({
                "signal": "pre_announcement_drift",
                "value_pct": round(pre_diff, 4),
                "description": f"Day before announcement returns {pre_diff:+.4f}% vs normal (possible info leakage)",
            })

        post_stats = results["post_announcement_returns"]
        if post_stats["mean"] is not None and norm_stats["mean"] is not None:
            post_diff = post_stats["mean"] - norm_stats["mean"]
            results["alpha_signals"].append({
                "signal": "post_announcement_momentum",
                "value_pct": round(post_diff, 4),
                "description": f"Day after announcement returns {post_diff:+.4f}% vs normal (delayed reaction = alpha)",
            })

        gap_ann = results["announcement_day_gaps"]
        gap_norm = results["normal_day_gaps"]
        if gap_ann["mean"] is not None and gap_norm["mean"] is not None:
            gap_diff = gap_ann["mean"] - gap_norm["mean"]
            results["alpha_signals"].append({
                "signal": "announcement_gap",
                "value_pct": round(gap_diff, 4),
                "description": f"Announcement-day opening gaps are {gap_diff:+.4f}% larger than normal (overnight reaction)",
            })

        return results
    finally:
        conn.close()


def print_study(results: dict) -> str:
    """Format the historical study results as a readable report."""
    lines = []
    lines.append("=" * 70)
    lines.append("OSLO BØRS NEWS REACTION STUDY")
    lines.append("=" * 70)
    lines.append("")

    if "error" in results:
        lines.append(f"Error: {results['error']}")
        return "\n".join(lines)

    lines.append(f"Tickers analyzed: {results['tickers_analyzed']}")
    lines.append(f"Total announcements: {results['total_announcements']}")
    lines.append("")

    # Return comparison
    lines.append("--- RETURN COMPARISON ---")
    for label, key in [
        ("Announcement day", "announcement_day_returns"),
        ("Normal day", "normal_day_returns"),
        ("Pre-announcement day", "pre_announcement_returns"),
        ("Post-announcement day", "post_announcement_returns"),
    ]:
        s = results[key]
        if s["mean"] is not None:
            lines.append(f"  {label:25s}  n={s['n']:5d}  mean={s['mean']:+.4f}%  median={s['median']:+.4f}%  std={s['std']:.4f}%")
    lines.append("")

    # Gap comparison
    lines.append("--- GAP ANALYSIS (open vs prev close) ---")
    for label, key in [
        ("Announcement day", "announcement_day_gaps"),
        ("Normal day", "normal_day_gaps"),
    ]:
        s = results[key]
        if s["mean"] is not None:
            lines.append(f"  {label:25s}  n={s['n']:5d}  mean={s['mean']:+.4f}%  median={s['median']:+.4f}%")
    lines.append("")

    # Alpha signals
    lines.append("--- ALPHA SIGNALS ---")
    for sig in results.get("alpha_signals", []):
        emoji = "!!!" if abs(sig["value_pct"]) > 0.1 else "   "
        lines.append(f"  {emoji} {sig['description']}")
    lines.append("")

    # Category breakdown
    lines.append("--- BY CATEGORY ---")
    for cat, data in sorted(results.get("category_analysis", {}).items()):
        r = data["returns"]
        g = data["gaps"]
        if r["mean"] is not None:
            lines.append(f"  {cat[:40]:40s}  n={r['n']:3d}  ret={r['mean']:+.3f}%  gap={g['mean']:+.3f}%")
    lines.append("")

    # Top ticker highlights
    lines.append("--- TOP TICKERS (largest ann-day vs normal spread) ---")
    for th in results.get("ticker_highlights", [])[:10]:
        lines.append(
            f"  {th['ticker']:8s}  ann_day={th['ann_day_mean_pct']:+.3f}%  "
            f"normal={th['normal_day_mean_pct']:+.3f}%  "
            f"spread={th['spread_pct']:+.3f}%  "
            f"gap={th['ann_day_gap_pct']:+.3f}%  "
            f"(n={th['ann_count']})"
        )
    lines.append("")
    lines.append("=" * 70)

    return "\n".join(lines)
