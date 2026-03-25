"""Export event results and announcements to CSV and JSON."""

from __future__ import annotations

import csv
import json
import logging
from dataclasses import asdict
from pathlib import Path

from obs_news_reaction.db.operations import get_announcements, get_event_results

log = logging.getLogger(__name__)


def export_announcements_csv(path: Path, ticker: str | None = None, since: str | None = None) -> int:
    anns = get_announcements(ticker=ticker, since=since)
    if not anns:
        return 0
    fields = ["id", "message_id", "ticker", "published_at", "category", "title", "url", "issuer_name", "fetched_at"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for a in anns:
            writer.writerow(asdict(a))
    log.info(f"Exported {len(anns)} announcements to {path}")
    return len(anns)


def export_announcements_json(path: Path, ticker: str | None = None, since: str | None = None) -> int:
    anns = get_announcements(ticker=ticker, since=since)
    if not anns:
        return 0
    with open(path, "w", encoding="utf-8") as f:
        json.dump([asdict(a) for a in anns], f, indent=2, ensure_ascii=False)
    log.info(f"Exported {len(anns)} announcements to {path}")
    return len(anns)


def export_results_csv(
    path: Path, ticker: str | None = None, window_name: str | None = None,
) -> int:
    results = get_event_results(ticker=ticker, window_name=window_name)
    if not results:
        return 0
    fields = [
        "id", "announcement_id", "ticker", "window_name", "abnormal_return",
        "cumulative_ar", "reaction_time_seconds", "pre_event_mean", "pre_event_std",
        "benchmark_return", "computed_at", "data_quality",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for r in results:
            writer.writerow(asdict(r))
    log.info(f"Exported {len(results)} results to {path}")
    return len(results)


def export_results_json(
    path: Path, ticker: str | None = None, window_name: str | None = None,
) -> int:
    results = get_event_results(ticker=ticker, window_name=window_name)
    if not results:
        return 0
    with open(path, "w", encoding="utf-8") as f:
        json.dump([asdict(r) for r in results], f, indent=2, ensure_ascii=False)
    log.info(f"Exported {len(results)} results to {path}")
    return len(results)
