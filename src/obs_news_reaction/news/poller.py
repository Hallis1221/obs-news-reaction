"""Continuous polling loop for NewsWeb announcements."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone

from obs_news_reaction.config import NEWSWEB_POLL_INTERVAL
from obs_news_reaction.db.operations import (
    get_known_message_ids,
    insert_announcement,
    log_fetch_start,
    log_fetch_end,
)
from obs_news_reaction.news.scraper import scrape_announcements

log = logging.getLogger(__name__)


def poll_once() -> int:
    """Run one poll cycle. Returns count of new announcements inserted."""
    fetch_id = log_fetch_start("newsweb")
    error = None
    inserted = 0

    try:
        known = get_known_message_ids()
        since = datetime.now(timezone.utc) - timedelta(days=1)
        raw = scrape_announcements(since=since, max_pages=3)

        for ann in raw:
            if ann["message_id"] in known:
                continue
            row_id = insert_announcement(
                message_id=ann["message_id"],
                ticker=ann["ticker"],
                published_at=ann["published_at"],
                category=ann["category"],
                title=ann["title"],
                url=ann["url"],
                issuer_name=ann.get("issuer_name"),
            )
            if row_id is not None:
                inserted += 1
                log.info(f"New: {ann['ticker']} — {ann['title'][:60]}")

    except Exception as e:
        error = str(e)
        log.error(f"Poll failed: {e}")
    finally:
        log_fetch_end(fetch_id, records_fetched=inserted, error_message=error)

    return inserted


def poll_loop() -> None:
    """Run polling indefinitely."""
    log.info(f"Starting poll loop (interval={NEWSWEB_POLL_INTERVAL}s)")
    while True:
        try:
            n = poll_once()
            log.info(f"Poll cycle done: {n} new announcements")
        except KeyboardInterrupt:
            log.info("Poll loop stopped by user")
            break
        except Exception as e:
            log.error(f"Poll cycle error: {e}")
        time.sleep(NEWSWEB_POLL_INTERVAL)
