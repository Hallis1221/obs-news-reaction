"""Historical announcement scraper — fetch announcements from past date ranges."""

from __future__ import annotations

import logging
import re
import time as _time
from datetime import datetime, timedelta, timezone

from obs_news_reaction.config import NEWSWEB_BASE_URL, NEWSWEB_RATE_LIMIT
from obs_news_reaction.db.operations import insert_announcement, get_known_message_ids
from obs_news_reaction.utils.oslo_tz import to_utc, OSLO_TZ

log = logging.getLogger(__name__)


def scrape_date_range(
    from_date: str, to_date: str, max_pages: int = 20,
) -> list[dict]:
    """Scrape NewsWeb announcements for a date range using Playwright.

    Args:
        from_date: Start date in YYYY-MM-DD format
        to_date: End date in YYYY-MM-DD format
        max_pages: Maximum pages to scrape per date range
    """
    from playwright.sync_api import sync_playwright

    announcements = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for page_num in range(1, max_pages + 1):
            url = (
                f"{NEWSWEB_BASE_URL}/search?"
                f"category=&issuer=&fromDate={from_date}&toDate={to_date}"
            )
            if page_num > 1:
                url += f"&page={page_num}"

            log.info(f"Historical scrape page {page_num}: {from_date} to {to_date}")
            try:
                page.goto(url, wait_until="networkidle", timeout=30000)
                _time.sleep(2)
            except Exception as e:
                log.warning(f"Page load failed: {e}")
                break

            try:
                page.wait_for_selector("table tbody tr", timeout=10000)
            except Exception:
                log.info("No table rows found, done with this range")
                break

            rows = page.query_selector_all("table tbody tr")
            if not rows:
                break

            page_msgs = []
            for row in rows:
                msg = _parse_historical_row(row)
                if msg:
                    page_msgs.append(msg)

            if not page_msgs:
                break

            announcements.extend(page_msgs)
            log.info(f"  Got {len(page_msgs)} announcements (total: {len(announcements)})")

            # Check if there's a next page button
            next_btn = page.query_selector("a[rel='next'], .pagination .next:not(.disabled)")
            if not next_btn:
                break

            _time.sleep(NEWSWEB_RATE_LIMIT)

        browser.close()

    log.info(f"Historical scrape {from_date} to {to_date}: {len(announcements)} total")
    return announcements


def _parse_historical_row(row_element) -> dict | None:
    """Parse a table row from the historical search results."""
    try:
        cells = row_element.query_selector_all("td")
        if len(cells) < 4:
            return None

        # Extract message_id from link
        link = row_element.query_selector("a[href*='/message/']")
        if not link:
            return None
        href = link.get_attribute("href") or ""
        match = re.search(r"/message/(\d+)", href)
        if not match:
            return None
        message_id = match.group(1)

        # td[0]=date, td[1]=market, td[2]=ticker, td[3]=title, td[6]=category
        raw_date = cells[0].inner_text().strip()
        ticker = cells[2].inner_text().strip().split("\n")[0].strip()
        title_text = cells[3].inner_text().strip()
        title = title_text.split("\n")[0].strip()

        category = "UNKNOWN"
        if len(cells) >= 7:
            cat_text = cells[6].inner_text().strip()
            category = cat_text.split("\n")[0].strip() or "UNKNOWN"

        # Parse date
        published_at = ""
        if raw_date:
            for fmt in ("%d.%m.%Y %H:%M", "%Y-%m-%d %H:%M", "%d.%m.%Y", "%Y-%m-%d"):
                try:
                    dt = datetime.strptime(raw_date, fmt).replace(tzinfo=OSLO_TZ)
                    published_at = to_utc(dt).isoformat()
                    break
                except ValueError:
                    continue

        if not ticker or not published_at:
            return None

        return {
            "message_id": message_id,
            "ticker": ticker.upper().strip(),
            "published_at": published_at,
            "category": category,
            "title": title or "(no title)",
            "url": f"{NEWSWEB_BASE_URL}/message/{message_id}",
            "issuer_name": None,
        }
    except Exception as e:
        log.debug(f"Historical row parse failed: {e}")
        return None


def backfill_announcements(
    months_back: int = 6, chunk_days: int = 7,
) -> int:
    """Backfill announcements by scraping historical data in weekly chunks.

    Args:
        months_back: How many months to go back
        chunk_days: Size of each date range chunk in days
    """
    known = get_known_message_ids()
    inserted = 0
    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=months_back * 30)

    current = start_date
    while current < end_date:
        chunk_end = min(current + timedelta(days=chunk_days), end_date)
        from_str = current.strftime("%Y-%m-%d")
        to_str = chunk_end.strftime("%Y-%m-%d")

        log.info(f"Backfilling {from_str} to {to_str}")
        raw = scrape_date_range(from_str, to_str, max_pages=10)

        chunk_inserted = 0
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
                chunk_inserted += 1
                known.add(ann["message_id"])

        inserted += chunk_inserted
        log.info(f"  {from_str} to {to_str}: {chunk_inserted} new ({len(raw)} total)")

        current = chunk_end + timedelta(days=1)
        _time.sleep(NEWSWEB_RATE_LIMIT)

    log.info(f"Historical backfill complete: {inserted} new announcements")
    return inserted
