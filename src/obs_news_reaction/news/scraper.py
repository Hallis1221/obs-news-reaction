"""NewsWeb scraper — API-first with Playwright DOM fallback."""

from __future__ import annotations

import json
import logging
import re
import time as _time
from datetime import datetime, timezone

import httpx

from obs_news_reaction.config import NEWSWEB_BASE_URL, NEWSWEB_RATE_LIMIT
from obs_news_reaction.utils.oslo_tz import to_utc, OSLO_TZ

log = logging.getLogger(__name__)


def scrape_announcements(
    since: datetime | None = None, max_pages: int = 10,
) -> list[dict]:
    """Fetch announcements from NewsWeb. Tries API first, falls back to DOM."""
    log.info(f"Scraping NewsWeb (since={since}, max_pages={max_pages})")
    results = _scrape_via_api(since, max_pages)
    if results:
        log.info(f"Got {len(results)} announcements via API")
        return results
    log.info("API returned nothing, trying DOM scraping")
    results = _scrape_via_dom(since, max_pages)
    log.info(f"Got {len(results)} announcements via DOM")
    return results


def _scrape_via_api(since: datetime | None, max_pages: int) -> list[dict]:
    announcements = []
    search_url = f"{NEWSWEB_BASE_URL}/search/category"

    for page_num in range(1, max_pages + 1):
        log.info(f"API page {page_num}: {search_url}")
        try:
            resp = httpx.get(
                search_url, params={"page": page_num},
                timeout=15, follow_redirects=True,
            )
            if resp.status_code != 200:
                log.warning(f"API {resp.status_code}, falling back")
                return []
            data = resp.json()
            messages = _extract_messages(data)
            if not messages:
                break
            for msg in messages:
                if since and msg.get("published_at"):
                    pub = datetime.fromisoformat(msg["published_at"])
                    if pub.tzinfo is None:
                        pub = pub.replace(tzinfo=timezone.utc)
                    if pub < since:
                        return announcements
                announcements.append(msg)
            _time.sleep(NEWSWEB_RATE_LIMIT)
        except Exception as e:
            log.warning(f"API request failed: {e}")
            return []
    return announcements


def _extract_messages(data: dict | list) -> list[dict]:
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        items = (
            data.get("messages") or data.get("data") or data.get("results")
            or data.get("items") or data.get("content") or []
        )
        if not isinstance(items, list):
            items = [data]
    else:
        return []
    return [m for m in (_normalize(i) for i in items if isinstance(i, dict)) if m]


def _normalize(item: dict) -> dict | None:
    message_id = (
        item.get("messageId") or item.get("message_id")
        or item.get("id") or item.get("disclosureId")
    )
    if message_id is None:
        return None
    message_id = str(message_id)

    ticker = (
        item.get("ticker") or item.get("issuerSign")
        or item.get("issuer_sign") or _ticker_from_issuer(item) or ""
    )
    if not ticker:
        return None

    published_at = (
        item.get("publishedTime") or item.get("published_at")
        or item.get("publishedAt") or item.get("published")
        or item.get("dateTime") or ""
    )
    if published_at:
        try:
            dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
            published_at = to_utc(dt).isoformat()
        except (ValueError, TypeError):
            pass

    category = (
        item.get("category") or item.get("categoryName")
        or item.get("type") or "UNKNOWN"
    )
    title = item.get("title") or item.get("headline") or item.get("subject") or ""
    url = item.get("url") or item.get("link") or ""
    if not url and message_id:
        url = f"{NEWSWEB_BASE_URL}/message/{message_id}"
    issuer_name = (
        item.get("issuerName") or item.get("issuer_name")
        or item.get("companyName") or item.get("company")
    )

    return {
        "message_id": message_id,
        "ticker": ticker.upper().strip(),
        "published_at": published_at,
        "category": category,
        "title": title,
        "url": url,
        "issuer_name": issuer_name,
    }


def _ticker_from_issuer(item: dict) -> str | None:
    issuer = item.get("issuer") or {}
    if isinstance(issuer, dict):
        return issuer.get("sign") or issuer.get("ticker")
    return None


def _scrape_via_dom(since: datetime | None, max_pages: int) -> list[dict]:
    from playwright.sync_api import sync_playwright

    announcements = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for page_num in range(1, max_pages + 1):
            url = NEWSWEB_BASE_URL
            if page_num > 1:
                url = f"{NEWSWEB_BASE_URL}?page={page_num}"
            log.info(f"DOM page {page_num}: {url}")
            page.goto(url, wait_until="networkidle", timeout=30000)
            _time.sleep(2)

            try:
                page.wait_for_selector(
                    "table, .message-list, [class*='message'], [class*='disclosure']",
                    timeout=10000,
                )
            except Exception:
                log.warning("No message elements found")
                break

            rows = page.query_selector_all(
                "table tbody tr, .message-list .message, [class*='message-row']"
            )
            if not rows:
                rows = page.query_selector_all("a[href*='/message/']")
            if not rows:
                break

            page_msgs = []
            for row in rows:
                msg = _parse_dom_row(row)
                if msg:
                    if since and msg.get("published_at"):
                        try:
                            dt = datetime.fromisoformat(msg["published_at"])
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                            if dt < since:
                                announcements.extend(page_msgs)
                                browser.close()
                                return announcements
                        except ValueError:
                            pass  # unparsed date, keep the message
                    page_msgs.append(msg)
            announcements.extend(page_msgs)
            _time.sleep(NEWSWEB_RATE_LIMIT)
        browser.close()
    return announcements


def _parse_dom_row(element) -> dict | None:
    try:
        # Try structured table row first (td cells)
        cells = element.query_selector_all("td")
        if len(cells) >= 4:
            return _parse_table_row(cells, element)
        # Fallback to link-based parsing
        return _parse_link_row(element)
    except Exception as e:
        log.debug(f"DOM parse failed: {e}")
        return None


def _parse_table_row(cells, row_element) -> dict | None:
    """Parse a structured table row with td[0]=date, td[1]=market, td[2]=ticker, td[3]=title, td[6]=category."""
    # Extract message_id from link
    link = row_element.query_selector("a[href*='/message/']")
    href = link.get_attribute("href") if link else ""
    match = re.search(r"/message/(\d+)", href or "")
    message_id = match.group(1) if match else None
    if not message_id:
        return None

    # td[0] = date/time, td[1] = market, td[2] = ticker, td[3] = title
    raw_date = cells[0].inner_text().strip()
    ticker = cells[2].inner_text().strip().split("\n")[0].strip()
    title_text = cells[3].inner_text().strip()
    # Title is duplicated with "W" markers — take first line
    title = title_text.split("\n")[0].strip()

    # Category from td[6] if available
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

    if not ticker:
        return None

    url = f"{NEWSWEB_BASE_URL}/message/{message_id}"
    return {
        "message_id": message_id,
        "ticker": ticker.upper().strip(),
        "published_at": published_at,
        "category": category,
        "title": title or "(no title)",
        "url": url,
        "issuer_name": None,
    }


def _parse_link_row(element) -> dict | None:
    """Fallback parser for non-table rows."""
    href = element.get_attribute("href") or ""
    match = re.search(r"/message/(\d+)", href)
    message_id = match.group(1) if match else None
    if not message_id:
        link = element.query_selector("a[href*='/message/']")
        if link:
            href = link.get_attribute("href") or ""
            match = re.search(r"/message/(\d+)", href)
            message_id = match.group(1) if match else None
    if not message_id:
        return None

    text = element.inner_text()
    parts = [p.strip() for p in re.split(r"[\n\t]+", text) if p.strip()]
    ticker = title = ""
    for part in parts:
        if re.match(r"^[A-Z]{2,10}$", part) and not ticker:
            ticker = part
        elif len(part) > 20 and not title:
            title = part
    if not ticker and parts:
        ticker = parts[0]

    url = f"{NEWSWEB_BASE_URL}/message/{message_id}" if not href.startswith("http") else href
    return {
        "message_id": message_id,
        "ticker": ticker.upper().strip(),
        "published_at": "",
        "category": "UNKNOWN",
        "title": title or "(no title)",
        "url": url,
        "issuer_name": None,
    }
