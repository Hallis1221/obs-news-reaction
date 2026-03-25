"""Parse PDMR (insider trade) message bodies from NewsWeb.

Extracts structured data from the full announcement text:
- Transaction type: BUY or SELL
- Person/entity name and role
- Number of shares
- Price per share
- Total transaction value
"""

from __future__ import annotations

import logging
import re
import time as _time
from dataclasses import dataclass

from obs_news_reaction.config import NEWSWEB_RATE_LIMIT

log = logging.getLogger(__name__)


@dataclass
class PDMRTransaction:
    ticker: str
    message_id: str
    transaction_type: str  # "BUY", "SELL", "UNKNOWN"
    person_name: str | None
    role: str | None  # "CEO", "CFO", "Director", "Board member", etc.
    shares: int | None
    price_per_share: float | None
    currency: str | None
    total_value: float | None
    confidence: float  # 0-1
    raw_text: str


# Keywords that indicate buying
BUY_PATTERNS = [
    r"(?:has |have )?(?:purchased|acquired|bought|buying)",
    r"(?:has |have )?(?:subscribed for|subscription of)",
    r"kjøpt|ervervet|tegnet",
]

# Keywords that indicate selling
SELL_PATTERNS = [
    r"(?:has |have )?(?:sold|disposed|selling|divested)",
    r"(?:sale of|disposal of)",
    r"solgt|avhendet",
]

# Role patterns
ROLE_PATTERNS = [
    (r"\bCEO\b", "CEO"),
    (r"\bCFO\b", "CFO"),
    (r"\bCOO\b", "COO"),
    (r"\bCTO\b", "CTO"),
    (r"\bChair(?:man|woman|person)?\b", "Chair"),
    (r"\bDirector\b", "Director"),
    (r"\b(?:Board )?[Mm]ember\b", "Board member"),
    (r"\bprimary insider\b", "Primary insider"),
    (r"\bprimærinnsider\b", "Primary insider"),
    (r"\bclose associate\b", "Close associate"),
    (r"\bnærstående\b", "Close associate"),
]

# Share count patterns
SHARE_PATTERNS = [
    r"(\d[\d,. ]*)\s*(?:shares|aksjer)",
    r"(?:purchased|acquired|bought|sold)\s+(\d[\d,. ]*)\s*(?:shares|aksjer)?",
]

# Price patterns
PRICE_PATTERNS = [
    r"(?:NOK|USD|EUR)\s*([\d,.]+)\s*(?:per share)?",
    r"(?:price of|pris)\s*(?:NOK|USD|EUR)?\s*([\d,.]+)",
    r"(?:at|til)\s*(?:NOK|USD|EUR)\s*([\d,.]+)",
    r"(?:average price|gjennomsnittspris).*?(?:NOK|USD|EUR)\s*([\d,.]+)",
]

CURRENCY_PATTERN = r"(NOK|USD|EUR)"


def _parse_number(s: str) -> float | None:
    """Parse a number string that may contain commas, spaces, or dots."""
    try:
        # Remove spaces, handle European decimals
        s = s.replace(" ", "").replace(",", "")
        return float(s)
    except (ValueError, AttributeError):
        return None


def parse_pdmr_body(ticker: str, message_id: str, body_text: str) -> PDMRTransaction:
    """Parse a PDMR notification body text into structured transaction data."""
    text = body_text
    text_lower = text.lower()

    # Determine transaction type
    buy_score = sum(1 for p in BUY_PATTERNS if re.search(p, text_lower))
    sell_score = sum(1 for p in SELL_PATTERNS if re.search(p, text_lower))

    if buy_score > sell_score:
        tx_type = "BUY"
        confidence = min(0.9, 0.5 + buy_score * 0.15)
    elif sell_score > buy_score:
        tx_type = "SELL"
        confidence = min(0.9, 0.5 + sell_score * 0.15)
    else:
        tx_type = "UNKNOWN"
        confidence = 0.3

    # Extract role
    role = None
    for pattern, role_name in ROLE_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            role = role_name
            break

    # Extract person name (heuristic: look near role keywords)
    person_name = None
    # Try "Name, Role" or "Role Name" patterns
    name_patterns = [
        r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+),?\s*(?:CEO|CFO|COO|CTO|Director|Chair)",
        r"(?:Mr\.|Ms\.|Mrs\.)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)",
        r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+),?\s*(?:primary insider|primærinnsider)",
    ]
    for np in name_patterns:
        m = re.search(np, text)
        if m:
            person_name = m.group(1).strip()
            break

    # Extract shares
    shares = None
    for sp in SHARE_PATTERNS:
        m = re.search(sp, text, re.IGNORECASE)
        if m:
            shares = int(_parse_number(m.group(1)) or 0) or None
            break

    # Extract price
    price = None
    currency = None
    for pp in PRICE_PATTERNS:
        m = re.search(pp, text, re.IGNORECASE)
        if m:
            price = _parse_number(m.group(1))
            break

    # Extract currency
    cm = re.search(CURRENCY_PATTERN, text)
    if cm:
        currency = cm.group(1)

    # Compute total value
    total_value = None
    if shares and price:
        total_value = shares * price

    return PDMRTransaction(
        ticker=ticker,
        message_id=message_id,
        transaction_type=tx_type,
        person_name=person_name,
        role=role,
        shares=shares,
        price_per_share=price,
        currency=currency,
        total_value=total_value,
        confidence=confidence,
        raw_text=text[:500],
    )


def fetch_and_parse_pdmr(url: str, ticker: str, message_id: str) -> PDMRTransaction | None:
    """Fetch a PDMR message from NewsWeb and parse it."""
    from playwright.sync_api import sync_playwright

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, wait_until="networkidle", timeout=30000)
            _time.sleep(2)
            body = page.inner_text("body")
            browser.close()

        return parse_pdmr_body(ticker, message_id, body)
    except Exception as e:
        log.error(f"Failed to fetch/parse {url}: {e}")
        return None


def analyze_all_pdmr() -> list[PDMRTransaction]:
    """Fetch and parse all PDMR announcements in the database."""
    from obs_news_reaction.db.schema import get_connection
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT id, ticker, url, message_id, title FROM announcements
            WHERE category LIKE '%MANDATORY NOTIFICATION%'
            ORDER BY published_at DESC
        """).fetchall()
    finally:
        conn.close()

    results = []
    for i, row in enumerate(rows):
        log.info(f"[{i+1}/{len(rows)}] Parsing {row['ticker']}: {row['title'][:50]}")
        tx = fetch_and_parse_pdmr(row["url"], row["ticker"], row["message_id"])
        if tx:
            results.append(tx)
        _time.sleep(NEWSWEB_RATE_LIMIT)

    return results


def print_pdmr_analysis(transactions: list[PDMRTransaction]) -> str:
    """Format PDMR analysis as a report."""
    lines = []
    lines.append("=" * 70)
    lines.append("PDMR TRANSACTION ANALYSIS (from message bodies)")
    lines.append("=" * 70)
    lines.append("")

    buys = [t for t in transactions if t.transaction_type == "BUY"]
    sells = [t for t in transactions if t.transaction_type == "SELL"]
    unknown = [t for t in transactions if t.transaction_type == "UNKNOWN"]

    lines.append(f"Total: {len(transactions)} | BUY: {len(buys)} | SELL: {len(sells)} | UNKNOWN: {len(unknown)}")
    lines.append("")

    for label, txs in [("BUYS", buys), ("SELLS", sells), ("UNKNOWN", unknown)]:
        if not txs:
            continue
        lines.append(f"--- {label} ({len(txs)}) ---")
        for t in txs:
            val_str = f" {t.currency} {t.total_value:,.0f}" if t.total_value and t.currency else ""
            shares_str = f" {t.shares:,} shares" if t.shares else ""
            role_str = f" ({t.role})" if t.role else ""
            name_str = t.person_name or "?"
            lines.append(
                f"  {t.ticker:8s} [{t.confidence:.0%}] {name_str}{role_str}{shares_str}{val_str}"
            )
        lines.append("")

    lines.append("=" * 70)
    return "\n".join(lines)
