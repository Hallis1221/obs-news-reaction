"""Real-time signal monitor — polls NewsWeb and alerts on high-score signals.

Combines scraping and signal detection into a continuous loop.
Supports stdout alerts and optional webhook delivery.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone

from obs_news_reaction.config import NEWSWEB_POLL_INTERVAL
from obs_news_reaction.db.operations import get_known_message_ids, insert_announcement
from obs_news_reaction.news.scraper import scrape_announcements
from obs_news_reaction.signals import score_announcement, Signal, SIGNAL_THRESHOLD

log = logging.getLogger(__name__)


def _format_alert(signal: Signal) -> str:
    """Format a signal as a human-readable alert."""
    a = signal.announcement
    liq = ""
    if signal.avg_daily_value_nok is not None:
        liq = f" | vol={signal.avg_daily_value_nok/1e6:.1f}M NOK/day"
    elif not signal.liquid:
        liq = " | ILLIQUID"

    return (
        f"[{signal.action}] {a.ticker} [{signal.score:+.1f}%]{liq}\n"
        f"  {a.title[:70]}\n"
        f"  {signal.reasoning}\n"
        f"  {a.url}"
    )


def _send_webhook(url: str, signal: Signal) -> bool:
    """Send a signal alert to a webhook URL (Slack/Discord compatible)."""
    try:
        import httpx
        a = signal.announcement
        payload = {
            "text": _format_alert(signal),
            # Discord/Slack compatible embed
            "embeds": [{
                "title": f"{signal.action} {a.ticker} [{signal.score:+.1f}%]",
                "description": a.title,
                "url": a.url,
                "color": 0x00FF00 if signal.action == "BUY" else 0xFF0000,
                "fields": [
                    {"name": "Category", "value": a.category[:50], "inline": True},
                    {"name": "Score", "value": f"{signal.score:+.1f}%", "inline": True},
                    {"name": "Reasoning", "value": signal.reasoning[:100], "inline": False},
                ],
                "timestamp": a.published_at,
            }],
        }
        resp = httpx.post(url, json=payload, timeout=10)
        return resp.status_code < 300
    except Exception as e:
        log.error(f"Webhook failed: {e}")
        return False


def monitor_loop(
    interval: int | None = None,
    min_score: float = SIGNAL_THRESHOLD,
    webhook_url: str | None = None,
    liquid_only: bool = True,
) -> None:
    """Continuously monitor for new high-score signals.

    Args:
        interval: Seconds between poll cycles (default: NEWSWEB_POLL_INTERVAL)
        min_score: Minimum signal score to alert on
        webhook_url: Optional webhook URL for Slack/Discord notifications
        liquid_only: Skip illiquid stocks
    """
    if interval is None:
        interval = NEWSWEB_POLL_INTERVAL

    log.info(f"Starting signal monitor (interval={interval}s, min_score={min_score})")
    if webhook_url:
        log.info(f"Webhook enabled: {webhook_url[:30]}...")

    seen_ids: set[str] = get_known_message_ids()
    alerts_sent = 0

    print("Signal monitor started. Watching for Oslo Børs announcements...")
    print(f"Min score: {min_score}, Liquid only: {liquid_only}")
    if webhook_url:
        print(f"Webhook: {webhook_url[:40]}...")
    print("-" * 60)

    while True:
        try:
            since = datetime.now(timezone.utc) - timedelta(hours=2)
            raw = scrape_announcements(since=since, max_pages=1)

            new_signals = []
            for ann_data in raw:
                if ann_data["message_id"] in seen_ids:
                    continue

                # Insert into DB
                row_id = insert_announcement(
                    message_id=ann_data["message_id"],
                    ticker=ann_data["ticker"],
                    published_at=ann_data["published_at"],
                    category=ann_data["category"],
                    title=ann_data["title"],
                    url=ann_data["url"],
                    issuer_name=ann_data.get("issuer_name"),
                )
                seen_ids.add(ann_data["message_id"])

                if row_id is None:
                    continue

                # Score the announcement
                from obs_news_reaction.db.operations import get_announcement_by_message_id
                ann_obj = get_announcement_by_message_id(ann_data["message_id"])
                if ann_obj is None:
                    continue

                sig = score_announcement(ann_obj)

                # Check liquidity
                from obs_news_reaction.signals import _check_liquidity
                is_liquid, avg_val = _check_liquidity(ann_obj.ticker)
                sig.liquid = is_liquid
                sig.avg_daily_value_nok = avg_val

                if abs(sig.score) >= min_score:
                    if liquid_only and not is_liquid and sig.action == "BUY":
                        continue
                    new_signals.append(sig)

            # Alert on new signals
            for sig in sorted(new_signals, key=lambda s: s.score, reverse=True):
                alert_text = _format_alert(sig)
                print(f"\n{'='*60}")
                print(f"NEW SIGNAL @ {datetime.now().strftime('%H:%M:%S')}")
                print(alert_text)
                print(f"{'='*60}")

                if webhook_url:
                    ok = _send_webhook(webhook_url, sig)
                    if ok:
                        log.info(f"Webhook sent for {sig.announcement.ticker}")
                    else:
                        log.warning(f"Webhook failed for {sig.announcement.ticker}")

                alerts_sent += 1

            if not new_signals:
                now = datetime.now().strftime("%H:%M:%S")
                print(f"  [{now}] No new signals (total alerts: {alerts_sent})", end="\r")

        except KeyboardInterrupt:
            print(f"\nMonitor stopped. Total alerts sent: {alerts_sent}")
            break
        except Exception as e:
            log.error(f"Monitor cycle error: {e}")

        time.sleep(interval)
