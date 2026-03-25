"""Configuration: env vars, bucket thresholds, constants."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

DB_PATH = Path(os.environ.get("OBS_NR_DB_PATH", "data/obs_nr.db"))
NEWSWEB_POLL_INTERVAL = int(os.environ.get("OBS_NR_POLL_INTERVAL", "300"))
REACTION_THRESHOLD_SIGMA = float(os.environ.get("OBS_NR_THRESHOLD_SIGMA", "2.0"))
BENCHMARK_TICKER = "OSEBX.OL"
BENCHMARK_FALLBACK = "OBX.OL"

NEWSWEB_BASE_URL = "https://newsweb.oslobors.no"
NEWSWEB_RATE_LIMIT = 2.0  # seconds between page loads
YFINANCE_RATE_LIMIT = 1.0  # seconds between ticker fetches

MCAP_BUCKETS = {
    "micro": (0, 1e9),
    "small": (1e9, 1e10),
    "mid": (1e10, 5e10),
    "large": (5e10, float("inf")),
}
VOLUME_BUCKETS = {
    "low": (0, 1e5),
    "medium": (1e5, 1e6),
    "high": (1e6, float("inf")),
}

# Trading hours for Oslo Bors (CET/CEST)
OSLO_OPEN_HOUR = 9
OSLO_OPEN_MINUTE = 0
OSLO_CLOSE_HOUR = 16
OSLO_CLOSE_MINUTE = 20

# Event study windows: (name, pre_minutes, post_minutes)
EVENT_WINDOWS = [
    ("[-5m,+5m]", -5, 5),
    ("[-15m,+15m]", -15, 15),
    ("[-1h,+1h]", -60, 60),
    ("[0,+1h]", 0, 60),
    ("[0,+1d]", 0, 480),  # ~8 trading hours
]

PRE_EVENT_LOOKBACK_MINUTES = 30
MIN_BARS_FOR_ANALYSIS = 5
