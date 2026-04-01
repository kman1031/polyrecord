"""
Polyrecord configuration.
All times are UTC. All timestamps are milliseconds.
"""

import os
from pathlib import Path

# ── Directories ──────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("POLYRECORD_DATA_DIR", BASE_DIR / "data"))

# ── Assets & Series ─────────────────────────────────────────
# Each entry: (display_name, binance_symbol, poly_series_slug)
ASSETS = [
    ("BTC", "btcusdt", "bitcoin"),
    ("ETH", "ethusdt", "ethereum"),
    ("SOL", "solusdt", "solana"),
    ("XRP", "xrpusdt", "xrp"),
]

BINANCE_SYMBOLS = [a[1] for a in ASSETS]  # ["btcusdt", ...]
POLY_SERIES_SLUGS = {a[0]: a[2] for a in ASSETS}  # {"BTC": "bitcoin", ...}

# Slug format: {prefix}-up-or-down-{month}-{day}-{year}-{hour}{am/pm}-et
# e.g. "bitcoin-up-or-down-april-1-2026-1pm-et"
# Discovery queries current hour + next N hours to pre-subscribe upcoming markets
DISCOVERY_LOOKAHEAD_HOURS = 2

# ── Polymarket endpoints ─────────────────────────────────────
POLY_CLOB_URL = "https://clob.polymarket.com"
POLY_GAMMA_URL = "https://gamma-api.polymarket.com"
POLY_WS_MARKET = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
POLY_RTDS_URL = "wss://ws-live-data.polymarket.com"

# ── Binance endpoints ────────────────────────────────────────
BINANCE_WS_URL = "wss://stream.binance.com:9443/stream"

# ── Timing (seconds) ─────────────────────────────────────────
POLY_WS_PING_INTERVAL = 9.0  # Poly requires PING every 10s, send at 9s for safety
POLY_RTDS_PING_INTERVAL = 4.0  # RTDS requires PING every 5s, send at 4s
POLY_WS_SILENCE_TIMEOUT = 20.0  # No message for this long → reconnect
POLY_RTDS_SILENCE_TIMEOUT = 10.0  # RTDS should have much more frequent messages
BINANCE_WS_SILENCE_TIMEOUT = 15.0

# ── Reconnection ─────────────────────────────────────────────
RECONNECT_BASE_DELAY = 1.0  # Initial delay in seconds
RECONNECT_MAX_DELAY = 30.0  # Cap
RECONNECT_MULTIPLIER = 2.0  # Exponential factor

# ── Database ─────────────────────────────────────────────────
DB_FLUSH_INTERVAL = 1.0  # BufferedWriter flush every N seconds
DB_FLUSH_MAX_ROWS = 500  # ... or when buffer reaches N rows

# ── Market discovery ─────────────────────────────────────────
DISCOVERY_POLL_INTERVAL = 300  # Query Gamma API every 5 minutes
MARKET_PRE_SUBSCRIBE_SECS = 300  # Subscribe to next market 5 min early

# ── REST snapshot ────────────────────────────────────────────
SNAPSHOT_INTERVAL = 30  # REST orderbook snapshot every 30 seconds

# ── Health check ─────────────────────────────────────────────
HEALTH_CHECK_INTERVAL = 30  # Check every 30 seconds

# ── Compression ──────────────────────────────────────────────
COMPRESS_AFTER_DAYS = 1  # Compress db files older than N days
DISK_USAGE_WARN_GB = 5.0  # Warn when free space below N GB
