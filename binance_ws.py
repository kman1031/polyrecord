"""
Binance combined WebSocket client.

Subscribes to bookTicker + kline_1m for all 4 assets via a single
combined stream connection. Records BBO and 1-minute klines.

Handles:
- Auto-reconnect (Binance forces disconnect every 24h)
- Silence detection
- Exponential backoff
"""

import asyncio
import json
import logging
from typing import TYPE_CHECKING

import websockets
from websockets.exceptions import ConnectionClosed

from config import BINANCE_WS_URL, BINANCE_SYMBOLS, BINANCE_WS_SILENCE_TIMEOUT
from utils import utc_now_ms, Reconnector

if TYPE_CHECKING:
    from db import BufferedWriter

logger = logging.getLogger("polyrecord.binance_ws")


def _ws_is_open(ws) -> bool:
    """Check if a websocket connection is open (compatible with websockets >=13)."""
    try:
        return ws is not None and not ws.close_code
    except Exception:
        return False


class BinanceWS:
    """Binance combined stream WebSocket recorder."""

    def __init__(self, writer: "BufferedWriter"):
        self._writer = writer
        self._ws: websockets.WebSocketClientProtocol | None = None
        self._reconnector = Reconnector()
        self._running = False
        self._last_msg_time: float = 0.0
        self._tasks: list[asyncio.Task] = []
        self._msg_count: int = 0
        self._connect_count: int = 0
        # Hourly price cache: tracks open/close for each symbol per hour.
        # Key: "symbol:hour_start_ms", Value: {"open": float, "close": float}
        # Used by recorder to fill bn_open_price/bn_close_price in markets table.
        self._hourly_prices: dict[str, dict[str, float]] = {}

    async def start(self):
        """Start the Binance WebSocket connection loop."""
        self._running = True
        self._tasks.append(asyncio.create_task(self._connection_loop()))
        logger.info("BinanceWS started")

    async def stop(self):
        """Stop the Binance WebSocket client."""
        self._running = False
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass
        if self._ws:
            await self._ws.close()
            self._ws = None
        logger.info("BinanceWS stopped. Total messages: %d", self._msg_count)

    def _build_url(self) -> str:
        """Build the combined stream URL."""
        streams = []
        for symbol in BINANCE_SYMBOLS:
            streams.append(f"{symbol}@bookTicker")
            streams.append(f"{symbol}@kline_1m")
        stream_param = "/".join(streams)
        return f"{BINANCE_WS_URL}?streams={stream_param}"

    async def _connection_loop(self):
        """Main loop: connect → read → reconnect."""
        while self._running:
            try:
                await self._connect_and_run()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("BinanceWS connection error")

            if not self._running:
                break

            delay = await self._reconnector.wait()
            self._log_connection_event(
                "reconnecting", f"attempt={self._reconnector.attempt}, delay={delay:.1f}s"
            )
            logger.warning(
                "BinanceWS reconnecting in %.1fs (attempt %d)", delay, self._reconnector.attempt
            )

    async def _connect_and_run(self):
        """Single connection lifecycle."""
        url = self._build_url()
        logger.info("Connecting to Binance: %d streams", len(BINANCE_SYMBOLS) * 2)

        async with websockets.connect(
            url,
            ping_interval=None,
            ping_timeout=None,
            close_timeout=5,
            max_size=2**20,  # 1 MB
        ) as ws:
            self._ws = ws
            self._reconnector.reset()
            self._connect_count += 1
            self._last_msg_time = asyncio.get_running_loop().time()
            self._log_connection_event("connected", f"streams={len(BINANCE_SYMBOLS) * 2}")

            silence_task = asyncio.create_task(self._silence_detector(ws))

            try:
                async for raw_msg in ws:
                    self._last_msg_time = asyncio.get_running_loop().time()
                    self._msg_count += 1
                    self._handle_message(raw_msg)
            except ConnectionClosed as e:
                logger.warning("BinanceWS closed: code=%s reason=%s", e.code, e.reason)
                self._log_connection_event("disconnected", f"code={e.code}")
            finally:
                silence_task.cancel()
                try:
                    await silence_task
                except asyncio.CancelledError:
                    pass
                self._ws = None

    async def _silence_detector(self, ws):
        """Close connection if no message for BINANCE_WS_SILENCE_TIMEOUT."""
        while True:
            await asyncio.sleep(5)
            elapsed = asyncio.get_running_loop().time() - self._last_msg_time
            if elapsed > BINANCE_WS_SILENCE_TIMEOUT:
                logger.warning("BinanceWS silence detected (%.1fs). Closing.", elapsed)
                self._log_connection_event("silence_timeout", f"elapsed={elapsed:.1f}s")
                await ws.close()
                return

    def _handle_message(self, raw_msg: str):
        """Parse and route a combined stream message."""
        recv_ts = utc_now_ms()

        try:
            wrapper = json.loads(raw_msg)
        except json.JSONDecodeError:
            return

        stream = wrapper.get("stream", "")
        data = wrapper.get("data")
        if not data:
            return

        if stream.endswith("@bookTicker"):
            self._handle_book_ticker(recv_ts, data)
        elif "@kline_" in stream:
            self._handle_kline(recv_ts, data)

    def _handle_book_ticker(self, recv_ts: int, data: dict):
        """Parse bookTicker into bn_bbo row."""
        symbol = data.get("s", "").lower()  # "BTCUSDT" → "btcusdt"
        self._writer.write(
            "bn_bbo",
            {
                "recv_ts_ms": recv_ts,
                "symbol": symbol,
                "bid_price": float(data.get("b", 0)),
                "bid_size": float(data.get("B", 0)),
                "ask_price": float(data.get("a", 0)),
                "ask_size": float(data.get("A", 0)),
            },
        )

    def _handle_kline(self, recv_ts: int, data: dict):
        """Parse kline into bn_kline_1m row.

        Only writes when the candle is finalized (is_closed=True)
        to avoid hundreds of duplicate rows per minute.
        In-progress candle data is available from bn_bbo for real-time price.

        Also maintains _hourly_prices cache: tracks the open of the first
        1m candle in each hour and the close of the last one.
        """
        k = data.get("k", {})
        is_closed = k.get("x", False)

        # Only store finalized candles to avoid data bloat
        if not is_closed:
            return

        symbol = k.get("s", "").lower()
        open_ts_ms = int(k.get("t", 0))
        open_price = float(k.get("o", 0))
        close_price = float(k.get("c", 0))

        self._writer.write(
            "bn_kline_1m",
            {
                "open_ts_ms": open_ts_ms,
                "close_ts_ms": int(k.get("T", 0)),
                "symbol": symbol,
                "open": open_price,
                "high": float(k.get("h", 0)),
                "low": float(k.get("l", 0)),
                "close": close_price,
                "volume": float(k.get("v", 0)),
                "is_closed": 1,
            },
        )

        # ── Update hourly price cache ────────────────────────────
        # hour_start = floor kline open_ts to the hour
        hour_start_ms = (open_ts_ms // 3_600_000) * 3_600_000
        cache_key = f"{symbol}:{hour_start_ms}"

        if cache_key not in self._hourly_prices:
            # First 1m candle of this hour → record the open
            self._hourly_prices[cache_key] = {
                "open": open_price,
                "close": close_price,
            }
        else:
            # Subsequent candles → update close (always the latest)
            self._hourly_prices[cache_key]["close"] = close_price

        # Evict old entries (keep only last 3 hours to bound memory)
        if len(self._hourly_prices) > len(BINANCE_SYMBOLS) * 3:
            oldest_key = min(self._hourly_prices.keys(), key=lambda k: int(k.split(":")[1]))
            del self._hourly_prices[oldest_key]

    def get_hourly_price(
        self, symbol: str, hour_start_ms: int
    ) -> tuple[float | None, float | None]:
        """Look up cached open/close for a given symbol and hour.

        Returns (open_price, close_price), or (None, None) if not available.
        Called by recorder when a market resolves to fill bn_open_price/bn_close_price.
        """
        cache_key = f"{symbol}:{hour_start_ms}"
        entry = self._hourly_prices.get(cache_key)
        if entry:
            return entry["open"], entry["close"]
        return None, None

    def _log_connection_event(self, event: str, detail: str = ""):
        """Write to connection_log table."""
        self._writer.write(
            "connection_log",
            {
                "ts_ms": utc_now_ms(),
                "source": "binance_ws",
                "event": event,
                "detail": detail,
            },
        )

    @property
    def stats(self) -> dict:
        return {
            "connected": _ws_is_open(self._ws),
            "msg_count": self._msg_count,
            "connect_count": self._connect_count,
        }
