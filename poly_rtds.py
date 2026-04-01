"""
Polymarket RTDS (Real-Time Data Socket) client.

Records the Binance price relay from Polymarket's infrastructure.
This shows what price Poly's internal systems see, useful for
measuring information delay vs direct Binance connection.

Stores data in the poly_rtds_prices table.
"""

import asyncio
import json
import logging
from typing import TYPE_CHECKING

import websockets
from websockets.exceptions import ConnectionClosed

from config import (
    POLY_RTDS_URL,
    POLY_RTDS_PING_INTERVAL,
    POLY_RTDS_SILENCE_TIMEOUT,
    BINANCE_SYMBOLS,
)
from utils import utc_now_ms, Reconnector

if TYPE_CHECKING:
    from db import BufferedWriter

logger = logging.getLogger("polyrecord.poly_rtds")


def _ws_is_open(ws) -> bool:
    """Check if a websocket connection is open (compatible with websockets >=13)."""
    try:
        return ws is not None and not ws.close_code
    except Exception:
        return False


class PolyRTDS:
    """Polymarket RTDS WebSocket recorder for crypto prices."""

    def __init__(self, writer: "BufferedWriter"):
        self._writer = writer
        self._ws: websockets.WebSocketClientProtocol | None = None
        self._reconnector = Reconnector()
        self._running = False
        self._last_msg_time: float = 0.0
        self._tasks: list[asyncio.Task] = []
        self._msg_count: int = 0
        self._connect_count: int = 0

    async def start(self):
        """Start the RTDS connection loop."""
        self._running = True
        self._tasks.append(asyncio.create_task(self._connection_loop()))
        logger.info("PolyRTDS started")

    async def stop(self):
        """Stop the RTDS client."""
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
        logger.info("PolyRTDS stopped. Total messages: %d", self._msg_count)

    async def _connection_loop(self):
        """Main loop: connect → subscribe → read → reconnect."""
        while self._running:
            try:
                await self._connect_and_run()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("PolyRTDS connection error")

            if not self._running:
                break

            delay = await self._reconnector.wait()
            self._log_connection_event(
                "reconnecting", f"attempt={self._reconnector.attempt}, delay={delay:.1f}s"
            )
            logger.warning(
                "RTDS reconnecting in %.1fs (attempt %d)", delay, self._reconnector.attempt
            )

    async def _connect_and_run(self):
        """Single connection lifecycle."""
        logger.info("Connecting to RTDS: %s", POLY_RTDS_URL)

        async with websockets.connect(
            POLY_RTDS_URL,
            ping_interval=None,
            ping_timeout=None,
            close_timeout=5,
            max_size=2**20,  # 1 MB
        ) as ws:
            self._ws = ws
            self._reconnector.reset()
            self._connect_count += 1
            self._last_msg_time = asyncio.get_running_loop().time()
            self._log_connection_event("connected", "")

            # Subscribe to crypto prices (no filters = receive all symbols)
            # Filters field expects a JSON object/array, not a comma-separated
            # string. Omitting filters is the simplest working approach.
            sub_msg = json.dumps(
                {
                    "action": "subscribe",
                    "subscriptions": [
                        {
                            "topic": "crypto_prices",
                            "type": "update",
                        }
                    ],
                }
            )
            await ws.send(sub_msg)
            logger.info("RTDS subscribed to crypto_prices (all symbols)")

            # Run ping + reader + silence detector concurrently
            ping_task = asyncio.create_task(self._ping_loop(ws))
            silence_task = asyncio.create_task(self._silence_detector(ws))

            try:
                async for raw_msg in ws:
                    self._last_msg_time = asyncio.get_running_loop().time()

                    if isinstance(raw_msg, str) and raw_msg.strip().upper() == "PONG":
                        continue

                    self._msg_count += 1
                    self._handle_message(raw_msg)
            except ConnectionClosed as e:
                logger.warning("RTDS closed: code=%s reason=%s", e.code, e.reason)
                self._log_connection_event("disconnected", f"code={e.code}")
            finally:
                ping_task.cancel()
                silence_task.cancel()
                try:
                    await ping_task
                except asyncio.CancelledError:
                    pass
                try:
                    await silence_task
                except asyncio.CancelledError:
                    pass
                self._ws = None

    async def _ping_loop(self, ws):
        """Send PING every POLY_RTDS_PING_INTERVAL seconds."""
        while True:
            try:
                await asyncio.sleep(POLY_RTDS_PING_INTERVAL)
                await ws.send("PING")
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.debug("RTDS ping send failed")
                return

    async def _silence_detector(self, ws):
        """Close connection if no message for POLY_RTDS_SILENCE_TIMEOUT."""
        while True:
            await asyncio.sleep(5)
            elapsed = asyncio.get_running_loop().time() - self._last_msg_time
            if elapsed > POLY_RTDS_SILENCE_TIMEOUT:
                logger.warning("RTDS silence detected (%.1fs). Closing.", elapsed)
                self._log_connection_event("silence_timeout", f"elapsed={elapsed:.1f}s")
                await ws.close()
                return

    def _handle_message(self, raw_msg: str):
        """Parse and store a crypto_prices message."""
        recv_ts = utc_now_ms()

        try:
            data = json.loads(raw_msg)
        except json.JSONDecodeError:
            return

        topic = data.get("topic", "")
        if topic != "crypto_prices":
            return

        payload = data.get("payload")
        if not payload:
            return

        symbol = payload.get("symbol", "").lower()
        source_ts = payload.get("timestamp")
        value = payload.get("value")

        if not symbol or value is None:
            return

        # Only store prices for our 4 target assets, discard everything else
        if symbol not in BINANCE_SYMBOLS:
            return

        source_ts_ms = None
        if source_ts is not None:
            source_ts_ms = int(source_ts) if source_ts > 1e12 else int(source_ts * 1000)

        self._writer.write(
            "poly_rtds_prices",
            {
                "recv_ts_ms": recv_ts,
                "source_ts_ms": source_ts_ms,
                "symbol": symbol.lower(),
                "price": float(value),
            },
        )

    def _log_connection_event(self, event: str, detail: str = ""):
        """Write to connection_log table."""
        self._writer.write(
            "connection_log",
            {
                "ts_ms": utc_now_ms(),
                "source": "poly_rtds",
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
