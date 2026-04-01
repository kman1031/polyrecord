"""
Polymarket Market WebSocket client.

Connects to the CLOB market channel, subscribes to all active token IDs,
and records every event to the database via BufferedWriter.

Handles:
- Heartbeat (PING every 9s)
- Dynamic subscribe/unsubscribe (market rotation)
- Auto-reconnect with exponential backoff
- Silence detection (reconnect if no message for 20s)
- Parsing events into structured tables (poly_orderbook, poly_trades, poly_bbo)
  while also storing raw JSON in poly_ws_raw
"""

import asyncio
import json
import logging
import time
from typing import TYPE_CHECKING

import websockets
from websockets.exceptions import ConnectionClosed

from config import POLY_WS_MARKET, POLY_WS_PING_INTERVAL, POLY_WS_SILENCE_TIMEOUT
from utils import utc_now_ms, Reconnector

if TYPE_CHECKING:
    from db import BufferedWriter
    from poly_discovery import MarketDiscovery
    from binance_ws import BinanceWS

logger = logging.getLogger("polyrecord.poly_ws")


def _ws_is_open(ws) -> bool:
    """Check if a websocket connection is open (compatible with websockets >=13)."""
    try:
        return ws is not None and not ws.close_code
    except Exception:
        return False


class PolyMarketWS:
    """Polymarket Market WebSocket recorder."""

    def __init__(
        self,
        writer: "BufferedWriter",
        discovery: "MarketDiscovery",
        binance_ws: "BinanceWS | None" = None,
    ):
        self._writer = writer
        self._discovery = discovery
        self._binance_ws = binance_ws
        self._ws: websockets.WebSocketClientProtocol | None = None
        self._reconnector = Reconnector()
        self._running = False
        self._last_msg_time: float = 0.0
        self._tasks: list[asyncio.Task] = []
        self._subscribed_tokens: set[str] = set()
        # Stats
        self._msg_count: int = 0
        self._connect_count: int = 0

    async def start(self):
        """Start the WebSocket connection loop."""
        self._running = True
        self._tasks.append(asyncio.create_task(self._connection_loop()))
        logger.info("PolyMarketWS started")

    async def stop(self):
        """Stop the WebSocket client."""
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
        logger.info("PolyMarketWS stopped. Total messages: %d", self._msg_count)

    async def subscribe(self, token_ids: list[str]):
        """Dynamically subscribe to new token IDs without reconnecting."""
        if not token_ids or not self._ws:
            return
        new_tokens = [t for t in token_ids if t not in self._subscribed_tokens]
        if not new_tokens:
            return
        msg = json.dumps(
            {
                "operation": "subscribe",
                "assets_ids": new_tokens,
            }
        )
        try:
            await self._ws.send(msg)
            self._subscribed_tokens.update(new_tokens)
            logger.info("Subscribed to %d new tokens", len(new_tokens))
        except Exception:
            logger.exception("Failed to send subscribe")

    async def unsubscribe(self, token_ids: list[str]):
        """Dynamically unsubscribe from token IDs."""
        if not token_ids or not self._ws:
            return
        old_tokens = [t for t in token_ids if t in self._subscribed_tokens]
        if not old_tokens:
            return
        msg = json.dumps(
            {
                "operation": "unsubscribe",
                "assets_ids": old_tokens,
            }
        )
        try:
            await self._ws.send(msg)
            self._subscribed_tokens -= set(old_tokens)
            logger.info("Unsubscribed from %d tokens", len(old_tokens))
        except Exception:
            logger.exception("Failed to send unsubscribe")

    async def _connection_loop(self):
        """Main loop: connect → subscribe → read messages → reconnect on failure."""
        while self._running:
            try:
                await self._connect_and_run()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("PolyMarketWS connection error")

            if not self._running:
                break

            delay = await self._reconnector.wait()
            self._log_connection_event(
                "reconnecting", f"attempt={self._reconnector.attempt}, delay={delay:.1f}s"
            )
            logger.warning("Reconnecting in %.1fs (attempt %d)", delay, self._reconnector.attempt)

    async def _connect_and_run(self):
        """Single connection lifecycle."""
        token_ids = self._discovery.get_all_active_token_ids()
        if not token_ids:
            logger.warning("No active tokens to subscribe. Waiting...")
            await asyncio.sleep(10)
            return

        logger.info("Connecting to %s with %d tokens", POLY_WS_MARKET, len(token_ids))

        async with websockets.connect(
            POLY_WS_MARKET,
            ping_interval=None,  # We handle pings manually
            ping_timeout=None,
            close_timeout=5,
            max_size=2**22,  # 4 MB max message size
        ) as ws:
            self._ws = ws
            self._reconnector.reset()
            self._connect_count += 1
            self._last_msg_time = asyncio.get_running_loop().time()
            self._log_connection_event("connected", f"tokens={len(token_ids)}")

            # Send initial subscription
            sub_msg = json.dumps(
                {
                    "assets_ids": token_ids,
                    "type": "market",
                    "custom_feature_enabled": True,
                }
            )
            await ws.send(sub_msg)
            self._subscribed_tokens = set(token_ids)
            logger.info("Sent subscription for %d tokens", len(token_ids))

            # Run ping + message reader concurrently
            ping_task = asyncio.create_task(self._ping_loop(ws))
            silence_task = asyncio.create_task(self._silence_detector(ws))

            try:
                async for raw_msg in ws:
                    self._last_msg_time = asyncio.get_running_loop().time()

                    # Ignore PONG responses
                    if isinstance(raw_msg, str) and raw_msg.strip().upper() == "PONG":
                        continue

                    self._msg_count += 1
                    await self._handle_message(raw_msg)
            except ConnectionClosed as e:
                logger.warning("Poly WS closed: code=%s reason=%s", e.code, e.reason)
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
        """Send PING every POLY_WS_PING_INTERVAL seconds."""
        while True:
            try:
                await asyncio.sleep(POLY_WS_PING_INTERVAL)
                await ws.send("PING")
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.debug("Ping send failed")
                return

    async def _silence_detector(self, ws):
        """Close connection if no message received for POLY_WS_SILENCE_TIMEOUT."""
        while True:
            await asyncio.sleep(5)
            elapsed = asyncio.get_running_loop().time() - self._last_msg_time
            if elapsed > POLY_WS_SILENCE_TIMEOUT:
                logger.warning("Silence detected (%.1fs). Closing connection.", elapsed)
                self._log_connection_event("silence_timeout", f"elapsed={elapsed:.1f}s")
                await ws.close()
                return

    async def _handle_message(self, raw_msg: str):
        """Parse and store a single WebSocket message."""
        recv_ts = utc_now_ms()

        try:
            data = json.loads(raw_msg)
        except json.JSONDecodeError:
            logger.warning("Non-JSON message: %s", raw_msg[:200])
            return

        # Handle both single events and arrays of events
        events = data if isinstance(data, list) else [data]

        for event in events:
            event_type = event.get("event_type", "unknown")
            token_id = event.get("asset_id", "")
            market_id = event.get("market", "")
            msg_ts = event.get("timestamp")

            # ── Filter: only process events for our subscribed tokens ────────
            # new_market and market_resolved may have no token_id — allow those
            # through so we can handle lifecycle events for our markets.
            # All other event types (book, price_change, trades, bbo) must
            # have a token_id that belongs to one of our active markets.
            if event_type not in ("new_market", "market_resolved"):
                if not token_id or token_id not in self._subscribed_tokens:
                    return

            # Convert timestamp to ms if it's a string
            msg_ts_ms = None
            if msg_ts:
                try:
                    if isinstance(msg_ts, str):
                        from datetime import datetime, timezone

                        dt = datetime.fromisoformat(msg_ts.replace("Z", "+00:00"))
                        msg_ts_ms = int(dt.timestamp() * 1000)
                    elif isinstance(msg_ts, (int, float)):
                        msg_ts_ms = int(msg_ts) if msg_ts > 1e12 else int(msg_ts * 1000)
                except (ValueError, TypeError):
                    pass

            # Only store raw events for our tokens / lifecycle events
            self._writer.write(
                "poly_ws_raw",
                {
                    "recv_ts_ms": recv_ts,
                    "msg_ts_ms": msg_ts_ms,
                    "market_id": market_id,
                    "token_id": token_id,
                    "event_type": event_type,
                    "payload": json.dumps(event),
                },
            )

            # Parse into structured tables
            if event_type == "book":
                self._parse_book(recv_ts, market_id, token_id, event)
            elif event_type == "price_change":
                self._parse_price_change(recv_ts, market_id, token_id, event)
            elif event_type == "last_trade_price":
                self._parse_trade(recv_ts, msg_ts_ms, market_id, token_id, event)
            elif event_type == "best_bid_ask":
                self._parse_bbo(recv_ts, token_id, event)
            elif event_type == "market_resolved":
                self._handle_resolved(event)
            elif event_type == "new_market":
                self._handle_new_market(event)
            # tick_size_change is stored raw only (in poly_ws_raw above)

    def _parse_book(self, recv_ts: int, market_id: str, token_id: str, event: dict):
        """Parse book event into poly_orderbook rows."""
        for side_key, side_label in [("bids", "bid"), ("asks", "ask")]:
            for level, entry in enumerate(event.get(side_key, [])):
                price = float(entry.get("price", 0))
                size = float(entry.get("size", 0))
                if size > 0:
                    self._writer.write(
                        "poly_orderbook",
                        {
                            "recv_ts_ms": recv_ts,
                            "market_id": market_id,
                            "token_id": token_id,
                            "side": side_label,
                            "level": level,
                            "price": price,
                            "size": size,
                            "source": "ws",
                        },
                    )

    def _parse_price_change(self, recv_ts: int, market_id: str, token_id: str, event: dict):
        """Parse price_change event into poly_bbo rows.

        price_change events contain incremental orderbook updates and also
        carry best_bid/best_ask fields, which we store in poly_bbo.
        The full price_changes array is already stored in poly_ws_raw.
        """
        best_bid = event.get("best_bid")
        best_ask = event.get("best_ask")
        if best_bid is not None or best_ask is not None:
            self._writer.write(
                "poly_bbo",
                {
                    "recv_ts_ms": recv_ts,
                    "token_id": token_id,
                    "best_bid": float(best_bid) if best_bid else None,
                    "best_ask": float(best_ask) if best_ask else None,
                    "spread": (float(best_ask) - float(best_bid))
                    if best_bid and best_ask
                    else None,
                },
            )

    def _parse_trade(
        self, recv_ts: int, msg_ts_ms: int | None, market_id: str, token_id: str, event: dict
    ):
        """Parse last_trade_price event into poly_trades row."""
        self._writer.write(
            "poly_trades",
            {
                "recv_ts_ms": recv_ts,
                "msg_ts_ms": msg_ts_ms,
                "market_id": market_id,
                "token_id": token_id,
                "price": float(event.get("price", 0)),
                "size": float(event.get("size", 0)),
                "side": event.get("side", ""),
                "fee_rate_bps": int(event.get("fee_rate_bps", 0))
                if event.get("fee_rate_bps")
                else None,
                "tx_hash": event.get("transaction_hash", ""),
            },
        )

    def _parse_bbo(self, recv_ts: int, token_id: str, event: dict):
        """Parse best_bid_ask event into poly_bbo row."""
        best_bid = float(event["best_bid"]) if event.get("best_bid") else None
        best_ask = float(event["best_ask"]) if event.get("best_ask") else None
        spread = float(event["spread"]) if event.get("spread") else None
        self._writer.write(
            "poly_bbo",
            {
                "recv_ts_ms": recv_ts,
                "token_id": token_id,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "spread": spread,
            },
        )

    def _handle_resolved(self, event: dict):
        """Handle market_resolved event.

        Updates discovery state and writes the settlement result + Binance
        open/close prices to the markets table.
        """
        market_id = event.get("market", "")
        winning = event.get("winning_outcome", "")
        logger.info("Market resolved: %s → %s", market_id[:16], winning)
        self._discovery.handle_market_resolved(market_id, winning)

        # Look up market info to get asset + start_time
        market = self._discovery.get_market_by_token(event.get("asset_id", ""))
        if not market:
            # Try by market_id in both registries
            market = self._discovery.current_markets.get(
                market_id
            ) or self._discovery.upcoming_markets.get(market_id)

        if market and market.start_time and self._binance_ws:
            # Find the Binance symbol for this asset
            from config import ASSETS

            bn_symbol = None
            for name, sym, _ in ASSETS:
                if name == market.asset:
                    bn_symbol = sym
                    break

            if bn_symbol:
                hour_start_ms = int(market.start_time.timestamp() * 1000)
                bn_open, bn_close = self._binance_ws.get_hourly_price(bn_symbol, hour_start_ms)

                if bn_open is not None:
                    self._writer.write_or_replace(
                        "markets",
                        {
                            "market_id": market.market_id,
                            "asset": market.asset,
                            "timeframe": "1h",
                            "start_time_ms": hour_start_ms,
                            "end_time_ms": int(market.end_time.timestamp() * 1000)
                            if market.end_time
                            else None,
                            "up_token_id": market.up_token_id,
                            "down_token_id": market.down_token_id,
                            "result": winning,
                            "bn_open_price": bn_open,
                            "bn_close_price": bn_close,
                            "created_at_ms": utc_now_ms(),
                        },
                    )
                    logger.info(
                        "Market %s Binance prices: open=%.4f close=%.4f",
                        market_id[:16],
                        bn_open,
                        bn_close,
                    )
                else:
                    logger.warning(
                        "Market %s resolved but Binance hourly price not yet cached", market_id[:16]
                    )

    def _handle_new_market(self, event: dict):
        """Handle new_market event (log only; discovery handles subscription)."""
        question = event.get("question", "")
        logger.debug("New market created: %s", question[:80])

    def _log_connection_event(self, event: str, detail: str = ""):
        """Write to connection_log table."""
        self._writer.write(
            "connection_log",
            {
                "ts_ms": utc_now_ms(),
                "source": "poly_ws",
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
            "subscribed_tokens": len(self._subscribed_tokens),
        }
