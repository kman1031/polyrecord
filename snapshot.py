"""
REST orderbook snapshot poller.

Periodically fetches full orderbook snapshots via the Polymarket CLOB
REST API. Serves as:
1. A backup/validation against WebSocket data
2. A way to recover full orderbook state after WS reconnection
3. Stored in poly_rest_snapshot table for cross-reference

Uses the batch endpoint POST /books to minimize requests.
"""

import asyncio
import logging
from typing import TYPE_CHECKING

import aiohttp

from config import POLY_CLOB_URL, SNAPSHOT_INTERVAL
from utils import utc_now_ms

if TYPE_CHECKING:
    from db import BufferedWriter
    from poly_discovery import MarketDiscovery

logger = logging.getLogger("polyrecord.snapshot")


class SnapshotPoller:
    """Periodically fetches orderbook snapshots from the REST API."""

    def __init__(self, writer: "BufferedWriter", discovery: "MarketDiscovery"):
        self._writer = writer
        self._discovery = discovery
        self._session: aiohttp.ClientSession | None = None
        self._running = False
        self._task: asyncio.Task | None = None
        self._snapshot_count: int = 0
        self._error_count: int = 0

    async def start(self):
        """Start the snapshot polling loop."""
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
        self._running = True
        self._task = asyncio.create_task(self._poll_loop())
        logger.info("SnapshotPoller started (interval=%ds)", SNAPSHOT_INTERVAL)

    async def stop(self):
        """Stop polling and cleanup."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._session:
            await self._session.close()
            self._session = None
        logger.info(
            "SnapshotPoller stopped. Snapshots: %d, Errors: %d",
            self._snapshot_count,
            self._error_count,
        )

    async def fetch_once(self):
        """Fetch one round of snapshots for all active tokens. Can be called externally (e.g., after WS reconnect)."""
        await self._fetch_snapshots()

    async def _poll_loop(self):
        """Periodic polling loop."""
        while self._running:
            try:
                await self._fetch_snapshots()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Error in snapshot poll")
                self._error_count += 1
            await asyncio.sleep(SNAPSHOT_INTERVAL)

    async def _fetch_snapshots(self):
        """Fetch orderbooks for all active tokens via batch endpoint."""
        if not self._session:
            return

        # Collect all active token IDs
        all_markets = {
            **self._discovery.current_markets,
            **self._discovery.upcoming_markets,
        }
        if not all_markets:
            return

        token_ids = []
        for m in all_markets.values():
            token_ids.extend(m.token_ids)

        if not token_ids:
            return

        # Build batch request body
        body = [{"token_id": tid} for tid in token_ids]

        url = f"{POLY_CLOB_URL}/books"
        recv_ts = utc_now_ms()

        try:
            async with self._session.post(url, json=body) as resp:
                if resp.status != 200:
                    logger.warning("Snapshot REST returned %d", resp.status)
                    self._error_count += 1
                    return
                books = await resp.json()
        except Exception:
            logger.exception("Failed to fetch snapshots")
            self._error_count += 1
            return

        # Parse and store each orderbook
        row_count = 0
        for book in books:
            market_id = book.get("market", "")
            token_id = book.get("asset_id", "")

            for side_key, side_label in [("bids", "bid"), ("asks", "ask")]:
                for level, entry in enumerate(book.get(side_key, [])):
                    price = float(entry.get("price", 0))
                    size = float(entry.get("size", 0))
                    if size > 0:
                        self._writer.write(
                            "poly_rest_snapshot",
                            {
                                "recv_ts_ms": recv_ts,
                                "market_id": market_id,
                                "token_id": token_id,
                                "side": side_label,
                                "level": level,
                                "price": price,
                                "size": size,
                            },
                        )
                        row_count += 1

        self._snapshot_count += 1
        if self._snapshot_count % 10 == 0:  # Log every ~5 minutes
            logger.info(
                "Snapshot #%d: %d tokens, %d orderbook rows",
                self._snapshot_count,
                len(token_ids),
                row_count,
            )

    @property
    def stats(self) -> dict:
        return {
            "snapshot_count": self._snapshot_count,
            "error_count": self._error_count,
        }
