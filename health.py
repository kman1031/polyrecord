"""
Health check module.

Periodically checks the status of all components (WebSocket connections,
database writer, market discovery) and logs a summary. Also detects
anomalies like missing data, stale connections, etc.
"""

import asyncio
import logging
from typing import TYPE_CHECKING

from config import HEALTH_CHECK_INTERVAL
from utils import utc_now_ms

if TYPE_CHECKING:
    from db import BufferedWriter
    from poly_ws import PolyMarketWS
    from poly_rtds import PolyRTDS
    from binance_ws import BinanceWS
    from snapshot import SnapshotPoller
    from poly_discovery import MarketDiscovery

logger = logging.getLogger("polyrecord.health")


class HealthChecker:
    """Periodic health checker for all recorder components."""

    def __init__(
        self,
        writer: "BufferedWriter",
        poly_ws: "PolyMarketWS",
        poly_rtds: "PolyRTDS",
        binance_ws: "BinanceWS",
        snapshot: "SnapshotPoller",
        discovery: "MarketDiscovery",
    ):
        self._writer = writer
        self._poly_ws = poly_ws
        self._poly_rtds = poly_rtds
        self._binance_ws = binance_ws
        self._snapshot = snapshot
        self._discovery = discovery
        self._running = False
        self._task: asyncio.Task | None = None
        self._check_count: int = 0
        self._last_stats: dict = {}

    async def start(self):
        """Start periodic health checks."""
        self._running = True
        self._task = asyncio.create_task(self._check_loop())
        logger.info("HealthChecker started (interval=%ds)", HEALTH_CHECK_INTERVAL)

    async def stop(self):
        """Stop health checks."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("HealthChecker stopped")

    def get_status(self) -> dict:
        """Return the latest health status snapshot."""
        return self._last_stats

    async def _check_loop(self):
        """Periodic check loop."""
        while self._running:
            try:
                await asyncio.sleep(HEALTH_CHECK_INTERVAL)
                self._run_check()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Error in health check")

    def _run_check(self):
        """Run one health check cycle."""
        self._check_count += 1

        poly_ws_stats = self._poly_ws.stats
        poly_rtds_stats = self._poly_rtds.stats
        binance_ws_stats = self._binance_ws.stats
        writer_stats = self._writer.stats
        snapshot_stats = self._snapshot.stats

        current_markets = len(self._discovery.current_markets)
        upcoming_markets = len(self._discovery.upcoming_markets)

        # Determine overall health
        issues = []

        if not poly_ws_stats.get("connected"):
            issues.append("poly_ws DISCONNECTED")
        if not poly_rtds_stats.get("connected"):
            issues.append("poly_rtds DISCONNECTED")
        if not binance_ws_stats.get("connected"):
            issues.append("binance_ws DISCONNECTED")
        if not writer_stats.get("connected"):
            issues.append("db writer NOT CONNECTED")
        if current_markets == 0:
            issues.append("NO active markets")
        if writer_stats.get("buffer_count", 0) > 5000:
            issues.append(f"buffer backlog: {writer_stats['buffer_count']}")

        status = "HEALTHY" if not issues else "DEGRADED"

        self._last_stats = {
            "check_num": self._check_count,
            "status": status,
            "issues": issues,
            "poly_ws": poly_ws_stats,
            "poly_rtds": poly_rtds_stats,
            "binance_ws": binance_ws_stats,
            "writer": writer_stats,
            "snapshot": snapshot_stats,
            "markets": {
                "current": current_markets,
                "upcoming": upcoming_markets,
            },
        }

        # Log summary
        if self._check_count % 2 == 0:  # Every ~60s
            logger.info(
                "[%s] poly_ws:%s(%d msgs) | rtds:%s(%d) | bn:%s(%d) | "
                "db:%d rows/%d buf | markets:%d+%d | snap:%d",
                status,
                "OK" if poly_ws_stats.get("connected") else "DOWN",
                poly_ws_stats.get("msg_count", 0),
                "OK" if poly_rtds_stats.get("connected") else "DOWN",
                poly_rtds_stats.get("msg_count", 0),
                "OK" if binance_ws_stats.get("connected") else "DOWN",
                binance_ws_stats.get("msg_count", 0),
                writer_stats.get("total_written", 0),
                writer_stats.get("buffer_count", 0),
                current_markets,
                upcoming_markets,
                snapshot_stats.get("snapshot_count", 0),
            )

        if issues:
            logger.warning("Health issues: %s", "; ".join(issues))

        # Write health event to connection_log for historical tracking
        self._writer.write(
            "connection_log",
            {
                "ts_ms": utc_now_ms(),
                "source": "health",
                "event": status.lower(),
                "detail": "; ".join(issues) if issues else "all systems nominal",
            },
        )
