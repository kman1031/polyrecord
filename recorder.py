"""
Polyrecord — Main recorder entry point.

Orchestrates all components:
1. BufferedWriter (database)
2. MarketDiscovery (find active Polymarket hourly markets)
3. PolyMarketWS (record Polymarket orderbook/trades/BBO)
4. PolyRTDS (record Polymarket's Binance price relay)
5. BinanceWS (record Binance BBO + 1m klines)
6. SnapshotPoller (periodic REST orderbook snapshots)
7. HealthChecker (monitor all components)

Lifecycle:
- Start all components in order
- Run until SIGINT/SIGTERM
- Graceful shutdown: flush buffers, close connections, close database
"""

import asyncio
import signal
import sys
import logging

from utils import setup_logging
from db import BufferedWriter
from poly_discovery import MarketDiscovery
from poly_ws import PolyMarketWS
from poly_rtds import PolyRTDS
from binance_ws import BinanceWS
from snapshot import SnapshotPoller
from health import HealthChecker

logger = logging.getLogger("polyrecord")


class Recorder:
    """Main recorder orchestrator."""

    def __init__(self):
        self._writer: BufferedWriter | None = None
        self._discovery: MarketDiscovery | None = None
        self._poly_ws: PolyMarketWS | None = None
        self._poly_rtds: PolyRTDS | None = None
        self._binance_ws: BinanceWS | None = None
        self._snapshot: SnapshotPoller | None = None
        self._health: HealthChecker | None = None
        self._shutdown_event = asyncio.Event()
        self._tasks: list[asyncio.Task] = []

    async def run(self):
        """Start all components and run until shutdown signal."""
        setup_logging("polyrecord")
        logger.info("=" * 60)
        logger.info("Polyrecord starting up...")
        logger.info("=" * 60)

        # Register signal handlers
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._signal_handler, sig)

        try:
            await self._start_all()
            logger.info("All components started. Recording data...")

            # Wait until shutdown signal
            await self._shutdown_event.wait()

        except Exception:
            logger.exception("Fatal error during startup")
        finally:
            await self._stop_all()
            logger.info("Polyrecord shut down complete.")

    async def _start_all(self):
        """Start all components in dependency order."""

        # 1. Database writer
        self._writer = BufferedWriter()
        await self._writer.start()
        logger.info("[1/7] BufferedWriter started")

        # 2. Market discovery
        # Note: on_market_change callback may fire during start(),
        # but poly_ws/snapshot are not yet started. The callback
        # guards against None components. Initial write happens below.
        async def on_market_change(to_sub: list[str], to_unsub: list[str]):
            """Called when active markets change."""
            if self._poly_ws:
                if to_unsub:
                    await self._poly_ws.unsubscribe(to_unsub)
                if to_sub:
                    await self._poly_ws.subscribe(to_sub)
            # Also trigger a snapshot after market change
            if self._snapshot:
                await self._snapshot.fetch_once()
            # Write market metadata to db
            if self._discovery and self._writer:
                await self._discovery.write_markets_to_db(self._writer)

        self._discovery = MarketDiscovery(on_market_change=on_market_change)
        await self._discovery.start()
        logger.info(
            "[2/7] MarketDiscovery started (%d current markets)",
            len(self._discovery.current_markets),
        )

        # 3. Binance WebSocket (started before PolyMarketWS so hourly price
        #    cache is available when market_resolved events arrive)
        self._binance_ws = BinanceWS(self._writer)
        await self._binance_ws.start()
        logger.info("[3/7] BinanceWS started")

        # 4. Polymarket Market WebSocket
        self._poly_ws = PolyMarketWS(self._writer, self._discovery, self._binance_ws)
        await self._poly_ws.start()
        logger.info("[4/7] PolyMarketWS started")

        # 5. Polymarket RTDS
        self._poly_rtds = PolyRTDS(self._writer)
        await self._poly_rtds.start()
        logger.info("[5/7] PolyRTDS started")

        # 6. REST snapshot poller
        self._snapshot = SnapshotPoller(self._writer, self._discovery)
        await self._snapshot.start()
        logger.info("[6/7] SnapshotPoller started")

        # 7. Health checker
        self._health = HealthChecker(
            writer=self._writer,
            poly_ws=self._poly_ws,
            poly_rtds=self._poly_rtds,
            binance_ws=self._binance_ws,
            snapshot=self._snapshot,
            discovery=self._discovery,
        )
        await self._health.start()
        logger.info("[7/7] HealthChecker started")

        # ── Post-start: write initial market metadata & take first snapshot ──
        await self._discovery.write_markets_to_db(self._writer)
        await self._snapshot.fetch_once()
        logger.info("Initial market metadata written and snapshot taken")

        # Start settlement polling loop (REST fallback for market_resolved events)
        self._tasks.append(asyncio.create_task(self._settlement_poll_loop()))

    async def _settlement_poll_loop(self):
        """Poll REST API every 5 minutes to catch settled markets.

        This is a fallback for when market_resolved WS events are not received.
        """
        POLL_INTERVAL = 300  # 5 minutes
        while True:
            try:
                await asyncio.sleep(POLL_INTERVAL)
                if self._discovery and self._writer:
                    n = await self._discovery.poll_settled_results(self._writer, self._binance_ws)
                    if n > 0:
                        logger.info("Settlement poll: %d newly settled markets", n)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Error in settlement poll loop")

    async def _stop_all(self):
        """Stop all components in reverse order."""
        logger.info("Shutting down all components...")
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass

        components = [
            ("HealthChecker", self._health),
            ("SnapshotPoller", self._snapshot),
            ("PolyRTDS", self._poly_rtds),
            ("PolyMarketWS", self._poly_ws),
            ("BinanceWS", self._binance_ws),
            ("MarketDiscovery", self._discovery),
            ("BufferedWriter", self._writer),
        ]

        for name, component in components:
            if component is not None:
                try:
                    await component.stop()
                    logger.info("  ✓ %s stopped", name)
                except Exception:
                    logger.exception("  ✗ Error stopping %s", name)

    def _signal_handler(self, sig):
        """Handle SIGINT/SIGTERM."""
        sig_name = signal.Signals(sig).name
        logger.info("Received %s, initiating shutdown...", sig_name)
        self._shutdown_event.set()


def main():
    """Entry point."""
    recorder = Recorder()

    if sys.platform != "win32":
        # Use uvloop if available for better performance
        try:
            import uvloop

            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        except ImportError:
            pass

    asyncio.run(recorder.run())


if __name__ == "__main__":
    main()
