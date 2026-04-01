"""
Polymarket market discovery.

Periodically queries the Gamma API to find active hourly markets
for each asset. Maintains a registry of current and upcoming markets
with their token IDs, so the WS client can subscribe/unsubscribe.
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Awaitable

import aiohttp

from config import (
    POLY_GAMMA_URL,
    POLY_SERIES_SLUGS,
    DISCOVERY_POLL_INTERVAL,
    MARKET_PRE_SUBSCRIBE_SECS,
)
from utils import utc_now_ms, utc_now

logger = logging.getLogger("polyrecord.discovery")


@dataclass
class MarketInfo:
    """Represents a single hourly Polymarket market."""

    market_id: str  # condition_id (0x...)
    asset: str  # BTC, ETH, SOL, XRP
    up_token_id: str  # CLOB token ID for "Up" outcome
    down_token_id: str  # CLOB token ID for "Down" outcome
    start_time: datetime | None = None
    end_time: datetime | None = None
    question: str = ""
    settled: bool = False
    result: str | None = None  # "Up" or "Down" after settlement

    @property
    def token_ids(self) -> list[str]:
        return [self.up_token_id, self.down_token_id]

    @property
    def is_active(self) -> bool:
        now = utc_now()
        if self.settled:
            return False
        if self.start_time and now < self.start_time:
            return False
        if self.end_time and now > self.end_time:
            return False
        return True

    @property
    def starts_within_secs(self) -> float | None:
        if self.start_time is None:
            return None
        delta = (self.start_time - utc_now()).total_seconds()
        return max(0, delta)


# Callback type: called when active markets change
# Receives (new_token_ids_to_subscribe, old_token_ids_to_unsubscribe)
MarketChangeCallback = Callable[[list[str], list[str]], Awaitable[None]]


class MarketDiscovery:
    """
    Discovers and tracks active Polymarket hourly markets.

    Maintains:
    - current_markets: markets for the current hour
    - upcoming_markets: markets for the next hour (pre-discovered)

    Calls on_market_change callback when the active set changes,
    so the WS client can dynamically subscribe/unsubscribe.
    """

    def __init__(self, on_market_change: MarketChangeCallback | None = None):
        self._session: aiohttp.ClientSession | None = None
        self._on_market_change = on_market_change

        # market_id → MarketInfo
        self.current_markets: dict[str, MarketInfo] = {}
        self.upcoming_markets: dict[str, MarketInfo] = {}

        # All known token_ids currently subscribed
        self._subscribed_tokens: set[str] = set()

        self._running = False
        self._poll_task: asyncio.Task | None = None

    async def start(self):
        """Start discovery: initial fetch + periodic polling."""
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
        self._running = True

        # Initial discovery
        await self._discover_all()

        # Start polling loop
        self._poll_task = asyncio.create_task(self._poll_loop())
        logger.info(
            "MarketDiscovery started. Tracking %d current markets.",
            len(self.current_markets),
        )

    async def stop(self):
        """Stop polling and cleanup."""
        self._running = False
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
        if self._session:
            await self._session.close()
            self._session = None
        logger.info("MarketDiscovery stopped.")

    def get_all_active_token_ids(self) -> list[str]:
        """Return all token IDs that should be subscribed to."""
        tokens = []
        for m in self.current_markets.values():
            tokens.extend(m.token_ids)
        for m in self.upcoming_markets.values():
            tokens.extend(m.token_ids)
        return tokens

    def get_market_by_token(self, token_id: str) -> MarketInfo | None:
        """Look up a market by either of its token IDs."""
        for m in {**self.current_markets, **self.upcoming_markets}.values():
            if token_id in m.token_ids:
                return m
        return None

    def handle_market_resolved(self, market_id: str, result: str):
        """Called when a market_resolved WS event is received."""
        for registry in (self.current_markets, self.upcoming_markets):
            if market_id in registry:
                registry[market_id].settled = True
                registry[market_id].result = result
                logger.info("Market %s resolved: %s", market_id[:16], result)
                return

    async def _poll_loop(self):
        """Periodic discovery polling."""
        while self._running:
            try:
                await asyncio.sleep(DISCOVERY_POLL_INTERVAL)
                await self._discover_all()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Error in discovery poll loop")

    async def _discover_all(self):
        """Query Gamma API for all assets' active markets."""
        new_current: dict[str, MarketInfo] = {}
        new_upcoming: dict[str, MarketInfo] = {}

        for asset, slug in POLY_SERIES_SLUGS.items():
            try:
                markets = await self._fetch_series_markets(slug, asset)
                for m in markets:
                    if m.settled:
                        continue

                    # Classify: current (active now) vs upcoming (starts soon)
                    if m.is_active:
                        new_current[m.market_id] = m
                    elif (
                        m.starts_within_secs is not None
                        and m.starts_within_secs <= MARKET_PRE_SUBSCRIBE_SECS
                    ):
                        new_upcoming[m.market_id] = m

            except Exception:
                logger.exception("Failed to discover markets for %s (slug=%s)", asset, slug)

        # Determine token changes
        old_tokens = self._subscribed_tokens
        new_tokens = set()
        for m in {**new_current, **new_upcoming}.values():
            new_tokens.update(m.token_ids)

        to_subscribe = list(new_tokens - old_tokens)
        to_unsubscribe = list(old_tokens - new_tokens)

        self.current_markets = new_current
        self.upcoming_markets = new_upcoming
        self._subscribed_tokens = new_tokens

        if to_subscribe or to_unsubscribe:
            logger.info(
                "Market change: +%d tokens, -%d tokens. Current: %d markets, Upcoming: %d markets",
                len(to_subscribe),
                len(to_unsubscribe),
                len(new_current),
                len(new_upcoming),
            )
            if self._on_market_change:
                try:
                    await self._on_market_change(to_subscribe, to_unsubscribe)
                except Exception:
                    logger.exception("Error in on_market_change callback")

    async def _fetch_series_markets(self, series_slug: str, asset: str) -> list[MarketInfo]:
        """Fetch active markets for a given series slug from Gamma API.

        Strategy:
        1. Try GET /series?slug={slug} to get precise series data with events.
        2. If that fails, fall back to GET /events with keyword filtering.
        """
        if not self._session:
            return []

        # ── Attempt 1: series-specific endpoint ──────────────────
        results = await self._fetch_via_series(series_slug, asset)
        if results:
            return results

        # ── Attempt 2: fallback to generic events query ──────────
        logger.debug(
            "Series endpoint returned nothing for %s, falling back to /events", series_slug
        )
        return await self._fetch_via_events(asset)

    async def _fetch_via_series(self, series_slug: str, asset: str) -> list[MarketInfo]:
        """Try to fetch markets via the /series?slug= endpoint."""
        url = f"{POLY_GAMMA_URL}/series"
        params = {"slug": series_slug}

        try:
            async with self._session.get(url, params=params) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
        except Exception:
            logger.debug("Series endpoint failed for %s", series_slug)
            return []

        # data may be a list or a single object depending on the API version
        series_list = data if isinstance(data, list) else [data]
        results: list[MarketInfo] = []

        for series in series_list:
            events = series.get("events", [])
            for event in events:
                closed = event.get("closed", False)
                if closed:
                    continue
                markets_data = event.get("markets", [])
                for mkt in markets_data:
                    mi = self._parse_market(mkt, event, asset)
                    if mi:
                        results.append(mi)

        return results

    async def _fetch_via_events(self, asset: str) -> list[MarketInfo]:
        """Fallback: fetch via generic /events endpoint with keyword filtering."""
        url = f"{POLY_GAMMA_URL}/events"
        params = {
            "closed": "false",
            "active": "true",
            "tag": "crypto",
            "limit": "100",
            "order": "startDate",
            "ascending": "true",
        }

        try:
            async with self._session.get(url, params=params) as resp:
                if resp.status != 200:
                    logger.warning("Gamma /events returned %d", resp.status)
                    return []
                events = await resp.json()
        except Exception:
            logger.exception("Failed to fetch /events")
            return []

        # Map asset names to possible keywords in questions
        asset_keywords = {
            "BTC": ["bitcoin", "btc"],
            "ETH": ["ethereum", "eth"],
            "SOL": ["solana", "sol"],
            "XRP": ["xrp"],
        }
        keywords = asset_keywords.get(asset, [asset.lower()])

        results: list[MarketInfo] = []
        for event in events:
            for mkt in event.get("markets", []):
                question_lower = mkt.get("question", "").lower()

                # Must match asset keyword
                if not any(kw in question_lower for kw in keywords):
                    continue
                # Must be an up/down market
                if "up" not in question_lower and "down" not in question_lower:
                    continue

                mi = self._parse_market(mkt, event, asset)
                if mi:
                    results.append(mi)

        return results

    def _parse_market(self, mkt: dict, event: dict, asset: str) -> MarketInfo | None:
        """Parse a single market object from the Gamma API into MarketInfo."""
        condition_id = mkt.get("conditionId", "")
        clob_tokens = mkt.get("clobTokenIds", [])
        outcomes = mkt.get("outcomes", [])
        enable_ob = mkt.get("enableOrderBook", False)

        if not condition_id or len(clob_tokens) < 2:
            return None
        if not enable_ob:
            return None

        # Determine up/down token ordering (outcomes is typically ["Up", "Down"])
        up_idx = 0
        down_idx = 1
        if len(outcomes) >= 2:
            found_up = False
            found_down = False
            for i, outcome in enumerate(outcomes):
                if outcome.lower() == "up":
                    up_idx = i
                    found_up = True
                elif outcome.lower() == "down":
                    down_idx = i
                    found_down = True

            if not found_up or not found_down:
                logger.warning(
                    "Unexpected outcomes for %s: %s (question: %s)",
                    condition_id[:16],
                    outcomes,
                    mkt.get("question", "")[:60],
                )

        # Parse times from the parent event
        start_time = None
        end_time = None
        try:
            start_str = event.get("startDate") or event.get("startDateIso")
            end_str = event.get("endDate") or event.get("endDateIso")
            if start_str:
                start_time = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
            if end_str:
                end_time = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            pass

        return MarketInfo(
            market_id=condition_id,
            asset=asset,
            up_token_id=clob_tokens[up_idx],
            down_token_id=clob_tokens[down_idx],
            start_time=start_time,
            end_time=end_time,
            question=mkt.get("question", ""),
        )

    async def write_markets_to_db(self, writer) -> None:
        """Write current market metadata to the database via BufferedWriter.
        Uses write_or_replace to handle duplicate market_ids safely.
        """
        for m in {**self.current_markets, **self.upcoming_markets}.values():
            writer.write_or_replace(
                "markets",
                {
                    "market_id": m.market_id,
                    "asset": m.asset,
                    "timeframe": "1h",
                    "start_time_ms": int(m.start_time.timestamp() * 1000) if m.start_time else None,
                    "end_time_ms": int(m.end_time.timestamp() * 1000) if m.end_time else None,
                    "up_token_id": m.up_token_id,
                    "down_token_id": m.down_token_id,
                    "result": m.result,
                    "bn_open_price": None,
                    "bn_close_price": None,
                    "created_at_ms": utc_now_ms(),
                },
            )
