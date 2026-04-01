"""
Polymarket market discovery.

Generates event slugs based on current UTC time and queries the Gamma API
directly by slug. This is required because the restricted hourly up/down
markets do not appear in generic list endpoints.

Slug format: {prefix}-up-or-down-{month}-{day}-{year}-{hour}{am/pm}-et
Example:     bitcoin-up-or-down-april-1-2026-1pm-et

ET offset: UTC-4 (EDT, March–November) / UTC-5 (EST, November–March)
"""

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Callable, Awaitable

import aiohttp

from config import (
    POLY_GAMMA_URL,
    POLY_SERIES_SLUGS,
    DISCOVERY_POLL_INTERVAL,
    MARKET_PRE_SUBSCRIBE_SECS,
    DISCOVERY_LOOKAHEAD_HOURS,
)
from utils import utc_now_ms, utc_now

logger = logging.getLogger("polyrecord.discovery")

# Month name mapping for slug generation
_MONTHS = {
    1: "january",
    2: "february",
    3: "march",
    4: "april",
    5: "may",
    6: "june",
    7: "july",
    8: "august",
    9: "september",
    10: "october",
    11: "november",
    12: "december",
}


def _et_offset(dt: datetime) -> int:
    """Return ET UTC offset in hours: -4 (EDT) or -5 (EST).

    EDT: second Sunday of March → first Sunday of November
    EST: otherwise
    """
    year = dt.year
    # Second Sunday of March
    march = datetime(year, 3, 8, 2, 0, tzinfo=timezone.utc)
    edt_start = march + timedelta(days=(6 - march.weekday()) % 7)
    # First Sunday of November
    november = datetime(year, 11, 1, 2, 0, tzinfo=timezone.utc)
    est_start = november + timedelta(days=(6 - november.weekday()) % 7)
    dt_utc = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
    return -4 if edt_start <= dt_utc < est_start else -5


def _build_slug(prefix: str, dt_utc: datetime) -> str:
    """Build an event slug for the given UTC hour.

    Converts UTC to ET, then formats as:
    {prefix}-up-or-down-{month}-{day}-{year}-{hour}{am/pm}-et
    """
    offset = _et_offset(dt_utc)
    dt_et = dt_utc + timedelta(hours=offset)
    hour_24 = dt_et.hour  # 0-23 in ET

    if hour_24 == 0:
        hour_str = "12am"
    elif hour_24 < 12:
        hour_str = f"{hour_24}am"
    elif hour_24 == 12:
        hour_str = "12pm"
    else:
        hour_str = f"{hour_24 - 12}pm"

    month = _MONTHS[dt_et.month]
    day = dt_et.day
    year = dt_et.year

    return f"{prefix}-up-or-down-{month}-{day}-{year}-{hour_str}-et"


@dataclass
class MarketInfo:
    """Represents a single hourly Polymarket market."""

    market_id: str  # condition_id (0x...)
    asset: str  # BTC, ETH, SOL, XRP
    up_token_id: str  # CLOB token ID for "Up" outcome
    down_token_id: str  # CLOB token ID for "Down" outcome
    start_time: datetime | None = None  # eventStartTime (candle open)
    end_time: datetime | None = None  # endDate (candle close / resolution)
    question: str = ""
    open_price: float | None = None  # eventMetadata.priceToBeat
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
MarketChangeCallback = Callable[[list[str], list[str]], Awaitable[None]]


class MarketDiscovery:
    """
    Discovers and tracks active Polymarket hourly markets.

    Strategy: generate event slugs based on current UTC time and query
    the Gamma API directly by slug. Generic list endpoints do not include
    restricted hourly markets.

    Maintains:
    - current_markets: markets for the current hour
    - upcoming_markets: markets for the next DISCOVERY_LOOKAHEAD_HOURS hours
    """

    def __init__(self, on_market_change: MarketChangeCallback | None = None):
        self._session: aiohttp.ClientSession | None = None
        self._on_market_change = on_market_change

        # market_id → MarketInfo
        self.current_markets: dict[str, MarketInfo] = {}
        self.upcoming_markets: dict[str, MarketInfo] = {}

        # All token_ids currently subscribed
        self._subscribed_tokens: set[str] = set()

        self._running = False
        self._poll_task: asyncio.Task | None = None

    async def start(self):
        """Start discovery: initial fetch + periodic polling."""
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
        self._running = True
        await self._discover_all()
        self._poll_task = asyncio.create_task(self._poll_loop())
        logger.info(
            "MarketDiscovery started. Current: %d markets, Upcoming: %d markets.",
            len(self.current_markets),
            len(self.upcoming_markets),
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
        """Return all token IDs that should currently be subscribed."""
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
        """Query Gamma API for all assets across current + upcoming hours."""
        now = utc_now()
        # Floor to the current hour in UTC
        current_hour = now.replace(minute=0, second=0, microsecond=0)

        new_current: dict[str, MarketInfo] = {}
        new_upcoming: dict[str, MarketInfo] = {}

        for asset, prefix in POLY_SERIES_SLUGS.items():
            for hour_offset in range(DISCOVERY_LOOKAHEAD_HOURS + 1):
                target_hour = current_hour + timedelta(hours=hour_offset)
                slug = _build_slug(prefix, target_hour)

                market = await self._fetch_by_slug(slug, asset)
                if market is None:
                    continue
                if market.settled:
                    continue

                if hour_offset == 0:
                    # Current hour
                    if market.is_active:
                        new_current[market.market_id] = market
                    # If not active yet (e.g. market not opened), treat as upcoming
                    elif market.starts_within_secs is not None:
                        new_upcoming[market.market_id] = market
                else:
                    # Future hour — pre-subscribe if within lookahead window
                    new_upcoming[market.market_id] = market

        # Compute token delta
        old_tokens = self._subscribed_tokens
        new_tokens: set[str] = set()
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

    async def _fetch_by_slug(self, slug: str, asset: str) -> MarketInfo | None:
        """Fetch a single event by exact slug and parse it into MarketInfo."""
        if not self._session:
            return None

        url = f"{POLY_GAMMA_URL}/events"
        params = {"slug": slug}

        try:
            async with self._session.get(url, params=params) as resp:
                if resp.status != 200:
                    logger.debug("Gamma API %d for slug=%s", resp.status, slug)
                    return None
                data = await resp.json()
        except Exception:
            logger.exception("Failed to fetch slug=%s", slug)
            return None

        if not data:
            logger.debug("No event found for slug=%s", slug)
            return None

        event = data[0]
        markets_data = event.get("markets", [])
        if not markets_data:
            return None

        mkt = markets_data[0]

        # clobTokenIds and outcomes are JSON strings — must parse them
        try:
            clob_tokens = json.loads(mkt.get("clobTokenIds", "[]"))
        except (json.JSONDecodeError, TypeError):
            clob_tokens = mkt.get("clobTokenIds", [])

        try:
            outcomes = json.loads(mkt.get("outcomes", "[]"))
        except (json.JSONDecodeError, TypeError):
            outcomes = mkt.get("outcomes", [])

        condition_id = mkt.get("conditionId", "")
        enable_ob = mkt.get("enableOrderBook", False)

        if not condition_id or len(clob_tokens) < 2:
            logger.debug("Incomplete market data for slug=%s", slug)
            return None
        if not enable_ob:
            logger.debug("Order book not enabled for slug=%s", slug)
            return None

        # Determine Up/Down token index
        up_idx, down_idx = 0, 1
        if len(outcomes) >= 2:
            for i, outcome in enumerate(outcomes):
                if isinstance(outcome, str):
                    if outcome.lower() == "up":
                        up_idx = i
                    elif outcome.lower() == "down":
                        down_idx = i

        # Parse times — use eventStartTime for candle open, endDate for close
        start_time = None
        end_time = None
        try:
            start_str = mkt.get("eventStartTime") or event.get("startDate")
            end_str = mkt.get("endDate") or event.get("endDate")
            if start_str:
                start_time = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
            if end_str:
                end_time = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            pass

        # Parse open price (the Binance price at candle open)
        open_price = None
        try:
            meta = event.get("eventMetadata") or {}
            if isinstance(meta, str):
                meta = json.loads(meta)
            open_price = float(meta.get("priceToBeat", 0)) or None
        except (ValueError, TypeError, json.JSONDecodeError):
            pass

        return MarketInfo(
            market_id=condition_id,
            asset=asset,
            up_token_id=clob_tokens[up_idx],
            down_token_id=clob_tokens[down_idx],
            start_time=start_time,
            end_time=end_time,
            question=mkt.get("question", ""),
            open_price=open_price,
        )

    async def write_markets_to_db(self, writer) -> None:
        """Write current market metadata to the database via BufferedWriter."""
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
                    "open_price": m.open_price,
                    "bn_open_price": None,
                    "bn_close_price": None,
                    "created_at_ms": utc_now_ms(),
                },
            )
