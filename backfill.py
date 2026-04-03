"""
Historical backfill script.

Fills in missing data for databases from 2026-04-01 to today:
  1. markets.result         — from Gamma API (outcomePrices)
  2. markets.open_price     — from Gamma API (eventMetadata.priceToBeat)
  3. markets.bn_open_price  — from bn_kline_1m (open of first candle in hour)
  4. markets.bn_close_price — from bn_kline_1m (close of last candle in hour)

Usage:
    python backfill.py              # backfill all databases
    python backfill.py --date 2026-04-02  # specific date only
    python backfill.py --dry-run    # show what would be updated, don't write
"""

import argparse
import asyncio
import json
import logging
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path

import aiohttp

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger("backfill")

GAMMA_URL = "https://gamma-api.polymarket.com"
DATA_DIR = Path(__file__).resolve().parent / "data"


def get_db_files(date_filter: str | None = None) -> list[Path]:
    """Return list of .db files to backfill."""
    if date_filter:
        path = DATA_DIR / f"{date_filter}.db"
        return [path] if path.exists() else []
    return sorted(DATA_DIR.glob("*.db"))


def get_markets_needing_backfill(db_path: Path) -> list[dict]:
    """Return markets where result IS NULL or bn_open_price IS NULL."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.execute("""
            SELECT market_id, asset, start_time_ms, end_time_ms,
                   result, open_price, bn_open_price, bn_close_price,
                   up_token_id, down_token_id
            FROM markets
            WHERE result IS NULL OR bn_open_price IS NULL
            ORDER BY start_time_ms
        """)
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def get_bn_hourly_price(
    db_path: Path, symbol: str, hour_start_ms: int
) -> tuple[float | None, float | None]:
    """Get open/close price from bn_kline_1m for the given symbol and hour."""
    hour_end_ms = hour_start_ms + 3_600_000
    conn = sqlite3.connect(str(db_path))
    try:
        # open = open of first 1m candle in the hour
        cursor = conn.execute(
            """
            SELECT open FROM bn_kline_1m
            WHERE symbol = ? AND open_ts_ms >= ? AND open_ts_ms < ?
            ORDER BY open_ts_ms ASC LIMIT 1
        """,
            (symbol, hour_start_ms, hour_end_ms),
        )
        row = cursor.fetchone()
        bn_open = row[0] if row else None

        # close = close of last 1m candle in the hour
        cursor = conn.execute(
            """
            SELECT close FROM bn_kline_1m
            WHERE symbol = ? AND open_ts_ms >= ? AND open_ts_ms < ?
            ORDER BY open_ts_ms DESC LIMIT 1
        """,
            (symbol, hour_start_ms, hour_end_ms),
        )
        row = cursor.fetchone()
        bn_close = row[0] if row else None

        return bn_open, bn_close
    finally:
        conn.close()


def update_market(db_path: Path, market_id: str, updates: dict, dry_run: bool = False):
    """Apply updates to a market row."""
    if not updates:
        return

    set_clauses = ", ".join(f"{k} = ?" for k in updates)
    values = list(updates.values()) + [market_id]
    sql = f"UPDATE markets SET {set_clauses} WHERE market_id = ?"

    if dry_run:
        logger.info("  DRY RUN: %s -> %s", market_id[:16], updates)
        return

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(sql, values)
        conn.commit()
    finally:
        conn.close()


ASSET_TO_SYMBOL = {
    "BTC": "btcusdt",
    "ETH": "ethusdt",
    "SOL": "solusdt",
    "XRP": "xrpusdt",
}


async def fetch_market_result(session: aiohttp.ClientSession, market_id: str) -> dict | None:
    """Fetch settlement data from Gamma API for a single market."""
    url = f"{GAMMA_URL}/markets"
    params = {"conditionId": market_id}
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            if not data:
                return None
            return data[0] if isinstance(data, list) else data
    except Exception as e:
        logger.debug("Failed to fetch market %s: %s", market_id[:16], e)
        return None


def parse_result_from_market(mkt: dict) -> tuple[str | None, float | None]:
    """Extract (result, open_price) from Gamma market data."""
    result = None
    open_price = None

    # Check if resolved — only trust umaResolutionStatus (closed=True just means trading ended)
    is_resolved = mkt.get("umaResolutionStatus") == "resolved"
    if is_resolved:
        try:
            outcome_prices = json.loads(mkt.get("outcomePrices", "[]"))
            outcomes = json.loads(mkt.get("outcomes", "[]"))
            for i, price in enumerate(outcome_prices):
                if float(price) >= 0.99 and i < len(outcomes):
                    result = outcomes[i]
                    break
        except (ValueError, TypeError, json.JSONDecodeError):
            pass

    # Try to get open_price from eventMetadata
    try:
        meta = mkt.get("eventMetadata") or {}
        if isinstance(meta, str):
            meta = json.loads(meta)
        val = float(meta.get("priceToBeat", 0))
        if val > 0:
            open_price = val
    except (ValueError, TypeError, json.JSONDecodeError):
        pass

    return result, open_price


async def backfill_db(db_path: Path, session: aiohttp.ClientSession, dry_run: bool = False):
    """Backfill a single database file."""
    markets = get_markets_needing_backfill(db_path)
    if not markets:
        logger.info("%s: nothing to backfill", db_path.name)
        return

    logger.info("%s: %d markets need backfill", db_path.name, len(markets))
    updated = 0
    skipped = 0

    for m in markets:
        market_id = m["market_id"]
        asset = m["asset"]
        start_ms = m["start_time_ms"]
        symbol = ASSET_TO_SYMBOL.get(asset)

        updates = {}

        # ── 1. Get result + open_price from Gamma API ─────────────────────
        if m["result"] is None:
            mkt_data = await fetch_market_result(session, market_id)
            if mkt_data:
                result, open_price = parse_result_from_market(mkt_data)
                if result:
                    updates["result"] = result
                    logger.info("  %s [%s] → result=%s", market_id[:16], asset, result)
                if open_price and m["open_price"] is None:
                    updates["open_price"] = open_price

        # ── 2. Get bn_open/close from bn_kline_1m ─────────────────────────
        if m["bn_open_price"] is None and start_ms and symbol:
            bn_open, bn_close = get_bn_hourly_price(db_path, symbol, start_ms)
            if bn_open is not None:
                updates["bn_open_price"] = bn_open
                updates["bn_close_price"] = bn_close
                logger.info(
                    "  %s [%s] → bn_open=%.4f bn_close=%.4f",
                    market_id[:16],
                    asset,
                    bn_open,
                    bn_close or 0,
                )

        if updates:
            update_market(db_path, market_id, updates, dry_run=dry_run)
            updated += 1
        else:
            skipped += 1

        # Small delay to avoid hammering the API
        await asyncio.sleep(0.2)

    logger.info("%s: done. updated=%d skipped=%d", db_path.name, updated, skipped)


async def main(date_filter: str | None = None, dry_run: bool = False):
    db_files = get_db_files(date_filter)
    if not db_files:
        logger.error("No database files found in %s", DATA_DIR)
        return

    logger.info("Backfilling %d database(s)%s", len(db_files), " [DRY RUN]" if dry_run else "")

    async with aiohttp.ClientSession() as session:
        for db_path in db_files:
            await backfill_db(db_path, session, dry_run=dry_run)

    logger.info("Backfill complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Polyrecord historical backfill")
    parser.add_argument("--date", help="Specific date to backfill (e.g. 2026-04-02)")
    parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be updated without writing"
    )
    args = parser.parse_args()
    asyncio.run(main(date_filter=args.date, dry_run=args.dry_run))
