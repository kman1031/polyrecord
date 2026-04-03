"""
Web dashboard server for Polyrecord.
Provides REST API and real-time WebSocket updates.
"""

import sys
import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent))

import aiosqlite
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi import Request

from config import DATA_DIR, ASSETS, BINANCE_SYMBOLS

logger = logging.getLogger("polyrecord.web")

app = FastAPI(title="Polyrecord Dashboard")

BASE_DIR = Path(__file__).resolve().parent
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/stats")
async def get_stats():
    """Get overall statistics."""
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    db_path = DATA_DIR / f"{today_str}.db"

    if not db_path.exists():
        return {
            "total_rows": 0,
            "tables": {},
            "db_size_mb": 0,
        }

    async with aiosqlite.connect(str(db_path)) as conn:
        stats = {}

        tables = [
            "bn_bbo",
            "bn_kline_1m",
            "poly_bbo",
            "poly_trades",
            "poly_orderbook",
            "poly_rest_snapshot",
            "poly_rtds_prices",
            "poly_ws_raw",
        ]

        total_rows = 0
        for table in tables:
            cursor = await conn.execute(f"SELECT COUNT(*) FROM {table}")
            count = (await cursor.fetchone())[0]
            stats[table] = count
            total_rows += count

        db_size_mb = db_path.stat().st_size / (1024 * 1024)

        return {
            "total_rows": total_rows,
            "tables": stats,
            "db_size_mb": round(db_size_mb, 2),
        }


@app.get("/api/markets")
async def get_markets():
    """Get current markets."""
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    db_path = DATA_DIR / f"{today_str}.db"

    if not db_path.exists():
        return []

    async with aiosqlite.connect(str(db_path)) as conn:
        conn.row_factory = aiosqlite.Row
        cursor = await conn.execute("""
            SELECT 
                market_id,
                asset,
                start_time_ms,
                end_time_ms,
                result,
                open_price,
                bn_open_price,
                bn_close_price
            FROM markets
            ORDER BY asset, start_time_ms
        """)
        rows = await cursor.fetchall()

        markets = []
        for row in rows:
            markets.append(
                {
                    "market_id": row["market_id"],
                    "asset": row["asset"],
                    "start_time": datetime.utcfromtimestamp(row["start_time_ms"] / 1000).isoformat()
                    if row["start_time_ms"]
                    else None,
                    "end_time": datetime.utcfromtimestamp(row["end_time_ms"] / 1000).isoformat()
                    if row["end_time_ms"]
                    else None,
                    "result": row["result"],
                    "open_price": row["open_price"],
                    "bn_open_price": row["bn_open_price"],
                    "bn_close_price": row["bn_close_price"],
                }
            )

        return markets


@app.get("/api/prices/binance/{symbol}")
async def get_binance_prices(symbol: str, minutes: int = 60):
    """Get recent Binance BBO prices."""
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    db_path = DATA_DIR / f"{today_str}.db"

    if not db_path.exists():
        return []

    cutoff_ms = int((datetime.utcnow() - timedelta(minutes=minutes)).timestamp() * 1000)

    async with aiosqlite.connect(str(db_path)) as conn:
        conn.row_factory = aiosqlite.Row
        cursor = await conn.execute(
            """
            SELECT 
                recv_ts_ms,
                symbol,
                bid_price,
                bid_size,
                ask_price,
                ask_size
            FROM bn_bbo
            WHERE symbol = ? AND recv_ts_ms >= ?
            ORDER BY recv_ts_ms DESC
            LIMIT 1000
        """,
            (symbol.lower(), cutoff_ms),
        )
        rows = await cursor.fetchall()

        prices = []
        for row in reversed(rows):
            prices.append(
                {
                    "timestamp": row["recv_ts_ms"],
                    "time": datetime.utcfromtimestamp(row["recv_ts_ms"] / 1000).isoformat(),
                    "bid": row["bid_price"],
                    "ask": row["ask_price"],
                    "mid": (row["bid_price"] + row["ask_price"]) / 2,
                }
            )

        return prices


@app.get("/api/prices/polymarket/{asset}")
async def get_polymarket_prices(asset: str, minutes: int = 60):
    """Get recent Polymarket BBO prices."""
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    db_path = DATA_DIR / f"{today_str}.db"

    if not db_path.exists():
        return []

    cutoff_ms = int((datetime.utcnow() - timedelta(minutes=minutes)).timestamp() * 1000)

    async with aiosqlite.connect(str(db_path)) as conn:
        conn.row_factory = aiosqlite.Row
        cursor = await conn.execute(
            """
            SELECT 
                recv_ts_ms,
                token_id,
                best_bid,
                best_ask
            FROM poly_bbo
            WHERE recv_ts_ms >= ?
            ORDER BY recv_ts_ms DESC
            LIMIT 1000
        """,
            (cutoff_ms,),
        )
        rows = await cursor.fetchall()

        prices = []
        for row in reversed(rows):
            prices.append(
                {
                    "timestamp": row["recv_ts_ms"],
                    "time": datetime.utcfromtimestamp(row["recv_ts_ms"] / 1000).isoformat(),
                    "token_id": row["token_id"],
                    "bid": row["best_bid"],
                    "ask": row["best_ask"],
                    "mid": (row["best_bid"] + row["best_ask"]) / 2
                    if row["best_bid"] and row["best_ask"]
                    else None,
                }
            )

        return prices


@app.get("/api/rtds/prices")
async def get_rtds_prices(minutes: int = 60):
    """Get recent RTDS prices."""
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    db_path = DATA_DIR / f"{today_str}.db"

    if not db_path.exists():
        return []

    cutoff_ms = int((datetime.utcnow() - timedelta(minutes=minutes)).timestamp() * 1000)

    async with aiosqlite.connect(str(db_path)) as conn:
        conn.row_factory = aiosqlite.Row
        cursor = await conn.execute(
            """
            SELECT 
                recv_ts_ms,
                symbol,
                price
            FROM poly_rtds_prices
            WHERE recv_ts_ms >= ?
            ORDER BY recv_ts_ms DESC
            LIMIT 1000
        """,
            (cutoff_ms,),
        )
        rows = await cursor.fetchall()

        prices = []
        for row in reversed(rows):
            prices.append(
                {
                    "timestamp": row["recv_ts_ms"],
                    "time": datetime.utcfromtimestamp(row["recv_ts_ms"] / 1000).isoformat(),
                    "symbol": row["symbol"],
                    "price": row["price"],
                }
            )

        return prices


@app.get("/api/health")
async def get_health():
    """Get system health status."""
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    log_path = DATA_DIR / "logs" / f"{today_str}.jsonl"

    if not log_path.exists():
        return {
            "status": "unknown",
            "last_update": None,
            "connections": {},
        }

    try:
        with open(log_path, "r") as f:
            lines = f.readlines()
            if lines:
                last_line = lines[-1].strip()
                import json

                last_entry = json.loads(last_line)

                return {
                    "status": "healthy"
                    if "healthy" in last_entry.get("message", "").lower()
                    else "warning",
                    "last_update": last_entry.get("timestamp"),
                    "connections": last_entry.get("components", {}),
                }
    except Exception as e:
        logger.error(f"Error reading health log: {e}")

    return {
        "status": "unknown",
        "last_update": None,
        "connections": {},
    }


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time updates."""
    await websocket.accept()

    try:
        while True:
            stats = await get_stats()
            markets = await get_markets()

            data = {
                "type": "update",
                "stats": stats,
                "markets": markets,
                "timestamp": datetime.utcnow().isoformat(),
            }

            await websocket.send_json(data)
            await asyncio.sleep(5)

    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
