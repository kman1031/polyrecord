# Polyrecord - Polymarket + Binance Data Recorder

Professional data recorder for Polymarket hourly crypto up/down markets with Binance price feeds.

## Features

- **Multi-source data collection**: Polymarket orderbook, trades, BBO + Binance BBO and klines
- **Real-time recording**: WebSocket connections to both Polymarket and Binance
- **Daily SQLite databases**: One database file per UTC day with WAL mode
- **Web dashboard**: Professional trading terminal interface for data visualization
- **Health monitoring**: Automatic reconnection and system health checks

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

Or install manually:

```bash
pip install websockets aiohttp aiosqlite fastapi uvicorn jinja2
```

### 2. Run the Data Recorder

```bash
python3 recorder.py
```

The recorder will:
- Connect to Polymarket and Binance WebSockets
- Discover active hourly markets (BTC, ETH, SOL, XRP)
- Record all data to `data/YYYY-MM-DD.db`

### 3. View the Dashboard

In a separate terminal:

```bash
python3 start_web.py
```

Then open your browser to: **http://localhost:8000**

## Web Dashboard Features

The dashboard provides a professional trading terminal interface with:

- **Real-time Statistics**: Total rows, database size, active markets
- **Price Charts**: Interactive BTC, ETH, SOL, XRP price charts (last hour)
- **Table Statistics**: Row counts for all data tables
- **Market Status**: Current and upcoming markets with resolution status
- **RTDS Prices**: Real-time price feeds from Polymarket's Binance relay
- **WebSocket Updates**: Automatic data refresh every 5 seconds

### API Endpoints

- `GET /` - Web dashboard
- `GET /api/stats` - Database statistics
- `GET /api/markets` - Current markets
- `GET /api/prices/binance/{symbol}` - Binance BBO prices
- `GET /api/prices/polymarket/{asset}` - Polymarket BBO prices
- `GET /api/rtds/prices` - RTDS price relay data
- `GET /api/health` - System health status
- `WS /ws` - WebSocket for real-time updates

## Configuration

All configuration is in `config.py`:

- **Assets**: BTC, ETH, SOL, XRP
- **Database**: Daily SQLite with buffered writes
- **WebSocket endpoints**: Polymarket and Binance URLs
- **Timing**: Ping intervals, reconnection delays, flush intervals
- **Market discovery**: Poll interval, pre-subscribe timing

## Database Schema

The database contains the following tables:

- `bn_bbo` - Binance best bid/offer
- `bn_kline_1m` - Binance 1-minute klines
- `poly_bbo` - Polymarket best bid/offer
- `poly_trades` - Polymarket trades
- `poly_orderbook` - Polymarket orderbook snapshots
- `poly_rest_snapshot` - REST API snapshots
- `poly_rtds_prices` - Polymarket's Binance price relay
- `poly_ws_raw` - Raw WebSocket messages
- `markets` - Market metadata
- `connection_log` - Connection events

## Architecture

```
recorder.py (Main orchestrator)
├── BufferedWriter (Database)
├── MarketDiscovery (Find active markets)
├── PolyMarketWS (Orderbook/trades/BBO)
├── PolyRTDS (Binance price relay)
├── BinanceWS (Binance BBO + klines)
├── SnapshotPoller (REST snapshots)
└── HealthChecker (Monitoring)
```

## File Structure

```
polyrecord/
├── recorder.py          # Main recorder entry point
├── start_web.py         # Web dashboard launcher
├── config.py            # Configuration
├── models.py            # Database models
├── db.py                # Database manager
├── poly_discovery.py    # Market discovery
├── poly_ws.py           # Polymarket WebSocket
├── poly_rtds.py         # Polymarket RTDS
├── binance_ws.py        # Binance WebSocket
├── snapshot.py          # REST snapshots
├── health.py            # Health checker
├── utils.py             # Utilities
├── verify.py            # Data verification
├── compress.py          # Database compression
├── web/
│   ├── server.py        # FastAPI web server
│   ├── templates/
│   │   └── index.html   # Dashboard template
│   └── static/
│       ├── css/
│       │   └── style.css
│       └── js/
│           └── app.js
└── data/                # Database files
    ├── YYYY-MM-DD.db
    └── logs/
```

## Development

### Run Tests

```bash
pytest
```

### Code Formatting

```bash
ruff format .
ruff check .
```

## Monitoring

Check the logs in `data/logs/YYYY-MM-DD.jsonl` for:
- Connection events
- Market discoveries
- Health check results
- Error messages

## License

MIT
