"""
Database schema definitions for polyrecord.
Each daily .db file contains all these tables.
"""

TABLES = {
    # ── Market metadata ──────────────────────────────────────
    "markets": """
        CREATE TABLE IF NOT EXISTS markets (
            market_id       TEXT PRIMARY KEY,
            asset           TEXT NOT NULL,
            timeframe       TEXT NOT NULL DEFAULT '1h',
            start_time_ms   INTEGER,
            end_time_ms     INTEGER,
            up_token_id     TEXT,
            down_token_id   TEXT,
            result          TEXT,
            open_price      REAL,
            bn_open_price   REAL,
            bn_close_price  REAL,
            created_at_ms   INTEGER
        )
    """,
    # ── Polymarket WS raw events (full payload) ──────────────
    "poly_ws_raw": """
        CREATE TABLE IF NOT EXISTS poly_ws_raw (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            recv_ts_ms      INTEGER NOT NULL,
            msg_ts_ms       INTEGER,
            market_id       TEXT,
            token_id        TEXT,
            event_type      TEXT NOT NULL,
            payload         TEXT NOT NULL
        )
    """,
    # ── Structured orderbook snapshots ───────────────────────
    "poly_orderbook": """
        CREATE TABLE IF NOT EXISTS poly_orderbook (
            recv_ts_ms      INTEGER NOT NULL,
            market_id       TEXT NOT NULL,
            token_id        TEXT NOT NULL,
            side            TEXT NOT NULL,
            level           INTEGER NOT NULL,
            price           REAL NOT NULL,
            size            REAL NOT NULL,
            source          TEXT NOT NULL DEFAULT 'ws'
        )
    """,
    # ── Polymarket trades ────────────────────────────────────
    "poly_trades": """
        CREATE TABLE IF NOT EXISTS poly_trades (
            recv_ts_ms      INTEGER NOT NULL,
            msg_ts_ms       INTEGER,
            market_id       TEXT,
            token_id        TEXT NOT NULL,
            price           REAL NOT NULL,
            size            REAL NOT NULL,
            side            TEXT,
            fee_rate_bps    INTEGER,
            tx_hash         TEXT
        )
    """,
    # ── Polymarket BBO (best bid/ask) snapshots ──────────────
    "poly_bbo": """
        CREATE TABLE IF NOT EXISTS poly_bbo (
            recv_ts_ms      INTEGER NOT NULL,
            token_id        TEXT NOT NULL,
            best_bid        REAL,
            best_ask        REAL,
            spread          REAL
        )
    """,
    # ── Polymarket RTDS price relay ──────────────────────────
    "poly_rtds_prices": """
        CREATE TABLE IF NOT EXISTS poly_rtds_prices (
            recv_ts_ms      INTEGER NOT NULL,
            source_ts_ms    INTEGER,
            symbol          TEXT NOT NULL,
            price           REAL NOT NULL
        )
    """,
    # ── Binance 1m klines ────────────────────────────────────
    "bn_kline_1m": """
        CREATE TABLE IF NOT EXISTS bn_kline_1m (
            open_ts_ms      INTEGER NOT NULL,
            close_ts_ms     INTEGER NOT NULL,
            symbol          TEXT NOT NULL,
            open            REAL NOT NULL,
            high            REAL NOT NULL,
            low             REAL NOT NULL,
            close           REAL NOT NULL,
            volume          REAL NOT NULL,
            is_closed       INTEGER NOT NULL DEFAULT 0
        )
    """,
    # ── Binance BBO ──────────────────────────────────────────
    "bn_bbo": """
        CREATE TABLE IF NOT EXISTS bn_bbo (
            recv_ts_ms      INTEGER NOT NULL,
            symbol          TEXT NOT NULL,
            bid_price       REAL NOT NULL,
            bid_size        REAL NOT NULL,
            ask_price       REAL NOT NULL,
            ask_size        REAL NOT NULL
        )
    """,
    # ── REST orderbook snapshots ─────────────────────────────
    "poly_rest_snapshot": """
        CREATE TABLE IF NOT EXISTS poly_rest_snapshot (
            recv_ts_ms      INTEGER NOT NULL,
            market_id       TEXT NOT NULL,
            token_id        TEXT NOT NULL,
            side            TEXT NOT NULL,
            level           INTEGER NOT NULL,
            price           REAL NOT NULL,
            size            REAL NOT NULL
        )
    """,
    # ── Connection / health log ──────────────────────────────
    "connection_log": """
        CREATE TABLE IF NOT EXISTS connection_log (
            ts_ms           INTEGER NOT NULL,
            source          TEXT NOT NULL,
            event           TEXT NOT NULL,
            detail          TEXT
        )
    """,
}

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_poly_ws_raw_recv    ON poly_ws_raw(recv_ts_ms)",
    "CREATE INDEX IF NOT EXISTS idx_poly_ws_raw_type    ON poly_ws_raw(event_type, market_id)",
    "CREATE INDEX IF NOT EXISTS idx_poly_ws_raw_token   ON poly_ws_raw(token_id, recv_ts_ms)",
    "CREATE INDEX IF NOT EXISTS idx_poly_ob_ts          ON poly_orderbook(recv_ts_ms, market_id)",
    "CREATE INDEX IF NOT EXISTS idx_poly_ob_token       ON poly_orderbook(token_id, recv_ts_ms)",
    "CREATE INDEX IF NOT EXISTS idx_poly_trades_ts      ON poly_trades(recv_ts_ms)",
    "CREATE INDEX IF NOT EXISTS idx_poly_trades_token   ON poly_trades(token_id, recv_ts_ms)",
    "CREATE INDEX IF NOT EXISTS idx_poly_bbo_ts         ON poly_bbo(recv_ts_ms, token_id)",
    "CREATE INDEX IF NOT EXISTS idx_rtds_ts             ON poly_rtds_prices(recv_ts_ms, symbol)",
    "CREATE INDEX IF NOT EXISTS idx_bn_kline_ts         ON bn_kline_1m(open_ts_ms, symbol)",
    "CREATE INDEX IF NOT EXISTS idx_bn_bbo_ts           ON bn_bbo(recv_ts_ms, symbol)",
    "CREATE INDEX IF NOT EXISTS idx_rest_snap_ts         ON poly_rest_snapshot(recv_ts_ms, market_id)",
    "CREATE INDEX IF NOT EXISTS idx_conn_log_ts         ON connection_log(ts_ms, source)",
]
