"""
SQLite daily database manager with buffered async writes.

Design:
- One .db file per UTC day: data/2026-04-02.db
- WAL journal mode for concurrent read/write
- BufferedWriter collects rows in memory, flushes periodically
- Day rollover at UTC midnight: flush old db → open new db → continue
"""

import asyncio
import aiosqlite
import logging
from collections import defaultdict
from typing import Any

from config import DATA_DIR, DB_FLUSH_INTERVAL, DB_FLUSH_MAX_ROWS
from models import TABLES, INDEXES
from utils import utc_now_ms, today_utc

logger = logging.getLogger("polyrecord.db")


async def init_db(conn: aiosqlite.Connection):
    """Create all tables, indexes, and set PRAGMAs on a fresh connection."""
    await conn.execute("PRAGMA journal_mode=WAL")
    await conn.execute("PRAGMA synchronous=NORMAL")
    await conn.execute("PRAGMA cache_size=-64000")  # 64 MB
    await conn.execute("PRAGMA temp_store=MEMORY")
    await conn.execute("PRAGMA mmap_size=268435456")  # 256 MB mmap

    for table_sql in TABLES.values():
        await conn.execute(table_sql)
    for index_sql in INDEXES:
        await conn.execute(index_sql)
    await conn.commit()
    logger.info("Database initialized with %d tables, %d indexes", len(TABLES), len(INDEXES))


async def open_db(date_str: str | None = None) -> aiosqlite.Connection:
    """Open (or create) the database file for the given UTC date."""
    if date_str is None:
        date_str = today_utc()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    db_path = DATA_DIR / f"{date_str}.db"
    conn = await aiosqlite.connect(str(db_path))
    await init_db(conn)
    logger.info("Opened database: %s", db_path)
    return conn


class BufferedWriter:
    """
    Async buffered database writer.

    Usage:
        writer = BufferedWriter()
        await writer.start()

        writer.write("poly_ws_raw", {
            "recv_ts_ms": 123456,
            "event_type": "book",
            "payload": '{"bids":[],...}',
        })

        # For tables with PRIMARY KEY that may get duplicate inserts:
        writer.write_or_replace("markets", {"market_id": "0x...", ...})

        # Rows accumulate in memory, flushed every DB_FLUSH_INTERVAL seconds
        # or when buffer reaches DB_FLUSH_MAX_ROWS.

        await writer.stop()  # final flush + close
    """

    def __init__(self):
        self._conn: aiosqlite.Connection | None = None
        self._current_date: str = ""
        self._buffer: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self._buffer_count: int = 0
        self._lock = asyncio.Lock()
        self._flush_task: asyncio.Task | None = None
        self._running = False
        self._total_written: int = 0
        self._total_flushed: int = 0
        # Tables that use INSERT OR REPLACE instead of INSERT
        self._replace_tables: set[str] = {"markets"}
        # Track which market_ids have been written this day (avoid redundant writes)
        self._written_market_ids: set[str] = set()

    async def start(self):
        """Open today's database and start the periodic flush loop."""
        self._current_date = today_utc()
        self._conn = await open_db(self._current_date)
        self._running = True
        self._written_market_ids.clear()
        self._flush_task = asyncio.create_task(self._flush_loop())
        logger.info("BufferedWriter started for %s", self._current_date)

    async def stop(self):
        """Flush remaining data and close the connection."""
        self._running = False
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        await self._flush()
        if self._conn:
            await self._conn.close()
            self._conn = None
        logger.info("BufferedWriter stopped. Total rows written: %d", self._total_written)

    def write(self, table: str, row: dict[str, Any]):
        """
        Enqueue a row for writing. Non-blocking.

        Args:
            table: Target table name (must exist in models.TABLES)
            row: Dict of column_name → value
        """
        self._buffer[table].append(row)
        self._buffer_count += 1

    def write_or_replace(self, table: str, row: dict[str, Any]):
        """
        Enqueue a row for writing with INSERT OR REPLACE semantics.
        Use for tables with PRIMARY KEY where duplicates are expected.
        """
        self._replace_tables.add(table)
        self._buffer[table].append(row)
        self._buffer_count += 1

    async def _flush_loop(self):
        """Periodic flush loop."""
        while self._running:
            try:
                await asyncio.sleep(DB_FLUSH_INTERVAL)

                # Check for day rollover
                new_date = today_utc()
                if new_date != self._current_date:
                    await self._rollover(new_date)

                # Flush if buffer has data
                if self._buffer_count > 0:
                    await self._flush()

            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Error in flush loop")

    async def _flush(self):
        """Flush all buffered rows to SQLite in a single transaction."""
        async with self._lock:
            if self._buffer_count == 0:
                return

            # Swap buffer while holding lock
            buffer = self._buffer
            count = self._buffer_count
            self._buffer = defaultdict(list)
            self._buffer_count = 0

            if not self._conn:
                logger.error("Cannot flush: no database connection")
                return

            try:
                async with self._conn.cursor() as cursor:
                    for table, rows in buffer.items():
                        if not rows:
                            continue

                        # Build column list from ALL rows' keys (union)
                        all_keys: set[str] = set()
                        for row in rows:
                            all_keys.update(row.keys())
                        columns = sorted(all_keys)

                        placeholders = ", ".join(["?"] * len(columns))
                        col_names = ", ".join(columns)

                        # Use INSERT OR REPLACE for tables with primary keys
                        if table in self._replace_tables:
                            sql = f"INSERT OR REPLACE INTO {table} ({col_names}) VALUES ({placeholders})"
                        else:
                            sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"

                        values = [tuple(row.get(c) for c in columns) for row in rows]
                        await cursor.executemany(sql, values)

                await self._conn.commit()
                self._total_written += count
                self._total_flushed += 1

                if self._total_flushed % 60 == 0:  # Log every ~60 flushes (~1 min)
                    logger.info(
                        "Flush #%d: %d rows this batch, %d total",
                        self._total_flushed,
                        count,
                        self._total_written,
                    )
            except Exception:
                logger.exception("Error flushing %d rows to database", count)
                # Re-add failed rows back to buffer for retry
                for table, rows in buffer.items():
                    self._buffer[table].extend(rows)
                    self._buffer_count += len(rows)

    async def _rollover(self, new_date: str):
        """Switch to a new day's database file."""
        async with self._lock:
            logger.info("Day rollover: %s → %s", self._current_date, new_date)

            # Flush remaining data to old db (lock is already held, call internal)
            if self._buffer_count > 0 and self._conn:
                # Inline flush since we already hold the lock
                buffer = self._buffer
                count = self._buffer_count
                self._buffer = defaultdict(list)
                self._buffer_count = 0

                try:
                    async with self._conn.cursor() as cursor:
                        for table, rows in buffer.items():
                            if not rows:
                                continue
                            all_keys: set[str] = set()
                            for row in rows:
                                all_keys.update(row.keys())
                            columns = sorted(all_keys)
                            placeholders = ", ".join(["?"] * len(columns))
                            col_names = ", ".join(columns)
                            if table in self._replace_tables:
                                sql = f"INSERT OR REPLACE INTO {table} ({col_names}) VALUES ({placeholders})"
                            else:
                                sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"
                            values = [tuple(row.get(c) for c in columns) for row in rows]
                            await cursor.executemany(sql, values)
                    await self._conn.commit()
                    self._total_written += count
                except Exception:
                    logger.exception("Error flushing during rollover, %d rows may be lost", count)

            # Close old db
            if self._conn:
                await self._conn.close()

            # Open new db
            self._current_date = new_date
            self._conn = await open_db(new_date)
            self._written_market_ids.clear()
            logger.info("Rollover complete, now writing to %s.db", new_date)

    @property
    def stats(self) -> dict:
        """Return current stats for health monitoring."""
        return {
            "current_date": self._current_date,
            "buffer_count": self._buffer_count,
            "total_written": self._total_written,
            "total_flushed": self._total_flushed,
            "connected": self._conn is not None,
        }
