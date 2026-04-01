"""
Data integrity verification tool.

Checks a daily .db file for completeness and quality issues.
Run daily (e.g., via cron) to validate yesterday's data.

Usage:
    python verify.py                    # verify yesterday
    python verify.py 2026-04-01        # verify specific date
    python verify.py --all             # verify all .db files
"""

import argparse
import sqlite3
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from config import DATA_DIR
from utils import setup_logging, today_utc

logger = setup_logging("polyrecord.verify")


def verify_db(date_str: str) -> dict:
    """
    Verify data integrity for a specific date.
    Returns a report dict with issues found.
    """
    db_path = DATA_DIR / f"{date_str}.db"
    if not db_path.exists():
        # Try compressed version
        gz_path = DATA_DIR / f"{date_str}.db.gz"
        if gz_path.exists():
            logger.info("Skipping %s (compressed, decompress first)", date_str)
            return {"date": date_str, "status": "compressed", "issues": []}
        logger.warning("No database found for %s", date_str)
        return {"date": date_str, "status": "missing", "issues": ["database file not found"]}

    report = {
        "date": date_str,
        "status": "ok",
        "issues": [],
        "stats": {},
    }

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row

        # 1. Check table row counts
        tables = [
            "markets",
            "poly_ws_raw",
            "poly_orderbook",
            "poly_trades",
            "poly_bbo",
            "poly_rtds_prices",
            "bn_kline_1m",
            "bn_bbo",
            "poly_rest_snapshot",
            "connection_log",
        ]
        row_counts = {}
        for table in tables:
            try:
                cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
                row_counts[table] = cursor.fetchone()[0]
            except sqlite3.OperationalError:
                row_counts[table] = -1
                report["issues"].append(f"table '{table}' missing or corrupt")

        report["stats"]["row_counts"] = row_counts

        # 2. Check markets coverage (expect ~96 for 4 assets × 24 hours)
        market_count = row_counts.get("markets", 0)
        if market_count < 90:
            report["issues"].append(f"only {market_count} markets recorded (expected ~96)")

        # 3. Check per-asset market coverage
        try:
            cursor = conn.execute("SELECT asset, COUNT(*) as cnt FROM markets GROUP BY asset")
            asset_counts = {row["asset"]: row["cnt"] for row in cursor}
            report["stats"]["markets_per_asset"] = asset_counts
            for asset, cnt in asset_counts.items():
                if cnt < 20:
                    report["issues"].append(f"{asset}: only {cnt} markets (expected ~24)")
        except sqlite3.OperationalError:
            pass

        # 4. Check settlement results
        try:
            cursor = conn.execute("SELECT COUNT(*) FROM markets WHERE result IS NOT NULL")
            settled = cursor.fetchone()[0]
            unsettled = market_count - settled
            report["stats"]["settled"] = settled
            report["stats"]["unsettled"] = unsettled
            if unsettled > 4:  # Some tolerance for the last hour(s)
                report["issues"].append(f"{unsettled} markets not settled")
        except sqlite3.OperationalError:
            pass

        # 5. Check for data gaps via connection_log
        try:
            cursor = conn.execute(
                "SELECT source, event, COUNT(*) as cnt FROM connection_log "
                "WHERE event IN ('disconnected', 'silence_timeout', 'reconnecting') "
                "GROUP BY source, event"
            )
            disconnects = {}
            for row in cursor:
                key = f"{row['source']}:{row['event']}"
                disconnects[key] = row["cnt"]
            report["stats"]["disconnects"] = disconnects

            total_disconnects = sum(disconnects.values())
            if total_disconnects > 10:
                report["issues"].append(f"{total_disconnects} connection issues detected")
        except sqlite3.OperationalError:
            pass

        # 6. Check data freshness (time range)
        for table, ts_col in [
            ("poly_ws_raw", "recv_ts_ms"),
            ("bn_bbo", "recv_ts_ms"),
            ("poly_rtds_prices", "recv_ts_ms"),
        ]:
            try:
                cursor = conn.execute(
                    f"SELECT MIN({ts_col}) as min_ts, MAX({ts_col}) as max_ts FROM {table}"
                )
                row = cursor.fetchone()
                if row["min_ts"] and row["max_ts"]:
                    span_hours = (row["max_ts"] - row["min_ts"]) / 3_600_000
                    report["stats"][f"{table}_span_hours"] = round(span_hours, 1)
                    if span_hours < 20:
                        report["issues"].append(
                            f"{table} only spans {span_hours:.1f} hours (expected ~24)"
                        )
            except sqlite3.OperationalError:
                pass

        # 7. Check key tables are not empty
        critical_tables = {
            "poly_ws_raw": 1000,
            "bn_bbo": 1000,
            "bn_kline_1m": 5000,  # 4 symbols × 1440 min
        }
        for table, min_rows in critical_tables.items():
            cnt = row_counts.get(table, 0)
            if cnt >= 0 and cnt < min_rows:
                report["issues"].append(f"{table} has only {cnt} rows (expected >{min_rows})")

        conn.close()

    except Exception as e:
        report["status"] = "error"
        report["issues"].append(f"verification error: {e}")

    # Set final status
    if report["issues"]:
        report["status"] = "issues_found"

    return report


def print_report(report: dict):
    """Pretty-print a verification report."""
    date = report["date"]
    status = report["status"].upper()
    icon = "✓" if status == "OK" else "⚠" if status == "ISSUES_FOUND" else "✗"

    print(f"\n{icon} {date} — {status}")

    if "stats" in report:
        stats = report["stats"]

        if "row_counts" in stats:
            print("  Row counts:")
            for table, cnt in stats["row_counts"].items():
                print(f"    {table:25s} {cnt:>10,}")

        if "markets_per_asset" in stats:
            print("  Markets per asset:")
            for asset, cnt in stats["markets_per_asset"].items():
                print(f"    {asset:10s} {cnt}")

        if "disconnects" in stats:
            print("  Connection issues:")
            for key, cnt in stats["disconnects"].items():
                print(f"    {key:35s} {cnt}")

        for key in stats:
            if key.endswith("_span_hours"):
                table = key.replace("_span_hours", "")
                print(f"  {table} time span: {stats[key]}h")

    if report["issues"]:
        print("  Issues:")
        for issue in report["issues"]:
            print(f"    ⚠ {issue}")

    print()


def main():
    parser = argparse.ArgumentParser(description="Polyrecord data integrity verifier")
    parser.add_argument(
        "date", nargs="?", default=None, help="Date to verify (YYYY-MM-DD). Default: yesterday"
    )
    parser.add_argument("--all", action="store_true", help="Verify all available .db files")
    args = parser.parse_args()

    if args.all:
        db_files = sorted(DATA_DIR.glob("*.db"))
        if not db_files:
            print("No database files found in", DATA_DIR)
            return

        all_ok = True
        for db_path in db_files:
            date_str = db_path.stem
            report = verify_db(date_str)
            print_report(report)
            if report["status"] != "ok":
                all_ok = False

        sys.exit(0 if all_ok else 1)

    else:
        if args.date:
            date_str = args.date
        else:
            # Default: yesterday
            yesterday = datetime.now(timezone.utc) - timedelta(days=1)
            date_str = yesterday.strftime("%Y-%m-%d")

        report = verify_db(date_str)
        print_report(report)
        sys.exit(0 if report["status"] == "ok" else 1)


if __name__ == "__main__":
    main()
