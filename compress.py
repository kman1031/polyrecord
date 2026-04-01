"""
Database archive and compression utility.

Compresses old daily .db files with gzip to save disk space.
Can be run as a standalone script or via cron.

Usage:
    python compress.py              # compress all eligible db files
    python compress.py --days 2     # compress files older than 2 days
    python compress.py --cleanup 60 # delete .db.gz files older than 60 days
"""

import argparse
import gzip
import logging
import os
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

from config import DATA_DIR, COMPRESS_AFTER_DAYS, DISK_USAGE_WARN_GB
from utils import setup_logging

logger = setup_logging("polyrecord.compress")


def get_db_files() -> list[Path]:
    """Find all uncompressed .db files in DATA_DIR."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return sorted(DATA_DIR.glob("*.db"))


def get_archive_files() -> list[Path]:
    """Find all compressed .db.gz files in DATA_DIR."""
    return sorted(DATA_DIR.glob("*.db.gz"))


def file_date(path: Path) -> datetime | None:
    """Extract date from filename like 2026-04-02.db."""
    try:
        stem = path.stem
        if stem.endswith(".db"):  # for .db.gz files
            stem = stem[:-3]
        return datetime.strptime(stem, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def compress_file(db_path: Path) -> Path | None:
    """Compress a .db file to .db.gz using gzip. Returns the compressed path."""
    gz_path = db_path.with_suffix(".db.gz")
    if gz_path.exists():
        logger.info("Already compressed: %s", gz_path.name)
        return gz_path

    try:
        # First VACUUM to reclaim space
        import sqlite3

        conn = sqlite3.connect(str(db_path))
        conn.execute("VACUUM")
        conn.close()
        logger.info("VACUUM completed: %s", db_path.name)
    except Exception:
        logger.warning("VACUUM failed for %s, compressing anyway", db_path.name)

    original_size = db_path.stat().st_size

    try:
        with open(db_path, "rb") as f_in:
            with gzip.open(gz_path, "wb", compresslevel=6) as f_out:
                shutil.copyfileobj(f_in, f_out)

        compressed_size = gz_path.stat().st_size
        ratio = compressed_size / original_size * 100 if original_size > 0 else 0

        logger.info(
            "Compressed %s: %.1f MB → %.1f MB (%.0f%%)",
            db_path.name,
            original_size / 1e6,
            compressed_size / 1e6,
            ratio,
        )

        # Remove original after successful compression
        db_path.unlink()
        return gz_path

    except Exception:
        logger.exception("Failed to compress %s", db_path.name)
        # Clean up partial gz file
        if gz_path.exists():
            gz_path.unlink()
        return None


def compress_old_files(days: int = COMPRESS_AFTER_DAYS):
    """Compress .db files older than `days` days."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    db_files = get_db_files()
    compressed = 0

    for db_path in db_files:
        dt = file_date(db_path)
        if dt and dt < cutoff:
            result = compress_file(db_path)
            if result:
                compressed += 1

    if compressed:
        logger.info("Compressed %d files", compressed)
    else:
        logger.info("No files to compress (cutoff=%s)", cutoff.strftime("%Y-%m-%d"))


def cleanup_old_archives(days: int):
    """Delete .db.gz files older than `days` days."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    archives = get_archive_files()
    deleted = 0

    for gz_path in archives:
        dt = file_date(gz_path)
        if dt and dt < cutoff:
            gz_path.unlink()
            logger.info("Deleted old archive: %s", gz_path.name)
            deleted += 1

    if deleted:
        logger.info("Deleted %d old archives", deleted)


def check_disk_space():
    """Warn if disk space is running low."""
    stat = os.statvfs(str(DATA_DIR))
    free_gb = (stat.f_bavail * stat.f_frsize) / (1024**3)

    if free_gb < DISK_USAGE_WARN_GB:
        logger.warning("LOW DISK SPACE: %.1f GB remaining!", free_gb)
    else:
        logger.info("Disk space: %.1f GB free", free_gb)

    return free_gb


def report():
    """Print a summary of data directory."""
    db_files = get_db_files()
    gz_files = get_archive_files()

    total_db = sum(f.stat().st_size for f in db_files)
    total_gz = sum(f.stat().st_size for f in gz_files)

    logger.info("Data directory: %s", DATA_DIR)
    logger.info("  Active .db files:    %d (%.1f MB)", len(db_files), total_db / 1e6)
    logger.info("  Archived .db.gz files: %d (%.1f MB)", len(gz_files), total_gz / 1e6)
    logger.info("  Total:               %.1f MB", (total_db + total_gz) / 1e6)
    check_disk_space()


def main():
    parser = argparse.ArgumentParser(description="Polyrecord database archiver")
    parser.add_argument(
        "--days",
        type=int,
        default=COMPRESS_AFTER_DAYS,
        help=f"Compress .db files older than N days (default: {COMPRESS_AFTER_DAYS})",
    )
    parser.add_argument(
        "--cleanup",
        type=int,
        default=0,
        help="Delete .db.gz archives older than N days (0=disabled)",
    )
    parser.add_argument("--report", action="store_true", help="Print storage report")
    args = parser.parse_args()

    if args.report:
        report()
        return

    compress_old_files(args.days)

    if args.cleanup > 0:
        cleanup_old_archives(args.cleanup)

    report()


if __name__ == "__main__":
    main()
