"""
Ingest IBB hourly traffic density CSV files into PostgreSQL with PostGIS geometry.

Supports multi-file ingestion. Tracks ingested files in the `ingested_files` table
to prevent duplicate ingestion. Safe to re-run — already-ingested files are skipped.

Usage:
    python ingest_data.py                    # ingest all pending CSVs
    python ingest_data.py --file path/to.csv # ingest a specific file
    python ingest_data.py --force            # re-ingest even if already tracked
    python ingest_data.py --reset            # DROP + recreate table (destructive!)
    python ingest_data.py --status           # show ingestion status only

CSV files are read from:
    data/raw/ibb_hourly_traffic_density/    (all 61 monthly files, including Jan 2025)

If January 2025 data is already in the database (from before the unified directory),
it is automatically detected and registered without re-ingesting.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import logging
import sys
import time
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import (  # noqa: E402
    BATCH_SIZE,
    DB_CONFIG,
    MANIFEST_PATH,
    RAW_DATA_DIR,
    ensure_cli_logging,
)

logger = logging.getLogger(__name__)

CSV_ENCODING = "utf-8"
CSV_EXPECTED_FIELD_COUNT = 8

# ── DDL ──────────────────────────────────────────────────────────────────────

_DDL_TRAFFIC_TABLE = """
CREATE TABLE IF NOT EXISTS ibb_traffic_density (
    id            SERIAL PRIMARY KEY,
    record_time   TIMESTAMP,
    lat           DOUBLE PRECISION,
    lon           DOUBLE PRECISION,
    geohash       VARCHAR(20),
    min_speed     INTEGER,
    max_speed     INTEGER,
    avg_speed     INTEGER,
    vehicle_count INTEGER,
    geom          GEOMETRY(Point, 32636)
);
"""

_DDL_GEOM_INDEX = """
CREATE INDEX IF NOT EXISTS ibb_traffic_density_geom_gist
    ON ibb_traffic_density USING GIST(geom);
"""

_DDL_TIME_INDEX = """
CREATE INDEX IF NOT EXISTS ibb_traffic_density_record_time_idx
    ON ibb_traffic_density(record_time);
"""

_DDL_INGESTED_FILES = """
CREATE TABLE IF NOT EXISTS ingested_files (
    id            SERIAL PRIMARY KEY,
    filename      TEXT         NOT NULL,
    local_path    TEXT,
    period        VARCHAR(7),
    sha256        CHAR(64),
    row_count     INTEGER,
    min_date      TIMESTAMP,
    max_date      TIMESTAMP,
    ingested_at   TIMESTAMP    DEFAULT NOW(),
    status        TEXT         DEFAULT 'ok',
    error_message TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS ingested_files_sha256_idx
    ON ingested_files(sha256) WHERE sha256 IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS ingested_files_period_idx
    ON ingested_files(period) WHERE period IS NOT NULL;
"""

_INSERT_SQL = """
INSERT INTO ibb_traffic_density (
    record_time, lat, lon, geohash,
    min_speed, max_speed, avg_speed, vehicle_count,
    geom
) VALUES %s
"""

_ROW_TEMPLATE = (
    "(%s, %s, %s, %s, %s, %s, %s, %s, "
    "ST_Transform(ST_SetSRID(ST_MakePoint(%s::double precision, %s::double precision), 4326), 32636))"
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65_536), b""):
            h.update(chunk)
    return h.hexdigest()


def parse_period_from_filename(filename: str) -> str | None:
    import re
    m = re.search(r"(\d{4})(\d{2})", filename)
    if m:
        year, month = int(m.group(1)), int(m.group(2))
        if 2000 <= year <= 2100 and 1 <= month <= 12:
            return f"{year:04d}-{month:02d}"
    return None


def parse_row(cols: list[str]) -> tuple | None:
    """Parse one CSV row. Returns insert tuple or None if invalid."""
    if len(cols) != CSV_EXPECTED_FIELD_COUNT:
        return None
    try:
        record_time = cols[0].strip().lstrip("﻿").strip('"')
        lat = float(cols[1].strip())
        lon = float(cols[2].strip())
        geohash = cols[3].strip().strip('"').strip("'")
        min_speed = int(float(cols[4].strip()))
        max_speed = int(float(cols[5].strip()))
        avg_speed = int(float(cols[6].strip()))
        vehicle_count = int(float(cols[7].strip()))
    except (ValueError, TypeError):
        return None
    return (record_time, lat, lon, geohash, min_speed, max_speed, avg_speed, vehicle_count, lon, lat)


# ── Schema setup ──────────────────────────────────────────────────────────────

def setup_schema(conn) -> None:
    cur = conn.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS postgis CASCADE")
    cur.execute(_DDL_TRAFFIC_TABLE)
    cur.execute(_DDL_GEOM_INDEX)
    cur.execute(_DDL_TIME_INDEX)
    cur.execute(_DDL_INGESTED_FILES)
    conn.commit()
    logger.info("Schema ready (ibb_traffic_density + ingested_files)")


def reset_schema(conn) -> None:
    """Destructive: drop ibb_traffic_density and ingested_files, then recreate."""
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS ibb_traffic_density CASCADE")
    cur.execute("DROP TABLE IF EXISTS ingested_files CASCADE")
    conn.commit()
    logger.warning("Tables dropped. Recreating…")
    setup_schema(conn)


# ── Ingestion tracking ────────────────────────────────────────────────────────

def _already_ingested(conn, sha256: str | None, period: str | None) -> bool:
    """Return True if this file (by SHA256 or period) is already tracked."""
    cur = conn.cursor()
    if sha256:
        cur.execute(
            "SELECT 1 FROM ingested_files WHERE sha256 = %s AND status = 'ok'",
            (sha256,),
        )
        if cur.fetchone():
            return True
    if period:
        cur.execute(
            "SELECT 1 FROM ingested_files WHERE period = %s AND status = 'ok'",
            (period,),
        )
        if cur.fetchone():
            return True
    return False


def _register_ingested(
    conn, filename: str, local_path: Path, period: str | None,
    sha256: str, row_count: int, min_date: str | None, max_date: str | None,
) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO ingested_files
            (filename, local_path, period, sha256, row_count, min_date, max_date, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, 'ok')
        ON CONFLICT (sha256) WHERE sha256 IS NOT NULL DO UPDATE
            SET status='ok', row_count=EXCLUDED.row_count,
                min_date=EXCLUDED.min_date, max_date=EXCLUDED.max_date,
                ingested_at=NOW()
        """,
        (filename, str(local_path), period, sha256, row_count, min_date, max_date),
    )
    conn.commit()


# ── Bootstrap January 2025 ────────────────────────────────────────────────────

def bootstrap_jan2025_if_needed(conn) -> None:
    """
    If ibb_traffic_density already has January 2025 data but ingested_files
    does not track it, register it now to prevent re-ingestion.
    """
    JAN2025_SHA256 = "9bb1e4600ea6bc979ab5084446eacf974ffddf719fc25120451cf87d67ebb4e1"
    JAN2025_PERIOD = "2025-01"

    cur = conn.cursor()
    cur.execute(
        "SELECT row_count, min_date::text, max_date::text "
        "FROM ingested_files WHERE period = %s AND status = 'ok'",
        (JAN2025_PERIOD,),
    )
    existing = cur.fetchone()
    if existing:
        # Already tracked — ensure manifest is in sync
        _update_manifest_ingested(
            JAN2025_PERIOD,
            row_count=existing[0] or 0,
            min_date=existing[1],
            max_date=existing[2],
        )
        return

    # Check if the DB contains January 2025 data
    cur.execute(
        """
        SELECT COUNT(*)::int, MIN(record_time)::text, MAX(record_time)::text
        FROM ibb_traffic_density
        WHERE record_time >= '2025-01-01' AND record_time < '2025-02-01'
        """
    )
    row = cur.fetchone()
    count, min_dt, max_dt = row
    if count == 0:
        return  # No data for Jan 2025 in DB — nothing to bootstrap

    # Find the local file in canonical raw directory
    canonical_path = RAW_DATA_DIR / "traffic_density_202501.csv"
    actual_path = str(canonical_path) if canonical_path.exists() else None

    logger.info(
        f"Bootstrapping January 2025: {count:,} rows already in DB — "
        "registering in ingested_files without re-ingesting"
    )
    cur.execute(
        """
        INSERT INTO ingested_files
            (filename, local_path, period, sha256, row_count, min_date, max_date, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, 'ok')
        ON CONFLICT DO NOTHING
        """,
        (
            "traffic_density_202501.csv",
            actual_path,
            JAN2025_PERIOD,
            JAN2025_SHA256,
            count,
            min_dt,
            max_dt,
        ),
    )
    conn.commit()
    logger.info("  January 2025 registered in ingested_files.")
    _update_manifest_ingested(JAN2025_PERIOD, row_count=count, min_date=min_dt, max_date=max_dt)


# ── File discovery ────────────────────────────────────────────────────────────

def find_csv_files() -> list[Path]:
    """Return all CSV files from the canonical raw directory."""
    if not RAW_DATA_DIR.exists():
        return []
    return sorted(RAW_DATA_DIR.glob("*.csv"), key=lambda p: p.name)


# ── Single-file ingestion ─────────────────────────────────────────────────────

def ingest_file(conn, path: Path, force: bool = False) -> dict:
    """
    Ingest one CSV file. Returns a result dict with keys:
    filename, period, status, row_count, skipped, error.
    """
    filename = path.name
    period = parse_period_from_filename(filename)
    sha256 = sha256_of_file(path)

    if not force and _already_ingested(conn, sha256, period):
        logger.info(f"  SKIP {filename} (period={period}, already ingested)")
        return {"filename": filename, "period": period, "status": "skipped",
                "row_count": 0, "skipped": True, "error": None}

    logger.info(f"  Ingesting {filename} (period={period}, {path.stat().st_size / 1e6:.1f} MB)…")
    start = time.monotonic()

    batch: list[tuple] = []
    total_inserted = 0
    skipped_rows = 0
    min_date: str | None = None
    max_date: str | None = None

    try:
        with path.open(newline="", encoding=CSV_ENCODING, errors="replace") as f:
            reader = csv.reader(f)
            next(reader, None)  # skip header
            cur = conn.cursor()

            for cols in reader:
                row = parse_row(cols)
                if row is None:
                    skipped_rows += 1
                    continue

                # Track date range (col 0 is record_time)
                dt_str = cols[0].strip().strip('"').lstrip("﻿")[:10]
                if min_date is None or dt_str < min_date:
                    min_date = dt_str
                if max_date is None or dt_str > max_date:
                    max_date = dt_str

                batch.append(row)
                if len(batch) >= BATCH_SIZE:
                    execute_values(cur, _INSERT_SQL, batch, template=_ROW_TEMPLATE, page_size=BATCH_SIZE)
                    total_inserted += len(batch)
                    conn.commit()
                    if total_inserted % 500_000 == 0:
                        logger.info(f"    … {total_inserted:,} rows inserted")
                    batch.clear()

            if batch:
                execute_values(cur, _INSERT_SQL, batch, template=_ROW_TEMPLATE, page_size=len(batch))
                total_inserted += len(batch)
                conn.commit()

        elapsed = time.monotonic() - start
        logger.info(
            f"  Done: {total_inserted:,} rows in {elapsed:.1f}s "
            f"({skipped_rows} invalid rows skipped), date range {min_date} – {max_date}"
        )
        _register_ingested(conn, filename, path, period, sha256, total_inserted, min_date, max_date)

        # Update manifest if it exists
        _update_manifest_ingested(period, row_count=total_inserted, min_date=min_date, max_date=max_date)

        return {
            "filename": filename, "period": period, "status": "ok",
            "row_count": total_inserted, "skipped": False, "error": None,
        }

    except Exception as exc:
        conn.rollback()
        logger.error(f"  FAILED {filename}: {exc}")
        return {
            "filename": filename, "period": period, "status": "error",
            "row_count": 0, "skipped": False, "error": str(exc),
        }


def _update_manifest_ingested(period: str | None, row_count: int, min_date: str | None, max_date: str | None) -> None:
    """Mark the period as ingested in the download manifest if it exists."""
    import json
    if not MANIFEST_PATH.exists() or period is None:
        return
    try:
        with open(MANIFEST_PATH, encoding="utf-8") as f:
            manifest = json.load(f)
        for r in manifest.get("resources", []):
            if r.get("period") == period:
                r["ingested"] = True
                r["row_count"] = row_count
                r["min_date"] = min_date
                r["max_date"] = max_date
                break
        with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
    except Exception:
        pass  # manifest update is best-effort


# ── Status display ────────────────────────────────────────────────────────────

def cmd_status(conn) -> None:
    cur = conn.cursor()
    cur.execute("""
        SELECT
            COUNT(*)::int                       AS total_files,
            SUM(row_count)::bigint              AS total_rows,
            MIN(min_date)::text                 AS earliest_date,
            MAX(max_date)::text                 AS latest_date,
            COUNT(DISTINCT period)::int         AS distinct_months
        FROM ingested_files
        WHERE status = 'ok'
    """)
    row = cur.fetchone()
    if row:
        total_f, total_r, earliest, latest, months = row
        logger.info(f"\nIngested files:   {total_f}")
        logger.info(f"Total rows:       {total_r:,}" if total_r else "Total rows: 0")
        logger.info(f"Date range:       {earliest} to {latest}")
        logger.info(f"Distinct months:  {months}")

    cur.execute("SELECT period, filename, row_count, status FROM ingested_files ORDER BY period")
    rows = cur.fetchall()
    if rows:
        logger.info("\nIngested files:")
        for r in rows:
            logger.info(f"  {r[0] or '?':8}  {r[2] or 0:>10,} rows  [{r[3]}]  {r[1]}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    ensure_cli_logging()
    parser = argparse.ArgumentParser(
        description="Multi-file IBB traffic density ingestion",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--file", metavar="PATH", help="Ingest a specific CSV file")
    parser.add_argument("--force", action="store_true",
                        help="Re-ingest files even if already tracked")
    parser.add_argument("--reset", action="store_true",
                        help="DROP tables and start fresh (destructive!)")
    parser.add_argument("--status", action="store_true",
                        help="Show ingestion status and exit")
    args = parser.parse_args()

    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except Exception as exc:
        logger.error(f"Cannot connect to database: {exc}\nStart: docker-compose up -d")
        sys.exit(1)

    if args.reset:
        confirm = input("This will DROP ibb_traffic_density and ingested_files. Type YES to confirm: ")
        if confirm.strip() != "YES":
            logger.info("Aborted.")
            sys.exit(0)
        reset_schema(conn)

    setup_schema(conn)
    bootstrap_jan2025_if_needed(conn)

    if args.status:
        cmd_status(conn)
        conn.close()
        return

    if args.file:
        files = [Path(args.file)]
    else:
        files = find_csv_files()

    if not files:
        logger.error(
            "No CSV files found.\n"
            f"  Raw data dir: {RAW_DATA_DIR}\n\n"
            "Download files first:\n"
            "  python download_data.py --discover\n"
            "  python download_data.py --download"
        )
        sys.exit(1)

    logger.info(f"Found {len(files)} CSV file(s) to process")
    results = []
    for path in files:
        if not path.exists():
            logger.warning(f"  File not found: {path}")
            results.append({"filename": path.name, "status": "not_found", "row_count": 0})
            continue
        result = ingest_file(conn, path, force=args.force)
        results.append(result)

    conn.close()

    # Summary
    ok = [r for r in results if r["status"] == "ok"]
    skipped = [r for r in results if r["status"] == "skipped"]
    failed = [r for r in results if r["status"] == "error"]
    total_rows = sum(r.get("row_count", 0) for r in ok)

    logger.info(f"\n{'='*50}")
    logger.info(f"Ingestion summary:")
    logger.info(f"  Files found:     {len(results)}")
    logger.info(f"  Ingested:        {len(ok)}")
    logger.info(f"  Skipped:         {len(skipped)}")
    logger.info(f"  Failed:          {len(failed)}")
    logger.info(f"  New rows added:  {total_rows:,}")
    if failed:
        logger.error(f"  Failed files:    {[r['filename'] for r in failed]}")


if __name__ == "__main__":
    main()
