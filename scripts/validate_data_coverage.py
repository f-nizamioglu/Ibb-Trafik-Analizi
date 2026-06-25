"""
Validate IBB traffic density dataset coverage.

Reports how many months are downloaded, ingested, and missing
relative to an expected total. Suitable for thesis/report validation.

Usage:
    python scripts/validate_data_coverage.py
    python scripts/validate_data_coverage.py --expected-months 61
    python scripts/validate_data_coverage.py --start 2020-01 --end 2025-01
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import DB_CONFIG, MANIFEST_PATH, ensure_cli_logging  # noqa: E402

import psycopg2  # noqa: E402

logger = logging.getLogger(__name__)


def _all_months_in_range(start: str, end: str) -> list[str]:
    """Generate all YYYY-MM strings from start to end inclusive."""
    from datetime import date

    sy, sm = int(start[:4]), int(start[5:7])
    ey, em = int(end[:4]), int(end[5:7])
    months = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        months.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months


def load_manifest_summary() -> dict:
    """Load manifest and return summary dict."""
    if not MANIFEST_PATH.exists():
        return {"exists": False, "resources": []}
    with open(MANIFEST_PATH, encoding="utf-8") as f:
        data = json.load(f)
    return {"exists": True, "resources": data.get("resources", [])}


def query_db_coverage(conn) -> dict:
    """Query ingested_files and ibb_traffic_density for actual coverage."""
    cur = conn.cursor()
    result = {}

    # ingested_files table
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'ingested_files'
    """)
    has_tracking = cur.fetchone()[0] > 0
    result["has_tracking_table"] = has_tracking

    if has_tracking:
        cur.execute("""
            SELECT
                COUNT(*)::int           AS file_count,
                SUM(row_count)::bigint  AS total_rows,
                MIN(min_date)::text     AS earliest,
                MAX(max_date)::text     AS latest,
                array_agg(period ORDER BY period) AS periods
            FROM ingested_files
            WHERE status = 'ok'
        """)
        row = cur.fetchone()
        if row:
            result["ingested_file_count"] = row[0]
            result["ingested_total_rows"] = row[1] or 0
            result["ingested_earliest"] = row[2]
            result["ingested_latest"] = row[3]
            result["ingested_periods"] = [p for p in (row[4] or []) if p]

    # ibb_traffic_density row count + date range
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'ibb_traffic_density'
    """)
    has_table = cur.fetchone()[0] > 0
    result["has_traffic_table"] = has_table

    if has_table:
        cur.execute("""
            SELECT
                COUNT(*)::bigint        AS rows,
                MIN(record_time)::text  AS earliest,
                MAX(record_time)::text  AS latest
            FROM ibb_traffic_density
        """)
        row = cur.fetchone()
        if row:
            result["db_row_count"] = row[0]
            result["db_earliest"] = row[1]
            result["db_latest"] = row[2]

    return result


def validate(expected_months: int, start: str | None, end: str | None) -> None:
    """Print a coverage validation report."""
    manifest = load_manifest_summary()

    # Connect to DB (non-fatal if unavailable)
    db = {}
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG, connect_timeout=5)
        db = query_db_coverage(conn)
        conn.close()
    except Exception as exc:
        logger.warning(f"DB unavailable: {exc}. DB stats will be omitted.")

    # Manifest summary
    manifest_resources = manifest["resources"]
    downloaded = [r for r in manifest_resources if r.get("downloaded")]
    ingested_in_manifest = [r for r in manifest_resources if r.get("ingested")]
    failed_dl = [r for r in manifest_resources if r.get("status") == "error"]
    manifest_periods = sorted(r["period"] for r in downloaded if r.get("period"))
    all_manifest_periods = sorted(r["period"] for r in manifest_resources if r.get("period"))

    # Determine expected range for missing-month calculation
    if start and end:
        expected_range = _all_months_in_range(start, end)
    elif all_manifest_periods:
        expected_range = _all_months_in_range(all_manifest_periods[0], all_manifest_periods[-1])
    else:
        expected_range = []

    covered = set(manifest_periods)
    ingested_periods = set(db.get("ingested_periods", []))
    missing_from_manifest = sorted(set(expected_range) - covered)
    duplicate_check: dict[str, int] = {}
    for r in manifest_resources:
        p = r.get("period", "")
        if p:
            duplicate_check[p] = duplicate_check.get(p, 0) + 1
    duplicates = {p: c for p, c in duplicate_check.items() if c > 1}

    jan2025_ok = any(
        r.get("period") == "2025-01" and r.get("downloaded")
        for r in manifest_resources
    ) or "2025-01" in ingested_periods

    print()
    print("=" * 62)
    print("  IBB Traffic Density - Data Coverage Validation")
    print("=" * 62)
    print()
    print(f"Expected months:          {expected_months}")
    if manifest["exists"]:
        print(f"Manifest resources total: {len(manifest_resources)}")
        print(f"Downloaded files:         {len(downloaded)}")
        print(f"Failed downloads:         {len(failed_dl)}")
        print(f"Marked as ingested:       {len(ingested_in_manifest)}")
    else:
        print(f"Manifest:                 NOT FOUND — run --discover first")

    if all_manifest_periods:
        print(f"Manifest period range:    {all_manifest_periods[0]} to {all_manifest_periods[-1]}")
    if manifest_periods:
        print(f"Downloaded period range:  {manifest_periods[0]} to {manifest_periods[-1]}")

    if covered:
        print(f"Covered months:           {len(covered)}")
    if missing_from_manifest:
        print(f"Missing months:           {len(missing_from_manifest)}")
        if len(missing_from_manifest) <= 12:
            print(f"  {missing_from_manifest}")
    if duplicates:
        print(f"Duplicate periods:        {list(duplicates.keys())}")
    else:
        print(f"Duplicate periods:        none")

    print(f"January 2025 present:     {'YES' if jan2025_ok else 'NO'}")

    print()
    print("-- Database --------------------------------------------------")
    if db.get("has_traffic_table"):
        print(f"ibb_traffic_density rows: {db.get('db_row_count', 0):,}")
        print(f"DB date range:            {db.get('db_earliest', '?')} to {db.get('db_latest', '?')}")
    else:
        print("ibb_traffic_density:      table not found")

    if db.get("has_tracking_table"):
        print(f"ingested_files count:     {db.get('ingested_file_count', 0)}")
        tracked_periods = db.get("ingested_periods", [])
        if tracked_periods:
            print(f"Tracked period range:     {tracked_periods[0]} to {tracked_periods[-1]}")
        # Months in DB but not in manifest
        missing_in_db = sorted(covered - ingested_periods)
        if missing_in_db:
            print(f"Downloaded but not in DB: {missing_in_db}")
    else:
        print("ingested_files table:     not found (run ingest_data.py)")

    print()
    print("-- Summary ---------------------------------------------------")
    if len(manifest_resources) < expected_months:
        shortfall = expected_months - len(manifest_resources)
        print(
            f"WARNING: {shortfall} month(s) short of expected {expected_months}.\n"
            f"  The IBB dataset currently publishes {len(manifest_resources)} months "
            f"(Jan 2020 - Jan 2025).\n"
            f"  Re-run --discover periodically to pick up newly published months."
        )
    elif len(manifest_resources) >= expected_months:
        print(f"OK: {len(manifest_resources)} months found (expected {expected_months}).")

    print()
    print("-- Next steps ------------------------------------------------")
    if not manifest["exists"]:
        print("  python download_data.py --discover --expected-months", expected_months)
    if len(downloaded) < len(manifest_resources):
        print("  python download_data.py --download")
    if db.get("ingested_file_count", 0) < len(downloaded):
        print("  python ingest_data.py")
    if not missing_from_manifest and db.get("ingested_file_count", 0) == len(downloaded):
        print("  All downloaded files appear to be ingested.")
    print()


def main() -> None:
    ensure_cli_logging()
    parser = argparse.ArgumentParser(description="Validate IBB dataset coverage")
    parser.add_argument("--expected-months", type=int, default=61, metavar="N")
    parser.add_argument("--start", metavar="YYYY-MM")
    parser.add_argument("--end", metavar="YYYY-MM")
    args = parser.parse_args()
    validate(args.expected_months, args.start, args.end)


if __name__ == "__main__":
    main()
