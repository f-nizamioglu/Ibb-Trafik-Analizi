"""
Audit the ibb_traffic_density database table.

Reports: total rows, date range, rows per month, distinct geohashes,
duplicate check, null counts, speed/vehicle statistics.

Usage:
    python scripts/audit_database_coverage.py
    python scripts/audit_database_coverage.py --output outputs/data_audit

Output:
    outputs/data_audit/database_audit.csv
    outputs/data_audit/database_audit.md
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import DB_CONFIG, ensure_cli_logging  # noqa: E402

import psycopg2  # noqa: E402

logger = logging.getLogger(__name__)


def run_audit(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        conn = psycopg2.connect(**DB_CONFIG, connect_timeout=5)
    except Exception as exc:
        print(f"Cannot connect to database: {exc}")
        print(f"Start with: docker-compose up -d")
        sys.exit(1)

    cur = conn.cursor()

    # Check table exists
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema='public' AND table_name='ibb_traffic_density'"
    )
    if cur.fetchone()[0] == 0:
        print("ibb_traffic_density table not found.")
        print("Run: python ingest_data.py")
        conn.close()
        sys.exit(1)

    print("Querying ibb_traffic_density ...")

    # --- Total rows and date range ---
    cur.execute(
        "SELECT COUNT(*), MIN(record_time)::text, MAX(record_time)::text "
        "FROM ibb_traffic_density"
    )
    total_rows, min_date, max_date = cur.fetchone()
    print(f"  Total rows: {total_rows:,}")
    print(f"  Date range: {min_date} to {max_date}")

    # --- Distinct timestamps ---
    cur.execute("SELECT COUNT(DISTINCT record_time)::int FROM ibb_traffic_density")
    distinct_timestamps = cur.fetchone()[0]
    print(f"  Distinct timestamps: {distinct_timestamps:,}")

    # --- Distinct geohashes ---
    cur.execute("SELECT COUNT(DISTINCT geohash)::int FROM ibb_traffic_density")
    distinct_geohashes = cur.fetchone()[0]
    print(f"  Distinct geohashes: {distinct_geohashes:,}")

    # --- Duplicate check (record_time + geohash as natural key) ---
    cur.execute(
        "SELECT COUNT(*) FROM ("
        "  SELECT record_time, geohash, COUNT(*) AS cnt "
        "  FROM ibb_traffic_density "
        "  GROUP BY record_time, geohash "
        "  HAVING COUNT(*) > 1"
        ") dups"
    )
    duplicate_combos = cur.fetchone()[0]
    print(f"  Duplicate (record_time + geohash) combos: {duplicate_combos}")

    # --- Null counts ---
    null_counts: dict[str, int] = {}
    for col in ["record_time", "geohash", "lat", "lon", "avg_speed", "vehicle_count"]:
        cur.execute(f"SELECT COUNT(*) FROM ibb_traffic_density WHERE {col} IS NULL")
        null_counts[col] = cur.fetchone()[0]

    # --- Speed statistics ---
    cur.execute(
        "SELECT MIN(avg_speed), MAX(avg_speed), "
        "AVG(avg_speed)::numeric(6,1), "
        "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_speed)::numeric(6,1) "
        "FROM ibb_traffic_density"
    )
    speed_min, speed_max, speed_mean, speed_median = cur.fetchone()

    # --- Vehicle count statistics ---
    cur.execute(
        "SELECT MIN(vehicle_count), MAX(vehicle_count), "
        "AVG(vehicle_count)::numeric(8,1), "
        "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vehicle_count)::numeric(8,1) "
        "FROM ibb_traffic_density"
    )
    veh_min, veh_max, veh_mean, veh_median = cur.fetchone()

    # --- Rows per month ---
    cur.execute(
        "SELECT TO_CHAR(DATE_TRUNC('month', record_time), 'YYYY-MM') AS month, "
        "COUNT(*)::int AS row_count, "
        "COUNT(DISTINCT geohash)::int AS distinct_geohashes "
        "FROM ibb_traffic_density "
        "GROUP BY 1 ORDER BY 1"
    )
    monthly_rows = cur.fetchall()

    conn.close()

    # --- Write CSV (per-month) ---
    csv_path = output_dir / "database_audit.csv"
    csv_fieldnames = ["month", "row_count", "distinct_geohashes"]
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=csv_fieldnames)
        writer.writeheader()
        for month, rows, ghashes in monthly_rows:
            writer.writerow({"month": month, "row_count": rows, "distinct_geohashes": ghashes})
    print(f"\nCSV: {csv_path}")

    # --- Write Markdown ---
    md_path = output_dir / "database_audit.md"
    _write_md(
        md_path,
        total_rows=total_rows,
        min_date=min_date,
        max_date=max_date,
        distinct_timestamps=distinct_timestamps,
        distinct_geohashes=distinct_geohashes,
        duplicate_combos=duplicate_combos,
        null_counts=null_counts,
        speed_min=speed_min,
        speed_max=speed_max,
        speed_mean=speed_mean,
        speed_median=speed_median,
        veh_min=veh_min,
        veh_max=veh_max,
        veh_mean=veh_mean,
        veh_median=veh_median,
        monthly_rows=monthly_rows,
    )
    print(f"MD:  {md_path}")


def _write_md(path: Path, **stats) -> None:
    lines: list[str] = []
    a = lines.append

    a("# Database Coverage Audit — ibb_traffic_density")
    a("")
    a("## Summary")
    a("")
    a(f"| Metric | Value |")
    a(f"|--------|-------|")
    a(f"| Total rows | {stats['total_rows']:,} |")
    a(f"| Date range | {stats['min_date']} to {stats['max_date']} |")
    a(f"| Distinct timestamps | {stats['distinct_timestamps']:,} |")
    a(f"| Distinct geohashes | {stats['distinct_geohashes']:,} |")
    a(f"| Duplicate (time+geohash) combos | {stats['duplicate_combos']} |")
    a("")
    a("## Null Counts")
    a("")
    a("| Column | Null rows |")
    a("|--------|-----------|")
    for col, n in stats["null_counts"].items():
        a(f"| {col} | {n} |")

    a("")
    a("## Speed Statistics (avg_speed, km/h)")
    a("")
    a(f"| Min | Max | Mean | Median |")
    a(f"|-----|-----|------|--------|")
    a(f"| {stats['speed_min']} | {stats['speed_max']} | {stats['speed_mean']} | {stats['speed_median']} |")

    a("")
    a("## Vehicle Count Statistics")
    a("")
    a(f"| Min | Max | Mean | Median |")
    a(f"|-----|-----|------|--------|")
    a(f"| {stats['veh_min']} | {stats['veh_max']} | {stats['veh_mean']} | {stats['veh_median']} |")

    a("")
    a("## Rows per Month")
    a("")
    a("| Month | Row Count | Distinct Geohashes |")
    a("|-------|-----------|-------------------|")
    for month, rows, ghashes in stats["monthly_rows"]:
        a(f"| {month} | {rows:,} | {ghashes:,} |")

    a("")
    a("## Data Quality Notes")
    a("")
    if stats["duplicate_combos"] == 0:
        a("- No duplicate (record_time, geohash) combinations found.")
    else:
        a(f"- WARNING: {stats['duplicate_combos']} duplicate (record_time, geohash) combinations found.")

    all_nulls = sum(stats["null_counts"].values())
    if all_nulls == 0:
        a("- No null values in any key column.")
    else:
        a(f"- Null values found in key columns: {stats['null_counts']}")

    a("")
    a("## Completeness Note")
    a("")
    n_months = len(stats["monthly_rows"])
    if n_months >= 61:
        min_ym = stats['min_date'][:7] if stats.get('min_date') else '?'
        max_ym = stats['max_date'][:7] if stats.get('max_date') else '?'
        a(
            f"The database contains the full **{n_months}-month** dataset "
            f"({min_ym} – {max_ym}). "
            "All monthly files have been downloaded and ingested."
        )
    else:
        a(
            f"The database currently contains **{n_months} month(s)** of data. "
            "Download and ingest remaining months with:\n"
            "  python download_data.py --download\n"
            "  python ingest_data.py"
        )
    a("")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main() -> None:
    ensure_cli_logging()
    parser = argparse.ArgumentParser(description="Audit ibb_traffic_density database table")
    parser.add_argument(
        "--output",
        default="outputs/data_audit",
        metavar="DIR",
        help="Output directory (default: outputs/data_audit)",
    )
    args = parser.parse_args()
    run_audit(PROJECT_ROOT / args.output)


if __name__ == "__main__":
    main()
