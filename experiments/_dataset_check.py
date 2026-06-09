"""
Shared dataset coverage check for experiment scripts.

Call print_dataset_coverage(conn) at the start of any experiment to report
how much IBB traffic density data is available and warn if the expected
number of months is not present.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

DEFAULT_EXPECTED_MONTHS = 61


def print_dataset_coverage(conn, expected_months: int = DEFAULT_EXPECTED_MONTHS) -> None:
    """
    Print a brief coverage summary and warn if fewer months than expected are present.
    Designed to be called at the start of experiment main() functions.
    """
    cur = conn.cursor()

    # Total rows and date range
    cur.execute("""
        SELECT
            COUNT(*)::bigint                        AS total_rows,
            MIN(record_time)::text                  AS min_dt,
            MAX(record_time)::text                  AS max_dt,
            COUNT(DISTINCT DATE_TRUNC('month', record_time))::int AS distinct_months
        FROM ibb_traffic_density
    """)
    row = cur.fetchone()
    if not row or not row[0]:
        logger.warning("ibb_traffic_density appears to be empty.")
        return

    total_rows, min_dt, max_dt, distinct_months = row

    logger.info(
        f"Dataset: {total_rows:,} rows | "
        f"{distinct_months} month(s) | "
        f"range {(min_dt or '?')[:10]} to {(max_dt or '?')[:10]}"
    )

    if distinct_months < expected_months:
        logger.warning(
            f"WARNING: Only {distinct_months} month(s) of data present in the database, "
            f"but {expected_months} months are expected for the full analysis.\n"
            f"  Results from this experiment are based on partial data.\n"
            f"  Download and ingest remaining months:\n"
            f"    python download_data.py --discover --expected-months {expected_months}\n"
            f"    python download_data.py --download\n"
            f"    python ingest_data.py"
        )
