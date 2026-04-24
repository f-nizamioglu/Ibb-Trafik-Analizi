"""
Ingest IBB traffic density CSV data into PostgreSQL with PostGIS geometry.

Usage:
    python ingest_data.py
"""

import csv
import logging

import psycopg2
from psycopg2.extras import execute_values

from config import BATCH_SIZE, CSV_PATH, DB_CONFIG, ensure_cli_logging

logger = logging.getLogger(__name__)

CSV_EXPECTED_FIELD_COUNT = 8
CSV_ENCODING = "utf-8"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ibb_traffic_density (
    id SERIAL PRIMARY KEY,
    record_time TIMESTAMP,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    geohash VARCHAR(20),
    min_speed INTEGER,
    max_speed INTEGER,
    avg_speed INTEGER,
    vehicle_count INTEGER,
    geom GEOMETRY(Point, 4326)
);
"""

INSERT_SQL = """
INSERT INTO ibb_traffic_density (
    record_time, lat, lon, geohash,
    min_speed, max_speed, avg_speed, vehicle_count,
    geom
) VALUES %s
"""

ROW_TEMPLATE = (
    "(%s, %s, %s, %s, %s, %s, %s, %s, "
    "ST_SetSRID(ST_MakePoint(%s::double precision, %s::double precision), 4326))"
)


def parse_row(cols: list[str]) -> tuple | None:
    """Parse one CSV row into values for `INSERT` or return None if invalid.

    Expects the IBB traffic density column layout (8 fields): timestamp, lat, lon,
    geohash, min/max/avg speed, vehicle count. Returns a tuple suitable for
    `ROW_TEMPLATE` (including duplicate lon/lat for `ST_MakePoint`).

    Args:
        cols: Raw string fields from the CSV row.

    Returns:
        Tuple of values for insert, or None if the row length or types are invalid.
    """
    if len(cols) != CSV_EXPECTED_FIELD_COUNT:
        return None
    try:
        record_time = cols[0].strip()
        lat = float(cols[1].strip())
        lon = float(cols[2].strip())
        geohash = cols[3].strip().strip('"').strip("'")
        min_speed = int(float(cols[4].strip()))
        max_speed = int(float(cols[5].strip()))
        avg_speed = int(float(cols[6].strip()))
        vehicle_count = int(float(cols[7].strip()))
    except (ValueError, TypeError):
        return None
    return (
        record_time,
        lat,
        lon,
        geohash,
        min_speed,
        max_speed,
        avg_speed,
        vehicle_count,
        lon,
        lat,
    )


def main() -> None:
    ensure_cli_logging()
    logger.info("Connecting to DB...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    try:
        logger.info("Ensuring PostGIS extension...")
        cur.execute("CREATE EXTENSION IF NOT EXISTS postgis CASCADE;")

        logger.info("Creating table...")
        cur.execute(CREATE_TABLE_SQL)

        logger.info("Truncating table (fresh load)...")
        cur.execute("TRUNCATE ibb_traffic_density RESTART IDENTITY;")
        conn.commit()

        if not CSV_PATH.is_file():
            raise FileNotFoundError(f"CSV not found: {CSV_PATH}")

        logger.info(f"Ingesting data from {CSV_PATH}...")
        batch: list[tuple] = []
        total_inserted = 0
        skipped = 0
        line_no = 0

        with CSV_PATH.open(newline="", encoding=CSV_ENCODING) as f:
            reader = csv.reader(f)
            next(reader, None)
            line_no = 1
            for cols in reader:
                line_no += 1
                row = parse_row(cols)
                if row is None:
                    skipped += 1
                    continue
                batch.append(row)
                if len(batch) >= BATCH_SIZE:
                    execute_values(
                        cur,
                        INSERT_SQL,
                        batch,
                        template=ROW_TEMPLATE,
                        page_size=BATCH_SIZE,
                    )
                    total_inserted += len(batch)
                    logger.info(f"  ... inserted {total_inserted:,} rows so far")
                    batch.clear()

            if batch:
                execute_values(
                    cur,
                    INSERT_SQL,
                    batch,
                    template=ROW_TEMPLATE,
                    page_size=len(batch),
                )
                total_inserted += len(batch)

        conn.commit()
        logger.info("Success!")
        logger.info(f"  Total rows inserted: {total_inserted:,}")
        logger.info(f"  Rows skipped (invalid): {skipped:,}")
    finally:
        cur.close()
        conn.close()
        logger.info("Database connection closed.")


if __name__ == "__main__":
    main()
