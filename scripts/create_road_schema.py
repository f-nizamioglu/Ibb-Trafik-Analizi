"""
Create road-network schema in PostgreSQL/PostGIS.

Creates (idempotent — safe to re-run):
  road_segments               — imported OSM road geometries
  geohash_cells               — bounding-box polygons for each geohash in dataset
  geohash_road_lengths        — materialized view: total road km per geohash cell
  road_density_base           — view: ibb_traffic_density + vehicles_per_road_km
  road_density_congestion_candidates — view: filtered by speed + road density percentile

Execution order:
  1. python scripts/create_road_schema.py        # create schema + populate geohash_cells
  2. python scripts/import_road_network.py ...   # import OSM roads (refreshes matview)
  3. python scripts/create_road_schema.py --refresh  # if you need to force a refresh

Usage:
  python scripts/create_road_schema.py
  python scripts/create_road_schema.py --refresh
  python scripts/create_road_schema.py --drop-road-tables  # WARNING: destructive
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import (  # noqa: E402
    DB_CONFIG,
    HIGH_CONGESTION_MAX_AVG_SPEED,
    ROAD_DENSITY_PERCENTILE_THRESHOLD,
    ensure_cli_logging,
)
from scoring.geohash_utils import geohash_envelope_sql_args  # noqa: E402

import psycopg2  # noqa: E402
import psycopg2.extras  # noqa: E402

logger = logging.getLogger(__name__)

# ── Vehicle-relevant highway classes ─────────────────────────────────────────
VEHICLE_HIGHWAY_TYPES = {
    "motorway", "trunk", "primary", "secondary", "tertiary",
    "residential", "unclassified", "service",
    "motorway_link", "trunk_link", "primary_link",
    "secondary_link", "tertiary_link",
}

# ── DDL ──────────────────────────────────────────────────────────────────────

_DDL_ROAD_SEGMENTS = """
CREATE TABLE IF NOT EXISTS road_segments (
    id          SERIAL PRIMARY KEY,
    osm_id      BIGINT,
    highway     TEXT    NOT NULL,
    name        TEXT,
    geom_4326   GEOMETRY(Geometry, 4326),
    geom_32636  GEOMETRY(Geometry, 32636),
    length_m    DOUBLE PRECISION
);
"""

_DDL_ROAD_SEGMENTS_INDEXES = """
CREATE INDEX IF NOT EXISTS road_segments_geom_32636_gist
    ON road_segments USING GIST(geom_32636);
CREATE INDEX IF NOT EXISTS road_segments_highway_idx
    ON road_segments(highway);
"""

_DDL_GEOHASH_CELLS = """
CREATE TABLE IF NOT EXISTS geohash_cells (
    geohash     VARCHAR(12) PRIMARY KEY,
    geom_4326   GEOMETRY(Polygon, 4326)  NOT NULL,
    geom_32636  GEOMETRY(Polygon, 32636) NOT NULL,
    area_km2    DOUBLE PRECISION         NOT NULL
);
"""

_DDL_GEOHASH_CELLS_INDEX = """
CREATE INDEX IF NOT EXISTS geohash_cells_geom_32636_gist
    ON geohash_cells USING GIST(geom_32636);
"""

_DDL_GEOHASH_ROAD_LENGTHS = """
CREATE MATERIALIZED VIEW IF NOT EXISTS geohash_road_lengths AS
SELECT
    gc.geohash,
    COUNT(DISTINCT r.id)::int                                       AS segment_count,
    COALESCE(
        SUM(
            ST_Length(
                ST_Intersection(
                    ST_MakeValid(r.geom_32636),
                    gc.geom_32636
                )
            )
        ) / 1000.0,
        0.0
    )                                                               AS road_length_km
FROM geohash_cells gc
LEFT JOIN road_segments r
    ON ST_Intersects(ST_MakeValid(r.geom_32636), gc.geom_32636)
GROUP BY gc.geohash
WITH NO DATA;
"""

_DDL_GEOHASH_ROAD_LENGTHS_INDEX = """
CREATE UNIQUE INDEX IF NOT EXISTS geohash_road_lengths_geohash_idx
    ON geohash_road_lengths(geohash);
"""

# Uses ROAD_DENSITY_PERCENTILE_THRESHOLD and HIGH_CONGESTION_MAX_AVG_SPEED from config.
# These are baked into the view SQL at creation time; re-run create_road_schema.py
# after changing .env to update the thresholds.
_DDL_ROAD_DENSITY_BASE = """
CREATE OR REPLACE VIEW road_density_base AS
SELECT
    t.id,
    t.record_time,
    t.lat,
    t.lon,
    t.geohash,
    t.vehicle_count,
    t.avg_speed,
    t.min_speed,
    t.max_speed,
    t.geom,
    grl.road_length_km,
    grl.segment_count,
    CASE
        WHEN grl.road_length_km > 0
        THEN t.vehicle_count::double precision / grl.road_length_km
        ELSE NULL
    END AS vehicles_per_road_km
FROM ibb_traffic_density t
LEFT JOIN geohash_road_lengths grl ON t.geohash = grl.geohash;
"""

# road_density_congestion_candidates uses a dynamic percentile computed
# from the current data at query time. Speed threshold is baked in at
# view creation time from ROAD_DENSITY_PERCENTILE_THRESHOLD config.
_DDL_ROAD_DENSITY_CANDIDATES_TEMPLATE = """
CREATE OR REPLACE VIEW road_density_congestion_candidates AS
WITH percentile_threshold AS (
    SELECT percentile_cont({percentile}) WITHIN GROUP (
        ORDER BY vehicles_per_road_km
    ) AS threshold
    FROM road_density_base
    WHERE vehicles_per_road_km IS NOT NULL
      AND vehicles_per_road_km > 0
)
SELECT
    b.*,
    pt.threshold AS density_threshold,
    'road_length'::text AS filter_method
FROM road_density_base b
CROSS JOIN percentile_threshold pt
WHERE b.vehicles_per_road_km >= pt.threshold
  AND b.avg_speed < {speed_limit};
"""


def _check_preflight(conn) -> bool:
    """Verify ibb_traffic_density exists and has data."""
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'ibb_traffic_density'
    """)
    if cur.fetchone()[0] == 0:
        logger.error(
            "Table ibb_traffic_density not found.\n"
            "Run the pipeline first:\n"
            "  python download_data.py\n"
            "  python ingest_data.py\n"
            "  python create_views.py"
        )
        return False
    cur.execute("SELECT COUNT(*)::int FROM ibb_traffic_density")
    row_count = cur.fetchone()[0]
    if row_count == 0:
        logger.error("ibb_traffic_density is empty. Ingest data first.")
        return False
    logger.info(f"ibb_traffic_density: {row_count:,} rows — OK")
    return True


def create_tables(conn) -> None:
    """Create road_segments and geohash_cells tables."""
    cur = conn.cursor()
    cur.execute(_DDL_ROAD_SEGMENTS)
    cur.execute(_DDL_ROAD_SEGMENTS_INDEXES)
    cur.execute(_DDL_GEOHASH_CELLS)
    cur.execute(_DDL_GEOHASH_CELLS_INDEX)
    conn.commit()
    logger.info("[1/5] road_segments and geohash_cells tables — OK")


def populate_geohash_cells(conn) -> int:
    """Decode every distinct geohash in ibb_traffic_density into geohash_cells."""
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT geohash FROM ibb_traffic_density WHERE geohash IS NOT NULL AND geohash != ''")
    geohashes = [r[0] for r in cur.fetchall()]

    rows = []
    for gh in geohashes:
        try:
            min_lon, min_lat, max_lon, max_lat = geohash_envelope_sql_args(gh)
            rows.append((gh, min_lon, min_lat, max_lon, max_lat))
        except ValueError as exc:
            logger.warning(f"Skipping invalid geohash {gh!r}: {exc}")

    if not rows:
        logger.warning("No valid geohashes found.")
        return 0

    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO geohash_cells (geohash, geom_4326, geom_32636, area_km2)
        SELECT
            geohash,
            ST_MakeEnvelope(min_lon, min_lat, max_lon, max_lat, 4326),
            ST_Transform(ST_MakeEnvelope(min_lon, min_lat, max_lon, max_lat, 4326), 32636),
            ST_Area(ST_Transform(ST_MakeEnvelope(min_lon, min_lat, max_lon, max_lat, 4326), 32636)) / 1e6
        FROM (VALUES %s) AS t(geohash, min_lon, min_lat, max_lon, max_lat)
        ON CONFLICT (geohash) DO NOTHING
        """,
        rows,
    )
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM geohash_cells")
    total = cur.fetchone()[0]
    logger.info(f"[2/5] geohash_cells: {total:,} cells populated — OK")
    return total


def create_matview(conn) -> None:
    """Create geohash_road_lengths materialized view (WITH NO DATA)."""
    cur = conn.cursor()
    cur.execute(_DDL_GEOHASH_ROAD_LENGTHS)
    try:
        cur.execute(_DDL_GEOHASH_ROAD_LENGTHS_INDEX)
    except psycopg2.errors.DuplicateTable:
        pass
    conn.commit()
    logger.info("[3/5] geohash_road_lengths materialized view — created (empty until REFRESH)")


def _matview_is_populated(conn) -> bool:
    """Return True if geohash_road_lengths has been populated at least once.

    Uses pg_matviews.ispopulated — safe to call even on a WITH NO DATA view.
    Querying the view directly raises ObjectNotInPrerequisiteState when unpopulated.
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT ispopulated FROM pg_matviews WHERE matviewname = 'geohash_road_lengths'"
    )
    row = cur.fetchone()
    return bool(row and row[0])


def refresh_matview(conn) -> None:
    """Refresh geohash_road_lengths with safe first-refresh detection.

    PostgreSQL requires at least one non-concurrent REFRESH before
    CONCURRENTLY can be used.  We check pg_matviews.ispopulated:
      - False → first refresh → non-concurrent
      - True  → subsequent refresh → concurrent (with non-concurrent fallback)
    """
    cur = conn.cursor()
    populated = _matview_is_populated(conn)

    if populated:
        logger.info(
            "Existing populated materialized view detected; using concurrent refresh"
        )
        try:
            cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY geohash_road_lengths")
            conn.commit()
        except psycopg2.errors.FeatureNotSupported:
            logger.warning(
                "Concurrent refresh not supported in this state; "
                "falling back to non-concurrent refresh"
            )
            conn.rollback()
            cur = conn.cursor()
            cur.execute("REFRESH MATERIALIZED VIEW geohash_road_lengths")
            conn.commit()
    else:
        logger.info(
            "First materialized view refresh detected; using non-concurrent refresh"
        )
        cur.execute("REFRESH MATERIALIZED VIEW geohash_road_lengths")
        conn.commit()

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*), COUNT(NULLIF(road_length_km, 0)) FROM geohash_road_lengths")
    total, with_roads = cur.fetchone()
    logger.info(
        f"geohash_road_lengths refreshed: {total:,} cells, "
        f"{with_roads:,} have road coverage ({100 * with_roads / max(total, 1):.1f}%)"
    )


def create_views(conn) -> None:
    """Create road_density_base and road_density_congestion_candidates views."""
    cur = conn.cursor()
    cur.execute(_DDL_ROAD_DENSITY_BASE)
    percentile_frac = ROAD_DENSITY_PERCENTILE_THRESHOLD / 100.0
    candidates_sql = _DDL_ROAD_DENSITY_CANDIDATES_TEMPLATE.format(
        percentile=percentile_frac,
        speed_limit=HIGH_CONGESTION_MAX_AVG_SPEED,
    )
    cur.execute(candidates_sql)
    conn.commit()
    logger.info(
        f"[4/5] road_density_base view — OK\n"
        f"[5/5] road_density_congestion_candidates view "
        f"(speed<{HIGH_CONGESTION_MAX_AVG_SPEED}, "
        f"density≥p{ROAD_DENSITY_PERCENTILE_THRESHOLD:.0f}) — OK"
    )


def drop_road_tables(conn) -> None:
    """Drop road network tables/views (destructive — use only for reset)."""
    cur = conn.cursor()
    for obj in [
        "VIEW road_density_congestion_candidates",
        "VIEW road_density_base",
        "MATERIALIZED VIEW geohash_road_lengths",
        "TABLE geohash_cells",
        "TABLE road_segments",
    ]:
        cur.execute(f"DROP {obj} IF EXISTS CASCADE")
    conn.commit()
    logger.warning("Road network tables/views dropped.")


def main() -> None:
    ensure_cli_logging()
    parser = argparse.ArgumentParser(description="Create road-network schema in PostGIS")
    parser.add_argument("--refresh", action="store_true",
                        help="Refresh geohash_road_lengths materialized view")
    parser.add_argument("--drop-road-tables", action="store_true",
                        help="Drop and recreate road network tables (destructive)")
    args = parser.parse_args()

    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except Exception as exc:
        logger.error(
            f"Cannot connect to database: {exc}\n"
            "Start the database with: docker-compose up -d"
        )
        sys.exit(1)

    if args.drop_road_tables:
        confirm = input("This will DROP road_segments, geohash_cells, and related views. Type YES to confirm: ")
        if confirm.strip() != "YES":
            logger.info("Aborted.")
            sys.exit(0)
        drop_road_tables(conn)

    if not _check_preflight(conn):
        sys.exit(1)

    if args.refresh:
        cur = conn.cursor()
        cur.execute("""
            SELECT 1 FROM pg_matviews WHERE matviewname = 'geohash_road_lengths'
        """)
        if not cur.fetchone():
            logger.error("geohash_road_lengths does not exist. Run without --refresh first.")
            sys.exit(1)
        cur.execute("SELECT COUNT(*) FROM road_segments")
        if cur.fetchone()[0] == 0:
            logger.error(
                "road_segments is empty — import road data first:\n"
                "  python scripts/import_road_network.py --input data/road_network/Istanbul.osm.pbf"
            )
            sys.exit(1)
        refresh_matview(conn)
        conn.close()
        return

    create_tables(conn)
    populate_geohash_cells(conn)
    create_matview(conn)
    create_views(conn)

    logger.info("\nSchema ready. Next step:")
    logger.info("  python scripts/import_road_network.py --input data/road_network/Istanbul.osm.pbf")
    conn.close()


if __name__ == "__main__":
    main()
