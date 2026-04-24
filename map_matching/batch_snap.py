"""
Batch map matching — snap all unique geohash centroids in traffic_clusters
and update the database with snapped coordinates.

Usage:
    python -m map_matching.batch_snap
"""

from __future__ import annotations

import logging

import psycopg2

from config import DB_CONFIG, ensure_cli_logging
from map_matching.snap import snap_to_road

logger = logging.getLogger(__name__)

SNAP_PROGRESS_EVERY_N = 500


def main() -> None:
    """Load unsnapped cluster centroids, snap via OSRM, and write back to PostgreSQL."""
    ensure_cli_logging()
    logger.info("Connecting to DB...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    try:
        # ── Step 1: Get unique (lat, lon) pairs from traffic_clusters ──────
        logger.info("Fetching unique geohash centroids from traffic_clusters...")
        cur.execute("""
            SELECT DISTINCT lat, lon
            FROM traffic_clusters
            WHERE snapped_lat IS NULL
            ORDER BY lat, lon;
        """)
        unique_points = cur.fetchall()
        n = len(unique_points)
        logger.info(f"  Found {n:,} unique un-snapped centroids.")

        if n == 0:
            logger.info("Nothing to snap — all points already have snapped coordinates.")
            return

        # ── Step 2: Snap each unique point via OSRM ───────────────────────
        logger.info("Snapping points to road network via OSRM...")
        snap_map: dict[tuple[float, float], tuple] = {}
        failed = 0

        for i, (lat, lon) in enumerate(unique_points, 1):
            result = snap_to_road(lat, lon)
            snap_map[(lat, lon)] = (
                result.snapped_lat,
                result.snapped_lon,
                result.road_name,
                result.distance_m,
            )
            if not result.was_snapped:
                failed += 1

            if i % SNAP_PROGRESS_EVERY_N == 0 or i == n:
                logger.info(f"  ... snapped {i:,}/{n:,} points ({failed} un-snapped)")

        # ── Step 3: Batch update database ─────────────────────────────────
        logger.info("Updating traffic_clusters with snapped coordinates...")
        update_count = 0
        for (lat, lon), (s_lat, s_lon, road, dist) in snap_map.items():
            cur.execute("""
                UPDATE traffic_clusters
                SET snapped_lat = %s,
                    snapped_lon = %s,
                    road_name = %s,
                    snap_distance_m = %s
                WHERE lat = %s AND lon = %s
                  AND snapped_lat IS NULL;
            """, (s_lat, s_lon, road, dist, lat, lon))
            update_count += cur.rowcount

        conn.commit()
        logger.info(f"\nSuccess! Updated {update_count:,} rows across {n:,} unique centroids.")
        logger.info(f"  Failed to snap: {failed} points (returned original coordinates).")

        # ── Step 4: Summary statistics ────────────────────────────────────
        distances = [v[3] for v in snap_map.values() if v[3] > 0]
        if distances:
            avg_dist = sum(distances) / len(distances)
            max_dist = max(distances)
            logger.info(f"  Average snap distance: {avg_dist:.1f} m")
            logger.info(f"  Maximum snap distance: {max_dist:.1f} m")

    finally:
        cur.close()
        conn.close()
        logger.info("Database connection closed.")


if __name__ == "__main__":
    main()
