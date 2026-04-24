"""
Create spatial indexes and materialized views for ST-DBSCAN analysis.

Includes the high_congestion_zones view and the updated traffic_clusters
table schema with avg_speed + snapped coordinates for map matching.

Usage:
    python create_views.py
"""

import logging

import psycopg2

from config import DB_CONFIG, ensure_cli_logging

logger = logging.getLogger(__name__)

# Thresholds (must match business meaning of "high congestion" in documentation)
HIGH_CONGESTION_MAX_AVG_SPEED = 20
HIGH_CONGESTION_MIN_VEHICLE_COUNT = 500

# ─── Spatial Index ─────────────────────────────────────────────────────────
CREATE_SPATIAL_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_ibb_traffic_geom
ON ibb_traffic_density USING GIST (geom);
"""

# ─── High Congestion View ─────────────────────────────────────────────────
CREATE_VIEW_SQL = f"""
CREATE OR REPLACE VIEW high_congestion_zones AS
SELECT *
FROM ibb_traffic_density
WHERE avg_speed < {HIGH_CONGESTION_MAX_AVG_SPEED}
  AND vehicle_count > {HIGH_CONGESTION_MIN_VEHICLE_COUNT};
"""

# ─── Updated Clusters Table (includes avg_speed + snap columns) ───────────
CREATE_CLUSTERS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS traffic_clusters (
    id SERIAL PRIMARY KEY,
    record_time TIMESTAMP WITHOUT TIME ZONE,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    geohash VARCHAR(20),
    vehicle_count INTEGER,
    avg_speed INTEGER,
    cluster_id INTEGER,
    snapped_lat DOUBLE PRECISION,
    snapped_lon DOUBLE PRECISION,
    road_name TEXT,
    snap_distance_m DOUBLE PRECISION
);
"""

# ─── Spatial index on clusters for fast GeoJSON queries ───────────────────
CREATE_CLUSTERS_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_traffic_clusters_cluster_id
ON traffic_clusters (cluster_id);
"""

# ─── Date Index ───────────────────────────────────────────────────────────
CREATE_RECORD_TIME_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_traffic_clusters_record_time
ON traffic_clusters (record_time);
"""


def main() -> None:
    ensure_cli_logging()
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    try:
        logger.info("Creating spatial index on ibb_traffic_density.geom ...")
        cur.execute(CREATE_SPATIAL_INDEX_SQL)

        logger.info("Creating view high_congestion_zones ...")
        cur.execute(CREATE_VIEW_SQL)

        logger.info("Creating table traffic_clusters (with avg_speed + snap columns) ...")
        cur.execute(CREATE_CLUSTERS_TABLE_SQL)

        logger.info("Creating index on traffic_clusters.cluster_id ...")
        cur.execute(CREATE_CLUSTERS_INDEX_SQL)

        logger.info("Creating index on traffic_clusters.record_time ...")
        cur.execute(CREATE_RECORD_TIME_INDEX_SQL)

        conn.commit()
        logger.info(
            "\nSuccess: All indexes, views, and tables are ready.\n"
            "  - idx_ibb_traffic_geom (GiST spatial index)\n"
            f"  - high_congestion_zones (avg_speed < {HIGH_CONGESTION_MAX_AVG_SPEED}, "
            f"vehicle_count > {HIGH_CONGESTION_MIN_VEHICLE_COUNT})\n"
            "  - traffic_clusters (with avg_speed, geohash, snap columns)\n"
            "  - idx_traffic_clusters_cluster_id"
        )
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
