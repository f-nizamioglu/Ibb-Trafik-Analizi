"""
Create spatial indexes, the congestion candidate view, and the clusters table.

Pipeline stage 3-4:
  - GiST spatial index on ibb_traffic_density.geom (EPSG:32636)
  - high_congestion_zones VIEW: static baseline congestion candidate filter
  - traffic_clusters table: receives PostGIS spatial DBSCAN output

Thresholds are read from config (overridable via .env).

Usage:
    python create_views.py
"""

import logging

import psycopg2

from config import (
    DB_CONFIG,
    HIGH_CONGESTION_MAX_AVG_SPEED,
    HIGH_CONGESTION_MIN_VEHICLE_COUNT,
    ensure_cli_logging,
)

logger = logging.getLogger(__name__)

# ─── Spatial Index ─────────────────────────────────────────────────────────
CREATE_SPATIAL_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_ibb_traffic_geom
ON ibb_traffic_density USING GIST (geom);
"""

# ─── High Congestion View ─────────────────────────────────────────────────
# Static baseline filter. Not a final scientifically optimal threshold.
# Records with avg_speed below threshold AND vehicle_count above threshold
# are treated as congestion candidates for subsequent spatial clustering.
CREATE_VIEW_SQL = f"""
CREATE OR REPLACE VIEW high_congestion_zones AS
SELECT *
FROM ibb_traffic_density
WHERE avg_speed < {HIGH_CONGESTION_MAX_AVG_SPEED}
  AND vehicle_count > {HIGH_CONGESTION_MIN_VEHICLE_COUNT};
"""

# ─── Clusters Table ───────────────────────────────────────────────────────
CREATE_CLUSTERS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS traffic_clusters (
    id SERIAL PRIMARY KEY,
    record_time TIMESTAMP WITHOUT TIME ZONE,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    geohash VARCHAR(20),
    vehicle_count INTEGER,
    avg_speed INTEGER,
    cluster_id INTEGER
);
"""

# ─── Cluster indexes for fast API queries ─────────────────────────────────
CREATE_CLUSTERS_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_traffic_clusters_cluster_id
ON traffic_clusters (cluster_id);
"""

CREATE_RECORD_TIME_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_traffic_clusters_record_time
ON traffic_clusters (record_time);
"""


def main() -> None:
    ensure_cli_logging()

    logger.info("=" * 60)
    logger.info("STAGE 3/4: Spatial Index + Congestion Candidate Filter")
    logger.info("=" * 60)

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    try:
        logger.info("\n[1/5] Creating GiST spatial index on ibb_traffic_density.geom ...")
        cur.execute(CREATE_SPATIAL_INDEX_SQL)

        logger.info(
            f"[2/5] Creating view high_congestion_zones "
            f"(avg_speed < {HIGH_CONGESTION_MAX_AVG_SPEED} km/h, "
            f"vehicle_count > {HIGH_CONGESTION_MIN_VEHICLE_COUNT}) ..."
        )
        cur.execute(CREATE_VIEW_SQL)

        logger.info("[3/5] Creating table traffic_clusters ...")
        cur.execute(CREATE_CLUSTERS_TABLE_SQL)

        logger.info("[4/5] Creating index on traffic_clusters.cluster_id ...")
        cur.execute(CREATE_CLUSTERS_INDEX_SQL)

        logger.info("[5/5] Creating index on traffic_clusters.record_time ...")
        cur.execute(CREATE_RECORD_TIME_INDEX_SQL)

        conn.commit()
        logger.info(
            "\nSuccess. Objects created:\n"
            "  idx_ibb_traffic_geom          (GiST spatial index, EPSG:32636)\n"
            f"  high_congestion_zones         (avg_speed < {HIGH_CONGESTION_MAX_AVG_SPEED} km/h "
            f"AND vehicle_count > {HIGH_CONGESTION_MIN_VEHICLE_COUNT})\n"
            "  traffic_clusters              (receives ST_ClusterDBSCAN output)\n"
            "  idx_traffic_clusters_cluster_id\n"
            "  idx_traffic_clusters_record_time"
        )
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
