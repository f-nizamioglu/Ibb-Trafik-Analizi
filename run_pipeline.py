"""
Main pipeline entry point — PostGIS-native ST_ClusterDBSCAN.

Orchestrates the full analysis workflow entirely in the database layer:
  1. Run ST_ClusterDBSCAN on the high_congestion_zones view (PostGIS)
  2. Write clustered results to the traffic_clusters table
  3. Compute AIS (Anomaly Intensity Scores) from the clustered data

The clustering computation is offloaded to PostGIS, which uses the EPSG:32636
(UTM Zone 36N) geometry column for direct metric distance calculations.

Usage:
    python run_pipeline.py
"""

from __future__ import annotations

import argparse
import logging
import time

import psycopg2

from config import DB_CONFIG, EPS_METERS, MINPTS, ensure_cli_logging

logger = logging.getLogger(__name__)

# ── Tunables ───────────────────────────────────────────────────────────────
CLI_BANNER_WIDTH = 60
OUTPUT_CLUSTER_SCORES_CSV = "cluster_scores.csv"

DROP_TABLE_SQL = "DROP TABLE IF EXISTS traffic_clusters;"

CREATE_TABLE_SQL = """
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

# ── PostGIS-native DBSCAN clustering ──────────────────────────────────────
# ST_ClusterDBSCAN is a window function that assigns a cluster_id to each row.
# It operates on the EPSG:32636 geometry column, so eps is in metres directly.
# Noise points receive NULL from ST_ClusterDBSCAN; we COALESCE to -1.
CLUSTER_AND_INSERT_SQL = """
INSERT INTO traffic_clusters (
    record_time, lat, lon, geohash, vehicle_count, avg_speed, cluster_id
)
SELECT
    record_time,
    lat,
    lon,
    geohash,
    vehicle_count,
    avg_speed,
    COALESCE(
        ST_ClusterDBSCAN(geom, eps := %s, minpoints := %s) OVER (),
        -1
    ) AS cluster_id
FROM high_congestion_zones;
"""

COUNT_RESULTS_SQL = """
SELECT
    COUNT(*)::int                                           AS total_rows,
    COUNT(DISTINCT CASE WHEN cluster_id >= 0 THEN cluster_id END)::int AS n_clusters,
    COUNT(CASE WHEN cluster_id = -1 THEN 1 END)::int       AS n_noise
FROM traffic_clusters;
"""


def main() -> None:
    ensure_cli_logging()
    parser = argparse.ArgumentParser(
        description="Istanbul Traffic PostGIS ST_ClusterDBSCAN Pipeline"
    )
    parser.parse_args()

    logger.info("Connecting to DB...")
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # ── Step 1: Recreate traffic_clusters table ────────────────────
            logger.info("Recreating traffic_clusters table...")
            cur.execute(DROP_TABLE_SQL)
            cur.execute(CREATE_TABLE_SQL)
            conn.commit()

            # ── Step 2: Run PostGIS ST_ClusterDBSCAN ──────────────────────
            logger.info(
                f"\nRunning PostGIS ST_ClusterDBSCAN "
                f"(eps={EPS_METERS}m, minpoints={MINPTS})..."
            )
            t0 = time.perf_counter()
            cur.execute(CLUSTER_AND_INSERT_SQL, (EPS_METERS, MINPTS))
            elapsed = time.perf_counter() - t0
            conn.commit()

            # ── Step 3: Report results ────────────────────────────────────
            cur.execute(COUNT_RESULTS_SQL)
            row = cur.fetchone()
            total_rows, n_clusters, n_noise = row
            noise_pct = 100.0 * n_noise / total_rows if total_rows else 0.0

            logger.info(f"\n  Clustering complete in {elapsed:.2f}s")
            logger.info(f"  Total rows:    {total_rows:,}")
            logger.info(f"  Clusters found: {n_clusters}")
            logger.info(f"  Noise: {n_noise:,} / {total_rows:,} ({noise_pct:.2f}%)")

        # ── Step 4: AIS Scoring ───────────────────────────────────────────
        from scoring.anomaly_score import compute_cluster_scores, print_cluster_report

        logger.info("\nComputing Anomaly Intensity Scores...")

        import pandas as pd
        df = pd.read_sql(
            "SELECT record_time, vehicle_count, avg_speed, cluster_id "
            "FROM traffic_clusters;",
            conn,
        )
        scores = compute_cluster_scores(df)
        print_cluster_report(scores)

        # Save scores to CSV for reference
        scores.to_csv(OUTPUT_CLUSTER_SCORES_CSV)
        logger.info(f"\nScores saved to {OUTPUT_CLUSTER_SCORES_CSV}")

    logger.info("\nPipeline complete.")


if __name__ == "__main__":
    main()
