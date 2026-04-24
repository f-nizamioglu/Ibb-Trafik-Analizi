"""
Main pipeline entry point — replaces the old st_dbscan_analysis.py.

Orchestrates the full analysis workflow:
  1. Load data from high_congestion_zones view
  2. Run optimised ST-DBSCAN clustering
  3. Compute evaluation metrics
  4. Write results to traffic_clusters table
  5. Compute AIS (Anomaly Intensity Scores)

Usage:
    python run_pipeline.py
    python run_pipeline.py --validate    # also run sensitivity analysis
    python run_pipeline.py --partitioned # use geohash partitioning (for large data)
"""

from __future__ import annotations

import argparse
import logging
import time

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from config import BATCH_SIZE, DB_CONFIG, ensure_cli_logging

logger = logging.getLogger(__name__)

# ── Tunables (defaults; not loaded from .env) ─────────────────────────────
PANDAS_TIMESTAMP_NS_PER_S = 10**9
DEFAULT_GEOHASH_PREFIX_LEN = 4
CLI_BANNER_WIDTH = 60
OUTPUT_SENSITIVITY_CSV = "sensitivity_analysis.csv"
OUTPUT_CLUSTER_SCORES_CSV = "cluster_scores.csv"

LOAD_SQL = """
SELECT record_time, lat, lon, geohash, vehicle_count, avg_speed
FROM high_congestion_zones;
"""

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
    cluster_id INTEGER,
    snapped_lat DOUBLE PRECISION,
    snapped_lon DOUBLE PRECISION,
    road_name TEXT,
    snap_distance_m DOUBLE PRECISION
);
"""


def main() -> None:
    ensure_cli_logging()
    parser = argparse.ArgumentParser(description="Istanbul Traffic ST-DBSCAN Pipeline")
    parser.add_argument(
        "--validate", action="store_true",
        help="Run sensitivity analysis with multiple parameter configs",
    )
    parser.add_argument(
        "--partitioned", action="store_true",
        help="Use geohash partitioning (for datasets > MAX_CLUSTER_INPUT)",
    )
    args = parser.parse_args()

    # ── Step 1: Load data ──────────────────────────────────────────────────
    logger.info("Connecting to DB...")
    with psycopg2.connect(**DB_CONFIG) as conn:
        logger.info("Loading high_congestion_zones...")
        df = pd.read_sql(LOAD_SQL, conn)
        n_rows = len(df)

        if n_rows == 0:
            logger.info("No rows in high_congestion_zones; nothing to cluster.")
            return

        logger.info(f"  Loaded {n_rows:,} rows.")

        # ── Step 2: Preprocess ─────────────────────────────────────────────────
        logger.info("Preprocessing...")
        df["record_time"] = pd.to_datetime(df["record_time"])
        lat = df["lat"].to_numpy(dtype=np.float64)
        lon = df["lon"].to_numpy(dtype=np.float64)
        t_sec = (df["record_time"].astype("int64") // PANDAS_TIMESTAMP_NS_PER_S).to_numpy(
            dtype=np.float64
        )

        # ── Step 3: Cluster ────────────────────────────────────────────────────
        t0 = time.perf_counter()

        if args.partitioned:
            from clustering.partitioner import run_partitioned_st_dbscan
            logger.info("\nRunning PARTITIONED ST-DBSCAN (geohash-based)...")
            labels = run_partitioned_st_dbscan(
                df, prefix_len=DEFAULT_GEOHASH_PREFIX_LEN, verbose=True
            )
        else:
            from clustering.st_dbscan import run_st_dbscan
            logger.info("\nRunning ST-DBSCAN (BallTree optimised)...")
            labels = run_st_dbscan(lat, lon, t_sec, verbose=True)

        elapsed = time.perf_counter() - t0
        df["cluster_id"] = labels

        n_noise = int(np.sum(labels == -1))
        n_clusters = len(set(labels[labels >= 0]))
        noise_pct = 100.0 * n_noise / n_rows

        logger.info(f"\n  Clustering complete in {elapsed:.2f}s")
        logger.info(f"  Clusters found: {n_clusters}")
        logger.info(f"  Noise: {n_noise:,} / {n_rows:,} ({noise_pct:.2f}%)")

        # ── Step 4: Evaluate ───────────────────────────────────────────────────
        from clustering.validation import evaluate_clustering
        logger.info("\nComputing evaluation metrics...")
        metrics = evaluate_clustering(lat, lon, t_sec, labels)
        logger.info(f"  Silhouette Score:      {metrics['silhouette']:.4f}")
        logger.info(f"  Davies-Bouldin Index:  {metrics['davies_bouldin']:.4f}")
        logger.info(f"  DBCV:                  {metrics['dbcv']}")

        # ── Step 4b: Optional sensitivity analysis ─────────────────────────────
        if args.validate:
            from clustering.validation import run_sensitivity_analysis
            logger.info("\n" + "=" * CLI_BANNER_WIDTH)
            logger.info("SENSITIVITY ANALYSIS")
            results_df = run_sensitivity_analysis(lat, lon, t_sec, verbose=True)
            results_df.to_csv(OUTPUT_SENSITIVITY_CSV, index=False)
            logger.info(f"\nResults saved to {OUTPUT_SENSITIVITY_CSV}")

        # ── Step 5: Write to DB ────────────────────────────────────────────────
        logger.info("\nWriting results to traffic_clusters...")
        with conn.cursor() as cur:
            cur.execute(DROP_TABLE_SQL)
            cur.execute(CREATE_TABLE_SQL)

            out = df[["record_time", "lat", "lon", "geohash", "vehicle_count", "avg_speed", "cluster_id"]]
            rows = [
                (
                    row.record_time,
                    float(row.lat),
                    float(row.lon),
                    row.geohash,
                    int(row.vehicle_count),
                    int(row.avg_speed),
                    int(row.cluster_id),
                )
                for row in out.itertuples(index=False)
            ]
            execute_values(
                cur,
                """
                INSERT INTO traffic_clusters (
                    record_time, lat, lon, geohash, vehicle_count, avg_speed, cluster_id
                ) VALUES %s
                """,
                rows,
                page_size=BATCH_SIZE,
            )
            logger.info(f"  Wrote {len(rows):,} rows to traffic_clusters.")

        # ── Step 6: AIS Scoring ────────────────────────────────────────────────
        from scoring.anomaly_score import compute_cluster_scores, print_cluster_report
        logger.info("\nComputing Anomaly Intensity Scores...")
        scores = compute_cluster_scores(df)
        print_cluster_report(scores)

        # Save scores to CSV for reference
        scores.to_csv(OUTPUT_CLUSTER_SCORES_CSV)
        logger.info(f"\nScores saved to {OUTPUT_CLUSTER_SCORES_CSV}")

    logger.info("\nPipeline complete.")


if __name__ == "__main__":
    main()
