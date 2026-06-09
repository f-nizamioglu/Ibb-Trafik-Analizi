"""
Main pipeline entry point — PostGIS-native spatial DBSCAN + AIS scoring.

Pipeline stages:
  1. Raw IBB CSV download      (download_data.py — separate step)
  2. Data ingestion             (ingest_data.py — separate step)
  3. EPSG:32636 metric geometry (applied during ingestion)
  4. Congestion candidate filter (high_congestion_zones VIEW, create_views.py)
  5. PostGIS spatial DBSCAN     ← this script, Stage 1
  6. Temporal recurrence/duration analysis (via AIS components)
  7. AIS scoring                ← this script, Stage 2

Clustering is executed entirely within PostGIS using ST_ClusterDBSCAN over
the EPSG:32636 geometry column, which allows eps to be specified in metres.
Temporal behaviour is captured post-clustering through duration_hours and
recurrence_days components of the AIS score.

This is NOT a full Birant & Kut ST-DBSCAN implementation. There is no
temporal epsilon (ε₂) neighbourhood constraint during clustering.

Usage:
    python run_pipeline.py
"""

from __future__ import annotations

import logging
import time

import psycopg2

from config import DB_CONFIG, EPS_METERS, MINPTS, ensure_cli_logging

logger = logging.getLogger(__name__)

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

# ── Stage 5: PostGIS spatial DBSCAN ───────────────────────────────────────
# ST_ClusterDBSCAN is a window function that assigns a cluster_id to each row.
# It operates on the EPSG:32636 geometry column, so eps is in metres directly.
# Noise points receive NULL from ST_ClusterDBSCAN; mapped to -1.
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

    logger.info("=" * 60)
    logger.info("Istanbul Traffic Anomaly Analysis — Pipeline")
    logger.info("=" * 60)
    logger.info(f"  Methodology : PostGIS-based Spatial DBSCAN + Temporal Recurrence Analysis")
    logger.info(f"  eps         : {EPS_METERS} m (EPSG:32636 metric geometry)")
    logger.info(f"  minpoints   : {MINPTS}")

    logger.info("\nConnecting to DB...")
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:

            # ── Stage 5: Recreate traffic_clusters and run spatial DBSCAN ──
            logger.info("\n[Stage 5] PostGIS spatial DBSCAN clustering ...")
            logger.info("  Recreating traffic_clusters table ...")
            cur.execute(DROP_TABLE_SQL)
            cur.execute(CREATE_TABLE_SQL)
            conn.commit()

            logger.info(f"  Running ST_ClusterDBSCAN (eps={EPS_METERS}m, minpoints={MINPTS}) ...")
            t0 = time.perf_counter()
            cur.execute(CLUSTER_AND_INSERT_SQL, (EPS_METERS, MINPTS))
            elapsed = time.perf_counter() - t0
            conn.commit()

            cur.execute(COUNT_RESULTS_SQL)
            row = cur.fetchone()
            total_rows, n_clusters, n_noise = row
            noise_pct = 100.0 * n_noise / total_rows if total_rows else 0.0

            logger.info(f"  Clustering complete in {elapsed:.2f}s")
            logger.info(f"  Total candidate rows : {total_rows:,}")
            logger.info(f"  Clusters found       : {n_clusters}")
            logger.info(f"  Noise points         : {n_noise:,} ({noise_pct:.1f}%)")

        # ── Stage 6–7: Temporal analysis + AIS scoring ─────────────────────
        logger.info("\n[Stage 6-7] Temporal recurrence/duration analysis + AIS scoring ...")
        from scoring.anomaly_score import compute_cluster_scores, print_cluster_report

        import pandas as pd
        df = pd.read_sql(
            "SELECT record_time, vehicle_count, avg_speed, cluster_id "
            "FROM traffic_clusters;",
            conn,
        )
        scores = compute_cluster_scores(df)
        print_cluster_report(scores)

        scores.to_csv(OUTPUT_CLUSTER_SCORES_CSV)
        logger.info(f"\nScores saved to {OUTPUT_CLUSTER_SCORES_CSV}")

    logger.info("\nPipeline complete.")


if __name__ == "__main__":
    main()
