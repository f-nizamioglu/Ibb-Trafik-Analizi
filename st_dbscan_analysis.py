"""
ST-DBSCAN-style clustering on high_congestion_zones using a joint spatio-temporal
neighborhood: Euclidean distance in (lat, lon) degrees <= eps1 AND |dt| <= eps2.

This matches sklearn.DBSCAN(metric='precomputed', eps=1.0) when pairwise distances are
d(i,j) = max(h_dist(i,j) / eps1, |t_i - t_j| / eps2), where h_dist is hypot on lat/lon.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from sklearn.cluster import DBSCAN

from config import ensure_cli_logging

try:
    from sklearn.cluster._dbscan_inner import dbscan_inner
except ImportError:  # pragma: no cover
    dbscan_inner = None

logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "dbname": "istanbul_traffic",
    "user": "postgres",
    "password": "password123",
}

EPS1_DEG = 0.005
EPS2_SEC = 3600.0
MINPTS = 3

PANDAS_TIMESTAMP_NS_PER_S = 10**9
# Use neighbor-list + dbscan_inner above this n to avoid O(n²) distance matrix.
ST_DBSCAN_INNER_MODE_ROW_THRESHOLD = 8000
DB_INSERT_PAGE_SIZE = 10_000

LOAD_SQL = """
SELECT record_time, lat, lon, vehicle_count
FROM high_congestion_zones;
"""

DROP_TABLE_SQL = "DROP TABLE IF EXISTS traffic_clusters;"

CREATE_TABLE_SQL = """
CREATE TABLE traffic_clusters (
    id SERIAL PRIMARY KEY,
    record_time TIMESTAMP WITHOUT TIME ZONE,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    vehicle_count INTEGER,
    cluster_id INTEGER
);
"""


def _pairwise_st_distances(
    lat: np.ndarray, lon: np.ndarray, t_sec: np.ndarray
) -> np.ndarray:
    """Return float32 matrix D with d_ij = max(hypot(dlat,dlon)/eps1, |dt|/eps2)."""
    lat = np.asarray(lat, dtype=np.float64)
    lon = np.asarray(lon, dtype=np.float64)
    t_sec = np.asarray(t_sec, dtype=np.float64)
    n = lat.shape[0]
    d = np.empty((n, n), dtype=np.float32)
    for i in range(n):
        d_sp = np.hypot(lat[i] - lat, lon[i] - lon) / EPS1_DEG
        d_t = np.abs(t_sec[i] - t_sec) / EPS2_SEC
        d[i] = np.maximum(d_sp, d_t).astype(np.float32, copy=False)
    np.fill_diagonal(d, 0.0)
    return d


def _st_dbscan_via_inner(
    lat: np.ndarray, lon: np.ndarray, t_sec: np.ndarray
) -> np.ndarray:
    """Same clustering as DBSCAN(precomputed, eps=1) without storing full D."""
    if dbscan_inner is None:
        raise RuntimeError("sklearn dbscan_inner is not available")
    n = lat.shape[0]
    lat = np.asarray(lat, dtype=np.float64)
    lon = np.asarray(lon, dtype=np.float64)
    t_sec = np.asarray(t_sec, dtype=np.float64)
    neighborhoods = np.empty(n, dtype=object)
    for i in range(n):
        d_sp = np.hypot(lat[i] - lat, lon[i] - lon) / EPS1_DEG
        d_t = np.abs(t_sec[i] - t_sec) / EPS2_SEC
        idx = np.where(np.maximum(d_sp, d_t) <= 1.0)[0].astype(np.intp, copy=False)
        neighborhoods[i] = idx
    is_core = (
        np.fromiter((len(neighborhoods[i]) for i in range(n)), dtype=np.intp, count=n)
        >= MINPTS
    ).astype(np.uint8)
    labels = np.full(n, -1, dtype=np.intp)
    dbscan_inner(is_core, neighborhoods, labels)
    return labels.astype(np.int64)


def run_st_dbscan(lat: np.ndarray, lon: np.ndarray, t_sec: np.ndarray) -> np.ndarray:
    """Run ST-DBSCAN-style clustering; noise label is -1.

    Uses a precomputed spatiotemporal max-distance matrix and scikit-learn ``DBSCAN``,
    or switches to neighbor lists and ``dbscan_inner`` for large *n* or on memory
    errors (see module docstring for the distance definition).

    Args:
        lat: Latitude in degrees.
        lon: Longitude in degrees.
        t_sec: Timestamps in Unix seconds.

    Returns:
        Integer cluster labels, shape ``(n,)``; -1 indicates noise.
    """
    n = lat.shape[0]
    if n == 0:
        return np.array([], dtype=np.int64)
    # Avoid O(n^2) float32 distance matrix when n is large (same clustering via inner loop).
    if n > ST_DBSCAN_INNER_MODE_ROW_THRESHOLD and dbscan_inner is not None:
        logger.info(
            "Using neighbor lists + sklearn dbscan_inner (avoids full n×n distance matrix)."
        )
        return _st_dbscan_via_inner(lat, lon, t_sec)
    try:
        d = _pairwise_st_distances(lat, lon, t_sec)
        return DBSCAN(
            eps=1.0,
            min_samples=MINPTS,
            metric="precomputed",
            n_jobs=-1,
        ).fit_predict(d)
    except MemoryError:
        if dbscan_inner is None:
            logger.warning(
                "Out of memory building the distance matrix and dbscan_inner is unavailable."
            )
            raise
        logger.warning(
            "Distance matrix exceeded memory; falling back to neighbor lists + dbscan_inner "
            "(same result, slower)."
        )
        return _st_dbscan_via_inner(lat, lon, t_sec)


def main() -> None:
    ensure_cli_logging()
    logger.info("Connecting to DB...")
    conn = psycopg2.connect(**DB_CONFIG)

    logger.info("Loading high_congestion_zones...")
    df = pd.read_sql(LOAD_SQL, conn)
    n_rows = len(df)
    if n_rows == 0:
        logger.info("No rows in high_congestion_zones; nothing to cluster.")
        conn.close()
        return

    logger.info("Preprocessing record_time to Unix seconds...")
    df["record_time"] = pd.to_datetime(df["record_time"])
    ts = df["record_time"]
    t_sec = (ts.astype("int64") // PANDAS_TIMESTAMP_NS_PER_S).to_numpy(dtype=np.float64)

    logger.info(
        f"Running ST-DBSCAN (eps_spatial={EPS1_DEG} deg, "
        f"eps_temporal={EPS2_SEC:.0f} s, MinPts={MINPTS})..."
    )
    labels = run_st_dbscan(
        df["lat"].to_numpy(dtype=np.float64),
        df["lon"].to_numpy(dtype=np.float64),
        t_sec.astype(np.float64),
    )
    df["cluster_id"] = labels

    logger.info("Writing table traffic_clusters...")
    cur = conn.cursor()
    try:
        cur.execute(DROP_TABLE_SQL)
        cur.execute(CREATE_TABLE_SQL)
        out = df[["record_time", "lat", "lon", "vehicle_count", "cluster_id"]]
        rows = [
            (
                ts,
                float(lat),
                float(lon),
                int(vc),
                int(cid),
            )
            for ts, lat, lon, vc, cid in out.itertuples(index=False, name=None)
        ]
        execute_values(
            cur,
            """
            INSERT INTO traffic_clusters (
                record_time, lat, lon, vehicle_count, cluster_id
            ) VALUES %s
            """,
            rows,
            page_size=DB_INSERT_PAGE_SIZE,
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

    n_noise = int(np.sum(labels == -1))
    n_clusters = len(np.unique(labels[labels >= 0])) if np.any(labels >= 0) else 0
    noise_pct = 100.0 * n_noise / n_rows if n_rows else 0.0

    logger.info("Success!")
    logger.info(f"  Total clusters found (excluding noise): {n_clusters}")
    logger.info(f"  Noise (-1) share: {noise_pct:.2f}% ({n_noise} / {n_rows} rows)")


if __name__ == "__main__":
    main()
