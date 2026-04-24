"""
ST-DBSCAN — Optimised spatiotemporal clustering using BallTree spatial indexing.

Complexity
----------
Old:   O(n²) pairwise distance matrix → 11.56 TB for 1.7M rows.
New:   O(n log n) BallTree spatial query  +  O(n · k_avg) temporal filter.
       Memory is O(n · k_avg) instead of O(n²).

Algorithm
---------
1. Build a Haversine BallTree over (lat, lon) in radians.
2. For each point, query spatial neighbors within ε₁ (500 m).
3. Filter spatial neighbors by temporal proximity (|Δt| ≤ ε₂ = 1 hour).
4. Determine core points (|neighborhood| ≥ MinPts).
5. Propagate cluster labels via sklearn's dbscan_inner (BFS on core graph).

References
----------
- Birant, D. & Kut, A. (2007). ST-DBSCAN: An algorithm for clustering
  spatial–temporal data. Data & Knowledge Engineering.
"""

from __future__ import annotations

import logging
import time

import numpy as np
from sklearn.neighbors import BallTree

try:
    from sklearn.cluster._dbscan_inner import dbscan_inner
except ImportError:
    dbscan_inner = None

# ─── Import shared parameters from config ──────────────────────────────────
from config import EPS1_DEG, EPS2_SEC, MAX_CLUSTER_INPUT, MINPTS, ensure_cli_logging

logger = logging.getLogger(__name__)

# ─── Constants ─────────────────────────────────────────────────────────────
_DEG2RAD = np.pi / 180.0
_EARTH_RADIUS_M = 6_371_000.0  # mean Earth radius in metres
_EPS1_RAD = (EPS1_DEG * _DEG2RAD)  # ε₁ in radians for Haversine BallTree


def run_st_dbscan(
    lat: np.ndarray,
    lon: np.ndarray,
    t_sec: np.ndarray,
    *,
    eps1_deg: float = EPS1_DEG,
    eps2_sec: float = EPS2_SEC,
    min_pts: int = MINPTS,
    verbose: bool = True,
) -> np.ndarray:
    """
    Run ST-DBSCAN on coordinate + timestamp arrays.

    Parameters
    ----------
    lat, lon : array-like, shape (n,)
        Latitude and longitude in **degrees**.
    t_sec : array-like, shape (n,)
        Timestamps as Unix epoch seconds (float64).
    eps1_deg : float
        Spatial epsilon in degrees (~0.005° ≈ 500 m at 41°N).
    eps2_sec : float
        Temporal epsilon in seconds (3600 = 1 hour).
    min_pts : int
        Minimum neighbourhood size to be a core point.
    verbose : bool
        Emit log lines for BallTree build, neighbourhood stats, and label propagation.

    Returns
    -------
    labels : np.ndarray, shape (n,), dtype int64
        Cluster labels.  -1 = noise.
    """
    lat = np.asarray(lat, dtype=np.float64)
    lon = np.asarray(lon, dtype=np.float64)
    t_sec = np.asarray(t_sec, dtype=np.float64)
    n = lat.shape[0]

    if n == 0:
        return np.array([], dtype=np.int64)

    if verbose:
        ensure_cli_logging()

    if n > MAX_CLUSTER_INPUT:
        raise MemoryError(
            f"Input too large ({n:,} rows > MAX_CLUSTER_INPUT={MAX_CLUSTER_INPUT:,}). "
            f"Use geohash partitioning (clustering.partitioner) or pre-filter the data."
        )

    if dbscan_inner is None:
        raise RuntimeError(
            "sklearn.cluster._dbscan_inner is not available. "
            "Install scikit-learn >= 1.0."
        )

    eps1_rad = eps1_deg * _DEG2RAD

    # ── Step 1: Spatial index ──────────────────────────────────────────────
    t0 = time.perf_counter()
    coords_rad = np.column_stack([
        np.deg2rad(lat),
        np.deg2rad(lon),
    ])
    tree = BallTree(coords_rad, metric="haversine")
    if verbose:
        logger.info(f"  BallTree built in {time.perf_counter() - t0:.2f}s  (n={n:,})")

    # ── Step 2: Query spatial neighbours + temporal filter ─────────────────
    t1 = time.perf_counter()
    spatial_neighbors = tree.query_radius(coords_rad, r=eps1_rad)

    neighborhoods = np.empty(n, dtype=object)
    for i in range(n):
        s_neigh = spatial_neighbors[i]
        t_mask = np.abs(t_sec[s_neigh] - t_sec[i]) <= eps2_sec
        neighborhoods[i] = s_neigh[t_mask].astype(np.intp, copy=False)

    if verbose:
        avg_k = np.mean([len(neighborhoods[i]) for i in range(n)])
        logger.info(
            f"  Neighbourhoods computed in {time.perf_counter() - t1:.2f}s  "
            f"(avg neighbours/point: {avg_k:.1f})"
        )

    # ── Step 3: Core determination + label propagation ─────────────────────
    t2 = time.perf_counter()
    is_core = np.array(
        [len(neighborhoods[i]) >= min_pts for i in range(n)],
        dtype=np.uint8,
    )
    labels = np.full(n, -1, dtype=np.intp)
    dbscan_inner(is_core, neighborhoods, labels)

    if verbose:
        n_core = int(is_core.sum())
        n_noise = int(np.sum(labels == -1))
        n_clusters = len(set(labels[labels >= 0]))
        logger.info(
            f"  Label propagation in {time.perf_counter() - t2:.2f}s  "
            f"(cores={n_core:,}, clusters={n_clusters}, noise={n_noise:,})"
        )

    return labels.astype(np.int64)
