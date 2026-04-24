"""
Geohash-based spatial partitioning for ST-DBSCAN at scale.

Problem
-------
At 63 months of data (~100M+ records), even BallTree-based ST-DBSCAN cannot
process the full dataset in one pass.  We partition the data by **geohash prefix**
so that each partition fits within MAX_CLUSTER_INPUT and can be clustered
independently.

Strategy
--------
1. Group points by their N-char geohash prefix (default N=4, giving ~1.2 km × 0.6 km cells).
2. For each partition, run ST-DBSCAN independently.
3. Merge clusters that span partition borders:
   - Points within ε₁ of a border are members of a **halo** region.
   - After independent clustering, compare cluster labels in overlapping halos.
   - If two clusters (from different partitions) share ≥ MinPts points in the halo,
     merge them under a single global label.

Notes
-----
This is a well-known technique (sometimes called "distributed DBSCAN") adapted
for geohash grids.  For the January 2025 slice (~50K high-congestion rows), this
is NOT needed — BallTree handles it in seconds.  This module is built for when
we scale to the full 63-month dataset.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from clustering.st_dbscan import run_st_dbscan
from config import MINPTS, ensure_cli_logging

logger = logging.getLogger(__name__)


def partition_by_geohash(
    df: pd.DataFrame,
    prefix_len: int = 4,
    geohash_col: str = "geohash",
) -> dict[str, pd.DataFrame]:
    """
    Split a DataFrame into partitions based on geohash prefix.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain columns: geohash, lat, lon, record_time, vehicle_count, avg_speed
    prefix_len : int
        Number of geohash characters to use for partitioning.
        4 chars → ~40 km × 20 km cells  (coarse, fewer partitions)
        5 chars → ~5 km  × 5 km  cells  (fine, more partitions)
    geohash_col : str
        Column name containing the geohash string.

    Returns
    -------
    dict mapping geohash prefix → DataFrame subset.
    """
    df = df.copy()
    df["_gh_prefix"] = df[geohash_col].str[:prefix_len]
    partitions = {
        prefix: group.drop(columns=["_gh_prefix"]).reset_index(drop=True)
        for prefix, group in df.groupby("_gh_prefix")
    }
    return partitions


def run_partitioned_st_dbscan(
    df: pd.DataFrame,
    prefix_len: int = 4,
    geohash_col: str = "geohash",
    verbose: bool = True,
) -> np.ndarray:
    """
    Run ST-DBSCAN on each geohash partition and merge results.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain: geohash, lat, lon, record_time, vehicle_count, avg_speed.
        Index must be the original row positions (0..n-1).
    prefix_len : int
        Geohash prefix length for partitioning.
    verbose : bool
        Log per-partition progress and a short summary.

    Returns
    -------
    labels : np.ndarray, shape (len(df),), dtype int64
        Global cluster labels. -1 = noise. Local cluster ids from each partition
        are renumbered to distinct global ids (see module docstring for intended
        border-merge behaviour; this function performs partitioning and relabelling
        only).
    """
    n = len(df)
    if verbose:
        ensure_cli_logging()

    global_labels = np.full(n, -1, dtype=np.int64)
    next_global_id = 0

    # Store original index for label assignment
    df = df.copy()
    df["_orig_idx"] = np.arange(n)

    partitions = partition_by_geohash(df, prefix_len=prefix_len, geohash_col=geohash_col)

    if verbose:
        logger.info(
            f"Partitioned {n:,} rows into {len(partitions)} geohash partitions "
            f"(prefix_len={prefix_len})"
        )

    for i, (prefix, part_df) in enumerate(sorted(partitions.items())):
        part_n = len(part_df)
        if verbose:
            logger.info(f"\n  [{i+1}/{len(partitions)}] Partition '{prefix}': {part_n:,} rows")

        if part_n < MINPTS:
            if verbose:
                logger.info(f"    Skipped (fewer than MinPts={MINPTS} rows)")
            continue

        # Extract arrays
        lat = part_df["lat"].to_numpy(dtype=np.float64)
        lon = part_df["lon"].to_numpy(dtype=np.float64)
        ts = pd.to_datetime(part_df["record_time"])
        t_sec = (ts.astype("int64") // 10**9).to_numpy(dtype=np.float64)

        # Run clustering on this partition
        local_labels = run_st_dbscan(lat, lon, t_sec, verbose=verbose)

        # Map local labels to global labels
        orig_indices = part_df["_orig_idx"].to_numpy()
        local_cluster_ids = set(local_labels[local_labels >= 0])

        local_to_global = {}
        for local_id in sorted(local_cluster_ids):
            local_to_global[local_id] = next_global_id
            next_global_id += 1

        for j in range(part_n):
            if local_labels[j] >= 0:
                global_labels[orig_indices[j]] = local_to_global[local_labels[j]]
            # else: remains -1 (noise)

    if verbose:
        n_noise = int(np.sum(global_labels == -1))
        n_clusters = len(set(global_labels[global_labels >= 0]))
        logger.info(f"\n{'='*60}")
        logger.info("Partitioned ST-DBSCAN complete:")
        logger.info(f"  Total clusters: {n_clusters}")
        logger.info(f"  Noise: {n_noise:,} / {n:,} ({100*n_noise/n:.2f}%)")

    return global_labels
