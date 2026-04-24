"""
Internal evaluation metrics for ST-DBSCAN clustering quality.

Provides three metric families plus a sensitivity analysis runner to generate
the parameter configuration table for the thesis defense.

Metrics
-------
1. Silhouette Score  — mean (b(i) - a(i)) / max(a(i), b(i))
2. Davies-Bouldin Index — lower = better cluster separation
3. DBCV (Density-Based Cluster Validation) — designed for density-based clusters

All metrics use a **normalized spatiotemporal distance** so that spatial and
temporal dimensions are weighted equally:
    d(i,j) = sqrt( (Δlat/ε₁)² + (Δlon/ε₁)² + (Δt/ε₂)² )

Important: Noise points (label = -1) are EXCLUDED before metric computation,
following standard practice for density-based clustering evaluation.

References
----------
- Moulavi et al. (2014). Density-Based Clustering Validation (DBCV).
- Rousseeuw (1987). Silhouettes: a graphical aid for cluster analysis.
- Davies & Bouldin (1979). A cluster separation measure.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd
from sklearn.metrics import davies_bouldin_score, silhouette_score

from clustering.st_dbscan import run_st_dbscan
from config import EPS1_DEG, EPS2_SEC, ensure_cli_logging

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Container for a single parameter configuration's metrics."""
    eps1_deg: float
    eps2_sec: float
    min_pts: int
    n_clusters: int
    n_noise: int
    n_total: int
    noise_pct: float
    silhouette: Optional[float] = None
    davies_bouldin: Optional[float] = None
    dbcv: Optional[float] = None
    elapsed_sec: float = 0.0


def _build_st_feature_matrix(
    lat: np.ndarray,
    lon: np.ndarray,
    t_sec: np.ndarray,
    eps1: float = EPS1_DEG,
    eps2: float = EPS2_SEC,
) -> np.ndarray:
    """
    Build normalised spatiotemporal feature matrix for metric computation.

    Each dimension is scaled by its respective epsilon so that spatial and
    temporal distances contribute equally.
    """
    return np.column_stack([
        lat / eps1,
        lon / eps1,
        t_sec / eps2,
    ])


def _compute_dbcv(X: np.ndarray, labels: np.ndarray) -> float:
    """
    Compute DBCV (Density-Based Cluster Validation) score.

    This is a simplified implementation using mutual reachability distance.
    For production, consider using the `hdbscan` library's validity_index.
    """
    try:
        from hdbscan.validity import validity_index
        return float(validity_index(X, labels, metric="euclidean"))
    except ImportError:
        # Fallback: skip DBCV if hdbscan is not installed
        return float("nan")


def evaluate_clustering(
    lat: np.ndarray,
    lon: np.ndarray,
    t_sec: np.ndarray,
    labels: np.ndarray,
    eps1: float = EPS1_DEG,
    eps2: float = EPS2_SEC,
    silhouette_sample_size: int = 5_000,
) -> dict[str, float]:
    """
    Compute all evaluation metrics for a given set of cluster labels.

    Parameters
    ----------
    lat, lon, t_sec : array-like, shape (n,)
        Original data coordinates and timestamps.
    labels : array-like, shape (n,)
        Cluster labels from ST-DBSCAN. -1 = noise.
    eps1, eps2 : float
        Epsilon parameters used for normalization.
    silhouette_sample_size : int
        Subsample size for Silhouette Score (full computation is O(n²)).

    Returns
    -------
    dict with keys: n_clusters, n_noise, noise_pct, silhouette, davies_bouldin, dbcv
    """
    labels = np.asarray(labels)
    mask = labels >= 0
    n_total = len(labels)
    n_noise = int(np.sum(~mask))
    n_valid = int(np.sum(mask))

    unique_clusters = set(labels[mask])
    n_clusters = len(unique_clusters)

    result = {
        "n_clusters": n_clusters,
        "n_noise": n_noise,
        "noise_pct": 100.0 * n_noise / n_total if n_total else 0.0,
        "silhouette": float("nan"),
        "davies_bouldin": float("nan"),
        "dbcv": float("nan"),
    }

    # Need at least 2 clusters for meaningful metrics
    if n_clusters < 2 or n_valid < 10:
        return result

    # Build feature matrix (noise excluded)
    X = _build_st_feature_matrix(
        lat[mask], lon[mask], t_sec[mask],
        eps1=eps1, eps2=eps2,
    )
    labels_valid = labels[mask]

    # ── Silhouette Score ───────────────────────────────────────────────────
    try:
        sample_size = min(silhouette_sample_size, n_valid)
        result["silhouette"] = float(
            silhouette_score(
                X, labels_valid,
                metric="euclidean",
                sample_size=sample_size,
                random_state=42,
            )
        )
    except Exception:
        pass

    # ── Davies-Bouldin Index ───────────────────────────────────────────────
    try:
        result["davies_bouldin"] = float(davies_bouldin_score(X, labels_valid))
    except Exception:
        pass

    # ── DBCV ───────────────────────────────────────────────────────────────
    try:
        result["dbcv"] = _compute_dbcv(X, labels_valid)
    except Exception:
        pass

    return result


def run_sensitivity_analysis(
    lat: np.ndarray,
    lon: np.ndarray,
    t_sec: np.ndarray,
    configs: list[dict] | None = None,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Run ST-DBSCAN with multiple parameter configurations and collect metrics.

    Parameters
    ----------
    lat, lon, t_sec : array-like, shape (n,)
        Data arrays.
    configs : list of dicts, optional
        Each dict has keys: eps1_deg, eps2_sec, min_pts.
        If None, uses default configurations for sensitivity analysis.
    verbose : bool
        Log each configuration and a results table.

    Returns
    -------
    pd.DataFrame with one row per config, columns:
        eps1_deg, eps2_sec, min_pts, n_clusters, n_noise, noise_pct,
        silhouette, davies_bouldin, dbcv, elapsed_sec
    """
    if configs is None:
        configs = [
            # Baseline
            {"eps1_deg": 0.005, "eps2_sec": 3600.0, "min_pts": 3},
            # Tighter spatial
            {"eps1_deg": 0.004, "eps2_sec": 3600.0, "min_pts": 3},
            # Tighter temporal
            {"eps1_deg": 0.005, "eps2_sec": 1800.0, "min_pts": 3},
            # Higher MinPts
            {"eps1_deg": 0.005, "eps2_sec": 3600.0, "min_pts": 5},
            # Both tighter
            {"eps1_deg": 0.004, "eps2_sec": 1800.0, "min_pts": 5},
            # Looser spatial
            {"eps1_deg": 0.006, "eps2_sec": 3600.0, "min_pts": 3},
        ]

    results = []

    if verbose:
        ensure_cli_logging()

    for i, cfg in enumerate(configs):
        if verbose:
            logger.info(f"\n{'='*60}")
            logger.info(
                f"Config {i+1}/{len(configs)}: "
                f"ε₁={cfg['eps1_deg']}°, ε₂={cfg['eps2_sec']:.0f}s, "
                f"MinPts={cfg['min_pts']}"
            )

        t0 = time.perf_counter()
        labels = run_st_dbscan(
            lat, lon, t_sec,
            eps1_deg=cfg["eps1_deg"],
            eps2_sec=cfg["eps2_sec"],
            min_pts=cfg["min_pts"],
            verbose=verbose,
        )
        elapsed = time.perf_counter() - t0

        metrics = evaluate_clustering(
            lat, lon, t_sec, labels,
            eps1=cfg["eps1_deg"],
            eps2=cfg["eps2_sec"],
        )

        results.append(ValidationResult(
            eps1_deg=cfg["eps1_deg"],
            eps2_sec=cfg["eps2_sec"],
            min_pts=cfg["min_pts"],
            n_clusters=metrics["n_clusters"],
            n_noise=metrics["n_noise"],
            n_total=len(labels),
            noise_pct=metrics["noise_pct"],
            silhouette=metrics["silhouette"],
            davies_bouldin=metrics["davies_bouldin"],
            dbcv=metrics["dbcv"],
            elapsed_sec=elapsed,
        ))

        if verbose:
            logger.info(
                f"  → k={metrics['n_clusters']}, noise={metrics['noise_pct']:.1f}%, "
                f"Sil={metrics['silhouette']:.3f}, DBI={metrics['davies_bouldin']:.3f}, "
                f"time={elapsed:.1f}s"
            )

    # Build results DataFrame
    df = pd.DataFrame([
        {
            "ε₁ (deg)": r.eps1_deg,
            "ε₂ (sec)": r.eps2_sec,
            "MinPts": r.min_pts,
            "k (clusters)": r.n_clusters,
            "Noise %": round(r.noise_pct, 2),
            "Silhouette": round(r.silhouette, 4) if r.silhouette == r.silhouette else None,
            "DBI": round(r.davies_bouldin, 4) if r.davies_bouldin == r.davies_bouldin else None,
            "DBCV": round(r.dbcv, 4) if r.dbcv == r.dbcv else None,
            "Time (s)": round(r.elapsed_sec, 1),
        }
        for r in results
    ])

    if verbose:
        logger.info(f"\n{'='*60}")
        logger.info("Sensitivity Analysis Results:")
        logger.info("\n%s", df.to_string(index=False))

    return df
