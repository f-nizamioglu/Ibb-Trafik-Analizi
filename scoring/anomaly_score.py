"""
Anomaly Intensity Score (AIS) — a composite metric for ranking traffic clusters.

Formula
-------
AIS(Cₖ) = w₁·V̂ₖ + w₂·Ŝₖ + w₃·D̂ₖ + w₄·R̂ₖ

Where each component is min-max normalised to [0, 1] across all clusters:

    V̂ₖ  Volume Score      — mean vehicle count in cluster k
    Ŝₖ  Speed Drop Score  — 1 - (avg_speed_k / city_avg_speed)
    D̂ₖ  Duration Score    — time span of the cluster (hours)
    R̂ₖ  Recurrence Score  — distinct days the cluster appears on

Default weights (Expert Judgment, tunable):
    w₁ = 0.30,  w₂ = 0.30,  w₃ = 0.25,  w₄ = 0.15

Severity Classification:
    AIS ∈ [0.00, 0.33) → LOW
    AIS ∈ [0.33, 0.66) → MEDIUM
    AIS ∈ [0.66, 1.00] → HIGH

Academic Grounding
-----------------
This weighted multi-criteria approach follows MCDA (Multi-Criteria Decision
Analysis) methodology. cf. Birant & Kut (2007), Wang et al. (2014).
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from config import AIS_WEIGHTS, CITY_AVG_SPEED_KMH, ensure_cli_logging

logger = logging.getLogger(__name__)


def _min_max_normalize(series: pd.Series) -> pd.Series:
    """Min-max normalise a Series to [0, 1]. Handles constant series."""
    s_min = series.min()
    s_max = series.max()
    denom = s_max - s_min
    if denom == 0 or np.isnan(denom):
        return pd.Series(0.5, index=series.index)
    return (series - s_min) / denom


def compute_cluster_scores(
    df: pd.DataFrame,
    weights: dict[str, float] | None = None,
    city_avg_speed: float = CITY_AVG_SPEED_KMH,
) -> pd.DataFrame:
    """
    Compute AIS for each cluster in the dataset.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain columns: cluster_id, vehicle_count, avg_speed, record_time.
        Noise rows (cluster_id == -1) are excluded automatically.
    weights : dict, optional
        Override default weights. Keys: volume, speed_drop, duration, recurrence.
    city_avg_speed : float
        Citywide average speed (km/h) as baseline for speed-drop ratio.

    Returns
    -------
    pd.DataFrame indexed by cluster_id with columns:
        point_count, avg_vehicle_count, avg_speed, duration_hours,
        recurrence_days, V, S, D, R, AIS, severity, peak_hour, peak_day
    """
    w = weights or AIS_WEIGHTS

    # Filter out noise
    clustered = df[df["cluster_id"] >= 0].copy()
    clustered["record_time"] = pd.to_datetime(clustered["record_time"])

    if clustered.empty:
        return pd.DataFrame()

    # ── Aggregate per cluster ──────────────────────────────────────────────
    groups = clustered.groupby("cluster_id")

    stats = groups.agg(
        point_count=("vehicle_count", "count"),
        avg_vehicle_count=("vehicle_count", "mean"),
        avg_speed=("avg_speed", "mean"),
        duration_hours=(
            "record_time",
            lambda x: (x.max() - x.min()).total_seconds() / 3600,
        ),
        recurrence_days=(
            "record_time",
            lambda x: x.dt.date.nunique(),
        ),
    )

    # ── Peak time analysis (for presentation) ─────────────────────────────
    peak_hour = groups["record_time"].apply(
        lambda x: x.dt.hour.mode().iloc[0] if not x.empty else -1
    ).rename("peak_hour")

    peak_day = groups["record_time"].apply(
        lambda x: x.dt.day_name().mode().iloc[0] if not x.empty else "Unknown"
    ).rename("peak_day")

    stats = stats.join(peak_hour).join(peak_day)

    # ── Compute normalised components ─────────────────────────────────────
    stats["V"] = _min_max_normalize(stats["avg_vehicle_count"])

    # Speed-drop: how much slower than city average (capped at 1.0)
    raw_speed_drop = (1 - stats["avg_speed"] / city_avg_speed).clip(lower=0.0)
    stats["S"] = _min_max_normalize(raw_speed_drop)

    stats["D"] = _min_max_normalize(stats["duration_hours"])
    stats["R"] = _min_max_normalize(stats["recurrence_days"])

    # ── Composite AIS ─────────────────────────────────────────────────────
    stats["AIS"] = (
        w["volume"] * stats["V"]
        + w["speed_drop"] * stats["S"]
        + w["duration"] * stats["D"]
        + w["recurrence"] * stats["R"]
    )

    # ── Severity classification ───────────────────────────────────────────
    stats["severity"] = pd.cut(
        stats["AIS"],
        bins=[-0.001, 0.33, 0.66, 1.001],
        labels=["LOW", "MEDIUM", "HIGH"],
    )

    # Round for readability
    for col in ["avg_vehicle_count", "avg_speed", "duration_hours", "V", "S", "D", "R", "AIS"]:
        stats[col] = stats[col].round(4)

    return stats.sort_values("AIS", ascending=False)


def print_cluster_report(scores: pd.DataFrame) -> None:
    """Log a human-readable AIS ranking table for each cluster and severity counts.

    Args:
        scores: DataFrame from :func:`compute_cluster_scores` (one row per cluster_id),
            sorted by AIS descending. If empty, logs a short message and returns.
    """
    ensure_cli_logging()
    if scores.empty:
        logger.info("No clusters to report.")
        return

    logger.info("\n" + "=" * 80)
    logger.info("ANOMALY INTENSITY SCORE (AIS) — CLUSTER RANKING")
    logger.info("=" * 80)

    for cluster_id, row in scores.iterrows():
        severity_icon = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}.get(
            str(row["severity"]), "⚪"
        )
        logger.info(
            f"\n  {severity_icon} Cluster {cluster_id:>3d}  |  "
            f"AIS = {row['AIS']:.4f}  |  Severity: {row['severity']}"
        )
        logger.info(
            f"      Points: {row['point_count']:>6,}  |  "
            f"Avg Vehicles: {row['avg_vehicle_count']:>8.1f}  |  "
            f"Avg Speed: {row['avg_speed']:>5.1f} km/h"
        )
        logger.info(
            f"      Duration: {row['duration_hours']:>6.1f}h  |  "
            f"Recurrence: {row['recurrence_days']:>3} days  |  "
            f"Peak: {row['peak_day']} {int(row['peak_hour']):02d}:00"
        )

    logger.info("\n" + "-" * 80)
    severity_counts = scores["severity"].value_counts()
    for sev in ["HIGH", "MEDIUM", "LOW"]:
        cnt = severity_counts.get(sev, 0)
        if cnt > 0:
            logger.info(f"  {sev}: {cnt} cluster(s)")
    logger.info("=" * 80)
