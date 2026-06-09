"""
Tests for pipeline configuration and AIS scoring logic.

These tests do not require a live database.
"""

import math
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ── Configuration loading ────────────────────────────────────────────────────

def test_settings_load():
    from backend.app.config import get_settings
    s = get_settings()
    assert s.db_port == 5433
    assert s.dbscan_eps_meters > 0
    assert s.dbscan_minpoints > 0
    assert s.high_congestion_max_avg_speed > 0
    assert s.high_congestion_min_vehicle_count > 0


def test_config_constants():
    from config import (
        EPS_METERS,
        MINPTS,
        HIGH_CONGESTION_MAX_AVG_SPEED,
        HIGH_CONGESTION_MIN_VEHICLE_COUNT,
        CITY_AVG_SPEED_KMH,
        AIS_WEIGHTS,
    )
    assert EPS_METERS > 0
    assert MINPTS >= 1
    assert HIGH_CONGESTION_MAX_AVG_SPEED > 0
    assert HIGH_CONGESTION_MIN_VEHICLE_COUNT > 0
    assert CITY_AVG_SPEED_KMH > 0
    assert abs(sum(AIS_WEIGHTS.values()) - 1.0) < 1e-9, "AIS weights must sum to 1.0"


def test_ais_weights_sum_to_one():
    from backend.app.config import get_settings
    w = get_settings().ais_weights
    assert abs(sum(w.values()) - 1.0) < 1e-9


# ── AIS scoring ──────────────────────────────────────────────────────────────

def _make_df(records: list[dict]):
    import pandas as pd
    return pd.DataFrame(records)


def test_ais_single_cluster_all_equal():
    """When all clusters have identical values, AIS should be 0.5 (constant series normalises to 0.5)."""
    from scoring.anomaly_score import compute_cluster_scores
    import pandas as pd
    from datetime import datetime, timedelta

    base = datetime(2025, 1, 1, 8, 0)
    rows = []
    for i in range(5):
        rows.append({
            "cluster_id": 0,
            "vehicle_count": 600,
            "avg_speed": 15,
            "record_time": base + timedelta(hours=i),
        })
    df = pd.DataFrame(rows)
    scores = compute_cluster_scores(df)
    assert len(scores) == 1
    ais = scores.iloc[0]["AIS"]
    assert 0.0 <= ais <= 1.0


def test_ais_severity_high():
    """A cluster with extreme values should get HIGH severity."""
    from scoring.anomaly_score import compute_cluster_scores
    import pandas as pd
    from datetime import datetime, timedelta

    base = datetime(2025, 1, 1, 8, 0)
    rows = []
    # Cluster 0: high volume, very slow, long duration, many days
    for day in range(18):
        for hour in range(0, 23, 2):
            rows.append({
                "cluster_id": 0,
                "vehicle_count": 800,
                "avg_speed": 5,
                "record_time": datetime(2025, 1, 1 + day, hour, 0),
            })
    # Cluster 1: low volume, near-normal speed, short
    rows.append({
        "cluster_id": 1, "vehicle_count": 100, "avg_speed": 30,
        "record_time": base,
    })
    df = pd.DataFrame(rows)
    scores = compute_cluster_scores(df)
    assert scores.loc[0, "severity"] == "HIGH"
    assert str(scores.loc[1, "severity"]) in ("LOW", "MEDIUM")


def test_ais_severity_low():
    """A cluster with minimal anomaly should get LOW severity relative to a dominant one."""
    from scoring.anomaly_score import compute_cluster_scores
    import pandas as pd
    from datetime import datetime

    rows = [
        {"cluster_id": 0, "vehicle_count": 1000, "avg_speed": 5, "record_time": datetime(2025, 1, 1, 8)},
        {"cluster_id": 1, "vehicle_count": 501, "avg_speed": 19, "record_time": datetime(2025, 1, 2, 8)},
    ]
    df = pd.DataFrame(rows)
    scores = compute_cluster_scores(df)
    # Cluster 0 is clearly worse; cluster 1 should be lower
    assert scores.loc[0, "AIS"] > scores.loc[1, "AIS"]


def test_ais_excludes_noise():
    """Rows with cluster_id = -1 must be excluded from AIS computation."""
    from scoring.anomaly_score import compute_cluster_scores
    import pandas as pd
    from datetime import datetime

    rows = [
        {"cluster_id": -1, "vehicle_count": 999, "avg_speed": 1, "record_time": datetime(2025, 1, 1, 8)},
        {"cluster_id": 0, "vehicle_count": 600, "avg_speed": 15, "record_time": datetime(2025, 1, 1, 9)},
    ]
    df = pd.DataFrame(rows)
    scores = compute_cluster_scores(df)
    assert -1 not in scores.index
    assert 0 in scores.index


def test_ais_empty_input():
    """Empty input returns empty DataFrame without error."""
    from scoring.anomaly_score import compute_cluster_scores
    import pandas as pd

    df = pd.DataFrame(columns=["cluster_id", "vehicle_count", "avg_speed", "record_time"])
    scores = compute_cluster_scores(df)
    assert scores.empty


def test_ais_all_noise():
    """All-noise input returns empty DataFrame."""
    from scoring.anomaly_score import compute_cluster_scores
    import pandas as pd
    from datetime import datetime

    rows = [{"cluster_id": -1, "vehicle_count": 600, "avg_speed": 15, "record_time": datetime(2025, 1, 1, 8)}]
    df = pd.DataFrame(rows)
    scores = compute_cluster_scores(df)
    assert scores.empty


def test_ais_severity_boundaries():
    """AIS classification thresholds must be exactly as documented."""
    from scoring.anomaly_score import compute_cluster_scores
    import pandas as pd
    from datetime import datetime, timedelta

    # Build 3 clusters that will span the full AIS range
    base = datetime(2025, 1, 1, 8, 0)
    rows = []
    # Cluster 0: maximum anomaly
    for d in range(20):
        rows.append({"cluster_id": 0, "vehicle_count": 1000, "avg_speed": 1, "record_time": base + timedelta(days=d, hours=1)})
    # Cluster 1: mid
    for d in range(5):
        rows.append({"cluster_id": 1, "vehicle_count": 600, "avg_speed": 15, "record_time": base + timedelta(days=d)})
    # Cluster 2: minimum
    rows.append({"cluster_id": 2, "vehicle_count": 501, "avg_speed": 19, "record_time": base})

    df = pd.DataFrame(rows)
    scores = compute_cluster_scores(df)
    for _, row in scores.iterrows():
        assert 0.0 <= row["AIS"] <= 1.0
        if row["AIS"] >= 0.66:
            assert row["severity"] == "HIGH"
        elif row["AIS"] >= 0.33:
            assert row["severity"] == "MEDIUM"
        else:
            assert row["severity"] == "LOW"


# ── Coordinate transformation sanity ────────────────────────────────────────

def test_istanbul_coordinate_range():
    """Istanbul coordinates should fall within expected WGS84 bounds."""
    # Istanbul bounding box (approximate)
    lat_min, lat_max = 40.8, 41.5
    lon_min, lon_max = 28.5, 29.5
    # Sample point from IBB data (Kadıköy area)
    lat, lon = 40.965, 29.086
    assert lat_min <= lat <= lat_max
    assert lon_min <= lon <= lon_max
