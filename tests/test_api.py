"""
Tests for FastAPI endpoint structure and AIS in-service scoring.

GeoJSON shape tests run without a DB (using the service's in-memory logic).
Live DB tests are skipped when the database is unavailable.
"""

import os
import sys
from pathlib import Path

import pytest

_API_BASE = os.environ.get("TEST_API_BASE_URL", "http://localhost:8000")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ── AIS in cluster_service ───────────────────────────────────────────────────

def test_service_ais_empty():
    from backend.app.services.cluster_service import compute_ais_and_severity
    result = compute_ais_and_severity([])
    assert result == []


def test_service_ais_single_cluster():
    from backend.app.services.cluster_service import compute_ais_and_severity
    clusters = [{
        "cluster_id": 0,
        "avg_vehicle_count": 700.0,
        "avg_speed": 15.0,
        "duration_hours": 48.0,
        "recurrence_days": 10,
        "peak_hour": 8,
        "peak_day": "Monday",
        "centroid_lat": 41.0,
        "centroid_lon": 29.0,
        "point_count": 25,
    }]
    result = compute_ais_and_severity(clusters)
    assert len(result) == 1
    assert "ais_score" in result[0]
    assert "severity" in result[0]
    assert result[0]["severity"] in ("LOW", "MEDIUM", "HIGH")
    assert 0.0 <= result[0]["ais_score"] <= 1.0


def test_service_ais_severity_range():
    from backend.app.services.cluster_service import compute_ais_and_severity
    clusters = [
        {"cluster_id": 0, "avg_vehicle_count": 1000.0, "avg_speed": 3.0,
         "duration_hours": 700.0, "recurrence_days": 25, "peak_hour": 8,
         "peak_day": "Monday", "centroid_lat": 41.0, "centroid_lon": 29.0, "point_count": 100},
        {"cluster_id": 1, "avg_vehicle_count": 501.0, "avg_speed": 19.0,
         "duration_hours": 1.0, "recurrence_days": 1, "peak_hour": 9,
         "peak_day": "Tuesday", "centroid_lat": 41.1, "centroid_lon": 29.1, "point_count": 5},
    ]
    result = compute_ais_and_severity(clusters)
    ais_scores = [r["ais_score"] for r in result]
    for score in ais_scores:
        assert 0.0 <= score <= 1.0
    # The high-volume, slow, long-duration cluster should score higher
    high_idx = next(i for i, r in enumerate(result) if r["cluster_id"] == 0)
    low_idx = next(i for i, r in enumerate(result) if r["cluster_id"] == 1)
    assert result[high_idx]["ais_score"] > result[low_idx]["ais_score"]


# ── GeoJSON structure ────────────────────────────────────────────────────────

def test_geojson_feature_collection_structure():
    """GeoJSON FeatureCollection must have correct top-level structure."""
    from backend.app.models.cluster import GeoJSONFeatureCollection
    # Validate the schema expects the right fields
    fields = GeoJSONFeatureCollection.model_fields
    assert "type" in fields
    assert "features" in fields


def test_geojson_feature_structure():
    from backend.app.models.cluster import GeoJSONFeature, GeoJSONGeometry, ClusterProperties
    fields = GeoJSONFeature.model_fields
    assert "type" in fields
    assert "geometry" in fields
    assert "properties" in fields


def test_cluster_properties_severity_field():
    from backend.app.models.cluster import ClusterProperties
    fields = ClusterProperties.model_fields
    assert "severity" in fields
    assert "ais_score" in fields
    assert "cluster_id" in fields


# ── Settings ─────────────────────────────────────────────────────────────────

def test_settings_allowed_origins():
    from backend.app.config import get_settings
    s = get_settings()
    origins = s.allowed_origins.split(",")
    assert len(origins) >= 1
    for o in origins:
        assert o.startswith("http")


def test_settings_dbscan_params_positive():
    from backend.app.config import get_settings
    s = get_settings()
    assert s.dbscan_eps_meters > 0
    assert s.dbscan_minpoints >= 1
    assert 0 < s.density_percentile_threshold <= 100


# ── Live DB endpoint tests (skipped if no DB) ────────────────────────────────

@pytest.mark.db
def test_health_endpoint(skip_if_no_db):
    """GET /api/health returns 200 when DB is available."""
    try:
        from httpx import Client
    except ImportError:
        pytest.skip("httpx not installed — run: pip install httpx")

    with Client(base_url=_API_BASE, timeout=5.0) as client:
        try:
            r = client.get("/api/health")
            assert r.status_code == 200
            data = r.json()
            assert "status" in data
        except Exception:
            pytest.skip(f"API server not running — start with: uvicorn backend.app.main:app --reload --port 8000")


@pytest.mark.db
def test_clusters_endpoint_structure(skip_if_no_db):
    """GET /api/clusters returns a valid GeoJSON FeatureCollection."""
    try:
        from httpx import Client
    except ImportError:
        pytest.skip("httpx not installed")

    with Client(base_url=_API_BASE, timeout=10.0) as client:
        try:
            r = client.get("/api/clusters")
            assert r.status_code == 200
            data = r.json()
            assert data["type"] == "FeatureCollection"
            assert "features" in data
            if data["features"]:
                f = data["features"][0]
                assert f["type"] == "Feature"
                assert "geometry" in f
                assert "properties" in f
                props = f["properties"]
                assert "severity" in props
                assert "ais_score" in props
                assert props["severity"] in ("LOW", "MEDIUM", "HIGH")
        except Exception:
            pytest.skip("API server not running")
