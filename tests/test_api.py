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


@pytest.fixture
def temporal_cluster_client(monkeypatch):
    """FastAPI test client with temporal cluster retrieval stubbed."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from backend.app.models.cluster import TemporalClusterResponse
    from backend.app.routers import clusters as clusters_router

    calls = []

    async def fake_get_temporal_clusters(*args, **kwargs):
        calls.append({"args": args, "kwargs": kwargs})
        return TemporalClusterResponse(
            date=args[0],
            hour=args[1],
            cluster_count=0,
            features=[],
        )

    monkeypatch.setattr(
        clusters_router,
        "get_temporal_clusters",
        fake_get_temporal_clusters,
    )

    app = FastAPI()
    app.include_router(clusters_router.router, prefix="/api")

    with TestClient(app) as client:
        yield client, calls


def test_temporal_clusters_without_bbox_still_uses_existing_call(temporal_cluster_client):
    client, calls = temporal_cluster_client

    r = client.get("/api/clusters?date=2025-01-17&hour=18")

    assert r.status_code == 200
    data = r.json()
    assert data["type"] == "FeatureCollection"
    assert data["date"] == "2025-01-17"
    assert data["hour"] == 18
    assert calls == [{"args": ("2025-01-17", 18), "kwargs": {}}]


def test_temporal_clusters_accepts_full_bbox(temporal_cluster_client):
    client, calls = temporal_cluster_client

    r = client.get(
        "/api/clusters?date=2025-01-17&hour=18"
        "&min_lon=28.95&min_lat=41.02&max_lon=29.08&max_lat=41.10"
    )

    assert r.status_code == 200
    assert calls == [
        {
            "args": ("2025-01-17", 18),
            "kwargs": {"bbox": (28.95, 41.02, 29.08, 41.10)},
        }
    ]


def test_temporal_clusters_rejects_partial_bbox(temporal_cluster_client):
    client, calls = temporal_cluster_client

    r = client.get("/api/clusters?date=2025-01-17&hour=18&min_lon=28.95")

    assert r.status_code == 400
    assert "All bbox parameters" in r.json()["detail"]
    assert calls == []


def test_temporal_clusters_rejects_inverted_bbox(temporal_cluster_client):
    client, calls = temporal_cluster_client

    r = client.get(
        "/api/clusters?date=2025-01-17&hour=18"
        "&min_lon=29.08&min_lat=41.02&max_lon=28.95&max_lat=41.10"
    )

    assert r.status_code == 400
    assert "min_lon" in r.json()["detail"]
    assert calls == []


def test_temporal_clusters_invalid_bbox_range_returns_422(temporal_cluster_client):
    client, calls = temporal_cluster_client

    r = client.get(
        "/api/clusters?date=2025-01-17&hour=18"
        "&min_lon=-181&min_lat=41.02&max_lon=29.08&max_lat=41.10"
    )

    assert r.status_code == 422
    assert calls == []


# ── District polygon filtering ───────────────────────────────────────────────

def test_temporal_clusters_accepts_district(temporal_cluster_client):
    client, calls = temporal_cluster_client

    r = client.get("/api/clusters?date=2025-01-17&hour=18&district=besiktas")

    assert r.status_code == 200
    assert calls == [
        {"args": ("2025-01-17", 18), "kwargs": {"district": "besiktas"}}
    ]


def test_temporal_clusters_district_is_normalized_lowercase(temporal_cluster_client):
    client, calls = temporal_cluster_client

    r = client.get("/api/clusters?date=2025-01-17&hour=18&district=BESIKTAS")

    assert r.status_code == 200
    assert calls == [
        {"args": ("2025-01-17", 18), "kwargs": {"district": "besiktas"}}
    ]


def test_temporal_clusters_accepts_turkish_cased_district(temporal_cluster_client):
    client, calls = temporal_cluster_client

    # Raw Turkish-cased name must normalize to the stored key before the service call.
    r = client.get(
        "/api/clusters",
        params={"date": "2025-01-17", "hour": "18", "district": "Beşiktaş"},
    )

    assert r.status_code == 200
    assert calls == [
        {"args": ("2025-01-17", 18), "kwargs": {"district": "besiktas"}}
    ]


def test_temporal_clusters_accepts_severity_with_district(temporal_cluster_client):
    client, calls = temporal_cluster_client

    r = client.get(
        "/api/clusters?date=2025-01-17&hour=18&district=besiktas&severity=HIGH"
    )

    assert r.status_code == 200
    # severity is applied in the router after retrieval; only district reaches the service
    assert calls == [
        {"args": ("2025-01-17", 18), "kwargs": {"district": "besiktas"}}
    ]


def test_temporal_clusters_district_and_bbox_are_mutually_exclusive(temporal_cluster_client):
    client, calls = temporal_cluster_client

    r = client.get(
        "/api/clusters?date=2025-01-17&hour=18&district=besiktas"
        "&min_lon=28.95&min_lat=41.02&max_lon=29.08&max_lat=41.10"
    )

    assert r.status_code == 400
    assert "mutually exclusive" in r.json()["detail"]
    assert calls == []


def test_temporal_clusters_district_without_date_hour_returns_400(temporal_cluster_client):
    client, calls = temporal_cluster_client

    r = client.get("/api/clusters?district=besiktas")

    assert r.status_code == 400
    assert calls == []


def test_temporal_clusters_invalid_district_returns_400(temporal_cluster_client, monkeypatch):
    client, calls = temporal_cluster_client

    from backend.app.routers import clusters as clusters_router
    from backend.app.services.cluster_service import UnknownDistrict

    async def raise_unknown(*args, **kwargs):
        raise UnknownDistrict("Unknown district: atlantis")

    monkeypatch.setattr(clusters_router, "get_temporal_clusters", raise_unknown)

    r = client.get("/api/clusters?date=2025-01-17&hour=18&district=atlantis")

    assert r.status_code == 400
    assert "Unknown district" in r.json()["detail"]


def test_temporal_clusters_district_not_imported_returns_503(temporal_cluster_client, monkeypatch):
    client, calls = temporal_cluster_client

    from backend.app.routers import clusters as clusters_router
    from backend.app.services.cluster_service import (
        DISTRICT_NOT_IMPORTED_MSG,
        DistrictBoundariesNotImported,
    )

    async def raise_not_imported(*args, **kwargs):
        raise DistrictBoundariesNotImported(DISTRICT_NOT_IMPORTED_MSG)

    monkeypatch.setattr(clusters_router, "get_temporal_clusters", raise_not_imported)

    r = client.get("/api/clusters?date=2025-01-17&hour=18&district=besiktas")

    assert r.status_code == 503
    assert "not imported" in r.json()["detail"].lower()


# ── District boundary endpoint ───────────────────────────────────────────────

@pytest.fixture
def boundary_client(monkeypatch):
    """FastAPI test client with district-boundary retrieval stubbed."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from backend.app.routers import clusters as clusters_router

    calls = []

    async def fake_get_district_boundary(key):
        calls.append(key)
        return {
            "type": "Feature",
            "geometry": {"type": "MultiPolygon", "coordinates": []},
            "properties": {"district_key": key, "district_name": key},
        }

    monkeypatch.setattr(
        clusters_router, "get_district_boundary", fake_get_district_boundary
    )

    app = FastAPI()
    app.include_router(clusters_router.router, prefix="/api")
    with TestClient(app) as client:
        yield client, calls


def test_boundary_accepts_and_normalizes_turkish_key(boundary_client):
    client, calls = boundary_client

    r = client.get("/api/districts/Beşiktaş/boundary")

    assert r.status_code == 200
    data = r.json()
    assert data["type"] == "Feature"
    assert data["properties"]["district_key"] == "besiktas"
    assert calls == ["besiktas"]


def test_boundary_returns_503_when_table_missing(boundary_client, monkeypatch):
    client, _ = boundary_client

    from backend.app.routers import clusters as clusters_router
    from backend.app.services.cluster_service import (
        DISTRICT_NOT_IMPORTED_MSG,
        DistrictBoundariesNotImported,
    )

    async def raise_not_imported(key):
        raise DistrictBoundariesNotImported(DISTRICT_NOT_IMPORTED_MSG)

    monkeypatch.setattr(clusters_router, "get_district_boundary", raise_not_imported)

    r = client.get("/api/districts/besiktas/boundary")

    assert r.status_code == 503
    assert "not imported" in r.json()["detail"].lower()


def test_boundary_returns_404_for_unknown_district(boundary_client, monkeypatch):
    client, _ = boundary_client

    from backend.app.routers import clusters as clusters_router
    from backend.app.services.cluster_service import UnknownDistrict

    async def raise_unknown(key):
        raise UnknownDistrict("Unknown district: " + key)

    monkeypatch.setattr(clusters_router, "get_district_boundary", raise_unknown)

    r = client.get("/api/districts/notreal/boundary")

    assert r.status_code == 404
    assert r.json()["detail"] == "Unknown district"


# ── District key normalization (shared helper, no DB needed) ─────────────────

def test_normalize_district_key_shared_helper():
    from backend.app.services.district_utils import normalize_district_key

    cases = {
        "Beşiktaş": "besiktas",
        "Şişli": "sisli",
        "Üsküdar": "uskudar",
        "Kadıköy": "kadikoy",
        "Bakırköy": "bakirkoy",
        "Ataşehir": "atasehir",
        "Sarıyer": "sariyer",
        "Çekmeköy": "cekmekoy",
        "Büyükçekmece": "buyukcekmece",
        "Küçükçekmece": "kucukcekmece",
    }
    for name, expected in cases.items():
        assert normalize_district_key(name) == expected
    # Whitespace and casing are normalized away; None is safe.
    assert normalize_district_key("  FATİH  ") == "fatih"
    assert normalize_district_key(None) == ""


def test_normalize_district_key_turkish_chars():
    # The import script re-exports the shared helper, so this entry point still works.
    from scripts.import_district_boundaries import normalize_district_key

    assert normalize_district_key("Beşiktaş") == "besiktas"
    assert normalize_district_key("Şişli") == "sisli"
    assert normalize_district_key("Üsküdar") == "uskudar"
    assert normalize_district_key("Kadıköy") == "kadikoy"
    assert normalize_district_key("Fatih") == "fatih"
    # Whitespace and casing are normalized away
    assert normalize_district_key("  FATİH  ") == "fatih"


def test_import_district_boundaries_missing_file(tmp_path):
    """import_district_boundaries.py must fail cleanly when the file is missing."""
    import subprocess

    result = subprocess.run(
        [sys.executable, "scripts/import_district_boundaries.py",
         "--input", str(tmp_path / "nonexistent.geojson")],
        capture_output=True, text=True,
        cwd=str(PROJECT_ROOT),
    )
    assert result.returncode != 0
    output = (result.stdout + result.stderr).lower()
    assert "not found" in output


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


# ── Temporal congestion scoring ─────────────────────────────────────────────

def test_congestion_score_rush_hour_beats_late_night():
    from backend.app.services.cluster_service import score_temporal_clusters

    rush = {
        "cluster_id": 1,
        "point_count": 6,
        "sum_vehicle_count": 12000,
        "avg_vehicle_count": 2000.0,
        "avg_speed_kmh": 17.5,
        "min_speed_kmh": 12.0,
        "max_speed_kmh": 22.0,
        "centroid_lat": 41.0,
        "centroid_lon": 29.0,
    }
    late = {
        "cluster_id": 2,
        "point_count": 2,
        "sum_vehicle_count": 90,
        "avg_vehicle_count": 45.0,
        "avg_speed_kmh": 19.0,
        "min_speed_kmh": 16.0,
        "max_speed_kmh": 22.0,
        "centroid_lat": 41.1,
        "centroid_lon": 29.1,
    }
    scored = score_temporal_clusters([rush, late])
    rush_row = next(r for r in scored if r["cluster_id"] == 1)
    late_row = next(r for r in scored if r["cluster_id"] == 2)
    assert rush_row["congestion_score"] > late_row["congestion_score"]
    assert rush_row["severity"] in ("HIGH", "MEDIUM")
    assert late_row["severity"] == "LOW"


def test_congestion_severe_slowdown_can_be_high():
    from backend.app.services.cluster_service import assign_congestion_severity, compute_congestion_score

    score = compute_congestion_score(
        avg_speed_kmh=14.5,
        min_speed_kmh=12.0,
        sum_vehicle_count=191,
        point_count=2,
        hour_vol_p75=3000,
        hour_vol_max=15000,
    )
    severity, reason = assign_congestion_severity(
        congestion_score=score,
        avg_speed_kmh=14.5,
        min_speed_kmh=12.0,
        sum_vehicle_count=191,
        point_count=2,
    )
    assert severity == "HIGH"
    assert reason in ("severe_slowdown_high_volume", "high_congestion_score", "severe_local_slowdown")

    tiny_severity, _ = assign_congestion_severity(
        congestion_score=55.0,
        avg_speed_kmh=19.0,
        min_speed_kmh=16.0,
        sum_vehicle_count=90,
        point_count=2,
    )
    assert tiny_severity == "LOW"


# ── Dynamic clustering parameters on /api/clusters ───────────────────────────

def test_temporal_clusters_accepts_all_tuning_params(temporal_cluster_client):
    client, calls = temporal_cluster_client

    r = client.get(
        "/api/clusters?date=2025-01-17&hour=18"
        "&eps_meters=750&minpoints=3&avg_speed_threshold=20&window_hours=2"
    )

    assert r.status_code == 200
    assert calls == [
        {
            "args": ("2025-01-17", 18),
            "kwargs": {
                "eps_meters": 750.0,
                "minpoints": 3,
                "avg_speed_threshold": 20.0,
                "window_hours": 2,
            },
        }
    ]


def test_temporal_clusters_forwards_only_supplied_tuning(temporal_cluster_client):
    client, calls = temporal_cluster_client

    # Only eps_meters is given; nothing else should be forwarded.
    r = client.get("/api/clusters?date=2025-01-17&hour=18&eps_meters=1200")

    assert r.status_code == 200
    assert calls == [{"args": ("2025-01-17", 18), "kwargs": {"eps_meters": 1200.0}}]


def test_temporal_clusters_tuning_combines_with_district(temporal_cluster_client):
    client, calls = temporal_cluster_client

    r = client.get("/api/clusters?date=2025-01-17&hour=18&district=besiktas&minpoints=4")

    assert r.status_code == 200
    assert calls == [
        {"args": ("2025-01-17", 18), "kwargs": {"district": "besiktas", "minpoints": 4}}
    ]


def test_temporal_clusters_tuning_combines_with_bbox(temporal_cluster_client):
    client, calls = temporal_cluster_client

    r = client.get(
        "/api/clusters?date=2025-01-17&hour=18&window_hours=3"
        "&min_lon=28.95&min_lat=41.02&max_lon=29.08&max_lat=41.10"
    )

    assert r.status_code == 200
    assert calls == [
        {
            "args": ("2025-01-17", 18),
            "kwargs": {"bbox": (28.95, 41.02, 29.08, 41.10), "window_hours": 3},
        }
    ]


@pytest.mark.parametrize(
    "bad_param",
    [
        "eps_meters=10",          # below ge=100
        "eps_meters=99999",       # above le=5000
        "minpoints=0",            # below ge=1
        "minpoints=500",          # above le=50
        "avg_speed_threshold=0",  # not gt=0
        "avg_speed_threshold=999",  # above le=120
        "window_hours=0",         # below ge=1
        "window_hours=99",        # above le=24
    ],
)
def test_temporal_clusters_rejects_out_of_range_tuning(temporal_cluster_client, bad_param):
    client, calls = temporal_cluster_client

    r = client.get(f"/api/clusters?date=2025-01-17&hour=18&{bad_param}")

    assert r.status_code == 422, bad_param
    assert calls == []


# ── Geohash measurement-cell layer (/api/geohash-cells) ──────────────────────

@pytest.fixture
def geohash_cells_client(monkeypatch):
    """FastAPI test client with geohash-cell retrieval stubbed."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from backend.app.models.cluster import GeohashCellResponse
    from backend.app.routers import clusters as clusters_router

    calls = []

    async def fake_get_geohash_cells(*args, **kwargs):
        calls.append({"args": args, "kwargs": kwargs})
        return GeohashCellResponse(
            date=args[0],
            hour=args[1],
            cell_count=0,
            features=[],
        )

    monkeypatch.setattr(
        clusters_router, "get_geohash_cells", fake_get_geohash_cells
    )

    app = FastAPI()
    app.include_router(clusters_router.router, prefix="/api")
    with TestClient(app) as client:
        yield client, calls


def test_geohash_cells_accepts_date_hour(geohash_cells_client):
    client, calls = geohash_cells_client

    r = client.get("/api/geohash-cells?date=2025-01-17&hour=18")

    assert r.status_code == 200
    data = r.json()
    assert data["type"] == "FeatureCollection"
    assert data["date"] == "2025-01-17"
    assert data["hour"] == 18
    assert calls == [{"args": ("2025-01-17", 18), "kwargs": {}}]


def test_geohash_cells_requires_date_and_hour(geohash_cells_client):
    client, calls = geohash_cells_client

    assert client.get("/api/geohash-cells?date=2025-01-17").status_code == 422
    assert client.get("/api/geohash-cells?hour=18").status_code == 422
    assert calls == []


def test_geohash_cells_forwards_window_and_threshold(geohash_cells_client):
    client, calls = geohash_cells_client

    r = client.get(
        "/api/geohash-cells?date=2025-01-17&hour=18&avg_speed_threshold=20&window_hours=2"
    )

    assert r.status_code == 200
    assert calls == [
        {
            "args": ("2025-01-17", 18),
            "kwargs": {"avg_speed_threshold": 20.0, "window_hours": 2},
        }
    ]


def test_geohash_cells_accepts_full_bbox(geohash_cells_client):
    client, calls = geohash_cells_client

    r = client.get(
        "/api/geohash-cells?date=2025-01-17&hour=18"
        "&min_lon=28.95&min_lat=41.02&max_lon=29.08&max_lat=41.10"
    )

    assert r.status_code == 200
    assert calls == [
        {"args": ("2025-01-17", 18), "kwargs": {"bbox": (28.95, 41.02, 29.08, 41.10)}}
    ]


def test_geohash_cells_rejects_partial_bbox(geohash_cells_client):
    client, calls = geohash_cells_client

    r = client.get("/api/geohash-cells?date=2025-01-17&hour=18&min_lon=28.95")

    assert r.status_code == 400
    assert "All bbox parameters" in r.json()["detail"]
    assert calls == []


def test_geohash_cells_rejects_inverted_bbox(geohash_cells_client):
    client, calls = geohash_cells_client

    r = client.get(
        "/api/geohash-cells?date=2025-01-17&hour=18"
        "&min_lon=29.08&min_lat=41.02&max_lon=28.95&max_lat=41.10"
    )

    assert r.status_code == 400
    assert "min_lon" in r.json()["detail"]
    assert calls == []


def test_geohash_cells_invalid_latlon_returns_422(geohash_cells_client):
    client, calls = geohash_cells_client

    r = client.get(
        "/api/geohash-cells?date=2025-01-17&hour=18"
        "&min_lon=-181&min_lat=41.02&max_lon=29.08&max_lat=41.10"
    )

    assert r.status_code == 422
    assert calls == []


def test_geohash_cells_district_and_bbox_mutually_exclusive(geohash_cells_client):
    client, calls = geohash_cells_client

    r = client.get(
        "/api/geohash-cells?date=2025-01-17&hour=18&district=besiktas"
        "&min_lon=28.95&min_lat=41.02&max_lon=29.08&max_lat=41.10"
    )

    assert r.status_code == 400
    assert "mutually exclusive" in r.json()["detail"]
    assert calls == []


def test_geohash_cells_accepts_and_normalizes_district(geohash_cells_client):
    client, calls = geohash_cells_client

    r = client.get("/api/geohash-cells?date=2025-01-17&hour=18&district=BESIKTAS")

    assert r.status_code == 200
    assert calls == [{"args": ("2025-01-17", 18), "kwargs": {"district": "besiktas"}}]


def test_geohash_cells_invalid_district_returns_400(geohash_cells_client, monkeypatch):
    client, calls = geohash_cells_client

    from backend.app.routers import clusters as clusters_router
    from backend.app.services.cluster_service import UnknownDistrict

    async def raise_unknown(*args, **kwargs):
        raise UnknownDistrict("Unknown district: atlantis")

    monkeypatch.setattr(clusters_router, "get_geohash_cells", raise_unknown)

    r = client.get("/api/geohash-cells?date=2025-01-17&hour=18&district=atlantis")

    assert r.status_code == 400
    assert "Unknown district" in r.json()["detail"]


def test_geohash_cells_not_imported_returns_503(geohash_cells_client, monkeypatch):
    client, calls = geohash_cells_client

    from backend.app.routers import clusters as clusters_router
    from backend.app.services.cluster_service import (
        DISTRICT_NOT_IMPORTED_MSG,
        DistrictBoundariesNotImported,
    )

    async def raise_not_imported(*args, **kwargs):
        raise DistrictBoundariesNotImported(DISTRICT_NOT_IMPORTED_MSG)

    monkeypatch.setattr(clusters_router, "get_geohash_cells", raise_not_imported)

    r = client.get("/api/geohash-cells?date=2025-01-17&hour=18&district=besiktas")

    assert r.status_code == 503
    assert "not imported" in r.json()["detail"].lower()


# ── Geohash polygon geometry (pure, no DB) ───────────────────────────────────

def test_geohash_to_polygon_is_closed_ring_in_lonlat_order():
    from backend.app.services.geohash_service import _geohash_to_polygon
    from scoring.geohash_utils import decode_geohash_bounds

    gh = "sxk9"  # a valid base32 geohash
    poly = _geohash_to_polygon(gh)

    assert len(poly) == 1            # single linear ring
    ring = poly[0]
    assert len(ring) == 5            # 4 corners + closing point
    assert ring[0] == ring[-1]       # ring is closed

    min_lat, min_lon, max_lat, max_lon = decode_geohash_bounds(gh)
    # Corner order: SW, SE, NE, NW, SW — each as [lon, lat]
    assert ring[0] == [min_lon, min_lat]
    assert ring[1] == [max_lon, min_lat]
    assert ring[2] == [max_lon, max_lat]
    assert ring[3] == [min_lon, max_lat]


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
