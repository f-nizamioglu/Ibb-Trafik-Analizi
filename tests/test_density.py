"""
Tests for the Geohash Cell Density Prototype logic.

All tests run without a database connection.
"""

import math
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ── Geohash decoder ──────────────────────────────────────────────────────────

def test_decode_geohash_bounds_known_cell():
    """Known geohash 'u09tun' (Eiffel Tower area) should decode to correct bounds."""
    from experiments.dynamic_density import _decode_geohash_bounds
    min_lat, min_lon, max_lat, max_lon = _decode_geohash_bounds("u09tun")
    # Eiffel Tower is at ~48.858°N, 2.294°E
    assert min_lat < 48.858 < max_lat
    assert min_lon < 2.294 < max_lon


def test_decode_geohash_istanbul():
    """Istanbul geohash cell (Bostancı area) should decode to Istanbul coordinates."""
    from experiments.dynamic_density import _decode_geohash_bounds
    # sxk9jc is a 6-char Bostancı-area cell from check_geohash.py
    min_lat, min_lon, max_lat, max_lon = _decode_geohash_bounds("sxk9jc")
    # Istanbul range: lat 40.8–41.5, lon 28.5–29.5
    assert 40.8 <= min_lat < max_lat <= 41.5
    assert 28.5 <= min_lon < max_lon <= 29.5


def test_decode_geohash_bounds_ordered():
    """min_lat < max_lat and min_lon < max_lon for any valid geohash."""
    from experiments.dynamic_density import _decode_geohash_bounds
    for gh in ["sxk9jc", "sxk9j", "u4pru", "9q8yy"]:
        min_lat, min_lon, max_lat, max_lon = _decode_geohash_bounds(gh)
        assert min_lat < max_lat
        assert min_lon < max_lon


def test_decode_geohash_invalid_char():
    """Invalid geohash character should raise ValueError."""
    from experiments.dynamic_density import _decode_geohash_bounds
    with pytest.raises(ValueError, match="Invalid geohash character"):
        _decode_geohash_bounds("sxk9!!")


def test_cell_area_km2_6char():
    """6-character geohash cell area should be approximately 0.5–1.0 km² for Istanbul."""
    from experiments.dynamic_density import cell_area_km2
    area = cell_area_km2("sxk9jc")
    # At ~41°N a 6-char geohash cell is roughly 0.55 km²
    assert 0.2 < area < 2.0, f"Unexpected cell area: {area:.4f} km²"


def test_cell_area_4char_larger_than_6char():
    """A 4-char geohash cell must cover a larger area than a 6-char cell."""
    from experiments.dynamic_density import cell_area_km2
    area_4 = cell_area_km2("sxk9")
    area_6 = cell_area_km2("sxk9jc")
    assert area_4 > area_6


def test_cell_area_positive():
    """Cell area must always be positive."""
    from experiments.dynamic_density import cell_area_km2
    for gh in ["sxk9jc", "u4pru", "9q8yy", "ezs4"]:
        assert cell_area_km2(gh) > 0


# ── Density candidate selection ──────────────────────────────────────────────

def _make_aggs(geohashes_and_vehicles: list[tuple[str, float]]) -> list[dict]:
    """Build minimal geohash_aggs structure for testing."""
    from experiments.dynamic_density import cell_area_km2
    result = []
    for gh, avg_vc in geohashes_and_vehicles:
        area = cell_area_km2(gh)
        result.append({
            "geohash": gh,
            "record_count": 100,
            "avg_vehicle_count": avg_vc,
            "avg_speed": 18.0,
            "cell_area_km2": area,
            "vehicles_per_km2": avg_vc / area if area > 0 else 0.0,
        })
    return result


def test_density_candidates_basic():
    """Top percentile candidates should be the highest-density cells."""
    from experiments.dynamic_density import compute_density_candidates

    aggs = _make_aggs([
        ("sxk9jc", 1000.0),  # high density
        ("sxk9jb", 200.0),   # medium
        ("sxk9j5", 50.0),    # low (but above min_vehicles)
    ])
    selected, threshold = compute_density_candidates(aggs, percentile=66, min_vehicles=40)
    assert "sxk9jc" in selected  # highest density must be selected
    assert threshold > 0


def test_density_candidates_min_vehicles_filter():
    """Cells below min_vehicles threshold should not be ranked."""
    from experiments.dynamic_density import compute_density_candidates

    aggs = _make_aggs([
        ("sxk9jc", 1000.0),
        ("sxk9jb", 10.0),   # below min_vehicles
    ])
    selected, threshold = compute_density_candidates(aggs, percentile=50, min_vehicles=100)
    # sxk9jb has only 10 vehicles — below min_vehicles, should not affect threshold
    assert "sxk9jb" not in selected


def test_density_candidates_empty_input():
    """Empty input returns empty set and zero threshold."""
    from experiments.dynamic_density import compute_density_candidates
    selected, threshold = compute_density_candidates([], percentile=75, min_vehicles=50)
    assert selected == set()
    assert threshold == 0.0


def test_density_candidates_all_below_min_vehicles():
    """All cells below min_vehicles returns empty selection."""
    from experiments.dynamic_density import compute_density_candidates

    aggs = _make_aggs([("sxk9jc", 5.0), ("sxk9jb", 3.0)])
    selected, threshold = compute_density_candidates(aggs, percentile=75, min_vehicles=100)
    assert selected == set()
    assert threshold == 0.0


def test_density_candidates_percentile_100():
    """100th percentile should select only the highest-density cell."""
    from experiments.dynamic_density import compute_density_candidates

    aggs = _make_aggs([
        ("sxk9jc", 1000.0),
        ("sxk9jb", 200.0),
        ("sxk9j5", 100.0),
    ])
    selected_75, _ = compute_density_candidates(aggs, percentile=75, min_vehicles=50)
    selected_100, _ = compute_density_candidates(aggs, percentile=100, min_vehicles=50)
    # 100th percentile should select at least one cell
    assert len(selected_100) >= 1
    # 75th percentile should select at least as many
    assert len(selected_75) >= len(selected_100)
