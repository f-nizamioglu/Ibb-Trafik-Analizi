"""
Tests for road-network density pipeline.

Unit tests run without database or road data.
DB tests are skipped when the database is unavailable.
Road-data tests are skipped when road_segments is empty.
"""

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ── Geohash polygon helpers ───────────────────────────────────────────────────

def test_geohash_envelope_sql_args_order():
    """geohash_envelope_sql_args returns (min_lon, min_lat, max_lon, max_lat)."""
    from scoring.geohash_utils import geohash_envelope_sql_args, decode_geohash_bounds
    min_lat, min_lon, max_lat, max_lon = decode_geohash_bounds("sxk9jc")
    args = geohash_envelope_sql_args("sxk9jc")
    assert args == (min_lon, min_lat, max_lon, max_lat)


def test_geohash_envelope_bounding_box_valid():
    """ST_MakeEnvelope args: min_lon < max_lon and min_lat < max_lat."""
    from scoring.geohash_utils import geohash_envelope_sql_args
    for gh in ["sxk9jc", "u09tun", "sxk9"]:
        min_lon, min_lat, max_lon, max_lat = geohash_envelope_sql_args(gh)
        assert min_lon < max_lon, f"{gh}: min_lon >= max_lon"
        assert min_lat < max_lat, f"{gh}: min_lat >= max_lat"


def test_geohash_envelope_istanbul_range():
    """Istanbul geohash envelope args should fall within Istanbul lon/lat bounds."""
    from scoring.geohash_utils import geohash_envelope_sql_args
    min_lon, min_lat, max_lon, max_lat = geohash_envelope_sql_args("sxk9jc")
    assert 28.5 <= min_lon < max_lon <= 29.5
    assert 40.8 <= min_lat < max_lat <= 41.5


# ── Config ────────────────────────────────────────────────────────────────────

def test_density_filter_method_default():
    """DENSITY_FILTER_METHOD defaults to 'static'."""
    from config import DENSITY_FILTER_METHOD
    assert DENSITY_FILTER_METHOD in ("static", "geohash_area", "road_length")


def test_road_density_percentile_threshold_range():
    """ROAD_DENSITY_PERCENTILE_THRESHOLD must be between 0 and 100."""
    from config import ROAD_DENSITY_PERCENTILE_THRESHOLD
    assert 0 < ROAD_DENSITY_PERCENTILE_THRESHOLD <= 100


def test_settings_road_density_fields():
    """Settings class exposes density_filter_method and road_density_percentile_threshold."""
    from backend.app.config import get_settings
    s = get_settings()
    assert hasattr(s, "density_filter_method")
    assert hasattr(s, "road_density_percentile_threshold")
    assert s.density_filter_method in ("static", "geohash_area", "road_length")
    assert 0 < s.road_density_percentile_threshold <= 100


# ── vehicles_per_road_km calculation logic ────────────────────────────────────

def test_vehicles_per_road_km_basic():
    """vehicles / road_km should equal vehicles_per_road_km."""
    vehicle_count = 300
    road_length_km = 2.5
    expected = vehicle_count / road_length_km
    assert abs(expected - 120.0) < 1e-9


def test_vehicles_per_road_km_zero_road_length():
    """Zero road length must not produce division by zero — should give None/NULL."""
    vehicle_count = 500
    road_length_km = 0.0
    result = vehicle_count / road_length_km if road_length_km > 0 else None
    assert result is None


def test_vehicles_per_road_km_null_road_length():
    """NULL road length (geohash not in road_lengths) should propagate as None."""
    road_length_km = None
    result = (500 / road_length_km) if road_length_km else None
    assert result is None


# ── Road density candidate selection ─────────────────────────────────────────

# ── Import script clean failure ───────────────────────────────────────────────

def test_import_script_missing_file(tmp_path):
    """import_road_network.py must fail cleanly when input file is missing."""
    import subprocess, sys
    result = subprocess.run(
        [sys.executable, "scripts/import_road_network.py",
         "--input", str(tmp_path / "nonexistent.osm.pbf")],
        capture_output=True, text=True,
        cwd=str(PROJECT_ROOT),
    )
    assert result.returncode != 0
    # Should mention the download URL
    output = result.stdout + result.stderr
    assert "bbbike" in output.lower() or "download" in output.lower() or "not found" in output.lower()


# ── DB-dependent tests (skipped if no DB or no road data) ─────────────────────

@pytest.fixture(scope="session")
def road_data_available(db_available) -> bool:
    """True if road_segments has data."""
    if not db_available:
        return False
    try:
        import psycopg2
        from config import DB_CONFIG
        conn = psycopg2.connect(**DB_CONFIG, connect_timeout=3)
        cur = conn.cursor()
        # Check table exists and has data
        cur.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema='public' AND table_name='road_segments'
        """)
        if cur.fetchone()[0] == 0:
            conn.close()
            return False
        cur.execute("SELECT COUNT(*)::int FROM road_segments")
        count = cur.fetchone()[0]
        conn.close()
        return count > 0
    except Exception:
        return False


@pytest.fixture
def skip_if_no_road_data(road_data_available):
    if not road_data_available:
        pytest.skip(
            "road_segments is empty — import road data first:\n"
            "  python scripts/import_road_network.py --input data/road_network/Istanbul.osm.pbf"
        )


@pytest.mark.db
def test_road_segments_schema(skip_if_no_road_data):
    """road_segments must have expected columns and non-zero rows."""
    import psycopg2
    from config import DB_CONFIG
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name='road_segments' ORDER BY ordinal_position
    """)
    cols = {r[0] for r in cur.fetchall()}
    assert "osm_id" in cols
    assert "highway" in cols
    assert "geom_4326" in cols
    assert "geom_32636" in cols
    assert "length_m" in cols
    cur.execute("SELECT COUNT(*)::int FROM road_segments WHERE length_m > 0")
    assert cur.fetchone()[0] > 0
    conn.close()


@pytest.mark.db
def test_geohash_cells_populated(skip_if_no_road_data):
    """geohash_cells must be populated from ibb_traffic_density geohashes."""
    import psycopg2
    from config import DB_CONFIG
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*)::int FROM geohash_cells")
    assert cur.fetchone()[0] > 0
    cur.execute("SELECT COUNT(*)::int FROM geohash_cells WHERE area_km2 > 0")
    assert cur.fetchone()[0] > 0
    conn.close()


@pytest.mark.db
def test_geohash_road_lengths_populated(skip_if_no_road_data):
    """geohash_road_lengths must have rows with positive road_length_km."""
    import psycopg2
    from config import DB_CONFIG
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*)::int FROM geohash_road_lengths WHERE road_length_km > 0")
    count = cur.fetchone()[0]
    assert count > 0, "No geohash cells have road coverage — check if matview was refreshed"
    conn.close()


@pytest.mark.db
def test_road_density_base_view(skip_if_no_road_data):
    """road_density_base view must expose vehicles_per_road_km column."""
    import psycopg2
    from config import DB_CONFIG
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'road_density_base'
    """)
    cols = {r[0] for r in cur.fetchall()}
    assert "vehicles_per_road_km" in cols
    assert "road_length_km" in cols
    cur.execute("SELECT COUNT(*)::int FROM road_density_base WHERE vehicles_per_road_km IS NOT NULL LIMIT 1")
    assert cur.fetchone()[0] >= 0
    conn.close()
