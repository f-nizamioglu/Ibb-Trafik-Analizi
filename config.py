"""
Centralized configuration for Istanbul Traffic Anomaly Analysis.

All database and algorithm parameters are loaded from environment
variables (via .env file) so credentials never appear in source code.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from backend.app.config import get_settings

# ─── Load .env from project root ───────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent

settings = get_settings()

# ─── Database ──────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host": settings.db_host,
    "port": settings.db_port,
    "dbname": settings.db_name,
    "user": settings.db_user,
    "password": settings.db_password,
}

# Async DSN for asyncpg (FastAPI backend)
DATABASE_URL = settings.database_url

# ─── PostGIS ST_ClusterDBSCAN Parameters ──────────────────────────────────
# Geometry column is EPSG:32636 (UTM Zone 36N), so eps is in metres.
EPS_METERS = settings.dbscan_eps_meters
MINPTS = settings.dbscan_minpoints

# ─── Congestion Candidate Filter Thresholds ───────────────────────────────
# Static baseline filter applied before clustering.
# Records below this speed AND above this vehicle count are candidates.
HIGH_CONGESTION_MAX_AVG_SPEED = settings.high_congestion_max_avg_speed
HIGH_CONGESTION_MIN_VEHICLE_COUNT = settings.high_congestion_min_vehicle_count

# ─── Geohash Cell Density Prototype ───────────────────────────────────────
# Area-based approximation only. Not road-length-aware.
DENSITY_FILTER_ENABLED = settings.density_filter_enabled
DENSITY_PERCENTILE_THRESHOLD = settings.density_percentile_threshold

# ─── Road-Network Density Filtering ───────────────────────────────────────
# Requires: road_segments + geohash_road_lengths (run create_road_schema.py first).
DENSITY_FILTER_METHOD = settings.density_filter_method          # static|geohash_area|road_length
ROAD_DENSITY_PERCENTILE_THRESHOLD = settings.road_density_percentile_threshold

# ─── AIS (Anomaly Intensity Score) Weights ─────────────────────────────────
AIS_WEIGHTS = settings.ais_weights
CITY_AVG_SPEED_KMH = 35.0  # Istanbul citywide average speed (km/h) baseline

# ─── Data Paths ────────────────────────────────────────────────────────────
# Canonical raw data directory — all 61 monthly CSV files live here.
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw" / "ibb_hourly_traffic_density"
MANIFEST_PATH = PROJECT_ROOT / "data" / "download_manifest.json"

# IBB CKAN dataset identifier (confirmed working via API, 2025-06)
IBB_DATASET_SLUG = "hourly-traffic-density-data-set"
IBB_CKAN_BASE_URL = "https://data.ibb.gov.tr"

# ─── Ingestion ─────────────────────────────────────────────────────────────
BATCH_SIZE = 10_000


def ensure_cli_logging() -> None:
    """If the root logger has no handlers, emit INFO on stdout."""
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(message)s",
            stream=sys.stdout,
        )
