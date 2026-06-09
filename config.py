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

# ─── AIS (Anomaly Intensity Score) Weights ─────────────────────────────────
AIS_WEIGHTS = settings.ais_weights
CITY_AVG_SPEED_KMH = 35.0  # Istanbul citywide average speed (km/h) baseline

# ─── Data Paths ────────────────────────────────────────────────────────────
CSV_DIR = PROJECT_ROOT / "ibb_trafik_verileri"
CSV_PATH = CSV_DIR / "traffic_density_202501.csv"

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
