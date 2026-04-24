"""
Centralized configuration for Istanbul Traffic Anomaly Analysis.

All database, OSRM, and algorithm parameters are loaded from environment
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

# ─── OSRM (Map Matching) ──────────────────────────────────────────────────
OSRM_URL = settings.osrm_url

# ─── ST-DBSCAN Parameters ─────────────────────────────────────────────────
EPS1_DEG = 0.005          # ~500 m at Istanbul's latitude
EPS1_METERS = 500.0       # human-readable equivalent
EPS2_SEC = 3600.0         # 1 hour
MINPTS = 3                # minimum points to form a core

# ─── Safety Guards ─────────────────────────────────────────────────────────
MAX_CLUSTER_INPUT = 100_000  # max rows before requiring partitioning

# ─── AIS (Anomaly Intensity Score) Weights ─────────────────────────────────
AIS_WEIGHTS = settings.ais_weights
CITY_AVG_SPEED_KMH = 35.0  # Istanbul citywide average (baseline)

# ─── Data Paths ────────────────────────────────────────────────────────────
CSV_DIR = PROJECT_ROOT / "ibb_trafik_verileri"
CSV_PATH = CSV_DIR / "traffic_density_202501.csv"

# ─── Ingestion ─────────────────────────────────────────────────────────────
BATCH_SIZE = 10_000


def ensure_cli_logging() -> None:
    """If the root logger has no handlers, emit INFO on stdout (same stream as print)."""
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(message)s",
            stream=sys.stdout,
        )
