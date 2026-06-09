"""
FastAPI configuration — loads settings from environment variables via pydantic-settings.
"""

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings

# Resolve .env relative to THIS file so it works regardless of cwd
_ENV_FILE = Path(__file__).resolve().parent.parent.parent / ".env"


class Settings(BaseSettings):
    """Application settings loaded from .env file."""

    # Database
    db_host: str = "localhost"
    db_port: int = 5433
    db_name: str = "istanbul_traffic"
    db_user: str = "postgres"
    db_password: str = ""

    # App
    app_env: str = "development"
    allowed_origins: str = "http://localhost:3000,http://localhost:8000"

    # AIS Weights (tunable via .env)
    ais_weight_volume: float = 0.30
    ais_weight_speed_drop: float = 0.30
    ais_weight_duration: float = 0.25
    ais_weight_recurrence: float = 0.15

    # PostGIS ST_ClusterDBSCAN Parameters (metric, EPSG:32636)
    dbscan_eps_meters: float = 500.0
    dbscan_minpoints: int = 3

    # Congestion Candidate Filter Thresholds (baseline static filter)
    high_congestion_max_avg_speed: int = 20        # km/h
    high_congestion_min_vehicle_count: int = 500

    # Geohash Cell Density Prototype — opt-in, experimental approximation only.
    # Not true road-length-aware density. Requires no road network data.
    density_filter_enabled: bool = False
    density_percentile_threshold: float = 75.0     # vehicles/km² percentile cutoff

    # Road-network density filtering — requires road_segments + geohash_road_lengths.
    # Set DENSITY_FILTER_METHOD=road_length after running:
    #   python scripts/create_road_schema.py
    #   python scripts/import_road_network.py
    density_filter_method: str = "static"          # static | geohash_area | road_length
    road_density_percentile_threshold: float = 75.0  # vehicles/road-km percentile cutoff

    @property
    def database_url(self) -> str:
        return (
            f"postgresql://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    @property
    def ais_weights(self) -> dict:
        return {
            "volume": self.ais_weight_volume,
            "speed_drop": self.ais_weight_speed_drop,
            "duration": self.ais_weight_duration,
            "recurrence": self.ais_weight_recurrence,
        }

    model_config = {
        "env_file": str(_ENV_FILE),
        "env_file_encoding": "utf-8",
    }


@lru_cache()
def get_settings() -> Settings:
    return Settings()
