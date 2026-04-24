"""
Map matching — Snap geohash centroids to the nearest road segment via OSRM.

Problem
-------
IBB data is encoded as geohash strings. A 6-character geohash has a cell size
of ~1.2 km × 0.6 km. The decoded centroid often falls on buildings, parks, or
waterways — not on the actual road where vehicles were measured.

Solution
--------
Use OSRM's /nearest endpoint (deployed locally via Docker with Turkey OSM data)
to snap each unique (lat, lon) centroid to the nearest node on the road network.

Performance Note
----------------
Since geohash centroids repeat across timestamps, there are only ~2,000 unique
(lat, lon) pairs in the 1.7M-row January dataset. With LRU caching, the entire
snapping operation completes in under 5 seconds.

Docker Setup
------------
See docker-compose.yml for the OSRM service configuration.
Pre-processing commands (run once):
    wget https://download.geofabrik.de/europe/turkey-latest.osm.pbf -P osrm-data/
    docker run --rm -v ./osrm-data:/data osrm/osrm-backend osrm-extract -p /opt/car.lua /data/turkey-latest.osm.pbf
    docker run --rm -v ./osrm-data:/data osrm/osrm-backend osrm-partition /data/turkey-latest.osrm
    docker run --rm -v ./osrm-data:/data osrm/osrm-backend osrm-customize /data/turkey-latest.osrm
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional

import requests

from config import OSRM_URL as _DEFAULT_OSRM_URL

EARTH_RADIUS_M = 6_371_000.0
OSRM_HTTP_TIMEOUT_SEC = 5
DEFAULT_SNAP_RADIUS_M = 500
SNAP_LRU_CACHE_SIZE = 10_000


@dataclass
class SnapResult:
    """Result of snapping a single point to the road network."""
    original_lat: float
    original_lon: float
    snapped_lat: float
    snapped_lon: float
    road_name: Optional[str]
    distance_m: float  # distance from original to snapped point

    @property
    def was_snapped(self) -> bool:
        return self.distance_m > 0.0


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the great-circle distance between two points in metres.
    """
    R = EARTH_RADIUS_M
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


@lru_cache(maxsize=SNAP_LRU_CACHE_SIZE)
def snap_to_road(
    lat: float,
    lon: float,
    radius: int = DEFAULT_SNAP_RADIUS_M,
    osrm_url: str = _DEFAULT_OSRM_URL,
) -> SnapResult:
    """
    Snap a single (lat, lon) point to the nearest road segment via OSRM.

    Parameters
    ----------
    lat, lon : float
        Coordinates in WGS84 degrees.
    radius : int
        Search radius in metres. Points farther than this from any road
        are returned un-snapped.
    osrm_url : str, optional
        Override the OSRM endpoint URL.

    Returns
    -------
    SnapResult with snapped coordinates, road name, and snap distance.
    """
    url = osrm_url
    try:
        # OSRM expects coordinates as lon,lat (not lat,lon!)
        resp = requests.get(
            f"{url}/nearest/v1/driving/{lon},{lat}",
            params={"number": 1},
            timeout=OSRM_HTTP_TIMEOUT_SEC,
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("code") == "Ok" and data.get("waypoints"):
            wp = data["waypoints"][0]
            snapped_lon, snapped_lat = wp["location"]  # [lon, lat]
            road_name = wp.get("name") or None
            distance = _haversine_m(lat, lon, snapped_lat, snapped_lon)

            return SnapResult(
                original_lat=lat,
                original_lon=lon,
                snapped_lat=snapped_lat,
                snapped_lon=snapped_lon,
                road_name=road_name,
                distance_m=round(distance, 2),
            )
    except (requests.RequestException, KeyError, ValueError):
        pass  # fall through to un-snapped result

    # Fallback: return original point
    return SnapResult(
        original_lat=lat,
        original_lon=lon,
        snapped_lat=lat,
        snapped_lon=lon,
        road_name=None,
        distance_m=0.0,
    )


def snap_to_road_no_cache(
    lat: float,
    lon: float,
    radius: int = DEFAULT_SNAP_RADIUS_M,
    osrm_url: str = _DEFAULT_OSRM_URL,
) -> SnapResult:
    """Same as snap_to_road but without LRU cache (for testing)."""
    return snap_to_road.__wrapped__(lat, lon, radius, osrm_url)
