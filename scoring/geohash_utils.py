"""
Geohash utility functions — shared across experiments and scripts.

Pure Python, no external dependencies.
"""

from __future__ import annotations

import math

_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"


def decode_geohash_bounds(gh: str) -> tuple[float, float, float, float]:
    """Return (min_lat, min_lon, max_lat, max_lon) for a geohash cell."""
    lat_range = [-90.0, 90.0]
    lon_range = [-180.0, 180.0]
    is_lon = True
    for char in gh:
        try:
            bits = _BASE32.index(char)
        except ValueError:
            raise ValueError(f"Invalid geohash character: '{char}'")
        for i in range(4, -1, -1):
            bit = (bits >> i) & 1
            if is_lon:
                mid = (lon_range[0] + lon_range[1]) / 2
                if bit:
                    lon_range[0] = mid
                else:
                    lon_range[1] = mid
            else:
                mid = (lat_range[0] + lat_range[1]) / 2
                if bit:
                    lat_range[0] = mid
                else:
                    lat_range[1] = mid
            is_lon = not is_lon
    return lat_range[0], lon_range[0], lat_range[1], lon_range[1]


def cell_area_km2(gh: str) -> float:
    """Approximate cell area in km² using the geohash bounding box."""
    min_lat, min_lon, max_lat, max_lon = decode_geohash_bounds(gh)
    lat_km = (max_lat - min_lat) * 111.1
    mid_lat_rad = math.radians((min_lat + max_lat) / 2)
    lon_km = (max_lon - min_lon) * 111.1 * math.cos(mid_lat_rad)
    return lat_km * lon_km


def compute_density_candidates(
    geohash_aggs: list[dict],
    percentile: float,
    min_vehicles: int,
) -> tuple[set[str], float]:
    """
    Percentile-based density threshold selection.

    Returns (set of selected geohashes, density threshold used).
    """
    active = [r for r in geohash_aggs if r["avg_vehicle_count"] >= min_vehicles]
    if not active:
        return set(), 0.0

    densities = sorted(r["vehicles_per_km2"] for r in active)
    idx = max(0, int(len(densities) * percentile / 100) - 1)
    threshold = densities[idx]

    selected = {r["geohash"] for r in active if r["vehicles_per_km2"] >= threshold}
    return selected, threshold


def geohash_envelope_sql_args(gh: str) -> tuple[float, float, float, float]:
    """Return (min_lon, min_lat, max_lon, max_lat) suitable for ST_MakeEnvelope."""
    min_lat, min_lon, max_lat, max_lon = decode_geohash_bounds(gh)
    return (min_lon, min_lat, max_lon, max_lat)
