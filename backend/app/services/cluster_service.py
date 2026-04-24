"""
Business logic for cluster data retrieval and GeoJSON formatting.
"""

from __future__ import annotations

import time

from backend.app.config import get_settings
from backend.app.database import get_pool
from backend.app.models.cluster import (
    ClusterProperties,
    GeoJSONFeature,
    GeoJSONFeatureCollection,
    GeoJSONGeometry,
    HeatmapPoint,
    HeatmapResponse,
    StatsResponse,
)

_cache: dict = {}
_CACHE_TTL = 60

async def get_cached_cluster_summaries() -> list[dict]:
    now = time.monotonic()
    if "clusters" not in _cache or now - _cache.get("ts", 0) > _CACHE_TTL:
        _cache["clusters"] = await get_cluster_summaries()
        _cache["ts"] = now
    return _cache["clusters"]


async def get_cluster_summaries() -> list[dict]:
    """
    Get aggregated cluster statistics from the database.

    Returns per-cluster: centroid, avg metrics, duration, recurrence,
    peak time, road name, and AIS-relevant fields.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT
                cluster_id,
                COUNT(*)::int AS point_count,
                AVG(vehicle_count)::float AS avg_vehicle_count,
                AVG(avg_speed)::float AS avg_speed,
                EXTRACT(EPOCH FROM (MAX(record_time) - MIN(record_time))) / 3600.0::float AS duration_hours,
                COUNT(DISTINCT record_time::date)::int AS recurrence_days,
                MODE() WITHIN GROUP (ORDER BY EXTRACT(HOUR FROM record_time))::int AS peak_hour,
                MODE() WITHIN GROUP (ORDER BY TO_CHAR(record_time, 'Day')) AS peak_day,
                AVG(COALESCE(snapped_lat, lat))::float AS centroid_lat,
                AVG(COALESCE(snapped_lon, lon))::float AS centroid_lon,
                MODE() WITHIN GROUP (ORDER BY road_name) AS road_name
            FROM traffic_clusters
            WHERE cluster_id >= 0
            GROUP BY cluster_id
            ORDER BY avg_vehicle_count DESC;
        """)
    return [dict(r) for r in rows]


def compute_ais_and_severity(clusters: list[dict], city_avg_speed: float = 35.0) -> list[dict]:
    """
    Compute AIS scores and severity for a list of cluster summaries.

    This replicates the scoring logic from scoring/anomaly_score.py
    but operates on pre-aggregated dicts rather than DataFrames.
    """
    if not clusters:
        return []

    # Extract raw values
    volumes = [c["avg_vehicle_count"] for c in clusters]
    speeds = [max(0.0, 1 - c["avg_speed"] / city_avg_speed) for c in clusters]
    durations = [c["duration_hours"] for c in clusters]
    recurrences = [c["recurrence_days"] for c in clusters]

    def min_max(vals):
        v_min, v_max = min(vals), max(vals)
        d = v_max - v_min
        if d == 0:
            return [0.5] * len(vals)
        return [(v - v_min) / d for v in vals]

    v_norm = min_max([float(x) for x in volumes])
    s_norm = min_max([float(x) for x in speeds])
    d_norm = min_max([float(x) for x in durations])
    r_norm = min_max([float(x) for x in recurrences])

    settings = get_settings()
    w = settings.ais_weights

    for i, c in enumerate(clusters):
        ais = (
            w["volume"] * v_norm[i]
            + w["speed_drop"] * s_norm[i]
            + w["duration"] * d_norm[i]
            + w["recurrence"] * r_norm[i]
        )
        c["ais_score"] = round(ais, 4)
        if ais >= 0.66:
            c["severity"] = "HIGH"
        elif ais >= 0.33:
            c["severity"] = "MEDIUM"
        else:
            c["severity"] = "LOW"

    return sorted(clusters, key=lambda c: c["ais_score"], reverse=True)


def build_geojson(clusters: list[dict]) -> GeoJSONFeatureCollection:
    """Convert cluster summaries to a GeoJSON FeatureCollection."""
    features = []
    for c in clusters:
        feature = GeoJSONFeature(
            geometry=GeoJSONGeometry(
                coordinates=[c["centroid_lon"], c["centroid_lat"]]
            ),
            properties=ClusterProperties(
                cluster_id=c["cluster_id"],
                severity=c["severity"],
                ais_score=c["ais_score"],
                point_count=c["point_count"],
                avg_vehicle_count=round(c["avg_vehicle_count"], 1),
                avg_speed_kmh=round(c["avg_speed"], 1),
                duration_hours=round(c["duration_hours"], 1),
                recurrence_days=c["recurrence_days"],
                peak_hour=c["peak_hour"],
                peak_day=c.get("peak_day", "").strip(),
                peak_time=f"{c.get('peak_day', '').strip()} {int(c['peak_hour']):02d}:00",
                road_name=c.get("road_name"),
                snapped_lat=c.get("centroid_lat"),
                snapped_lon=c.get("centroid_lon"),
            ),
        )
        features.append(feature)

    return GeoJSONFeatureCollection(features=features)


async def get_heatmap_data(date_filter: str | None = None) -> HeatmapResponse:
    """
    Get raw traffic points for heatmap visualization.

    Parameters
    ----------
    date_filter : str, optional
        Filter by date in YYYY-MM-DD format.

    Returns
    -------
    HeatmapResponse with normalized intensity values.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        if date_filter:
            rows = await conn.fetch("""
                SELECT
                    COALESCE(snapped_lat, lat) AS lat,
                    COALESCE(snapped_lon, lon) AS lon,
                    vehicle_count,
                    avg_speed,
                    record_time::text AS record_time
                FROM traffic_clusters
                WHERE record_time::date = $1::date
                  AND cluster_id >= 0
                ORDER BY vehicle_count DESC
                LIMIT 5000;
            """, date_filter)
        else:
            rows = await conn.fetch("""
                SELECT
                    COALESCE(snapped_lat, lat) AS lat,
                    COALESCE(snapped_lon, lon) AS lon,
                    vehicle_count,
                    avg_speed,
                    record_time::text AS record_time
                FROM traffic_clusters
                WHERE cluster_id >= 0
                ORDER BY vehicle_count DESC
                LIMIT 5000;
            """)

    if not rows:
        return HeatmapResponse(date=date_filter, point_count=0, points=[])

    # Normalize intensity by max vehicle count
    max_vc = max(r["vehicle_count"] for r in rows)

    points = [
        HeatmapPoint(
            lat=r["lat"],
            lon=r["lon"],
            intensity=round(r["vehicle_count"] / max_vc, 4) if max_vc > 0 else 0,
            vehicle_count=r["vehicle_count"],
            avg_speed=r["avg_speed"],
            record_time=r["record_time"],
        )
        for r in rows
    ]

    return HeatmapResponse(date=date_filter, point_count=len(points), points=points)


async def get_global_stats() -> StatsResponse:
    """Get global statistics for the dashboard."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT
                COUNT(*)::int AS total_records,
                COUNT(DISTINCT CASE WHEN cluster_id >= 0 THEN cluster_id END)::int AS total_clusters,
                COUNT(CASE WHEN cluster_id = -1 THEN 1 END)::int AS total_noise_points,
                MIN(record_time)::text AS date_range_start,
                MAX(record_time)::text AS date_range_end
            FROM traffic_clusters;
        """)

    total = row["total_records"] or 0
    noise = row["total_noise_points"] or 0

    return StatsResponse(
        total_records=total,
        total_clusters=row["total_clusters"] or 0,
        total_noise_points=noise,
        noise_percentage=round(100 * noise / total, 2) if total > 0 else 0,
        high_severity_count=0,   # computed at endpoint level
        medium_severity_count=0,
        low_severity_count=0,
        date_range_start=row["date_range_start"],
        date_range_end=row["date_range_end"],
    )
