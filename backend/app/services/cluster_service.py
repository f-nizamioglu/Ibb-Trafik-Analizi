"""
Business logic for cluster data retrieval and GeoJSON formatting.

Clustering is performed entirely in PostGIS using ST_ClusterDBSCAN on the
high_congestion_zones view. The EPSG:32636 geometry enables direct metric
distance calculations. Centroids are transformed back to WGS84 (EPSG:4326)
for Leaflet.js consumption via ST_Transform.
"""

from __future__ import annotations

import json
import logging
import math
import time
from datetime import datetime, timedelta
from statistics import quantiles

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
    TemporalClusterProperties,
    TemporalClusterResponse,
    TemporalGeoJSONFeature,
)

logger = logging.getLogger(__name__)

# Message shown when district filtering is requested but the boundary table is
# missing or empty. Kept as a module constant so the API and tests stay in sync.
DISTRICT_NOT_IMPORTED_MSG = (
    "District boundaries are not imported. "
    "Run python scripts/import_district_boundaries.py."
)


class DistrictBoundariesNotImported(RuntimeError):
    """Raised when district filtering is requested but no boundaries are loaded."""


class UnknownDistrict(ValueError):
    """Raised when a requested district key is not present in the boundary table."""


# ── Cache ──────────────────────────────────────────────────────────────────
_cache: dict = {}
_CACHE_TTL = 60


async def get_cached_cluster_summaries() -> list[dict]:
    """Return cached cluster summaries, re-computing if stale."""
    now = time.monotonic()
    if "clusters" not in _cache or now - _cache.get("ts", 0) > _CACHE_TTL:
        _cache["clusters"] = await get_cluster_summaries()
        _cache["ts"] = now
    return _cache["clusters"]


# ── PostGIS ST_ClusterDBSCAN → aggregated cluster stats in one query ──────
_CLUSTER_SUMMARY_SQL = """
WITH clustered AS (
    SELECT
        record_time,
        vehicle_count,
        avg_speed,
        geom,
        COALESCE(
            ST_ClusterDBSCAN(geom, eps := $1, minpoints := $2) OVER (),
            -1
        ) AS cluster_id
    FROM high_congestion_zones
)
SELECT
    cluster_id,
    COUNT(*)::int                                                   AS point_count,
    AVG(vehicle_count)::float                                       AS avg_vehicle_count,
    AVG(avg_speed)::float                                           AS avg_speed,
    EXTRACT(EPOCH FROM (MAX(record_time) - MIN(record_time)))
        / 3600.0                                                    AS duration_hours,
    COUNT(DISTINCT record_time::date)::int                          AS recurrence_days,
    MODE() WITHIN GROUP (ORDER BY EXTRACT(HOUR FROM record_time))::int AS peak_hour,
    MODE() WITHIN GROUP (ORDER BY TO_CHAR(record_time, 'Day'))      AS peak_day,
    ST_Y(ST_Transform(ST_Centroid(ST_Collect(geom)), 4326))::float  AS centroid_lat,
    ST_X(ST_Transform(ST_Centroid(ST_Collect(geom)), 4326))::float  AS centroid_lon
FROM clustered
WHERE cluster_id >= 0
GROUP BY cluster_id
ORDER BY avg_vehicle_count DESC;
"""


async def get_cluster_summaries() -> list[dict]:
    """
    Run ST_ClusterDBSCAN on high_congestion_zones and return per-cluster statistics.

    The clustering is executed inside the database using PostGIS's native
    ST_ClusterDBSCAN window function. Cluster centroids are computed from the
    collected geometries and transformed back to WGS84 for frontend display.
    """
    settings = get_settings()
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            _CLUSTER_SUMMARY_SQL,
            settings.dbscan_eps_meters,
            settings.dbscan_minpoints,
        )
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
            ),
        )
        features.append(feature)

    return GeoJSONFeatureCollection(features=features)


# ── Heatmap SQL (transform back to WGS84 for Leaflet) ─────────────────────
_HEATMAP_SQL_FILTERED = """
SELECT
    ST_Y(ST_Transform(geom, 4326))::float AS lat,
    ST_X(ST_Transform(geom, 4326))::float AS lon,
    vehicle_count,
    avg_speed,
    record_time::text AS record_time
FROM high_congestion_zones
WHERE record_time::date = $1::date
ORDER BY vehicle_count DESC
LIMIT 5000;
"""

_HEATMAP_SQL_ALL = """
SELECT
    ST_Y(ST_Transform(geom, 4326))::float AS lat,
    ST_X(ST_Transform(geom, 4326))::float AS lon,
    vehicle_count,
    avg_speed,
    record_time::text AS record_time
FROM high_congestion_zones
ORDER BY vehicle_count DESC
LIMIT 5000;
"""


async def get_heatmap_data(date_filter: str | None = None) -> HeatmapResponse:
    """
    Get raw traffic points for heatmap visualization.

    Coordinates are transformed from EPSG:32636 back to WGS84 (EPSG:4326)
    for Leaflet.heat consumption.

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
            rows = await conn.fetch(_HEATMAP_SQL_FILTERED, date_filter)
        else:
            rows = await conn.fetch(_HEATMAP_SQL_ALL)

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


# ── Global stats (reads from traffic_clusters table for persistence) ──────
_STATS_SQL = """
SELECT
    (SELECT COUNT(*) FROM high_congestion_zones)::int                       AS total_records,
    COUNT(DISTINCT CASE WHEN cluster_id >= 0 THEN cluster_id END)::int      AS total_clusters,
    COUNT(CASE WHEN cluster_id = -1 THEN 1 END)::int                        AS total_noise_points,
    MIN(record_time)::text                                                  AS date_range_start,
    MAX(record_time)::text                                                  AS date_range_end
FROM traffic_clusters;
"""


async def get_global_stats() -> StatsResponse:
    """Get global statistics for the dashboard."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(_STATS_SQL)

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


# ── Temporal (Hourly) Clustering ──────────────────────────────────────────
# These parameters are tuned for single-hour time slices (~2,400 rows per hour)
# rather than the full dataset (99.6M rows). The existing pipeline defaults
# (eps=500, minpoints=3, avg_speed<20, vehicle_count>500) are too strict
# for hourly snapshots and yield zero clusters.
_TEMPORAL_EPS_METERS = 1000
_TEMPORAL_MINPOINTS = 2
_TEMPORAL_MAX_AVG_SPEED = 25  # km/h

_TEMPORAL_CLUSTER_SQL = """
SELECT
    cluster_id,
    COUNT(*)::int                                                   AS point_count,
    SUM(vehicle_count)::int                                         AS sum_vehicle_count,
    AVG(vehicle_count)::float                                       AS avg_vehicle_count,
    AVG(avg_speed)::float                                           AS avg_speed_kmh,
    MIN(avg_speed)::float                                           AS min_speed_kmh,
    MAX(avg_speed)::float                                           AS max_speed_kmh,
    ST_Y(ST_Transform(ST_Centroid(ST_Collect(geom)), 4326))::float  AS centroid_lat,
    ST_X(ST_Transform(ST_Centroid(ST_Collect(geom)), 4326))::float  AS centroid_lon
FROM (
    SELECT
        vehicle_count,
        avg_speed,
        geom,
        COALESCE(
            ST_ClusterDBSCAN(geom, eps := {eps}, minpoints := {minpts}) OVER (),
            -1
        ) AS cluster_id
    FROM ibb_traffic_density
    WHERE record_time >= $1::timestamp
      AND record_time <  $2::timestamp
      AND avg_speed < {max_speed}
) sub
WHERE cluster_id >= 0
GROUP BY cluster_id
ORDER BY sum_vehicle_count DESC;
""".format(
    eps=_TEMPORAL_EPS_METERS,
    minpts=_TEMPORAL_MINPOINTS,
    max_speed=_TEMPORAL_MAX_AVG_SPEED,
)

_TEMPORAL_CLUSTER_SQL_WITH_BBOX = """
SELECT
    cluster_id,
    COUNT(*)::int                                                   AS point_count,
    SUM(vehicle_count)::int                                         AS sum_vehicle_count,
    AVG(vehicle_count)::float                                       AS avg_vehicle_count,
    AVG(avg_speed)::float                                           AS avg_speed_kmh,
    MIN(avg_speed)::float                                           AS min_speed_kmh,
    MAX(avg_speed)::float                                           AS max_speed_kmh,
    ST_Y(ST_Transform(ST_Centroid(ST_Collect(geom)), 4326))::float  AS centroid_lat,
    ST_X(ST_Transform(ST_Centroid(ST_Collect(geom)), 4326))::float  AS centroid_lon
FROM (
    SELECT
        vehicle_count,
        avg_speed,
        geom,
        COALESCE(
            ST_ClusterDBSCAN(geom, eps := {eps}, minpoints := {minpts}) OVER (),
            -1
        ) AS cluster_id
    FROM ibb_traffic_density
    WHERE record_time >= $1::timestamp
      AND record_time <  $2::timestamp
      AND avg_speed < {max_speed}
      AND ST_Intersects(
          geom,
          ST_Transform(ST_MakeEnvelope($3, $4, $5, $6, 4326), 32636)
      )
) sub
WHERE cluster_id >= 0
GROUP BY cluster_id
ORDER BY sum_vehicle_count DESC;
""".format(
    eps=_TEMPORAL_EPS_METERS,
    minpts=_TEMPORAL_MINPOINTS,
    max_speed=_TEMPORAL_MAX_AVG_SPEED,
)

# District mode: filter the one-hour slice by the real district polygon BEFORE
# DBSCAN, then cluster the district subset. The polygon comes from
# istanbul_district_boundaries (EPSG:32636, same as ibb_traffic_density.geom).
#
# The one-hour window (~2.5k rows) is by far the most selective filter, so we
# force it to run first via a MATERIALIZED CTE driven by the record_time index.
# Without MATERIALIZED, the planner mis-estimates the spatial join and drives
# off the GiST index instead, scanning the whole table's history for the
# district bbox and timing out. The district polygon is materialized once and
# the && / ST_Intersects pair then refines the small hour slice.
_TEMPORAL_CLUSTER_SQL_WITH_DISTRICT = """
WITH hour_slice AS MATERIALIZED (
    SELECT vehicle_count, avg_speed, geom
    FROM ibb_traffic_density
    WHERE record_time >= $1::timestamp
      AND record_time <  $2::timestamp
      AND avg_speed < {max_speed}
),
dist AS MATERIALIZED (
    SELECT geom
    FROM istanbul_district_boundaries
    WHERE district_key = $3
)
SELECT
    cluster_id,
    COUNT(*)::int                                                   AS point_count,
    SUM(vehicle_count)::int                                         AS sum_vehicle_count,
    AVG(vehicle_count)::float                                       AS avg_vehicle_count,
    AVG(avg_speed)::float                                           AS avg_speed_kmh,
    MIN(avg_speed)::float                                           AS min_speed_kmh,
    MAX(avg_speed)::float                                           AS max_speed_kmh,
    ST_Y(ST_Transform(ST_Centroid(ST_Collect(geom)), 4326))::float  AS centroid_lat,
    ST_X(ST_Transform(ST_Centroid(ST_Collect(geom)), 4326))::float  AS centroid_lon
FROM (
    SELECT
        h.vehicle_count,
        h.avg_speed,
        h.geom,
        COALESCE(
            ST_ClusterDBSCAN(h.geom, eps := {eps}, minpoints := {minpts}) OVER (),
            -1
        ) AS cluster_id
    FROM hour_slice h
    JOIN dist d
      ON h.geom && d.geom
     AND ST_Intersects(h.geom, d.geom)
) sub
WHERE cluster_id >= 0
GROUP BY cluster_id
ORDER BY sum_vehicle_count DESC;
""".format(
    eps=_TEMPORAL_EPS_METERS,
    minpts=_TEMPORAL_MINPOINTS,
    max_speed=_TEMPORAL_MAX_AVG_SPEED,
)

# Volume gates derived from 2025-01-17 hourly validation (cluster sum_vehicle_count).
# Hour 18 peak cluster ~15k vehicles; hour 23 peak ~1.9k; smallest meaningful
# HIGH candidate had 191 vehicles at 14.5 km/h.
_HIGH_ABS_MIN_VOLUME = 500
_MEDIUM_ABS_MIN_VOLUME = 120
_VOLUME_ABS_FLOOR = 80
_SEVERE_SLOWDOWN_MAX_SPEED = 14.5
_SEVERE_SLOWDOWN_MIN_VOLUME = 180


def _linear_inverse(value: float, low: float, high: float) -> float:
    """Return 1.0 at *low*, 0.0 at *high*, linear between."""
    if value <= low:
        return 1.0
    if value >= high:
        return 0.0
    return (high - value) / (high - low)


def speed_component(avg_speed_kmh: float) -> float:
    """Higher when average cluster speed is low (10 km/h worst, 30 km/h free)."""
    return _linear_inverse(avg_speed_kmh, 10.0, 30.0)


def min_speed_component(min_speed_kmh: float) -> float:
    """Captures locally severe slowdowns inside a cluster."""
    return _linear_inverse(min_speed_kmh, 8.0, 25.0)


def coverage_component(point_count: int) -> float:
    """Larger clusters carry more spatial confidence."""
    if point_count <= 1:
        return 0.35
    if point_count == 2:
        return 0.60
    if point_count <= 4:
        return 0.75
    if point_count <= 8:
        return 0.90
    return 1.0


def volume_component(
    sum_vehicle_count: int,
    hour_vol_p75: float,
    hour_vol_max: int,
) -> float:
    """Log-scaled hourly-relative volume with an absolute low-volume guard."""
    if sum_vehicle_count < _VOLUME_ABS_FLOOR:
        return 0.08 + 0.22 * (sum_vehicle_count / _VOLUME_ABS_FLOOR)

    log_v = math.log1p(sum_vehicle_count)
    ref = max(math.log1p(hour_vol_p75), math.log1p(500))
    rel = min(1.0, log_v / ref) if ref > 0 else 0.0

    if hour_vol_max > 0 and sum_vehicle_count >= hour_vol_max * 0.5:
        rel = min(1.0, rel * 1.05)
    return rel


def compute_congestion_score(
    avg_speed_kmh: float,
    min_speed_kmh: float | None,
    sum_vehicle_count: int,
    point_count: int,
    hour_vol_p75: float,
    hour_vol_max: int,
) -> float:
    """Explainable 0-100 traffic intensity score for a single-hour cluster."""
    if min_speed_kmh is not None:
        w_speed, w_vol, w_cov, w_min = 0.45, 0.35, 0.15, 0.05
        min_c = min_speed_component(min_speed_kmh)
    else:
        w_speed, w_vol, w_cov, w_min = 0.50, 0.35, 0.15, 0.0
        min_c = 0.0

    raw = (
        w_speed * speed_component(avg_speed_kmh)
        + w_vol * volume_component(sum_vehicle_count, hour_vol_p75, hour_vol_max)
        + w_cov * coverage_component(point_count)
        + w_min * min_c
    )
    return round(100 * raw, 1)


def assign_congestion_severity(
    congestion_score: float,
    avg_speed_kmh: float,
    min_speed_kmh: float | None,
    sum_vehicle_count: int,
    point_count: int,
) -> tuple[str, str]:
    """Map congestion_score and guards to LOW | MEDIUM | HIGH."""
    effective_min = min_speed_kmh if min_speed_kmh is not None else avg_speed_kmh

    if (
        avg_speed_kmh <= _SEVERE_SLOWDOWN_MAX_SPEED
        and sum_vehicle_count >= _SEVERE_SLOWDOWN_MIN_VOLUME
        and point_count >= 2
    ):
        return "HIGH", "severe_slowdown_high_volume"

    if (
        congestion_score >= 75
        and avg_speed_kmh < 22
        and sum_vehicle_count >= _HIGH_ABS_MIN_VOLUME
        and point_count >= 2
    ):
        return "HIGH", "high_congestion_score"

    if effective_min <= 11 and sum_vehicle_count >= 300 and point_count >= 2:
        return "HIGH", "severe_local_slowdown"

    if (
        congestion_score >= 48
        and avg_speed_kmh < 27
        and sum_vehicle_count >= _MEDIUM_ABS_MIN_VOLUME
    ):
        return "MEDIUM", "moderate_congestion_score"

    return "LOW", "light_congestion_cluster"


def score_temporal_clusters(rows: list[dict]) -> list[dict]:
    """Attach congestion_score, severity, and severity_reason to cluster rows."""
    if not rows:
        return []

    volumes = [int(r["sum_vehicle_count"]) for r in rows]
    hour_vol_max = max(volumes)
    if len(volumes) >= 4:
        hour_vol_p75 = quantiles(volumes, n=4)[2]
    elif len(volumes) == 1:
        hour_vol_p75 = float(volumes[0])
    else:
        hour_vol_p75 = sorted(volumes)[len(volumes) // 2]

    scored = []
    for r in rows:
        congestion_score = compute_congestion_score(
            avg_speed_kmh=float(r["avg_speed_kmh"]),
            min_speed_kmh=float(r["min_speed_kmh"]) if r.get("min_speed_kmh") is not None else None,
            sum_vehicle_count=int(r["sum_vehicle_count"]),
            point_count=int(r["point_count"]),
            hour_vol_p75=hour_vol_p75,
            hour_vol_max=hour_vol_max,
        )
        severity, severity_reason = assign_congestion_severity(
            congestion_score=congestion_score,
            avg_speed_kmh=float(r["avg_speed_kmh"]),
            min_speed_kmh=float(r["min_speed_kmh"]) if r.get("min_speed_kmh") is not None else None,
            sum_vehicle_count=int(r["sum_vehicle_count"]),
            point_count=int(r["point_count"]),
        )
        scored.append(
            {
                **r,
                "congestion_score": congestion_score,
                "severity": severity,
                "severity_reason": severity_reason,
            }
        )
    return scored


def assign_hourly_severity(avg_speed_kmh: float) -> str:
    """Deprecated speed-only helper kept for backward-compatible imports/tests."""
    if avg_speed_kmh < 15:
        return "HIGH"
    if avg_speed_kmh < 20:
        return "MEDIUM"
    return "LOW"


async def _ensure_district_available(conn, district_key: str) -> None:
    """Validate that district boundaries are loaded and the key exists.

    Raises
    ------
    DistrictBoundariesNotImported
        If the boundary table is missing or empty.
    UnknownDistrict
        If the boundary table exists but has no row for *district_key*.
    """
    table_exists = await conn.fetchval(
        """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = 'istanbul_district_boundaries'
        )
        """
    )
    if not table_exists:
        raise DistrictBoundariesNotImported(DISTRICT_NOT_IMPORTED_MSG)

    total = await conn.fetchval("SELECT COUNT(*) FROM istanbul_district_boundaries")
    if not total:
        raise DistrictBoundariesNotImported(DISTRICT_NOT_IMPORTED_MSG)

    exists = await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM istanbul_district_boundaries WHERE district_key = $1)",
        district_key,
    )
    if not exists:
        raise UnknownDistrict(f"Unknown district: {district_key}")


async def get_district_boundary(district_key: str) -> dict:
    """Return the imported district polygon as a WGS84 GeoJSON Feature.

    The geometry is transformed from EPSG:32636 back to WGS84 (EPSG:4326) for
    Leaflet display. Raises DistrictBoundariesNotImported (table missing/empty)
    or UnknownDistrict (no such key); the router maps those to 503/404.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        await _ensure_district_available(conn, district_key)
        row = await conn.fetchrow(
            """
            SELECT district_name,
                   ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geojson
            FROM istanbul_district_boundaries
            WHERE district_key = $1
            """,
            district_key,
        )

    # _ensure_district_available guarantees a matching row exists here.
    return {
        "type": "Feature",
        "geometry": json.loads(row["geojson"]),
        "properties": {
            "district_key": district_key,
            "district_name": row["district_name"],
        },
    }


async def get_temporal_clusters(
    date_str: str,
    hour: int,
    bbox: tuple[float, float, float, float] | None = None,
    district: str | None = None,
) -> TemporalClusterResponse:
    """Run spatial DBSCAN on a single-hour slice and return clustered GeoJSON.

    When *district* is given, traffic measurements are filtered by the real
    district polygon (from istanbul_district_boundaries) BEFORE clustering, so
    DBSCAN is recalculated on the district subset. *bbox* and *district* are
    mutually exclusive and the caller is expected to enforce that.

    Parameters
    ----------
    date_str : str
        Date in YYYY-MM-DD format.
    hour : int
        Hour of day (0-23).
    bbox : tuple[float, float, float, float], optional
        WGS84 bbox as min_lon, min_lat, max_lon, max_lat.
    district : str, optional
        Normalized district key (e.g. "besiktas") to filter by real polygon.

    Returns
    -------
    TemporalClusterResponse
        GeoJSON FeatureCollection with per-cluster centroids, severity,
        vehicle counts, and response metadata.
    """
    if hour < 0 or hour > 23:
        raise ValueError("Invalid hour. Expected an integer between 0 and 23.")

    try:
        selected_date = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(
            "Invalid date. Expected a real calendar date in YYYY-MM-DD format."
        ) from exc

    start_ts = selected_date.replace(hour=hour, minute=0, second=0, microsecond=0)
    end_ts = start_ts + timedelta(hours=1)

    pool = await get_pool()
    t0 = time.monotonic()
    async with pool.acquire() as conn:
        if district is not None:
            await _ensure_district_available(conn, district)
            rows = await conn.fetch(
                _TEMPORAL_CLUSTER_SQL_WITH_DISTRICT, start_ts, end_ts, district
            )
        elif bbox is None:
            rows = await conn.fetch(_TEMPORAL_CLUSTER_SQL, start_ts, end_ts)
        else:
            rows = await conn.fetch(
                _TEMPORAL_CLUSTER_SQL_WITH_BBOX,
                start_ts,
                end_ts,
                bbox[0],
                bbox[1],
                bbox[2],
                bbox[3],
            )
    elapsed_ms = (time.monotonic() - t0) * 1000

    logger.info(
        "Temporal clustering: date=%s hour=%02d district=%s clusters=%d elapsed=%.1fms",
        date_str, hour, district or "-", len(rows), elapsed_ms,
    )

    if not rows:
        return TemporalClusterResponse(
            date=date_str,
            hour=hour,
            cluster_count=0,
            features=[],
        )

    scored_rows = score_temporal_clusters([dict(r) for r in rows])

    features = []
    total_points = 0
    total_vehicles = 0
    speed_sum = 0.0
    high_count = 0
    medium_count = 0
    low_count = 0

    for r in scored_rows:
        severity = r["severity"]
        if severity == "HIGH":
            high_count += 1
        elif severity == "MEDIUM":
            medium_count += 1
        else:
            low_count += 1

        total_points += r["point_count"]
        total_vehicles += r["sum_vehicle_count"]
        speed_sum += r["avg_speed_kmh"] * r["point_count"]

        feature = TemporalGeoJSONFeature(
            geometry=GeoJSONGeometry(
                coordinates=[r["centroid_lon"], r["centroid_lat"]]
            ),
            properties=TemporalClusterProperties(
                cluster_id=r["cluster_id"],
                severity=severity,
                congestion_score=r["congestion_score"],
                point_count=r["point_count"],
                avg_speed_kmh=round(r["avg_speed_kmh"], 1),
                min_speed_kmh=round(r["min_speed_kmh"], 1)
                if r.get("min_speed_kmh") is not None
                else None,
                max_speed_kmh=round(r["max_speed_kmh"], 1)
                if r.get("max_speed_kmh") is not None
                else None,
                sum_vehicle_count=r["sum_vehicle_count"],
                avg_vehicle_count=round(r["avg_vehicle_count"], 1),
                severity_reason=r["severity_reason"],
            ),
        )
        features.append(feature)

    overall_avg_speed = round(speed_sum / total_points, 1) if total_points > 0 else None

    return TemporalClusterResponse(
        date=date_str,
        hour=hour,
        cluster_count=len(features),
        total_points=total_points,
        total_vehicles=total_vehicles,
        avg_speed=overall_avg_speed,
        high_count=high_count,
        medium_count=medium_count,
        low_count=low_count,
        features=features,
    )
