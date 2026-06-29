"""
Geohash measurement-cell layer.

Unlike the cluster centroid markers (which are abstract DBSCAN cluster centers),
this layer exposes the real İBB measurement cells for the selected time window
as their rectangular geohash bounding boxes. It visualizes the raw spatial
resolution of the source data — the same avg_speed-filtered measurements that
feed PostGIS ST_ClusterDBSCAN — so the pipeline "raw cells → clustering →
centroids" can be shown directly on the map.

Geometry is built in Python from the stored geohash string via the shared,
pure ``decode_geohash_bounds`` helper, so no extra PostGIS geohash functions
are required. All user-supplied numbers flow into SQL as bind parameters.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta

from backend.app.database import get_pool
from backend.app.models.cluster import (
    GeohashCellFeature,
    GeohashCellGeometry,
    GeohashCellProperties,
    GeohashCellResponse,
)
from backend.app.services.cluster_service import (
    _TEMPORAL_MAX_AVG_SPEED,
    _TEMPORAL_WINDOW_HOURS,
    _ensure_district_available,
)
from scoring.geohash_utils import decode_geohash_bounds

logger = logging.getLogger(__name__)

# Defaults are shared with the clustering pipeline so the cells shown match the
# measurements DBSCAN clusters by default.
_DEFAULT_AVG_SPEED_THRESHOLD = _TEMPORAL_MAX_AVG_SPEED
_DEFAULT_WINDOW_HOURS = _TEMPORAL_WINDOW_HOURS

# Hard cap on returned cells to keep the frontend responsive. We fetch one extra
# row to detect (and report) truncation. Cells are ordered by vehicle volume so
# the most significant ones survive the cap.
_GEOHASH_CELL_LIMIT = 4000
_FETCH_CAP = _GEOHASH_CELL_LIMIT + 1

# Bind-parameter layout:
#   $1 = window start (timestamp)   $3 = avg_speed threshold (km/h)
#   $2 = window end   (timestamp)
# Region-specific params follow: bbox uses $4..$7, district uses $4.
_GEOHASH_CELL_SQL = f"""
SELECT
    geohash,
    COUNT(*)::int             AS measurement_count,
    AVG(avg_speed)::float     AS avg_speed_kmh,
    MIN(avg_speed)::float     AS min_speed_kmh,
    MAX(avg_speed)::float     AS max_speed_kmh,
    SUM(vehicle_count)::int   AS sum_vehicle_count,
    AVG(vehicle_count)::float AS avg_vehicle_count
FROM ibb_traffic_density
WHERE record_time >= $1::timestamp
  AND record_time <  $2::timestamp
  AND avg_speed < $3::double precision
  AND geohash IS NOT NULL
  AND geohash <> ''
GROUP BY geohash
ORDER BY sum_vehicle_count DESC
LIMIT {_FETCH_CAP};
"""

# Materialize the selective one-hour slice first, then apply the bbox filter to
# that small set — mirroring the cluster service. This keeps the parameterized
# (generic-plan) query plan-stable and fast for wide bboxes instead of letting
# the planner drive a GiST scan over all history. See cluster_service for the
# full rationale. Results are unchanged.
_GEOHASH_CELL_SQL_WITH_BBOX = f"""
WITH hour_slice AS MATERIALIZED (
    SELECT geohash, avg_speed, vehicle_count, geom
    FROM ibb_traffic_density
    WHERE record_time >= $1::timestamp
      AND record_time <  $2::timestamp
      AND avg_speed < $3::double precision
      AND geohash IS NOT NULL
      AND geohash <> ''
)
SELECT
    geohash,
    COUNT(*)::int             AS measurement_count,
    AVG(avg_speed)::float     AS avg_speed_kmh,
    MIN(avg_speed)::float     AS min_speed_kmh,
    MAX(avg_speed)::float     AS max_speed_kmh,
    SUM(vehicle_count)::int   AS sum_vehicle_count,
    AVG(vehicle_count)::float AS avg_vehicle_count
FROM hour_slice
WHERE ST_Intersects(
      geom,
      ST_Transform(ST_MakeEnvelope($4, $5, $6, $7, 4326), 32636)
  )
GROUP BY geohash
ORDER BY sum_vehicle_count DESC
LIMIT {_FETCH_CAP};
"""

# District mode mirrors the cluster service: filter the window slice by the real
# district polygon BEFORE aggregating. The MATERIALIZED CTEs force the very
# selective time-window scan to run first (see cluster_service for the rationale).
_GEOHASH_CELL_SQL_WITH_DISTRICT = f"""
WITH hour_slice AS MATERIALIZED (
    SELECT geohash, avg_speed, vehicle_count, geom
    FROM ibb_traffic_density
    WHERE record_time >= $1::timestamp
      AND record_time <  $2::timestamp
      AND avg_speed < $3::double precision
      AND geohash IS NOT NULL
      AND geohash <> ''
),
dist AS MATERIALIZED (
    SELECT geom
    FROM istanbul_district_boundaries
    WHERE district_key = $4
)
SELECT
    h.geohash                   AS geohash,
    COUNT(*)::int               AS measurement_count,
    AVG(h.avg_speed)::float     AS avg_speed_kmh,
    MIN(h.avg_speed)::float     AS min_speed_kmh,
    MAX(h.avg_speed)::float     AS max_speed_kmh,
    SUM(h.vehicle_count)::int   AS sum_vehicle_count,
    AVG(h.vehicle_count)::float AS avg_vehicle_count
FROM hour_slice h
JOIN dist d
  ON h.geom && d.geom
 AND ST_Intersects(h.geom, d.geom)
GROUP BY h.geohash
ORDER BY sum_vehicle_count DESC
LIMIT {_FETCH_CAP};
"""


def _geohash_to_polygon(gh: str) -> list[list[list[float]]]:
    """Return a closed GeoJSON polygon ring for a geohash cell (WGS84, lon/lat).

    Raises ValueError for an invalid geohash string (propagated from
    decode_geohash_bounds), which the caller skips defensively.
    """
    min_lat, min_lon, max_lat, max_lon = decode_geohash_bounds(gh)
    ring = [
        [min_lon, min_lat],
        [max_lon, min_lat],
        [max_lon, max_lat],
        [min_lon, max_lat],
        [min_lon, min_lat],  # close the ring
    ]
    return [ring]


async def get_geohash_cells(
    date_str: str,
    hour: int,
    bbox: tuple[float, float, float, float] | None = None,
    district: str | None = None,
    *,
    avg_speed_threshold: float = _DEFAULT_AVG_SPEED_THRESHOLD,
    window_hours: int = _DEFAULT_WINDOW_HOURS,
) -> GeohashCellResponse:
    """Aggregate the window's measurements by geohash and return cell polygons.

    The filter mirrors the clustering pipeline (time window + avg_speed
    threshold + optional region), so the returned cells are exactly the
    measurements DBSCAN would cluster for the same parameters. *bbox* and
    *district* are mutually exclusive; the caller enforces that.

    Returns a GeoJSON FeatureCollection of Polygon features (one per geohash
    cell), each carrying aggregated speed/volume stats.
    """
    if hour < 0 or hour > 23:
        raise ValueError("Invalid hour. Expected an integer between 0 and 23.")
    if avg_speed_threshold <= 0:
        raise ValueError("Invalid avg_speed_threshold. Expected a positive speed in km/h.")
    if window_hours < 1:
        raise ValueError("Invalid window_hours. Expected an integer >= 1.")

    try:
        selected_date = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(
            "Invalid date. Expected a real calendar date in YYYY-MM-DD format."
        ) from exc

    start_ts = selected_date.replace(hour=hour, minute=0, second=0, microsecond=0)
    end_ts = start_ts + timedelta(hours=window_hours)

    pool = await get_pool()
    t0 = time.monotonic()
    async with pool.acquire() as conn:
        if district is not None:
            await _ensure_district_available(conn, district)
            rows = await conn.fetch(
                _GEOHASH_CELL_SQL_WITH_DISTRICT,
                start_ts, end_ts, avg_speed_threshold, district,
            )
        elif bbox is None:
            rows = await conn.fetch(
                _GEOHASH_CELL_SQL, start_ts, end_ts, avg_speed_threshold,
            )
        else:
            rows = await conn.fetch(
                _GEOHASH_CELL_SQL_WITH_BBOX,
                start_ts, end_ts, avg_speed_threshold,
                bbox[0], bbox[1], bbox[2], bbox[3],
            )
    elapsed_ms = (time.monotonic() - t0) * 1000

    truncated = len(rows) > _GEOHASH_CELL_LIMIT
    if truncated:
        rows = rows[:_GEOHASH_CELL_LIMIT]

    logger.info(
        "Geohash cells: date=%s hour=%02d window=%dh speed<%.1f district=%s "
        "cells=%d truncated=%s elapsed=%.1fms",
        date_str, hour, window_hours, avg_speed_threshold, district or "-",
        len(rows), truncated, elapsed_ms,
    )

    features: list[GeohashCellFeature] = []
    for r in rows:
        gh = r["geohash"]
        if not gh:
            continue
        try:
            coords = _geohash_to_polygon(gh)
        except ValueError:
            # Skip an unexpectedly malformed geohash rather than failing the layer.
            continue
        features.append(
            GeohashCellFeature(
                geometry=GeohashCellGeometry(coordinates=coords),
                properties=GeohashCellProperties(
                    geohash=gh,
                    measurement_count=r["measurement_count"],
                    avg_speed_kmh=round(r["avg_speed_kmh"], 1),
                    min_speed_kmh=round(r["min_speed_kmh"], 1),
                    max_speed_kmh=round(r["max_speed_kmh"], 1),
                    sum_vehicle_count=r["sum_vehicle_count"],
                    avg_vehicle_count=round(r["avg_vehicle_count"], 1),
                ),
            )
        )

    return GeohashCellResponse(
        date=date_str,
        hour=hour,
        window_hours=window_hours,
        avg_speed_threshold=avg_speed_threshold,
        cell_count=len(features),
        truncated=truncated,
        features=features,
    )
