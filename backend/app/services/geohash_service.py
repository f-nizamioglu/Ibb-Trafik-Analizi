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
from typing import Literal

from backend.app.database import get_pool
from backend.app.models.cluster import (
    GeohashCellFeature,
    GeohashCellGeometry,
    GeohashCellProperties,
    GeohashCellResponse,
    SeverityCounts,
)
from backend.app.services.cluster_service import (
    _TEMPORAL_MAX_AVG_SPEED,
    _TEMPORAL_WINDOW_HOURS,
    _ensure_district_available,
)
from scoring.geohash_utils import decode_geohash_bounds

logger = logging.getLogger(__name__)

CellLayerMode = Literal["congested", "all"]

# Defaults are shared with the clustering pipeline so the cells shown match the
# measurements DBSCAN clusters by default.
_DEFAULT_AVG_SPEED_THRESHOLD = _TEMPORAL_MAX_AVG_SPEED
_DEFAULT_WINDOW_HOURS = _TEMPORAL_WINDOW_HOURS

# Hard cap on returned cells to keep the frontend responsive. We fetch one extra
# row to detect (and report) truncation. Cells are ordered by vehicle volume so
# the most significant ones survive the cap.
_GEOHASH_CELL_LIMIT = 4000
_FETCH_CAP = _GEOHASH_CELL_LIMIT + 1

_CELL_SEVERITY_META = {
    "free_flow": {"label": "Akıcı", "color": "#66bb6a"},
    "moderate": {"label": "Orta", "color": "#ffee58"},
    "congested": {"label": "Yoğun", "color": "#ffa726"},
    "severe": {"label": "Çok yoğun", "color": "#ef5350"},
}

# Bind-parameter layout:
#   $1 = window start (timestamp)   $3 = mode ('congested' | 'all')
#   $2 = window end   (timestamp)   $4 = avg_speed threshold (km/h)
# Region-specific params follow: bbox uses $5..$8, district uses $5.
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
  AND ($3::text = 'all' OR avg_speed < $4::double precision)
  AND geohash IS NOT NULL
  AND geohash <> ''
GROUP BY geohash
ORDER BY sum_vehicle_count DESC, geohash ASC
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
      AND ($3::text = 'all' OR avg_speed < $4::double precision)
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
      ST_Transform(ST_MakeEnvelope($5, $6, $7, $8, 4326), 32636)
  )
GROUP BY geohash
ORDER BY sum_vehicle_count DESC, geohash ASC
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
      AND ($3::text = 'all' OR avg_speed < $4::double precision)
      AND geohash IS NOT NULL
      AND geohash <> ''
),
dist AS MATERIALIZED (
    SELECT geom
    FROM istanbul_district_boundaries
    WHERE district_key = $5
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
ORDER BY sum_vehicle_count DESC, h.geohash ASC
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


def classify_cell_severity(avg_speed_kmh: float) -> str:
    """Speed-only MVP classification for one measurement cell."""
    if avg_speed_kmh < 20:
        return "severe"
    if avg_speed_kmh < 35:
        return "congested"
    if avg_speed_kmh < 50:
        return "moderate"
    return "free_flow"


def compute_cell_congestion_score(avg_speed_kmh: float) -> float:
    """Speed-derived 0-100 display score for cell coloring, not prediction."""
    if avg_speed_kmh <= 20:
        return 100.0
    if avg_speed_kmh >= 50:
        return 0.0
    return round(((50.0 - avg_speed_kmh) / 30.0) * 100.0, 1)


async def get_geohash_cells(
    date_str: str,
    hour: int,
    bbox: tuple[float, float, float, float] | None = None,
    district: str | None = None,
    *,
    avg_speed_threshold: float = _DEFAULT_AVG_SPEED_THRESHOLD,
    window_hours: int = _DEFAULT_WINDOW_HOURS,
    mode: CellLayerMode = "congested",
    max_cells: int = _GEOHASH_CELL_LIMIT,
) -> GeohashCellResponse:
    """Aggregate the window's measurements by geohash and return cell polygons.

    Default ``mode="congested"`` preserves the original behavior: only cells
    below the avg_speed threshold are returned. ``mode="all"`` returns every
    measured cell in the scoped window and classifies it by avg_speed. *bbox*
    and *district* are mutually exclusive; the caller enforces that.

    Returns a GeoJSON FeatureCollection of Polygon features (one per geohash
    cell), each carrying aggregated speed/volume stats.
    """
    if hour < 0 or hour > 23:
        raise ValueError("Invalid hour. Expected an integer between 0 and 23.")
    if avg_speed_threshold <= 0:
        raise ValueError("Invalid avg_speed_threshold. Expected a positive speed in km/h.")
    if window_hours < 1:
        raise ValueError("Invalid window_hours. Expected an integer >= 1.")
    if mode not in ("congested", "all"):
        raise ValueError("Invalid mode. Expected 'congested' or 'all'.")
    if max_cells < 1:
        raise ValueError("Invalid max_cells. Expected an integer >= 1.")
    if mode == "all" and bbox is None and district is None:
        raise ValueError("All-cell geohash mode requires bbox or district selection.")

    try:
        selected_date = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(
            "Invalid date. Expected a real calendar date in YYYY-MM-DD format."
        ) from exc

    start_ts = selected_date.replace(hour=hour, minute=0, second=0, microsecond=0)
    end_ts = start_ts + timedelta(hours=window_hours)
    response_limit = min(max_cells, _GEOHASH_CELL_LIMIT)

    pool = await get_pool()
    t0 = time.monotonic()
    async with pool.acquire() as conn:
        if district is not None:
            await _ensure_district_available(conn, district)
            rows = await conn.fetch(
                _GEOHASH_CELL_SQL_WITH_DISTRICT,
                start_ts, end_ts, mode, avg_speed_threshold, district,
            )
        elif bbox is None:
            rows = await conn.fetch(
                _GEOHASH_CELL_SQL, start_ts, end_ts, mode, avg_speed_threshold,
            )
        else:
            rows = await conn.fetch(
                _GEOHASH_CELL_SQL_WITH_BBOX,
                start_ts, end_ts, mode, avg_speed_threshold,
                bbox[0], bbox[1], bbox[2], bbox[3],
            )
    elapsed_ms = (time.monotonic() - t0) * 1000

    truncated = len(rows) > response_limit
    if truncated:
        rows = rows[:response_limit]

    logger.info(
        "Geohash cells: date=%s hour=%02d window=%dh mode=%s speed<%.1f "
        "district=%s cells=%d truncated=%s elapsed=%.1fms",
        date_str,
        hour,
        window_hours,
        mode,
        avg_speed_threshold,
        district or "-",
        len(rows),
        truncated,
        elapsed_ms,
    )

    features: list[GeohashCellFeature] = []
    for r in rows:
        gh = r["geohash"]
        if not gh:
            continue
        if r["avg_speed_kmh"] is None:
            continue
        try:
            coords = _geohash_to_polygon(gh)
        except ValueError:
            # Skip an unexpectedly malformed geohash rather than failing the layer.
            continue
        avg_speed = float(r["avg_speed_kmh"])
        severity = classify_cell_severity(avg_speed)
        meta = _CELL_SEVERITY_META[severity]
        measurement_count = r["measurement_count"]
        features.append(
            GeohashCellFeature(
                geometry=GeohashCellGeometry(coordinates=coords),
                properties=GeohashCellProperties(
                    geohash=gh,
                    measurement_count=measurement_count,
                    record_count=measurement_count,
                    avg_speed_kmh=round(avg_speed, 1),
                    min_speed_kmh=round(r["min_speed_kmh"], 1),
                    max_speed_kmh=round(r["max_speed_kmh"], 1),
                    sum_vehicle_count=r["sum_vehicle_count"],
                    avg_vehicle_count=round(r["avg_vehicle_count"], 1),
                    congestion_score=compute_cell_congestion_score(avg_speed),
                    severity=severity,
                    severity_label=meta["label"],
                    color=meta["color"],
                ),
            )
        )

    sev_counts = {"free_flow": 0, "moderate": 0, "congested": 0, "severe": 0}
    for f in features:
        sev_counts[f.properties.severity] += 1

    # total_cells: approximate count including cells dropped by the cap.
    # When truncated, the DB returned _FETCH_CAP rows (limit+1), so the real
    # total is at least that many; use len(rows) before capping as the best
    # available approximation.
    total_matched = len(features) + (1 if truncated else 0)

    return GeohashCellResponse(
        date=date_str,
        hour=hour,
        mode=mode,
        window_hours=window_hours,
        avg_speed_threshold=avg_speed_threshold if mode == "congested" else None,
        max_cells=response_limit,
        cell_count=len(features),
        total_cells=total_matched,
        returned_cells=len(features),
        severity_counts=SeverityCounts(**sev_counts),
        truncated=truncated,
        features=features,
    )
