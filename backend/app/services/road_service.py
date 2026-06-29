"""
Road overlay for congested geohash measurement cells.

The layer is deliberately opt-in and scope-limited. It reuses the same
time-window, speed-threshold, and spatial pre-filters as the geohash-cell layer,
then clips imported road_segments geometry to the selected congested cell area.
"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, timedelta
from typing import Literal

from backend.app.database import get_pool
from backend.app.models.cluster import (
    CongestedRoadResponse,
    RoadLineFeature,
    RoadLineGeometry,
    RoadLineProperties,
)
from backend.app.services.cluster_service import (
    _TEMPORAL_MAX_AVG_SPEED,
    _TEMPORAL_WINDOW_HOURS,
    _ensure_district_available,
)

logger = logging.getLogger(__name__)

ROAD_NETWORK_NOT_IMPORTED_MSG = (
    "Road network LineString data is not imported. "
    "Run python scripts/create_road_schema.py and "
    "python scripts/import_road_network.py --input data/road_network/Istanbul.osm.pbf."
)


class RoadNetworkUnavailable(RuntimeError):
    """Raised when the imported road LineString schema/data is unavailable."""


_DEFAULT_AVG_SPEED_THRESHOLD = _TEMPORAL_MAX_AVG_SPEED
_DEFAULT_WINDOW_HOURS = _TEMPORAL_WINDOW_HOURS
_ROAD_LIMIT = 1000
_CORRIDOR_ROAD_LIMIT = 60
_CORRIDOR_NAME_PATTERN = (
    r"(D[- ]?100|E[- ]?5|O[- ]?[0-9]+|E80|E881|"
    r"(^|[^0-9A-Za-zÇĞİÖŞÜçğıöşü])TEM([^0-9A-Za-zÇĞİÖŞÜçğıöşü]|$)|"
    r"Çevre Yolu|Cevre Yolu|Otoyolu|Köprüsü|Koprusu|Tüneli|Tuneli)"
)
_CORRIDOR_BASE_HIGHWAYS = ("motorway", "trunk")
_CORRIDOR_NAME_HIGHWAYS = (
    "primary",
    "motorway_link",
    "trunk_link",
    "primary_link",
    "secondary_link",
)
_MAIN_ROAD_HIGHWAYS = (
    *_CORRIDOR_BASE_HIGHWAYS,
    *_CORRIDOR_NAME_HIGHWAYS,
)

RoadLevel = Literal["main", "all"]


def _matches_corridor_name(name: str | None) -> bool:
    """Return whether a road name looks like a recognizable traffic corridor."""
    return bool(name and re.search(_CORRIDOR_NAME_PATTERN, name, re.IGNORECASE))


_ROAD_SQL_WITH_BBOX_MAIN = """
WITH region AS MATERIALIZED (
    SELECT ST_Transform(ST_MakeEnvelope($4, $5, $6, $7, 4326), 32636) AS geom_32636
),
hour_slice AS MATERIALIZED (
    SELECT geohash, avg_speed, vehicle_count, geom
    FROM ibb_traffic_density
    WHERE record_time >= $1::timestamp
      AND record_time <  $2::timestamp
      AND avg_speed < $3::double precision
      AND geohash IS NOT NULL
      AND geohash <> ''
),
filtered_cells AS MATERIALIZED (
    SELECT
        gc.geohash,
        ST_CollectionExtract(ST_Intersection(gc.geom_32636, r.geom_32636), 3)
            AS geom_32636
    FROM hour_slice h
    JOIN geohash_cells gc ON gc.geohash = h.geohash
    CROSS JOIN region r
    WHERE h.geom && r.geom_32636
      AND ST_Intersects(h.geom, r.geom_32636)
      AND gc.geom_32636 && r.geom_32636
      AND ST_Intersects(gc.geom_32636, r.geom_32636)
    GROUP BY gc.geohash, gc.geom_32636, r.geom_32636
),
cell_area AS MATERIALIZED (
    SELECT ST_UnaryUnion(ST_Collect(geom_32636)) AS geom_32636
    FROM filtered_cells
    WHERE geom_32636 IS NOT NULL
      AND NOT ST_IsEmpty(geom_32636)
),
raw AS MATERIALIZED (
    SELECT
        r.id,
        r.osm_id,
        r.highway,
        CASE
            WHEN LOWER(BTRIM(r.name)) IN ('', 'nan') THEN NULL
            ELSE BTRIM(r.name)
        END AS name,
        ST_Multi(
            ST_CollectionExtract(
                ST_Intersection(ST_MakeValid(r.geom_32636), ca.geom_32636),
                2
            )
        ) AS geom_32636
    FROM road_segments r
    CROSS JOIN cell_area ca
    WHERE ca.geom_32636 IS NOT NULL
      AND r.geom_32636 IS NOT NULL
      AND r.geom_32636 && ca.geom_32636
      AND ST_Intersects(ST_MakeValid(r.geom_32636), ca.geom_32636)
      AND (
          r.highway = ANY($8::text[])
          OR r.highway = ANY($9::text[])
      )
),
eligible AS MATERIALIZED (
    SELECT
        i.*,
        ST_Length(i.geom_32636)::float AS segment_length_m,
        CASE
            WHEN i.highway = ANY($8::text[]) THEN 1
            WHEN i.highway = ANY($9::text[]) THEN 2
            ELSE 99
        END AS road_priority,
        CASE WHEN i.name ~* $10::text THEN 0 ELSE 1 END AS name_priority
    FROM raw i
    WHERE i.name IS NOT NULL
      AND NOT ST_IsEmpty(i.geom_32636)
      AND (
          i.highway = ANY($8::text[])
          OR (i.highway = ANY($9::text[]) AND i.name ~* $10::text)
      )
),
grouped AS MATERIALIZED (
    SELECT
        MIN(id) AS id,
        (ARRAY_AGG(osm_id ORDER BY road_priority, segment_length_m DESC, id))[1] AS osm_id,
        (ARRAY_AGG(highway ORDER BY road_priority, segment_length_m DESC, id))[1] AS highway,
        name,
        SUM(segment_length_m)::float AS clipped_length_m,
        MIN(road_priority) AS road_priority,
        MIN(name_priority) AS name_priority,
        ST_Multi(
            ST_CollectionExtract(
                ST_LineMerge(ST_UnaryUnion(ST_Collect(geom_32636))),
                2
            )
        ) AS geom_32636
    FROM eligible
    GROUP BY name
)
SELECT
    id,
    osm_id,
    highway,
    name,
    clipped_length_m,
    ST_AsGeoJSON(ST_Transform(geom_32636, 4326), 6) AS geojson
FROM grouped
WHERE NOT ST_IsEmpty(geom_32636)
ORDER BY
    road_priority ASC,
    name_priority ASC,
    clipped_length_m DESC,
    name ASC
LIMIT $11::int;
"""


_ROAD_SQL_WITH_DISTRICT_MAIN = """
WITH dist AS MATERIALIZED (
    SELECT geom AS geom_32636
    FROM istanbul_district_boundaries
    WHERE district_key = $4
),
hour_slice AS MATERIALIZED (
    SELECT geohash, avg_speed, vehicle_count, geom
    FROM ibb_traffic_density
    WHERE record_time >= $1::timestamp
      AND record_time <  $2::timestamp
      AND avg_speed < $3::double precision
      AND geohash IS NOT NULL
      AND geohash <> ''
),
filtered_cells AS MATERIALIZED (
    SELECT
        gc.geohash,
        ST_CollectionExtract(ST_Intersection(gc.geom_32636, d.geom_32636), 3)
            AS geom_32636
    FROM hour_slice h
    JOIN dist d
      ON h.geom && d.geom_32636
     AND ST_Intersects(h.geom, d.geom_32636)
    JOIN geohash_cells gc ON gc.geohash = h.geohash
    WHERE gc.geom_32636 && d.geom_32636
      AND ST_Intersects(gc.geom_32636, d.geom_32636)
    GROUP BY gc.geohash, gc.geom_32636, d.geom_32636
),
cell_area AS MATERIALIZED (
    SELECT ST_UnaryUnion(ST_Collect(geom_32636)) AS geom_32636
    FROM filtered_cells
    WHERE geom_32636 IS NOT NULL
      AND NOT ST_IsEmpty(geom_32636)
),
raw AS MATERIALIZED (
    SELECT
        r.id,
        r.osm_id,
        r.highway,
        CASE
            WHEN LOWER(BTRIM(r.name)) IN ('', 'nan') THEN NULL
            ELSE BTRIM(r.name)
        END AS name,
        ST_Multi(
            ST_CollectionExtract(
                ST_Intersection(ST_MakeValid(r.geom_32636), ca.geom_32636),
                2
            )
        ) AS geom_32636
    FROM road_segments r
    CROSS JOIN cell_area ca
    WHERE ca.geom_32636 IS NOT NULL
      AND r.geom_32636 IS NOT NULL
      AND r.geom_32636 && ca.geom_32636
      AND ST_Intersects(ST_MakeValid(r.geom_32636), ca.geom_32636)
      AND (
          r.highway = ANY($5::text[])
          OR r.highway = ANY($6::text[])
      )
),
eligible AS MATERIALIZED (
    SELECT
        i.*,
        ST_Length(i.geom_32636)::float AS segment_length_m,
        CASE
            WHEN i.highway = ANY($5::text[]) THEN 1
            WHEN i.highway = ANY($6::text[]) THEN 2
            ELSE 99
        END AS road_priority,
        CASE WHEN i.name ~* $7::text THEN 0 ELSE 1 END AS name_priority
    FROM raw i
    WHERE i.name IS NOT NULL
      AND NOT ST_IsEmpty(i.geom_32636)
      AND (
          i.highway = ANY($5::text[])
          OR (i.highway = ANY($6::text[]) AND i.name ~* $7::text)
      )
),
grouped AS MATERIALIZED (
    SELECT
        MIN(id) AS id,
        (ARRAY_AGG(osm_id ORDER BY road_priority, segment_length_m DESC, id))[1] AS osm_id,
        (ARRAY_AGG(highway ORDER BY road_priority, segment_length_m DESC, id))[1] AS highway,
        name,
        SUM(segment_length_m)::float AS clipped_length_m,
        MIN(road_priority) AS road_priority,
        MIN(name_priority) AS name_priority,
        ST_Multi(
            ST_CollectionExtract(
                ST_LineMerge(ST_UnaryUnion(ST_Collect(geom_32636))),
                2
            )
        ) AS geom_32636
    FROM eligible
    GROUP BY name
)
SELECT
    id,
    osm_id,
    highway,
    name,
    clipped_length_m,
    ST_AsGeoJSON(ST_Transform(geom_32636, 4326), 6) AS geojson
FROM grouped
WHERE NOT ST_IsEmpty(geom_32636)
ORDER BY
    road_priority ASC,
    name_priority ASC,
    clipped_length_m DESC,
    name ASC
LIMIT $8::int;
"""


_ROAD_SQL_WITH_BBOX_ALL = """
WITH region AS MATERIALIZED (
    SELECT ST_Transform(ST_MakeEnvelope($4, $5, $6, $7, 4326), 32636) AS geom_32636
),
hour_slice AS MATERIALIZED (
    SELECT geohash, avg_speed, vehicle_count, geom
    FROM ibb_traffic_density
    WHERE record_time >= $1::timestamp
      AND record_time <  $2::timestamp
      AND avg_speed < $3::double precision
      AND geohash IS NOT NULL
      AND geohash <> ''
),
filtered_cells AS MATERIALIZED (
    SELECT
        gc.geohash,
        ST_CollectionExtract(ST_Intersection(gc.geom_32636, r.geom_32636), 3)
            AS geom_32636
    FROM hour_slice h
    JOIN geohash_cells gc ON gc.geohash = h.geohash
    CROSS JOIN region r
    WHERE h.geom && r.geom_32636
      AND ST_Intersects(h.geom, r.geom_32636)
      AND gc.geom_32636 && r.geom_32636
      AND ST_Intersects(gc.geom_32636, r.geom_32636)
    GROUP BY gc.geohash, gc.geom_32636, r.geom_32636
),
cell_area AS MATERIALIZED (
    SELECT ST_UnaryUnion(ST_Collect(geom_32636)) AS geom_32636
    FROM filtered_cells
    WHERE geom_32636 IS NOT NULL
      AND NOT ST_IsEmpty(geom_32636)
),
intersected AS MATERIALIZED (
    SELECT
        r.id,
        r.osm_id,
        r.highway,
        CASE
            WHEN LOWER(BTRIM(r.name)) IN ('', 'nan') THEN NULL
            ELSE BTRIM(r.name)
        END AS name,
        ST_Multi(
            ST_CollectionExtract(
                ST_Intersection(ST_MakeValid(r.geom_32636), ca.geom_32636),
                2
            )
        ) AS geom_32636
    FROM road_segments r
    CROSS JOIN cell_area ca
    WHERE ca.geom_32636 IS NOT NULL
      AND r.geom_32636 IS NOT NULL
      AND r.geom_32636 && ca.geom_32636
      AND ST_Intersects(ST_MakeValid(r.geom_32636), ca.geom_32636)
)
SELECT
    id,
    osm_id,
    highway,
    name,
    ST_Length(geom_32636)::float AS clipped_length_m,
    ST_AsGeoJSON(ST_Transform(geom_32636, 4326), 6) AS geojson
FROM intersected
WHERE NOT ST_IsEmpty(geom_32636)
ORDER BY clipped_length_m DESC, id ASC
LIMIT $8::int;
"""


_ROAD_SQL_WITH_DISTRICT_ALL = """
WITH dist AS MATERIALIZED (
    SELECT geom AS geom_32636
    FROM istanbul_district_boundaries
    WHERE district_key = $4
),
hour_slice AS MATERIALIZED (
    SELECT geohash, avg_speed, vehicle_count, geom
    FROM ibb_traffic_density
    WHERE record_time >= $1::timestamp
      AND record_time <  $2::timestamp
      AND avg_speed < $3::double precision
      AND geohash IS NOT NULL
      AND geohash <> ''
),
filtered_cells AS MATERIALIZED (
    SELECT
        gc.geohash,
        ST_CollectionExtract(ST_Intersection(gc.geom_32636, d.geom_32636), 3)
            AS geom_32636
    FROM hour_slice h
    JOIN dist d
      ON h.geom && d.geom_32636
     AND ST_Intersects(h.geom, d.geom_32636)
    JOIN geohash_cells gc ON gc.geohash = h.geohash
    WHERE gc.geom_32636 && d.geom_32636
      AND ST_Intersects(gc.geom_32636, d.geom_32636)
    GROUP BY gc.geohash, gc.geom_32636, d.geom_32636
),
cell_area AS MATERIALIZED (
    SELECT ST_UnaryUnion(ST_Collect(geom_32636)) AS geom_32636
    FROM filtered_cells
    WHERE geom_32636 IS NOT NULL
      AND NOT ST_IsEmpty(geom_32636)
),
intersected AS MATERIALIZED (
    SELECT
        r.id,
        r.osm_id,
        r.highway,
        CASE
            WHEN LOWER(BTRIM(r.name)) IN ('', 'nan') THEN NULL
            ELSE BTRIM(r.name)
        END AS name,
        ST_Multi(
            ST_CollectionExtract(
                ST_Intersection(ST_MakeValid(r.geom_32636), ca.geom_32636),
                2
            )
        ) AS geom_32636
    FROM road_segments r
    CROSS JOIN cell_area ca
    WHERE ca.geom_32636 IS NOT NULL
      AND r.geom_32636 IS NOT NULL
      AND r.geom_32636 && ca.geom_32636
      AND ST_Intersects(ST_MakeValid(r.geom_32636), ca.geom_32636)
)
SELECT
    id,
    osm_id,
    highway,
    name,
    ST_Length(geom_32636)::float AS clipped_length_m,
    ST_AsGeoJSON(ST_Transform(geom_32636, 4326), 6) AS geojson
FROM intersected
WHERE NOT ST_IsEmpty(geom_32636)
ORDER BY clipped_length_m DESC, id ASC
LIMIT $5::int;
"""


async def _ensure_road_network_available(conn) -> None:
    """Verify that road_segments and geohash_cells exist and contain data."""
    road_table_exists = await conn.fetchval("SELECT to_regclass('public.road_segments') IS NOT NULL")
    if not road_table_exists:
        raise RoadNetworkUnavailable(ROAD_NETWORK_NOT_IMPORTED_MSG)

    geohash_table_exists = await conn.fetchval("SELECT to_regclass('public.geohash_cells') IS NOT NULL")
    if not geohash_table_exists:
        raise RoadNetworkUnavailable(ROAD_NETWORK_NOT_IMPORTED_MSG)

    road_count = await conn.fetchval(
        "SELECT COUNT(*) FROM road_segments WHERE geom_32636 IS NOT NULL"
    )
    if not road_count:
        raise RoadNetworkUnavailable(ROAD_NETWORK_NOT_IMPORTED_MSG)

    cell_count = await conn.fetchval("SELECT COUNT(*) FROM geohash_cells")
    if not cell_count:
        raise RoadNetworkUnavailable(ROAD_NETWORK_NOT_IMPORTED_MSG)


def _effective_parameters(
    *,
    date_str: str,
    hour: int,
    start_ts: datetime,
    end_ts: datetime,
    avg_speed_threshold: float,
    window_hours: int,
    bbox: tuple[float, float, float, float] | None,
    district: str | None,
    road_level: RoadLevel,
    road_filter: str,
) -> dict:
    params = {
        "date": date_str,
        "hour": hour,
        "window_start": start_ts.isoformat(sep=" "),
        "window_end": end_ts.isoformat(sep=" "),
        "window_hours": window_hours,
        "avg_speed_threshold": avg_speed_threshold,
        "road_level": road_level,
        "road_filter": road_filter,
        "scope": "district" if district is not None else "bbox",
    }
    if district is not None:
        params["district"] = district
    if bbox is not None:
        params["bbox"] = {
            "min_lon": bbox[0],
            "min_lat": bbox[1],
            "max_lon": bbox[2],
            "max_lat": bbox[3],
        }
    return params


async def get_congested_roads(
    date_str: str,
    hour: int,
    bbox: tuple[float, float, float, float] | None = None,
    district: str | None = None,
    *,
    avg_speed_threshold: float = _DEFAULT_AVG_SPEED_THRESHOLD,
    window_hours: int = _DEFAULT_WINDOW_HOURS,
    road_level: RoadLevel = "main",
    limit: int = _ROAD_LIMIT,
) -> CongestedRoadResponse:
    """Return imported road lines clipped to the selected congested cell area."""
    if hour < 0 or hour > 23:
        raise ValueError("Invalid hour. Expected an integer between 0 and 23.")
    if avg_speed_threshold <= 0:
        raise ValueError("Invalid avg_speed_threshold. Expected a positive speed in km/h.")
    if window_hours < 1:
        raise ValueError("Invalid window_hours. Expected an integer >= 1.")
    if limit < 1:
        raise ValueError("Invalid limit. Expected an integer >= 1.")
    if road_level not in ("main", "all"):
        raise ValueError("Invalid road_level. Expected 'main' or 'all'.")
    if bbox is None and district is None:
        raise ValueError("Road lines require bbox or district selection.")

    try:
        selected_date = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(
            "Invalid date. Expected a real calendar date in YYYY-MM-DD format."
        ) from exc

    start_ts = selected_date.replace(hour=hour, minute=0, second=0, microsecond=0)
    end_ts = start_ts + timedelta(hours=window_hours)
    response_limit = min(limit, _CORRIDOR_ROAD_LIMIT) if road_level == "main" else limit
    road_filter = "corridor_summary" if road_level == "main" else "all_segments"
    fetch_limit = response_limit + 1

    pool = await get_pool()
    t0 = time.monotonic()
    async with pool.acquire() as conn:
        await _ensure_road_network_available(conn)
        if district is not None:
            await _ensure_district_available(conn, district)
            if road_level == "main":
                rows = await conn.fetch(
                    _ROAD_SQL_WITH_DISTRICT_MAIN,
                    start_ts,
                    end_ts,
                    avg_speed_threshold,
                    district,
                    list(_CORRIDOR_BASE_HIGHWAYS),
                    list(_CORRIDOR_NAME_HIGHWAYS),
                    _CORRIDOR_NAME_PATTERN,
                    fetch_limit,
                )
            else:
                rows = await conn.fetch(
                    _ROAD_SQL_WITH_DISTRICT_ALL,
                    start_ts,
                    end_ts,
                    avg_speed_threshold,
                    district,
                    fetch_limit,
                )
        else:
            if road_level == "main":
                rows = await conn.fetch(
                    _ROAD_SQL_WITH_BBOX_MAIN,
                    start_ts,
                    end_ts,
                    avg_speed_threshold,
                    bbox[0],
                    bbox[1],
                    bbox[2],
                    bbox[3],
                    list(_CORRIDOR_BASE_HIGHWAYS),
                    list(_CORRIDOR_NAME_HIGHWAYS),
                    _CORRIDOR_NAME_PATTERN,
                    fetch_limit,
                )
            else:
                rows = await conn.fetch(
                    _ROAD_SQL_WITH_BBOX_ALL,
                    start_ts,
                    end_ts,
                    avg_speed_threshold,
                    bbox[0],
                    bbox[1],
                    bbox[2],
                    bbox[3],
                    fetch_limit,
                )
    elapsed_ms = (time.monotonic() - t0) * 1000

    truncated = len(rows) > response_limit
    if truncated:
        rows = rows[:response_limit]

    features: list[RoadLineFeature] = []
    for r in rows:
        if not r["geojson"]:
            continue
        geometry = json.loads(r["geojson"])
        if geometry.get("type") not in {"LineString", "MultiLineString"}:
            continue
        features.append(
            RoadLineFeature(
                geometry=RoadLineGeometry(
                    type=geometry["type"],
                    coordinates=geometry["coordinates"],
                ),
                properties=RoadLineProperties(
                    road_id=r["id"],
                    osm_id=r["osm_id"],
                    highway=r["highway"],
                    name=r["name"],
                    clipped_length_m=round(float(r["clipped_length_m"] or 0), 1),
                ),
            )
        )

    logger.info(
        "Congested roads: date=%s hour=%02d window=%dh speed<%.1f level=%s district=%s "
        "roads=%d truncated=%s elapsed=%.1fms",
        date_str, hour, window_hours, avg_speed_threshold, road_level, district or "-",
        len(features), truncated, elapsed_ms,
    )

    return CongestedRoadResponse(
        date=date_str,
        hour=hour,
        window_hours=window_hours,
        avg_speed_threshold=avg_speed_threshold,
        road_count=len(features),
        limit=response_limit,
        truncated=truncated,
        effective_parameters=_effective_parameters(
            date_str=date_str,
            hour=hour,
            start_ts=start_ts,
            end_ts=end_ts,
            avg_speed_threshold=avg_speed_threshold,
            window_hours=window_hours,
            bbox=bbox,
            district=district,
            road_level=road_level,
            road_filter=road_filter,
        ),
        features=features,
    )
