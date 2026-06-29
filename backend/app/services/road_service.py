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
    SeverityCounts,
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
RoadStyle = Literal["plain", "traffic"]

# Cell-derived corridor severity. The thresholds mirror the geohash cell layer's
# classify_cell_severity so corridor and cell colors tell a consistent story.
# Line colors are intentionally darker/more saturated than the cell fills so the
# corridors read clearly on top of the subdued cell choropleth. These are a
# visualization category, NOT a road-segment speed ground truth.
_CORRIDOR_SEVERITY_META = {
    "free_flow": {"label": "Akıcı", "color": "#2e7d32"},
    "moderate": {"label": "Orta", "color": "#f9a825"},
    "congested": {"label": "Yoğun", "color": "#ef6c00"},
    "severe": {"label": "Çok yoğun", "color": "#c62828"},
}


def _matches_corridor_name(name: str | None) -> bool:
    """Return whether a road name looks like a recognizable traffic corridor."""
    return bool(name and re.search(_CORRIDOR_NAME_PATTERN, name, re.IGNORECASE))


def classify_corridor_severity(avg_speed_kmh: float) -> str:
    """Cell-derived corridor category from the corridor's length-weighted speed.

    Same thresholds as the geohash cell layer (see classify_cell_severity):
    severe <20, congested <35, moderate <50, free_flow otherwise.
    """
    if avg_speed_kmh < 20:
        return "severe"
    if avg_speed_kmh < 35:
        return "congested"
    if avg_speed_kmh < 50:
        return "moderate"
    return "free_flow"


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


# ── Traffic-corridor style (cell-derived severity) ───────────────────────────
# Unlike the plain styles above, the traffic style does NOT pre-filter the hour
# slice by avg_speed: it keeps every measured cell so corridors can be colored
# across the full free_flow→severe range. Each major corridor is clipped to the
# union of the cells it intersects and assigned a length-weighted average of
# those cells' speeds. The per-cell × per-road join is bounded by the required
# bbox/district scope (and, for bbox, the all-cell area guard in the router).
# Geometry handling reuses the same proven PostGIS building blocks as the plain
# queries; only the speed aggregation per corridor is new.
#
# Bind layout (bbox): $1 start  $2 end  $3..$6 bbox  $7 base highways[]
#                     $8 name highways[]  $9 corridor-name pattern  $10 limit
_ROAD_SQL_WITH_BBOX_TRAFFIC = """
WITH region AS MATERIALIZED (
    SELECT ST_Transform(ST_MakeEnvelope($3, $4, $5, $6, 4326), 32636) AS geom_32636
),
hour_slice AS MATERIALIZED (
    SELECT geohash, avg_speed, geom
    FROM ibb_traffic_density
    WHERE record_time >= $1::timestamp
      AND record_time <  $2::timestamp
      AND geohash IS NOT NULL
      AND geohash <> ''
),
cell_speed AS MATERIALIZED (
    SELECT
        gc.geohash,
        AVG(h.avg_speed)::float AS avg_speed,
        gc.geom_32636           AS geom_32636
    FROM hour_slice h
    JOIN geohash_cells gc ON gc.geohash = h.geohash
    CROSS JOIN region rg
    WHERE h.geom && rg.geom_32636
      AND ST_Intersects(h.geom, rg.geom_32636)
      AND gc.geom_32636 && rg.geom_32636
      AND ST_Intersects(gc.geom_32636, rg.geom_32636)
    GROUP BY gc.geohash, gc.geom_32636
),
roads AS MATERIALIZED (
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
                ST_Intersection(ST_MakeValid(r.geom_32636), rg.geom_32636),
                2
            )
        ) AS geom_32636
    FROM road_segments r
    CROSS JOIN region rg
    WHERE r.geom_32636 IS NOT NULL
      AND r.geom_32636 && rg.geom_32636
      AND ST_Intersects(ST_MakeValid(r.geom_32636), rg.geom_32636)
      AND (
          r.highway = ANY($7::text[])
          OR r.highway = ANY($8::text[])
      )
),
road_cell AS MATERIALIZED (
    SELECT
        rd.id,
        rd.osm_id,
        rd.highway,
        rd.name,
        cs.geohash,
        cs.avg_speed AS cell_speed,
        ST_CollectionExtract(ST_Intersection(rd.geom_32636, cs.geom_32636), 2)
            AS seg_geom,
        CASE
            WHEN rd.highway = ANY($7::text[]) THEN 1
            WHEN rd.highway = ANY($8::text[]) THEN 2
            ELSE 99
        END AS road_priority
    FROM roads rd
    JOIN cell_speed cs
      ON rd.geom_32636 && cs.geom_32636
     AND ST_Intersects(rd.geom_32636, cs.geom_32636)
    WHERE rd.name IS NOT NULL
      AND NOT ST_IsEmpty(rd.geom_32636)
      AND (
          rd.highway = ANY($7::text[])
          OR (rd.highway = ANY($8::text[]) AND rd.name ~* $9::text)
      )
),
road_seg AS MATERIALIZED (
    SELECT
        id, osm_id, highway, name, geohash, cell_speed, road_priority, seg_geom,
        ST_Length(seg_geom)::float AS seg_len
    FROM road_cell
    WHERE seg_geom IS NOT NULL
      AND NOT ST_IsEmpty(seg_geom)
),
corridor AS MATERIALIZED (
    SELECT
        MIN(id) AS id,
        (ARRAY_AGG(osm_id ORDER BY road_priority, seg_len DESC, id))[1] AS osm_id,
        (ARRAY_AGG(highway ORDER BY road_priority, seg_len DESC, id))[1] AS highway,
        name,
        (SUM(cell_speed * seg_len) / NULLIF(SUM(seg_len), 0))::float AS avg_speed_kmh,
        COUNT(DISTINCT geohash)::int AS cell_count,
        SUM(seg_len)::float AS clipped_length_m,
        MIN(road_priority) AS road_priority,
        ST_Multi(
            ST_CollectionExtract(
                ST_LineMerge(ST_UnaryUnion(ST_Collect(seg_geom))),
                2
            )
        ) AS geom_32636
    FROM road_seg
    GROUP BY name
)
SELECT
    id,
    osm_id,
    highway,
    name,
    avg_speed_kmh,
    cell_count,
    clipped_length_m,
    ST_AsGeoJSON(ST_Transform(geom_32636, 4326), 6) AS geojson
FROM corridor
WHERE geom_32636 IS NOT NULL
  AND NOT ST_IsEmpty(geom_32636)
  AND avg_speed_kmh IS NOT NULL
ORDER BY road_priority ASC, clipped_length_m DESC, name ASC
LIMIT $10::int;
"""


# Bind layout (district): $1 start  $2 end  $3 district_key  $4 base highways[]
#                         $5 name highways[]  $6 corridor-name pattern  $7 limit
_ROAD_SQL_WITH_DISTRICT_TRAFFIC = """
WITH dist AS MATERIALIZED (
    SELECT geom AS geom_32636
    FROM istanbul_district_boundaries
    WHERE district_key = $3
),
hour_slice AS MATERIALIZED (
    SELECT geohash, avg_speed, geom
    FROM ibb_traffic_density
    WHERE record_time >= $1::timestamp
      AND record_time <  $2::timestamp
      AND geohash IS NOT NULL
      AND geohash <> ''
),
cell_speed AS MATERIALIZED (
    SELECT
        gc.geohash,
        AVG(h.avg_speed)::float AS avg_speed,
        gc.geom_32636           AS geom_32636
    FROM hour_slice h
    JOIN dist d
      ON h.geom && d.geom_32636
     AND ST_Intersects(h.geom, d.geom_32636)
    JOIN geohash_cells gc ON gc.geohash = h.geohash
    WHERE gc.geom_32636 && d.geom_32636
      AND ST_Intersects(gc.geom_32636, d.geom_32636)
    GROUP BY gc.geohash, gc.geom_32636
),
roads AS MATERIALIZED (
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
                ST_Intersection(ST_MakeValid(r.geom_32636), d.geom_32636),
                2
            )
        ) AS geom_32636
    FROM road_segments r
    CROSS JOIN dist d
    WHERE r.geom_32636 IS NOT NULL
      AND r.geom_32636 && d.geom_32636
      AND ST_Intersects(ST_MakeValid(r.geom_32636), d.geom_32636)
      AND (
          r.highway = ANY($4::text[])
          OR r.highway = ANY($5::text[])
      )
),
road_cell AS MATERIALIZED (
    SELECT
        rd.id,
        rd.osm_id,
        rd.highway,
        rd.name,
        cs.geohash,
        cs.avg_speed AS cell_speed,
        ST_CollectionExtract(ST_Intersection(rd.geom_32636, cs.geom_32636), 2)
            AS seg_geom,
        CASE
            WHEN rd.highway = ANY($4::text[]) THEN 1
            WHEN rd.highway = ANY($5::text[]) THEN 2
            ELSE 99
        END AS road_priority
    FROM roads rd
    JOIN cell_speed cs
      ON rd.geom_32636 && cs.geom_32636
     AND ST_Intersects(rd.geom_32636, cs.geom_32636)
    WHERE rd.name IS NOT NULL
      AND NOT ST_IsEmpty(rd.geom_32636)
      AND (
          rd.highway = ANY($4::text[])
          OR (rd.highway = ANY($5::text[]) AND rd.name ~* $6::text)
      )
),
road_seg AS MATERIALIZED (
    SELECT
        id, osm_id, highway, name, geohash, cell_speed, road_priority, seg_geom,
        ST_Length(seg_geom)::float AS seg_len
    FROM road_cell
    WHERE seg_geom IS NOT NULL
      AND NOT ST_IsEmpty(seg_geom)
),
corridor AS MATERIALIZED (
    SELECT
        MIN(id) AS id,
        (ARRAY_AGG(osm_id ORDER BY road_priority, seg_len DESC, id))[1] AS osm_id,
        (ARRAY_AGG(highway ORDER BY road_priority, seg_len DESC, id))[1] AS highway,
        name,
        (SUM(cell_speed * seg_len) / NULLIF(SUM(seg_len), 0))::float AS avg_speed_kmh,
        COUNT(DISTINCT geohash)::int AS cell_count,
        SUM(seg_len)::float AS clipped_length_m,
        MIN(road_priority) AS road_priority,
        ST_Multi(
            ST_CollectionExtract(
                ST_LineMerge(ST_UnaryUnion(ST_Collect(seg_geom))),
                2
            )
        ) AS geom_32636
    FROM road_seg
    GROUP BY name
)
SELECT
    id,
    osm_id,
    highway,
    name,
    avg_speed_kmh,
    cell_count,
    clipped_length_m,
    ST_AsGeoJSON(ST_Transform(geom_32636, 4326), 6) AS geojson
FROM corridor
WHERE geom_32636 IS NOT NULL
  AND NOT ST_IsEmpty(geom_32636)
  AND avg_speed_kmh IS NOT NULL
ORDER BY road_priority ASC, clipped_length_m DESC, name ASC
LIMIT $7::int;
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
    style: RoadStyle,
) -> dict:
    params = {
        "date": date_str,
        "hour": hour,
        "window_start": start_ts.isoformat(sep=" "),
        "window_end": end_ts.isoformat(sep=" "),
        "window_hours": window_hours,
        "avg_speed_threshold": avg_speed_threshold,
        "style": style,
        "road_level": road_level,
        "road_filter": road_filter,
        "scope": "district" if district is not None else "bbox",
    }
    if style == "traffic":
        # Make it unambiguous in the API that corridor colors come from the
        # intersecting cells, not from a road-segment speed measurement.
        params["severity_basis"] = "cell_derived"
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
    style: RoadStyle = "plain",
    limit: int = _ROAD_LIMIT,
) -> CongestedRoadResponse:
    """Return imported road lines clipped to the selected congested cell area.

    With ``style="traffic"`` (the Trafik Koridorları view) the major corridors
    are instead colored by **cell-derived** severity — the length-weighted mean
    speed of the geohash cells each road intersects. See ``_get_traffic_corridors``.
    """
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
    if style not in ("plain", "traffic"):
        raise ValueError("Invalid style. Expected 'plain' or 'traffic'.")
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

    if style == "traffic":
        return await _get_traffic_corridors(
            date_str=date_str,
            hour=hour,
            start_ts=start_ts,
            end_ts=end_ts,
            bbox=bbox,
            district=district,
            avg_speed_threshold=avg_speed_threshold,
            window_hours=window_hours,
            road_level=road_level,
            limit=limit,
        )

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
        style="plain",
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
            style="plain",
        ),
        features=features,
    )


async def _get_traffic_corridors(
    *,
    date_str: str,
    hour: int,
    start_ts: datetime,
    end_ts: datetime,
    bbox: tuple[float, float, float, float] | None,
    district: str | None,
    avg_speed_threshold: float,
    window_hours: int,
    road_level: RoadLevel,
    limit: int,
) -> CongestedRoadResponse:
    """Traffic-corridor style: major roads colored by cell-derived severity.

    Each major corridor (motorway/trunk always; primary/links only when the name
    matches a recognizable corridor) is clipped to the union of the geohash cells
    it intersects and assigned the length-weighted average of those cells' speeds.
    Severity is then classified with the same thresholds as the cell layer. The
    avg_speed pre-filter is intentionally NOT applied here so corridors can be
    colored across the full free_flow→severe range; ``avg_speed_threshold`` is
    still echoed back for transparency. Output is bounded to the corridor cap.
    """
    response_limit = min(limit, _CORRIDOR_ROAD_LIMIT)
    fetch_limit = response_limit + 1

    pool = await get_pool()
    t0 = time.monotonic()
    async with pool.acquire() as conn:
        await _ensure_road_network_available(conn)
        if district is not None:
            await _ensure_district_available(conn, district)
            rows = await conn.fetch(
                _ROAD_SQL_WITH_DISTRICT_TRAFFIC,
                start_ts,
                end_ts,
                district,
                list(_CORRIDOR_BASE_HIGHWAYS),
                list(_CORRIDOR_NAME_HIGHWAYS),
                _CORRIDOR_NAME_PATTERN,
                fetch_limit,
            )
        else:
            rows = await conn.fetch(
                _ROAD_SQL_WITH_BBOX_TRAFFIC,
                start_ts,
                end_ts,
                bbox[0],
                bbox[1],
                bbox[2],
                bbox[3],
                list(_CORRIDOR_BASE_HIGHWAYS),
                list(_CORRIDOR_NAME_HIGHWAYS),
                _CORRIDOR_NAME_PATTERN,
                fetch_limit,
            )
    elapsed_ms = (time.monotonic() - t0) * 1000

    truncated = len(rows) > response_limit
    if truncated:
        rows = rows[:response_limit]

    sev_counts = {"free_flow": 0, "moderate": 0, "congested": 0, "severe": 0}
    features: list[RoadLineFeature] = []
    for r in rows:
        if not r["geojson"] or r["avg_speed_kmh"] is None:
            continue
        geometry = json.loads(r["geojson"])
        if geometry.get("type") not in {"LineString", "MultiLineString"}:
            continue
        avg_speed = float(r["avg_speed_kmh"])
        severity = classify_corridor_severity(avg_speed)
        meta = _CORRIDOR_SEVERITY_META[severity]
        sev_counts[severity] += 1
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
                    avg_speed_kmh=round(avg_speed, 1),
                    cell_count=r["cell_count"],
                    severity=severity,
                    severity_label=meta["label"],
                    color=meta["color"],
                ),
            )
        )

    logger.info(
        "Traffic corridors: date=%s hour=%02d window=%dh district=%s "
        "corridors=%d truncated=%s elapsed=%.1fms",
        date_str, hour, window_hours, district or "-",
        len(features), truncated, elapsed_ms,
    )

    return CongestedRoadResponse(
        date=date_str,
        hour=hour,
        window_hours=window_hours,
        avg_speed_threshold=avg_speed_threshold,
        style="traffic",
        road_count=len(features),
        limit=response_limit,
        truncated=truncated,
        severity_counts=SeverityCounts(**sev_counts),
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
            road_filter="traffic_corridors",
            style="traffic",
        ),
        features=features,
    )
