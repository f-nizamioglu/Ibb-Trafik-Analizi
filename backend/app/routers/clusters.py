"""
Cluster API endpoints — GeoJSON responses for Leaflet.js.

Endpoints:
    GET /api/clusters                                  → Historical aggregate clusters
    GET /api/clusters?date=YYYY-MM-DD&hour=HH          → Temporal (hourly) clusters
    GET /api/clusters?date=...&hour=...&severity=HIGH   → Temporal + severity filter
    GET /api/clusters/{cluster_id}                      → Single cluster detail
    GET /api/stats                                      → Global statistics
"""

from __future__ import annotations

from typing import Literal, Optional, Union

from fastapi import APIRouter, HTTPException, Query, Request

from backend.app.limiter import limiter
from backend.app.models.cluster import (
    CongestedRoadResponse,
    GeoJSONFeatureCollection,
    GeohashCellResponse,
    StatsResponse,
    TemporalClusterResponse,
)
from backend.app.services.cluster_service import (
    DistrictBoundariesNotImported,
    UnknownDistrict,
    build_geojson,
    compute_ais_and_severity,
    get_cached_cluster_summaries,
    get_district_boundary,
    get_global_stats,
    get_temporal_clusters,
)
from backend.app.services.district_utils import normalize_district_key
from backend.app.services.geohash_service import get_geohash_cells
from backend.app.services.road_service import (
    RoadNetworkUnavailable,
    get_congested_roads,
)

router = APIRouter()


@router.get(
    "/clusters",
    response_model=Union[TemporalClusterResponse, GeoJSONFeatureCollection],
)
@limiter.limit("60/minute")
async def list_clusters(
    request: Request,
    date: Optional[str] = Query(
        None,
        description="Date filter (YYYY-MM-DD). Requires 'hour' parameter.",
        pattern=r"^\d{4}-\d{2}-\d{2}$",
    ),
    hour: Optional[int] = Query(
        None,
        description="Hour of day (0-23). Requires 'date' parameter.",
        ge=0,
        le=23,
    ),
    severity: Optional[str] = Query(
        None,
        description="Filter by severity level: LOW, MEDIUM, or HIGH",
        pattern="^(LOW|MEDIUM|HIGH)$",
    ),
    min_lon: Optional[float] = Query(
        None,
        description="Minimum longitude for optional WGS84 bbox filter.",
        ge=-180,
        le=180,
    ),
    min_lat: Optional[float] = Query(
        None,
        description="Minimum latitude for optional WGS84 bbox filter.",
        ge=-90,
        le=90,
    ),
    max_lon: Optional[float] = Query(
        None,
        description="Maximum longitude for optional WGS84 bbox filter.",
        ge=-180,
        le=180,
    ),
    max_lat: Optional[float] = Query(
        None,
        description="Maximum latitude for optional WGS84 bbox filter.",
        ge=-90,
        le=90,
    ),
    district: Optional[str] = Query(
        None,
        description=(
            "Optional district key (e.g. 'besiktas') to filter by the real "
            "district polygon. Requires 'date' and 'hour'; mutually exclusive "
            "with the bbox parameters."
        ),
    ),
    eps_meters: Optional[float] = Query(
        None,
        description=(
            "Temporal mode only: DBSCAN neighborhood radius in meters "
            "(EPSG:32636). Default 1000 when omitted."
        ),
        ge=100,
        le=5000,
    ),
    minpoints: Optional[int] = Query(
        None,
        description=(
            "Temporal mode only: DBSCAN minimum points to form a cluster. "
            "Default 2 when omitted."
        ),
        ge=1,
        le=50,
    ),
    avg_speed_threshold: Optional[float] = Query(
        None,
        description=(
            "Temporal mode only: keep measurements with avg_speed below this "
            "(km/h) before DBSCAN. Default 25 when omitted."
        ),
        gt=0,
        le=120,
    ),
    window_hours: Optional[int] = Query(
        None,
        description=(
            "Temporal mode only: number of hours in the time window starting "
            "at the selected hour. Default 1 when omitted."
        ),
        ge=1,
        le=24,
    ),
):
    """
    Get traffic anomaly clusters as GeoJSON.

    **Temporal mode** (date + hour provided):
    Runs live PostGIS ST_ClusterDBSCAN on the selected hour slice from
    ibb_traffic_density. Uses hourly-tuned parameters (eps=1000m,
    minpoints=2, avg_speed<25). Severity uses congestion_score combining
    speed drop, hourly vehicle volume, and cluster coverage.

    **Region filters** (temporal mode only): an optional WGS84 bbox
    (min_lon/min_lat/max_lon/max_lat) or a `district` key may be supplied to
    restrict the analysis to a sub-area. bbox and district are mutually
    exclusive; in both cases filtering happens BEFORE DBSCAN, so clusters are
    recalculated on the selected subset.

    **Legacy mode** (no date/hour):
    Returns cached historical aggregate clusters with AIS scoring.
    Kept for backward compatibility.
    """
    bbox_values = (min_lon, min_lat, max_lon, max_lat)
    bbox_flags = [value is not None for value in bbox_values]
    if any(bbox_flags) and not all(bbox_flags):
        raise HTTPException(
            status_code=400,
            detail=(
                "All bbox parameters must be provided together: "
                "min_lon, min_lat, max_lon, max_lat."
            ),
        )

    bbox = None
    if all(bbox_flags):
        if min_lon >= max_lon:
            raise HTTPException(
                status_code=400,
                detail="'min_lon' must be less than 'max_lon'.",
            )
        if min_lat >= max_lat:
            raise HTTPException(
                status_code=400,
                detail="'min_lat' must be less than 'max_lat'.",
            )
        bbox = (min_lon, min_lat, max_lon, max_lat)

    # ── District param (real polygon mode) ────────────────────────────
    district_key = None
    if district is not None:
        # Normalize identically to the import script so Turkish-cased names
        # (e.g. "Beşiktaş") map to the stored key ("besiktas").
        district_key = normalize_district_key(district)
        if not district_key:
            raise HTTPException(
                status_code=400,
                detail="'district' must be a non-empty value.",
            )
        if bbox is not None:
            raise HTTPException(
                status_code=400,
                detail="'district' and bbox parameters are mutually exclusive.",
            )

    # Optional clustering tuning params (temporal mode only). Only forwarded
    # when explicitly supplied, so the default call path — and its tests — stays
    # exactly as before (get_temporal_clusters(date, hour) with empty kwargs).
    tuning: dict = {}
    if eps_meters is not None:
        tuning["eps_meters"] = eps_meters
    if minpoints is not None:
        tuning["minpoints"] = minpoints
    if avg_speed_threshold is not None:
        tuning["avg_speed_threshold"] = avg_speed_threshold
    if window_hours is not None:
        tuning["window_hours"] = window_hours

    # ── Temporal mode ─────────────────────────────────────────────────
    if date is not None and hour is not None:
        try:
            if district_key is not None:
                result = await get_temporal_clusters(date, hour, district=district_key, **tuning)
            elif bbox is None:
                result = await get_temporal_clusters(date, hour, **tuning)
            else:
                result = await get_temporal_clusters(date, hour, bbox=bbox, **tuning)
        except DistrictBoundariesNotImported as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        if severity:
            sev_upper = severity.upper()
            result.features = [
                f for f in result.features
                if f.properties.severity == sev_upper
            ]
            result.cluster_count = len(result.features)
            # Recompute summary stats after filtering
            result.total_points = sum(
                f.properties.point_count for f in result.features
            )
            result.total_vehicles = sum(
                f.properties.sum_vehicle_count for f in result.features
            )
            if result.total_points > 0:
                result.avg_speed = round(
                    sum(
                        f.properties.avg_speed_kmh * f.properties.point_count
                        for f in result.features
                    ) / result.total_points,
                    1,
                )
            else:
                result.avg_speed = None
            result.high_count = sum(
                1 for f in result.features if f.properties.severity == "HIGH"
            )
            result.medium_count = sum(
                1 for f in result.features if f.properties.severity == "MEDIUM"
            )
            result.low_count = sum(
                1 for f in result.features if f.properties.severity == "LOW"
            )

        return result

    # ── Validate: both or neither ─────────────────────────────────────
    if (date is None) != (hour is None):
        raise HTTPException(
            status_code=400,
            detail="Both 'date' and 'hour' must be provided for temporal mode.",
        )

    if bbox is not None:
        raise HTTPException(
            status_code=400,
            detail="Bbox filtering requires both 'date' and 'hour' parameters.",
        )

    if district_key is not None:
        raise HTTPException(
            status_code=400,
            detail="District filtering requires both 'date' and 'hour' parameters.",
        )

    # ── Legacy mode (backward compatible) ─────────────────────────────
    clusters = await get_cached_cluster_summaries()
    if not clusters:
        return GeoJSONFeatureCollection(features=[])

    scored = compute_ais_and_severity(clusters)

    if severity:
        scored = [c for c in scored if c["severity"] == severity.upper()]

    return build_geojson(scored)


@router.get("/geohash-cells", response_model=GeohashCellResponse)
@limiter.limit("60/minute")
async def list_geohash_cells(
    request: Request,
    date: str = Query(
        ...,
        description="Date (YYYY-MM-DD). Required.",
        pattern=r"^\d{4}-\d{2}-\d{2}$",
    ),
    hour: int = Query(
        ...,
        description="Start hour of day (0-23). Required.",
        ge=0,
        le=23,
    ),
    avg_speed_threshold: Optional[float] = Query(
        None,
        description="Keep measurements with avg_speed below this (km/h). Default 25.",
        gt=0,
        le=120,
    ),
    window_hours: Optional[int] = Query(
        None,
        description="Number of hours in the window starting at 'hour'. Default 1.",
        ge=1,
        le=24,
    ),
    min_lon: Optional[float] = Query(
        None, description="Minimum longitude for optional WGS84 bbox filter.",
        ge=-180, le=180,
    ),
    min_lat: Optional[float] = Query(
        None, description="Minimum latitude for optional WGS84 bbox filter.",
        ge=-90, le=90,
    ),
    max_lon: Optional[float] = Query(
        None, description="Maximum longitude for optional WGS84 bbox filter.",
        ge=-180, le=180,
    ),
    max_lat: Optional[float] = Query(
        None, description="Maximum latitude for optional WGS84 bbox filter.",
        ge=-90, le=90,
    ),
    district: Optional[str] = Query(
        None,
        description=(
            "Optional district key (e.g. 'besiktas') to filter by the real "
            "district polygon. Mutually exclusive with the bbox parameters."
        ),
    ),
):
    """
    Geohash measurement-cell layer as a GeoJSON Polygon FeatureCollection.

    Returns the real İBB measurement cells (geohash grid) for the selected
    window, each as its rectangular geohash bounding box with aggregated
    speed/volume stats. The same window + avg_speed pre-filter (+ optional
    bbox/district region) used for clustering is applied, so these polygons
    are exactly the measurements that feed DBSCAN — not cluster centroids.

    bbox and district are mutually exclusive; both filter measurements BEFORE
    aggregation. Region rules mirror /api/clusters.
    """
    bbox_values = (min_lon, min_lat, max_lon, max_lat)
    bbox_flags = [value is not None for value in bbox_values]
    if any(bbox_flags) and not all(bbox_flags):
        raise HTTPException(
            status_code=400,
            detail=(
                "All bbox parameters must be provided together: "
                "min_lon, min_lat, max_lon, max_lat."
            ),
        )

    bbox = None
    if all(bbox_flags):
        if min_lon >= max_lon:
            raise HTTPException(
                status_code=400, detail="'min_lon' must be less than 'max_lon'.",
            )
        if min_lat >= max_lat:
            raise HTTPException(
                status_code=400, detail="'min_lat' must be less than 'max_lat'.",
            )
        bbox = (min_lon, min_lat, max_lon, max_lat)

    district_key = None
    if district is not None:
        district_key = normalize_district_key(district)
        if not district_key:
            raise HTTPException(
                status_code=400, detail="'district' must be a non-empty value.",
            )
        if bbox is not None:
            raise HTTPException(
                status_code=400,
                detail="'district' and bbox parameters are mutually exclusive.",
            )

    tuning: dict = {}
    if avg_speed_threshold is not None:
        tuning["avg_speed_threshold"] = avg_speed_threshold
    if window_hours is not None:
        tuning["window_hours"] = window_hours

    try:
        if district_key is not None:
            return await get_geohash_cells(date, hour, district=district_key, **tuning)
        if bbox is None:
            return await get_geohash_cells(date, hour, **tuning)
        return await get_geohash_cells(date, hour, bbox=bbox, **tuning)
    except DistrictBoundariesNotImported as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        # Includes UnknownDistrict (a ValueError subclass) → 400, matching /clusters.
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/congested-roads", response_model=CongestedRoadResponse)
@limiter.limit("30/minute")
async def list_congested_roads(
    request: Request,
    date: str = Query(
        ...,
        description="Date (YYYY-MM-DD). Required.",
        pattern=r"^\d{4}-\d{2}-\d{2}$",
    ),
    hour: int = Query(
        ...,
        description="Start hour of day (0-23). Required.",
        ge=0,
        le=23,
    ),
    avg_speed_threshold: Optional[float] = Query(
        None,
        description="Keep measurements with avg_speed below this (km/h). Default 25.",
        gt=0,
        le=120,
    ),
    window_hours: Optional[int] = Query(
        None,
        description="Number of hours in the window starting at 'hour'. Default 1.",
        ge=1,
        le=24,
    ),
    min_lon: Optional[float] = Query(
        None,
        description="Minimum longitude for required WGS84 bbox filter.",
        ge=-180,
        le=180,
    ),
    min_lat: Optional[float] = Query(
        None,
        description="Minimum latitude for required WGS84 bbox filter.",
        ge=-90,
        le=90,
    ),
    max_lon: Optional[float] = Query(
        None,
        description="Maximum longitude for required WGS84 bbox filter.",
        ge=-180,
        le=180,
    ),
    max_lat: Optional[float] = Query(
        None,
        description="Maximum latitude for required WGS84 bbox filter.",
        ge=-90,
        le=90,
    ),
    district: Optional[str] = Query(
        None,
        description=(
            "Optional district key (e.g. 'besiktas') to filter by the real "
            "district polygon. Mutually exclusive with the bbox parameters."
        ),
    ),
    road_level: Literal["main", "all"] = Query(
        "main",
        description=(
            "Road detail level. 'main' returns summarized traffic corridors; "
            "'all' returns the previous capped/clipped segment behavior."
        ),
    ),
):
    """
    Road lines clipped to the selected congested geohash measurement cells.

    The endpoint requires either a bbox or a district so the road overlay cannot
    accidentally request a full-city geometry payload. It applies the same
    date/hour/window + avg_speed filter used by /api/geohash-cells, clips road
    lines to the resulting congested geohash-cell area, and returns capped
    WGS84 GeoJSON LineString/MultiLineString features. The default road level
    summarizes recognizable corridors for presentation clarity.
    """
    bbox_values = (min_lon, min_lat, max_lon, max_lat)
    bbox_flags = [value is not None for value in bbox_values]
    if any(bbox_flags) and not all(bbox_flags):
        raise HTTPException(
            status_code=400,
            detail=(
                "All bbox parameters must be provided together: "
                "min_lon, min_lat, max_lon, max_lat."
            ),
        )

    bbox = None
    if all(bbox_flags):
        if min_lon >= max_lon:
            raise HTTPException(
                status_code=400,
                detail="'min_lon' must be less than 'max_lon'.",
            )
        if min_lat >= max_lat:
            raise HTTPException(
                status_code=400,
                detail="'min_lat' must be less than 'max_lat'.",
            )
        bbox = (min_lon, min_lat, max_lon, max_lat)

    district_key = None
    if district is not None:
        district_key = normalize_district_key(district)
        if not district_key:
            raise HTTPException(
                status_code=400,
                detail="'district' must be a non-empty value.",
            )
        if bbox is not None:
            raise HTTPException(
                status_code=400,
                detail="'district' and bbox parameters are mutually exclusive.",
            )

    if bbox is None and district_key is None:
        raise HTTPException(
            status_code=400,
            detail="Road lines require bbox or district selection.",
        )

    tuning: dict = {}
    if avg_speed_threshold is not None:
        tuning["avg_speed_threshold"] = avg_speed_threshold
    if window_hours is not None:
        tuning["window_hours"] = window_hours
    tuning["road_level"] = road_level

    try:
        if district_key is not None:
            return await get_congested_roads(date, hour, district=district_key, **tuning)
        return await get_congested_roads(date, hour, bbox=bbox, **tuning)
    except RoadNetworkUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except DistrictBoundariesNotImported as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        # Includes UnknownDistrict (a ValueError subclass), matching /clusters.
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/clusters/{cluster_id}", response_model=GeoJSONFeatureCollection)
@limiter.limit("60/minute")
async def get_cluster(request: Request, cluster_id: int):
    """
    Get a single cluster by ID as a GeoJSON FeatureCollection.

    Returns all data points belonging to this cluster, not just the centroid.
    """
    clusters = await get_cached_cluster_summaries()
    if not clusters:
        raise HTTPException(status_code=404, detail="No clusters found")

    scored = compute_ais_and_severity(clusters)
    matched = [c for c in scored if c["cluster_id"] == cluster_id]

    if not matched:
        raise HTTPException(
            status_code=404,
            detail=f"Cluster {cluster_id} not found",
        )

    return build_geojson(matched)


@router.get("/districts/{district_key}/boundary")
@limiter.limit("60/minute")
async def district_boundary(request: Request, district_key: str):
    """
    Return the imported district polygon boundary as a WGS84 GeoJSON Feature.

    Used by the frontend to draw the real district outline on the map. The
    geometry comes from istanbul_district_boundaries (transformed to EPSG:4326);
    the local GeoJSON source file is never read or exposed here.
    """
    key = normalize_district_key(district_key)
    if not key:
        raise HTTPException(status_code=400, detail="Unknown district")
    try:
        return await get_district_boundary(key)
    except DistrictBoundariesNotImported as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except UnknownDistrict as exc:
        raise HTTPException(status_code=404, detail="Unknown district") from exc


@router.get("/stats", response_model=StatsResponse)
@limiter.limit("30/minute")
async def stats(request: Request):
    """
    Get global statistics for the dashboard.

    Returns total records, cluster counts, noise percentage,
    severity distribution, and date range.
    """
    global_stats = await get_global_stats()

    # Add severity counts from cluster data
    clusters = await get_cached_cluster_summaries()
    if clusters:
        scored = compute_ais_and_severity(clusters)
        global_stats.high_severity_count = sum(1 for c in scored if c["severity"] == "HIGH")
        global_stats.medium_severity_count = sum(1 for c in scored if c["severity"] == "MEDIUM")
        global_stats.low_severity_count = sum(1 for c in scored if c["severity"] == "LOW")

    return global_stats
