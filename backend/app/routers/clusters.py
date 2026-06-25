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

from typing import Optional, Union

from fastapi import APIRouter, HTTPException, Query, Request

from backend.app.limiter import limiter
from backend.app.models.cluster import (
    GeoJSONFeatureCollection,
    StatsResponse,
    TemporalClusterResponse,
)
from backend.app.services.cluster_service import (
    build_geojson,
    compute_ais_and_severity,
    get_cached_cluster_summaries,
    get_global_stats,
    get_temporal_clusters,
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
):
    """
    Get traffic anomaly clusters as GeoJSON.

    **Temporal mode** (date + hour provided):
    Runs live PostGIS ST_ClusterDBSCAN on the selected hour slice from
    ibb_traffic_density. Uses hourly-tuned parameters (eps=1000m,
    minpoints=2, avg_speed<25). Severity uses congestion_score combining
    speed drop, hourly vehicle volume, and cluster coverage.

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

    # ── Temporal mode ─────────────────────────────────────────────────
    if date is not None and hour is not None:
        try:
            if bbox is None:
                result = await get_temporal_clusters(date, hour)
            else:
                result = await get_temporal_clusters(date, hour, bbox=bbox)
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

    # ── Legacy mode (backward compatible) ─────────────────────────────
    clusters = await get_cached_cluster_summaries()
    if not clusters:
        return GeoJSONFeatureCollection(features=[])

    scored = compute_ais_and_severity(clusters)

    if severity:
        scored = [c for c in scored if c["severity"] == severity.upper()]

    return build_geojson(scored)


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
