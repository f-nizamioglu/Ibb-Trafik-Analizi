"""
Cluster API endpoints — GeoJSON responses for Leaflet.js.

Endpoints:
    GET /api/clusters              → All clusters as GeoJSON FeatureCollection
    GET /api/clusters/{cluster_id} → Single cluster detail
    GET /api/clusters?severity=HIGH → Filter by severity
    GET /api/stats                  → Global statistics
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request

from backend.app.limiter import limiter
from backend.app.models.cluster import GeoJSONFeatureCollection, StatsResponse
from backend.app.services.cluster_service import (
    build_geojson,
    compute_ais_and_severity,
    get_cached_cluster_summaries,
    get_global_stats,
)

router = APIRouter()


@router.get("/clusters", response_model=GeoJSONFeatureCollection)
@limiter.limit("30/minute")
async def list_clusters(
    request: Request,
    severity: Optional[str] = Query(
        None,
        description="Filter by severity level: LOW, MEDIUM, or HIGH",
        pattern="^(LOW|MEDIUM|HIGH)$",
    ),
):
    """
    Get all traffic anomaly clusters as a GeoJSON FeatureCollection.

    Each Feature contains:
    - geometry: centroid Point (snapped to road if available)
    - properties: AIS score, severity, metrics, peak time, road name

    Leaflet usage: `L.geoJSON(data).addTo(map)`
    """
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
