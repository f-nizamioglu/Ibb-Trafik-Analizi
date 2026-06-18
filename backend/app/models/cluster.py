"""
Pydantic models for cluster data and GeoJSON responses.

These models ensure type-safe serialization and provide automatic
Swagger documentation for all API endpoints.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

# ─── Cluster Properties ───────────────────────────────────────────────────

class ClusterProperties(BaseModel):
    """Properties embedded in each GeoJSON Feature."""
    cluster_id: int
    severity: str = Field(..., description="LOW | MEDIUM | HIGH")
    ais_score: float = Field(..., description="Anomaly Intensity Score [0, 1]")
    point_count: int
    avg_vehicle_count: float
    avg_speed_kmh: float
    duration_hours: float
    recurrence_days: int
    peak_hour: int
    peak_day: str
    peak_time: str = Field(..., description="Combined peak day/hour for display")


class ClusterSummary(BaseModel):
    """Lightweight cluster info for list endpoints."""
    cluster_id: int
    severity: str
    ais_score: float
    point_count: int
    avg_speed_kmh: float
    peak_day: str
    peak_hour: int


# ─── GeoJSON Structures ───────────────────────────────────────────────────

class GeoJSONGeometry(BaseModel):
    """GeoJSON Point geometry."""
    type: str = "Point"
    coordinates: list[float] = Field(
        ..., description="[longitude, latitude] — GeoJSON order"
    )


class GeoJSONFeature(BaseModel):
    """A single GeoJSON Feature with cluster properties."""
    type: str = "Feature"
    geometry: GeoJSONGeometry
    properties: ClusterProperties


class GeoJSONFeatureCollection(BaseModel):
    """GeoJSON FeatureCollection — ready for L.geoJSON() in Leaflet."""
    type: str = "FeatureCollection"
    features: list[GeoJSONFeature]


# ─── Temporal (Hourly) Cluster Models ─────────────────────────────────────

class TemporalClusterProperties(BaseModel):
    """Properties for a single-hour cluster snapshot.

    Unlike ClusterProperties, this does NOT include duration_hours,
    recurrence_days, or ais_score — those are historical aggregate
    metrics that have no meaning for a single-hour time slice.
    """
    cluster_id: int
    severity: str = Field(..., description="LOW | MEDIUM | HIGH")
    point_count: int = Field(..., description="Number of measurement points in the cluster")
    avg_speed_kmh: float = Field(..., description="Mean speed across cluster points (km/h)")
    sum_vehicle_count: int = Field(..., description="Total vehicles across cluster points")
    avg_vehicle_count: float = Field(..., description="Mean vehicles per point (internal)")


class TemporalGeoJSONFeature(BaseModel):
    """A single GeoJSON Feature for temporal cluster data."""
    type: str = "Feature"
    geometry: GeoJSONGeometry
    properties: TemporalClusterProperties


class TemporalClusterResponse(BaseModel):
    """Response wrapper for temporal cluster queries.

    Contains the GeoJSON FeatureCollection plus metadata about
    the requested date, hour, and summary statistics.
    """
    type: str = "FeatureCollection"
    date: str = Field(..., description="Requested date (YYYY-MM-DD)")
    hour: int = Field(..., description="Requested hour (0-23)")
    cluster_count: int
    total_points: int = Field(0, description="Total measurement points across all clusters")
    total_vehicles: int = Field(0, description="Sum of vehicle counts across all clusters")
    avg_speed: Optional[float] = Field(None, description="Overall avg speed across clusters (km/h)")
    high_count: int = 0
    medium_count: int = 0
    low_count: int = 0
    features: list[TemporalGeoJSONFeature]


# ─── Heatmap Point ────────────────────────────────────────────────────────

class HeatmapPoint(BaseModel):
    """A single point for Leaflet.heat heatmap layer."""
    lat: float
    lon: float
    intensity: float = Field(..., description="Normalized intensity [0, 1]")
    vehicle_count: int
    avg_speed: float
    record_time: str


class HeatmapResponse(BaseModel):
    """Response for heatmap endpoint."""
    date: Optional[str] = None
    point_count: int
    points: list[HeatmapPoint]


# ─── Statistics ────────────────────────────────────────────────────────────

class StatsResponse(BaseModel):
    """Global statistics for the dashboard."""
    total_records: int
    total_clusters: int
    total_noise_points: int
    noise_percentage: float
    high_severity_count: int
    medium_severity_count: int
    low_severity_count: int
    date_range_start: Optional[str] = None
    date_range_end: Optional[str] = None
