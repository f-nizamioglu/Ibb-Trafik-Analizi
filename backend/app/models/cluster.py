"""
Pydantic models for cluster data and GeoJSON responses.

These models ensure type-safe serialization and provide automatic
Swagger documentation for all API endpoints.
"""

from __future__ import annotations

from typing import Any, Optional

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
    congestion_score: float = Field(
        ...,
        description="User-facing traffic intensity score [0, 100]",
    )
    point_count: int = Field(..., description="Number of measurement points in the cluster")
    avg_speed_kmh: float = Field(..., description="Mean speed across cluster points (km/h)")
    min_speed_kmh: Optional[float] = Field(
        None, description="Minimum speed among cluster measurement cells (km/h)"
    )
    max_speed_kmh: Optional[float] = Field(
        None, description="Maximum speed among cluster measurement cells (km/h)"
    )
    sum_vehicle_count: int = Field(..., description="Total vehicles across cluster points")
    avg_vehicle_count: float = Field(..., description="Mean vehicles per point (internal)")
    severity_reason: Optional[str] = Field(
        None,
        description="Short code explaining the assigned congestion severity",
    )


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
    # ── Active clustering parameters (echoed back for the UI) ──────────────
    # Optional so older callers / stubs that omit them still validate. The live
    # service always populates these with the parameters actually used.
    eps_meters: Optional[float] = Field(
        None, description="DBSCAN eps (meters, EPSG:32636) used for this query"
    )
    minpoints: Optional[int] = Field(
        None, description="DBSCAN minpoints used for this query"
    )
    avg_speed_threshold: Optional[float] = Field(
        None, description="avg_speed < threshold (km/h) pre-filter used for this query"
    )
    window_hours: Optional[int] = Field(
        None, description="Length of the time window (hours) used for this query"
    )
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


# ─── Geohash Measurement-Cell Layer ───────────────────────────────────────
# Unlike the cluster centroid markers, these features are the real İBB
# measurement cells (geohash grid) for the selected window, drawn as the
# rectangular geohash bounding box. They visualize the raw spatial resolution
# of the data that feeds DBSCAN — not cluster centroids.

class GeohashCellProperties(BaseModel):
    """Aggregated stats for one geohash measurement cell over the window."""
    geohash: str
    measurement_count: int = Field(
        ..., description="Number of hourly measurements aggregated for this cell"
    )
    record_count: Optional[int] = Field(
        None, description="Alias for measurement_count for frontend/debug displays"
    )
    avg_speed_kmh: float = Field(..., description="Mean speed across the cell's measurements")
    min_speed_kmh: float = Field(..., description="Minimum speed across the cell's measurements")
    max_speed_kmh: float = Field(..., description="Maximum speed across the cell's measurements")
    sum_vehicle_count: int = Field(..., description="Total vehicles across the cell's measurements")
    avg_vehicle_count: float = Field(..., description="Mean vehicles per measurement (cell)")
    congestion_score: float = Field(
        ...,
        description="Speed-derived display score [0, 100] for cell coloring; not predictive",
    )
    severity: str = Field(
        ...,
        description="Cell traffic category: free_flow | moderate | congested | severe",
    )
    severity_label: str = Field(..., description="Human-readable Turkish traffic category")
    color: str = Field(..., description="Suggested fill color for the cell category")


class GeohashCellGeometry(BaseModel):
    """GeoJSON Polygon geometry for a geohash cell bounding box (WGS84)."""
    type: str = "Polygon"
    coordinates: list[list[list[float]]] = Field(
        ..., description="Single linear ring: [[[lon, lat], ...]] in GeoJSON order"
    )


class GeohashCellFeature(BaseModel):
    """A single GeoJSON Feature for a geohash measurement cell."""
    type: str = "Feature"
    geometry: GeohashCellGeometry
    properties: GeohashCellProperties


class SeverityCounts(BaseModel):
    """Per-class cell counts for the data-aware legend."""
    free_flow: int = 0
    moderate: int = 0
    congested: int = 0
    severe: int = 0


class GeohashCellResponse(BaseModel):
    """Response wrapper for the geohash measurement-cell layer."""
    type: str = "FeatureCollection"
    date: str = Field(..., description="Requested date (YYYY-MM-DD)")
    hour: int = Field(..., description="Requested start hour (0-23)")
    mode: str = Field(
        "congested",
        description="'congested' keeps the avg_speed filter; 'all' returns all scoped cells",
    )
    window_hours: int = Field(1, description="Length of the time window (hours)")
    avg_speed_threshold: Optional[float] = Field(
        25.0,
        description="avg_speed < threshold (km/h) pre-filter used in congested mode",
    )
    max_cells: int = Field(4000, description="Maximum number of cells returned")
    cell_count: int = Field(0, description="Number of geohash cells returned")
    total_cells: int = Field(
        0,
        description="Approximate total cells matched before any cap (may exceed returned_cells)",
    )
    returned_cells: int = Field(0, description="Alias for cell_count — cells in this response")
    severity_counts: SeverityCounts = Field(
        default_factory=SeverityCounts,
        description="Count of cells by traffic severity class",
    )
    truncated: bool = Field(
        False,
        description="True when more cells matched than the response cap and the "
        "lowest-volume cells were dropped.",
    )
    features: list[GeohashCellFeature]


# Road line overlay inside congested geohash measurement cells. These are
# optional visualization features backed by the imported road_segments table.

class RoadLineGeometry(BaseModel):
    """GeoJSON LineString or MultiLineString geometry in lon/lat order."""
    type: str = Field(..., description="LineString or MultiLineString")
    coordinates: list[Any] = Field(..., description="GeoJSON coordinates in lon/lat order")


class RoadLineProperties(BaseModel):
    """Metadata for one clipped road segment.

    The severity/color/avg_speed_kmh/cell_count fields are populated only in the
    traffic-corridor style (``style=traffic``). They are **cell-derived**: the
    corridor's speed is the length-weighted average of the geohash measurement
    cells the road line intersects, NOT a road-segment speed ground truth.
    """
    road_id: int
    osm_id: Optional[int] = None
    highway: Optional[str] = None
    name: Optional[str] = None
    clipped_length_m: float = Field(
        ..., description="Length of the returned clipped geometry in meters"
    )
    avg_speed_kmh: Optional[float] = Field(
        None,
        description=(
            "Cell-derived corridor speed (km/h): length-weighted mean of the "
            "intersecting geohash cells. Traffic-corridor style only."
        ),
    )
    cell_count: Optional[int] = Field(
        None,
        description="Number of geohash cells the corridor intersects (traffic style).",
    )
    severity: Optional[str] = Field(
        None,
        description=(
            "Cell-derived corridor category: free_flow | moderate | congested | "
            "severe. Traffic-corridor style only."
        ),
    )
    severity_label: Optional[str] = Field(
        None, description="Human-readable Turkish corridor category (traffic style)."
    )
    color: Optional[str] = Field(
        None, description="Suggested line color for the corridor severity (traffic style)."
    )


class RoadLineFeature(BaseModel):
    """A single GeoJSON Feature for a road line clipped to congested cells."""
    type: str = "Feature"
    geometry: RoadLineGeometry
    properties: RoadLineProperties


class CongestedRoadResponse(BaseModel):
    """Road lines intersecting the selected congested geohash-cell area."""
    type: str = "FeatureCollection"
    date: str = Field(..., description="Requested date (YYYY-MM-DD)")
    hour: int = Field(..., description="Requested start hour (0-23)")
    window_hours: int = Field(1, description="Length of the time window (hours)")
    avg_speed_threshold: float = Field(
        25.0, description="avg_speed < threshold (km/h) pre-filter used for this query"
    )
    style: str = Field(
        "plain",
        description=(
            "'plain' clips road lines to the congested cell area (neutral context "
            "overlay); 'traffic' colors major corridors by cell-derived severity."
        ),
    )
    road_count: int = Field(0, description="Number of road features returned")
    limit: int = Field(..., description="Maximum number of road features returned")
    truncated: bool = Field(
        False,
        description="True when more road features matched than the response cap.",
    )
    severity_counts: Optional[SeverityCounts] = Field(
        None,
        description=(
            "Per-class corridor counts for the legend. Populated only in the "
            "traffic-corridor style; None for the plain overlay."
        ),
    )
    effective_parameters: dict[str, Any] = Field(
        default_factory=dict,
        description="Backend parameters and spatial scope used for the road query",
    )
    features: list[RoadLineFeature]
