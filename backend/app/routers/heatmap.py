"""
Heatmap endpoint — provides intensity-weighted points for Leaflet.heat.

Endpoints:
    GET /api/heatmap              → All clustered points with intensity
    GET /api/heatmap?date=2025-01-17 → Points for a specific date
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query

from backend.app.models.cluster import HeatmapResponse
from backend.app.services.cluster_service import get_heatmap_data

router = APIRouter()


@router.get("/heatmap", response_model=HeatmapResponse)
async def heatmap(
    date: Optional[str] = Query(
        None,
        description="Filter by date (YYYY-MM-DD format)",
        pattern=r"^\d{4}-\d{2}-\d{2}$",
    ),
):
    """
    Get traffic density points with normalised intensity for heatmap rendering.

    Each point has:
    - lat, lon: coordinates (snapped to road if available)
    - intensity: normalised value [0, 1] based on vehicle count
    - vehicle_count, avg_speed: raw values

    Leaflet.heat usage:
    ```js
    const heat = L.heatLayer(
        data.points.map(p => [p.lat, p.lon, p.intensity]),
        { radius: 25, blur: 15 }
    ).addTo(map);
    ```
    """
    return await get_heatmap_data(date_filter=date)
