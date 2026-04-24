"""
Health check endpoint.
"""

import logging

from fastapi import APIRouter

from backend.app.database import get_pool

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/health")
async def health_check():
    """Check database connectivity and return service status."""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1;")
        return {
            "status": "ok",
            "database": "connected",
            "service": "Istanbul Traffic Anomaly API",
        }
    except Exception as e:
        logger.error("Health check failed: %s", e)
        return {
            "status": "error",
            "database": "disconnected",
        }
