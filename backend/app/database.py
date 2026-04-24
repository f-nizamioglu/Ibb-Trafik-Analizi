"""
Async database connection pool using asyncpg.

Provides a global pool that is initialized at FastAPI startup and
closed at shutdown (managed via lifespan in main.py).
"""

from __future__ import annotations

import logging

import asyncpg

from backend.app.config import get_settings

logger = logging.getLogger(__name__)

_pool: asyncpg.Pool | None = None


async def init_pool() -> None:
    """Create the asyncpg connection pool."""
    global _pool
    settings = get_settings()
    _pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=2,
        max_size=10,
        command_timeout=30,
    )
    logger.info(
        "Database pool created: %s:%s/%s",
        settings.db_host,
        settings.db_port,
        settings.db_name,
    )


async def close_pool() -> None:
    """Close the asyncpg connection pool."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("Database pool closed.")


async def get_pool() -> asyncpg.Pool:
    """Get the active connection pool. Raises if not initialized."""
    if _pool is None:
        raise RuntimeError(
            "Database pool is not initialized. "
            "Ensure init_pool() is awaited in the FastAPI lifespan before the first request."
        )
    return _pool
