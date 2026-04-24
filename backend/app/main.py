"""
FastAPI application — serves clustered traffic data as GeoJSON for the Leaflet.js frontend.

Run with:
    uvicorn backend.app.main:app --reload --port 8000

API Docs:
    http://localhost:8000/docs  (Swagger UI)
    http://localhost:8000/redoc (ReDoc)
"""

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from backend.app.config import get_settings
from backend.app.database import close_pool, init_pool
from backend.app.limiter import limiter
from backend.app.routers import clusters, health, heatmap

settings = get_settings()

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_INDEX_HTML = _PROJECT_ROOT / "index.html"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage database connection pool lifecycle."""
    await init_pool()
    yield
    await close_pool()


app = FastAPI(
    title="Istanbul Traffic Anomaly API",
    description=(
        "RESTful API serving ST-DBSCAN clustered traffic anomaly data "
        "from IBB (Istanbul Municipality) open data. "
        "Designed for consumption by a Leaflet.js frontend."
    ),
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs" if settings.app_env != "production" else None,
    redoc_url="/redoc" if settings.app_env != "production" else None,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# ─── CORS — allow authorized origins ──────────────────────────────────────
allowed_origins = [o.strip() for o in settings.allowed_origins.split(",")]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["Content-Type", "Accept"],
)

# ─── Routers ──────────────────────────────────────────────────────────────
app.include_router(health.router, prefix="/api", tags=["Health"])
app.include_router(clusters.router, prefix="/api", tags=["Clusters"])
app.include_router(heatmap.router, prefix="/api", tags=["Heatmap"])


@app.get("/", tags=["Root"])
async def root():
    return FileResponse(str(_INDEX_HTML))
