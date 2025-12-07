"""REST API routes for dashboard with caching."""

from fastapi import APIRouter, HTTPException, Query, Response
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from typing import Optional, List
from services.stats import StatsService, clear_cache

router = APIRouter(prefix="/api", tags=["api"])


def cached_response(data: dict, max_age: int = 30) -> JSONResponse:
    """Return JSON response with cache headers."""
    return JSONResponse(
        content=jsonable_encoder(data),
        headers={
            "Cache-Control": f"public, max-age={max_age}",
            "X-Cache-TTL": str(max_age)
        }
    )


@router.get("/stats")
async def get_stats():
    """Get overall dashboard statistics."""
    data = StatsService.get_overview()
    return cached_response(data, max_age=30)


@router.get("/countries")
async def get_countries(
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=20, ge=1, le=100),
    sort_by: str = Query(default="source_total"),
    sort_order: str = Query(default="desc")
):
    """Get progress per country with pagination and sorting."""
    result = StatsService.get_countries_paginated(
        page=page,
        limit=limit,
        sort_by=sort_by,
        sort_order=sort_order
    )
    return cached_response(result, max_age=60)


@router.get("/servers")
async def get_servers():
    """Get server status."""
    data = {"servers": StatsService.get_servers()}
    return cached_response(data, max_age=15)


@router.get("/recent")
async def get_recent(limit: int = Query(default=100, ge=1, le=500)):
    """Get recent scraping activity."""
    data = {"activity": StatsService.get_recent_activity(limit)}
    return cached_response(data, max_age=10)


@router.get("/hourly")
async def get_hourly(days: int = Query(default=7, ge=1, le=30)):
    """Get hourly statistics for charts."""
    data = {"stats": StatsService.get_hourly_stats(days)}
    return cached_response(data, max_age=300)


@router.get("/jobs")
async def get_jobs():
    """Get GMPAS job status."""
    data = {"jobs": StatsService.get_gmaps_jobss()}
    return cached_response(data, max_age=30)


@router.post("/cache/clear")
async def clear_api_cache():
    """Clear server-side cache."""
    clear_cache()
    return {"status": "cache cleared"}


@router.get("/health")
async def health_check():
    """API health check."""
    return {"status": "ok"}
