"""REST API routes for dashboard."""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List
from ..services.stats import StatsService

router = APIRouter(prefix="/api", tags=["api"])


@router.get("/stats")
async def get_stats():
    """Get overall dashboard statistics."""
    return StatsService.get_overview()


@router.get("/countries")
async def get_countries():
    """Get progress per country."""
    return {"countries": StatsService.get_countries()}


@router.get("/servers")
async def get_servers():
    """Get server status."""
    return {"servers": StatsService.get_servers()}


@router.get("/recent")
async def get_recent(limit: int = Query(default=100, ge=1, le=500)):
    """Get recent scraping activity."""
    return {"activity": StatsService.get_recent_activity(limit)}


@router.get("/hourly")
async def get_hourly(days: int = Query(default=7, ge=1, le=30)):
    """Get hourly statistics for charts."""
    return {"stats": StatsService.get_hourly_stats(days)}


@router.get("/jobs")
async def get_jobs():
    """Get GMPAS job status."""
    return {"jobs": StatsService.get_gmpas_jobs()}


@router.get("/health")
async def health_check():
    """API health check."""
    return {"status": "ok"}
