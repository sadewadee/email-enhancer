#!/usr/bin/env python3
"""
InsightHub Dashboard - Standalone Monitoring Service

A FastAPI-based dashboard for monitoring the email enhancer scraping system.
Monitors: zen_contacts, results, gmaps_jobs tables.

Usage:
    python app.py [--port 8080] [--host 0.0.0.0]
"""

import sys
import os
import argparse
import logging
from pathlib import Path

# Add dashboard directory to path for imports
dashboard_dir = Path(__file__).parent
sys.path.insert(0, str(dashboard_dir))

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import uvicorn

from config import load_config
from database import init_database, get_database
from routes.api import router as api_router
from routes.export import router as export_router

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load config
config = load_config()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan event handler for startup and shutdown."""
    # Startup
    logger.info("Starting InsightHub Dashboard...")
    db = init_database(config)

    tables_status = {
        'zen_contacts': db.table_exists('zen_contacts'),
        'results': db.table_exists('results'),
        'zen_servers': db.table_exists('zen_servers'),
        'gmaps_jobs': db.table_exists('gmaps_jobs')
    }
    logger.info(f"Tables status: {tables_status}")

    yield

    # Shutdown
    try:
        db = get_database()
        db.close()
    except:
        pass
    logger.info("Dashboard shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="InsightHub Dashboard",
    description="Monitoring dashboard for email enhancer scraping system",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files
static_path = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(static_path)), name="static")

# Templates
templates_path = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(templates_path))

# Include routers
app.include_router(api_router)
app.include_router(export_router)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Main dashboard page."""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/export", response_class=HTMLResponse)
async def export_page(request: Request):
    """Export page."""
    return templates.TemplateResponse("export.html", {"request": request})


def main():
    """Run the dashboard server."""
    parser = argparse.ArgumentParser(description='InsightHub Dashboard')
    parser.add_argument('--host', default=config.host, help='Host to bind')
    parser.add_argument('--port', type=int, default=config.port, help='Port to bind')
    parser.add_argument('--reload', action='store_true', help='Enable auto-reload')
    args = parser.parse_args()

    logger.info(f"Starting dashboard on {args.host}:{args.port}")

    uvicorn.run(
        "app:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level="info"
    )


if __name__ == "__main__":
    main()
