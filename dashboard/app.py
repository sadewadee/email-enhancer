#!/usr/bin/env python3
"""
Zenvoyer Dashboard - Standalone Monitoring Service

A FastAPI-based dashboard for monitoring the email enhancer scraping system.
Monitors: zen_contacts, results, gmpas_job tables.

Usage:
    python app.py [--port 8080] [--host 0.0.0.0]
"""

import sys
import os
import argparse
import logging
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

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

# Create FastAPI app
app = FastAPI(
    title="Zenvoyer Dashboard",
    description="Monitoring dashboard for email enhancer scraping system",
    version="1.0.0"
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


@app.on_event("startup")
async def startup_event():
    """Initialize database connection on startup."""
    logger.info("Starting Zenvoyer Dashboard...")
    db = init_database(config)
    
    # Check available tables
    tables_status = {
        'zen_contacts': db.table_exists('zen_contacts'),
        'results': db.table_exists('results'),
        'zen_servers': db.table_exists('zen_servers'),
        'gmpas_job': db.table_exists('gmpas_job')
    }
    
    logger.info(f"Tables status: {tables_status}")


@app.on_event("shutdown")
async def shutdown_event():
    """Close database connection on shutdown."""
    try:
        db = get_database()
        db.close()
    except:
        pass
    logger.info("Dashboard shutdown complete")


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
    parser = argparse.ArgumentParser(description='Zenvoyer Dashboard')
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
