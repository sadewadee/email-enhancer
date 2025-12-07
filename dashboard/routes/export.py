"""CSV export routes."""

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from services.exporter import ExportService, AVAILABLE_COLUMNS, ALL_COLUMNS

router = APIRouter(prefix="/api", tags=["export"])


class ExportRequest(BaseModel):
    columns: List[str] = Field(default=['business_name', 'emails', 'phones', 'country_code'])
    filters: Dict[str, Any] = Field(default_factory=dict)
    limit: int = Field(default=10000, ge=1, le=100000)


@router.get("/export/columns")
async def get_available_columns():
    """Get available columns for export."""
    return {
        "categories": AVAILABLE_COLUMNS,
        "all_columns": ALL_COLUMNS
    }


@router.post("/export/preview")
async def preview_export(request: ExportRequest):
    """Preview export data before download."""
    try:
        preview = ExportService.get_export_preview(
            columns=request.columns,
            filters=request.filters,
            limit=10
        )
        return preview
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/export")
async def export_csv(request: ExportRequest):
    """Export data to CSV file."""
    try:
        csv_buffer = ExportService.export_to_csv(
            columns=request.columns,
            filters=request.filters,
            limit=request.limit
        )

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"insighthub_export_{timestamp}.csv"

        return StreamingResponse(
            iter([csv_buffer.getvalue()]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
