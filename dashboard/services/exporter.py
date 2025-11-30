"""CSV export service."""

import csv
import io
from typing import Dict, Any, List, Optional
from datetime import datetime
from ..database import get_database
import logging

logger = logging.getLogger(__name__)

AVAILABLE_COLUMNS = {
    'identity': ['id', 'source_link', 'source_id'],
    'business': ['business_name', 'business_category', 'business_website'],
    'location': ['country_code', 'country_name', 'city', 'state', 'address', 'postal_code', 'latitude', 'longitude', 'timezone'],
    'gmaps': ['gmaps_phone', 'gmaps_rating', 'gmaps_review_count', 'gmaps_price_range'],
    'contacts': ['emails', 'emails_count', 'phones', 'phones_count', 'whatsapp', 'whatsapp_count'],
    'social': ['social_facebook', 'social_instagram', 'social_tiktok', 'social_youtube', 'social_linkedin', 'social_twitter'],
    'metadata': ['scrape_status', 'scrape_error', 'scrape_time_seconds', 'created_at', 'updated_at', 'scrape_count']
}

ALL_COLUMNS = sum(AVAILABLE_COLUMNS.values(), [])


class ExportService:
    """Service for exporting data to CSV."""
    
    @staticmethod
    def validate_columns(columns: List[str]) -> List[str]:
        """Validate and filter requested columns."""
        valid = []
        for col in columns:
            if col in ALL_COLUMNS:
                valid.append(col)
        return valid if valid else ['business_name', 'emails', 'phones', 'country_code']
    
    @staticmethod
    def build_export_query(
        columns: List[str],
        filters: Dict[str, Any],
        limit: int
    ) -> tuple:
        """Build SQL query for export."""
        
        # Escape columns
        safe_columns = [f'"{c}"' if c in ['id'] else c for c in columns]
        
        # Handle array columns
        select_cols = []
        for col in columns:
            if col in ['emails', 'phones', 'whatsapp']:
                select_cols.append(f"array_to_string({col}, ', ') AS {col}")
            else:
                select_cols.append(col)
        
        query_parts = [
            f"SELECT {', '.join(select_cols)}",
            "FROM zen_contacts",
            "WHERE 1=1"
        ]
        
        params = []
        
        # Apply filters
        if filters.get('country_code'):
            codes = filters['country_code']
            if isinstance(codes, str):
                codes = [codes]
            placeholders = ', '.join(['%s'] * len(codes))
            query_parts.append(f"AND country_code IN ({placeholders})")
            params.extend(codes)
        
        if filters.get('scrape_status'):
            query_parts.append("AND scrape_status = %s")
            params.append(filters['scrape_status'])
        
        if filters.get('has_email') is True:
            query_parts.append("AND has_email = TRUE")
        
        if filters.get('has_phone') is True:
            query_parts.append("AND has_phone = TRUE")
        
        if filters.get('has_whatsapp') is True:
            query_parts.append("AND has_whatsapp = TRUE")
        
        if filters.get('date_from'):
            query_parts.append("AND updated_at >= %s")
            params.append(filters['date_from'])
        
        if filters.get('date_to'):
            query_parts.append("AND updated_at <= %s")
            params.append(filters['date_to'])
        
        if filters.get('category'):
            query_parts.append("AND business_category ILIKE %s")
            params.append(f"%{filters['category']}%")
        
        query_parts.append("ORDER BY updated_at DESC")
        query_parts.append(f"LIMIT {min(limit, 100000)}")
        
        return ' '.join(query_parts), tuple(params)
    
    @staticmethod
    def export_to_csv(
        columns: List[str],
        filters: Dict[str, Any],
        limit: int = 10000
    ) -> io.StringIO:
        """Export data to CSV buffer."""
        db = get_database()
        
        # Validate columns
        columns = ExportService.validate_columns(columns)
        
        # Build query
        query, params = ExportService.build_export_query(columns, filters, limit)
        
        logger.info(f"Export query: {query[:200]}... params: {params}")
        
        # Execute
        rows = db.execute_query(query, params)
        
        # Write CSV
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=columns)
        writer.writeheader()
        
        for row in rows:
            # Convert datetime to string
            for k, v in row.items():
                if isinstance(v, datetime):
                    row[k] = v.isoformat()
            writer.writerow(row)
        
        output.seek(0)
        logger.info(f"Exported {len(rows)} rows")
        
        return output
    
    @staticmethod
    def get_export_preview(
        columns: List[str],
        filters: Dict[str, Any],
        limit: int = 10
    ) -> Dict[str, Any]:
        """Get preview of export data."""
        db = get_database()
        
        columns = ExportService.validate_columns(columns)
        query, params = ExportService.build_export_query(columns, filters, limit)
        
        # Get count
        count_query = f"""
        SELECT COUNT(*) FROM zen_contacts WHERE 1=1
        """
        count_params = []
        
        if filters.get('country_code'):
            codes = filters['country_code']
            if isinstance(codes, str):
                codes = [codes]
            placeholders = ', '.join(['%s'] * len(codes))
            count_query += f" AND country_code IN ({placeholders})"
            count_params.extend(codes)
        
        if filters.get('scrape_status'):
            count_query += " AND scrape_status = %s"
            count_params.append(filters['scrape_status'])
        
        if filters.get('has_email') is True:
            count_query += " AND has_email = TRUE"
        
        total_count = db.execute_scalar(count_query, tuple(count_params)) or 0
        
        # Get sample
        rows = db.execute_query(query, params)
        
        # Convert datetime
        for row in rows:
            for k, v in row.items():
                if isinstance(v, datetime):
                    row[k] = v.isoformat()
        
        return {
            'total_matching': total_count,
            'preview_rows': rows,
            'columns': columns
        }
