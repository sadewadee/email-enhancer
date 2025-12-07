"""Statistics service for dashboard queries with caching."""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from database import get_database
import logging
import time

logger = logging.getLogger(__name__)

# In-memory cache with configurable TTL per data type
_cache: Dict[str, Any] = {}
_cache_times: Dict[str, float] = {}

# Different TTLs for different data types (in seconds)
CACHE_TTL = {
    'overview': 30,      # Stats update every 30s
    'countries': 60,     # Countries update every 60s
    'servers': 15,       # Server status more frequent
    'activity': 10,      # Recent activity needs fresher data
    'hourly': 300,       # Hourly stats cache 5 min
    'default': 30
}


def _get_cached(key: str, ttl_key: str = None):
    """Get cached value if not expired."""
    if key in _cache and key in _cache_times:
        ttl = CACHE_TTL.get(ttl_key or key.split('_')[0], CACHE_TTL['default'])
        if time.time() - _cache_times[key] < ttl:
            return _cache[key]
    return None


def _set_cached(key: str, value: Any):
    """Set cache value."""
    _cache[key] = value
    _cache_times[key] = time.time()


def clear_cache(key: str = None):
    """Clear cache - all or specific key."""
    global _cache, _cache_times
    if key:
        _cache.pop(key, None)
        _cache_times.pop(key, None)
    else:
        _cache = {}
        _cache_times = {}


class StatsService:
    """Service for fetching dashboard statistics."""

    @staticmethod
    def get_overview() -> Dict[str, Any]:
        """Get main dashboard overview stats."""
        cached = _get_cached('overview')
        if cached:
            return cached

        db = get_database()

        # Check what tables exist
        has_results = db.table_exists('results')
        has_zen_contacts = db.table_exists('zen_contacts')
        has_zen_servers = db.table_exists('zen_servers')
        has_gmaps_jobs = db.table_exists('gmaps_jobs')

        stats = {
            'source_total': 0,
            'enriched_total': 0,
            'enriched_success': 0,
            'enriched_failed': 0,
            'enriched_pending': 0,
            'pending_total': 0,
            'total_emails': 0,
            'total_phones': 0,
            'total_whatsapp': 0,
            'rows_with_email': 0,
            'rows_with_phone': 0,
            'rows_with_whatsapp': 0,
            'rows_with_social': 0,
            'total_social': 0,
            'countries_processed': 0,
            'servers_online': 0,
            'servers_total': 0,
            'processed_24h': 0,
            'processed_1h': 0,
            'rate_per_hour': 0,
            'gmaps_jobss_total': 0,
            'gmaps_jobss_running': 0,
            'generated_at': datetime.utcnow().isoformat()
        }

        try:
            # Source (results) stats
            if has_results:
                stats['source_total'] = db.execute_scalar("SELECT COUNT(*) FROM results") or 0

            # Enriched (zen_contacts) stats
            if has_zen_contacts:
                enriched = db.execute_query("""
                    SELECT
                        COUNT(*) AS total,
                        COUNT(*) FILTER (WHERE scrape_status IN ('success', 'no_contacts_found')) AS success,
                        COUNT(*) FILTER (WHERE scrape_status IN ('failed', 'error', 'timeout')) AS failed,
                        COUNT(*) FILTER (WHERE scrape_status IS NULL OR scrape_status IN ('pending', 'processing', 'queued')) AS processing,
                        COALESCE(SUM(emails_count), 0) AS emails,
                        COALESCE(SUM(phones_count), 0) AS phones,
                        COALESCE(SUM(whatsapp_count), 0) AS whatsapp,
                        COUNT(*) FILTER (WHERE has_email = TRUE OR emails_count > 0) AS with_email,
                        COUNT(*) FILTER (WHERE has_phone = TRUE OR phones_count > 0) AS with_phone,
                        COUNT(*) FILTER (WHERE has_whatsapp = TRUE OR whatsapp_count > 0) AS with_whatsapp,
                        COUNT(*) FILTER (WHERE
                            social_facebook IS NOT NULL OR
                            social_instagram IS NOT NULL OR
                            social_tiktok IS NOT NULL OR
                            social_youtube IS NOT NULL
                        ) AS with_social,
                        (
                            COUNT(*) FILTER (WHERE social_facebook IS NOT NULL) +
                            COUNT(*) FILTER (WHERE social_instagram IS NOT NULL) +
                            COUNT(*) FILTER (WHERE social_tiktok IS NOT NULL) +
                            COUNT(*) FILTER (WHERE social_youtube IS NOT NULL)
                        ) AS social_total,
                        COUNT(DISTINCT country_code) AS countries,
                        COUNT(*) FILTER (WHERE updated_at > NOW() - INTERVAL '24 hours' AND scrape_status IS NOT NULL) AS last_24h,
                        COUNT(*) FILTER (WHERE updated_at > NOW() - INTERVAL '1 hour' AND scrape_status IS NOT NULL) AS last_1h
                    FROM zen_contacts
                """)

                if enriched:
                    e = enriched[0]
                    stats['enriched_total'] = e['total']
                    stats['enriched_success'] = e['success']
                    stats['enriched_failed'] = e['failed']
                    stats['enriched_pending'] = e['processing']
                    stats['total_emails'] = int(e['emails'])
                    stats['total_phones'] = int(e['phones'])
                    stats['total_whatsapp'] = int(e['whatsapp'])
                    stats['rows_with_email'] = e['with_email']
                    stats['rows_with_phone'] = e['with_phone']
                    stats['rows_with_whatsapp'] = e['with_whatsapp']
                    stats['rows_with_social'] = e['with_social']
                    stats['total_social'] = int(e['social_total'])
                    stats['countries_processed'] = e['countries']
                    stats['processed_24h'] = e['last_24h']
                    stats['processed_1h'] = e['last_1h']
                    # Calculate rate per hour (based on last 1h activity)
                    stats['rate_per_hour'] = e['last_1h']

            # Calculate pending (source - enriched processed)
            # Use simple subtraction instead of slow NOT EXISTS
            if has_results and has_zen_contacts:
                stats['pending_total'] = max(0, stats['source_total'] - (stats['enriched_success'] + stats['enriched_failed']))

            # Server stats
            if has_zen_servers:
                servers = db.execute_query("""
                    SELECT
                        COUNT(*) AS total,
                        COUNT(*) FILTER (WHERE status = 'online') AS online
                    FROM zen_servers
                """)
                if servers:
                    stats['servers_total'] = servers[0]['total']
                    stats['servers_online'] = servers[0]['online']

            # GMPAS job stats (if table exists)
            if has_gmaps_jobs:
                jobs = db.execute_query("""
                    SELECT
                        COUNT(*) AS total,
                        COUNT(*) FILTER (WHERE status = 'running') AS running
                    FROM gmaps_jobs
                """)
                if jobs:
                    stats['gmaps_jobss_total'] = jobs[0]['total']
                    stats['gmaps_jobss_running'] = jobs[0]['running']

        except Exception as e:
            logger.error(f"Error fetching overview stats: {e}")

        _set_cached('overview', stats)
        return stats

    @staticmethod
    def get_countries_paginated(
        page: int = 1,
        limit: int = 20,
        sort_by: str = "source_total",
        sort_order: str = "desc"
    ) -> Dict[str, Any]:
        """Get progress per country with pagination and sorting."""
        db = get_database()

        if not db.table_exists('zen_contacts'):
            return {"countries": [], "total": 0, "page": page, "limit": limit, "pages": 0}

        has_results = db.table_exists('results')

        # Allowed sort columns to prevent SQL injection
        allowed_sort = {
            'country_code': 'country_code',
            'source_total': 'source_total',
            'enriched_total': 'enriched_total',
            'pending': 'pending',
            'progress_percent': 'progress_percent',
            'emails_total': 'emails_total',
            'whatsapp_total': 'whatsapp_total',
            'avg_scrape_time': 'avg_scrape_time'
        }

        sort_col = allowed_sort.get(sort_by, 'source_total')
        sort_dir = 'DESC' if sort_order.lower() == 'desc' else 'ASC'
        offset = (page - 1) * limit

        try:
            if has_results:
                # Count total countries first
                count_query = """
                WITH source_counts AS (
                    SELECT UPPER(LEFT(COALESCE(r.data->'complete_address'->>'country', 'XX'), 2)) AS country_code
                    FROM results r
                    GROUP BY 1
                ),
                enriched_counts AS (
                    SELECT country_code FROM zen_contacts GROUP BY country_code
                )
                SELECT COUNT(DISTINCT COALESCE(s.country_code, e.country_code))
                FROM source_counts s
                FULL OUTER JOIN enriched_counts e ON s.country_code = e.country_code
                """
                total = db.execute_scalar(count_query) or 0

                # Main query with pagination
                query = f"""
                WITH source_counts AS (
                    SELECT
                        UPPER(LEFT(COALESCE(r.data->'complete_address'->>'country', 'XX'), 2)) AS country_code,
                        COUNT(*) AS source_count
                    FROM results r
                    GROUP BY 1
                ),
                enriched_counts AS (
                    SELECT
                        country_code,
                        COUNT(*) AS enriched_count,
                        COUNT(*) FILTER (WHERE scrape_status = 'success') AS success_count,
                        COUNT(*) FILTER (WHERE scrape_status = 'failed') AS failed_count,
                        COALESCE(SUM(emails_count), 0) AS emails_total,
                        COALESCE(SUM(phones_count), 0) AS phones_total,
                        COALESCE(SUM(whatsapp_count), 0) AS whatsapp_total,
                        COUNT(*) FILTER (WHERE has_email = TRUE) AS rows_with_email,
                        COUNT(*) FILTER (WHERE has_whatsapp = TRUE) AS rows_with_whatsapp,
                        ROUND(AVG(scrape_time_seconds)::NUMERIC, 2) AS avg_scrape_time,
                        MAX(updated_at) AS last_activity
                    FROM zen_contacts
                    GROUP BY country_code
                )
                SELECT
                    COALESCE(s.country_code, e.country_code) AS country_code,
                    COALESCE(s.source_count, 0) AS source_total,
                    COALESCE(e.enriched_count, 0) AS enriched_total,
                    COALESCE(s.source_count, 0) - COALESCE(e.enriched_count, 0) AS pending,
                    CASE
                        WHEN COALESCE(s.source_count, 0) > 0
                        THEN ROUND((COALESCE(e.enriched_count, 0)::NUMERIC / s.source_count * 100), 1)
                        ELSE 0
                    END AS progress_percent,
                    COALESCE(e.success_count, 0) AS success_count,
                    COALESCE(e.failed_count, 0) AS failed_count,
                    COALESCE(e.emails_total, 0) AS emails_total,
                    COALESCE(e.phones_total, 0) AS phones_total,
                    COALESCE(e.whatsapp_total, 0) AS whatsapp_total,
                    COALESCE(e.rows_with_email, 0) AS rows_with_email,
                    COALESCE(e.rows_with_whatsapp, 0) AS rows_with_whatsapp,
                    COALESCE(e.avg_scrape_time, 0) AS avg_scrape_time,
                    e.last_activity
                FROM source_counts s
                FULL OUTER JOIN enriched_counts e ON s.country_code = e.country_code
                ORDER BY {sort_col} {sort_dir} NULLS LAST
                LIMIT %s OFFSET %s
                """
                result = db.execute_query(query, (limit, offset))
            else:
                # Count total
                count_query = "SELECT COUNT(DISTINCT country_code) FROM zen_contacts"
                total = db.execute_scalar(count_query) or 0

                query = f"""
                SELECT
                    country_code,
                    0 AS source_total,
                    COUNT(*) AS enriched_total,
                    0 AS pending,
                    100.0 AS progress_percent,
                    COUNT(*) FILTER (WHERE scrape_status = 'success') AS success_count,
                    COUNT(*) FILTER (WHERE scrape_status = 'failed') AS failed_count,
                    COALESCE(SUM(emails_count), 0) AS emails_total,
                    COALESCE(SUM(phones_count), 0) AS phones_total,
                    COALESCE(SUM(whatsapp_count), 0) AS whatsapp_total,
                    COUNT(*) FILTER (WHERE has_email = TRUE) AS rows_with_email,
                    COUNT(*) FILTER (WHERE has_whatsapp = TRUE) AS rows_with_whatsapp,
                    ROUND(AVG(scrape_time_seconds)::NUMERIC, 2) AS avg_scrape_time,
                    MAX(updated_at) AS last_activity
                FROM zen_contacts
                GROUP BY country_code
                ORDER BY {sort_col} {sort_dir} NULLS LAST
                LIMIT %s OFFSET %s
                """
                result = db.execute_query(query, (limit, offset))

            pages = (total + limit - 1) // limit if limit > 0 else 0

            return {
                "countries": result,
                "total": total,
                "page": page,
                "limit": limit,
                "pages": pages
            }

        except Exception as e:
            logger.error(f"Error fetching countries: {e}")
            return {"countries": [], "total": 0, "page": page, "limit": limit, "pages": 0}

    @staticmethod
    def get_servers() -> List[Dict[str, Any]]:
        """Get server status."""
        db = get_database()

        if not db.table_exists('zen_servers'):
            return []

        try:
            query = """
            SELECT
                server_id,
                server_name,
                COALESCE(server_ip, '') AS server_ip,
                COALESCE(server_hostname, '') AS server_hostname,
                COALESCE(server_region, '') AS server_region,
                status,
                workers_count,
                current_task,
                CASE
                    WHEN last_heartbeat > NOW() - INTERVAL '2 minutes' THEN 'healthy'
                    WHEN last_heartbeat > NOW() - INTERVAL '5 minutes' THEN 'warning'
                    ELSE 'critical'
                END AS health,
                EXTRACT(EPOCH FROM (NOW() - last_heartbeat))::INTEGER AS seconds_since_heartbeat,
                total_processed,
                total_success,
                total_failed,
                success_rate,
                urls_per_minute,
                avg_time_per_url,
                session_processed,
                session_errors,
                last_heartbeat,
                last_activity,
                started_at,
                session_started
            FROM zen_servers
            ORDER BY
                CASE status
                    WHEN 'online' THEN 1
                    WHEN 'paused' THEN 2
                    WHEN 'error' THEN 3
                    ELSE 4
                END,
                last_heartbeat DESC NULLS LAST
            """
            return db.execute_query(query)

        except Exception as e:
            logger.error(f"Error fetching servers: {e}")
            return []

    @staticmethod
    def get_recent_activity(limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent scraping activity."""
        db = get_database()

        if not db.table_exists('zen_contacts'):
            return []

        try:
            query = """
            SELECT
                id,
                country_code,
                business_name,
                business_category,
                business_website,
                scrape_status,
                emails_count,
                phones_count,
                whatsapp_count,
                social_facebook IS NOT NULL AS has_facebook,
                social_instagram IS NOT NULL AS has_instagram,
                scrape_time_seconds,
                last_scrape_server,
                updated_at
            FROM zen_contacts
            ORDER BY updated_at DESC
            LIMIT %s
            """
            return db.execute_query(query, (limit,))

        except Exception as e:
            logger.error(f"Error fetching recent activity: {e}")
            return []

    @staticmethod
    def get_hourly_stats(days: int = 7) -> List[Dict[str, Any]]:
        """Get hourly statistics for charts."""
        db = get_database()

        if not db.table_exists('zen_contacts'):
            return []

        try:
            query = """
            SELECT
                date_trunc('hour', updated_at) AS hour,
                COUNT(*) AS rows_processed,
                COUNT(*) FILTER (WHERE scrape_status = 'success') AS success,
                COUNT(*) FILTER (WHERE scrape_status = 'failed') AS failed,
                COALESCE(SUM(emails_count), 0) AS emails_found,
                COALESCE(SUM(whatsapp_count), 0) AS whatsapp_found,
                ROUND(AVG(scrape_time_seconds)::NUMERIC, 2) AS avg_time,
                COUNT(DISTINCT last_scrape_server) AS active_servers
            FROM zen_contacts
            WHERE updated_at > NOW() - INTERVAL '%s days'
            GROUP BY 1
            ORDER BY 1 DESC
            """
            return db.execute_query(query % days)

        except Exception as e:
            logger.error(f"Error fetching hourly stats: {e}")
            return []

    @staticmethod
    def get_gmaps_jobss() -> List[Dict[str, Any]]:
        """Get GMPAS job status (if table exists)."""
        db = get_database()

        if not db.table_exists('gmaps_jobs'):
            return []

        try:
            query = """
            SELECT *
            FROM gmaps_jobs
            ORDER BY created_at DESC
            LIMIT 100
            """
            return db.execute_query(query)

        except Exception as e:
            logger.error(f"Error fetching GMPAS jobs: {e}")
            return []
