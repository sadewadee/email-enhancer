"""Statistics service for dashboard queries."""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from database import get_database
import logging

logger = logging.getLogger(__name__)


class StatsService:
    """Service for fetching dashboard statistics."""
    
    @staticmethod
    def get_overview() -> Dict[str, Any]:
        """Get main dashboard overview stats."""
        db = get_database()
        
        # Check what tables exist
        has_results = db.table_exists('results')
        has_zen_contacts = db.table_exists('zen_contacts')
        has_zen_servers = db.table_exists('zen_servers')
        has_gmpas_job = db.table_exists('gmpas_job')
        
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
            'countries_processed': 0,
            'servers_online': 0,
            'servers_total': 0,
            'processed_24h': 0,
            'processed_1h': 0,
            'gmpas_jobs_total': 0,
            'gmpas_jobs_running': 0,
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
                        COUNT(*) FILTER (WHERE scrape_status = 'success') AS success,
                        COUNT(*) FILTER (WHERE scrape_status = 'failed') AS failed,
                        COUNT(*) FILTER (WHERE scrape_status = 'pending') AS pending,
                        COALESCE(SUM(emails_count), 0) AS emails,
                        COALESCE(SUM(phones_count), 0) AS phones,
                        COALESCE(SUM(whatsapp_count), 0) AS whatsapp,
                        COUNT(*) FILTER (WHERE has_email = TRUE) AS with_email,
                        COUNT(*) FILTER (WHERE has_phone = TRUE) AS with_phone,
                        COUNT(*) FILTER (WHERE has_whatsapp = TRUE) AS with_whatsapp,
                        COUNT(DISTINCT country_code) AS countries,
                        COUNT(*) FILTER (WHERE updated_at > NOW() - INTERVAL '24 hours') AS last_24h,
                        COUNT(*) FILTER (WHERE updated_at > NOW() - INTERVAL '1 hour') AS last_1h
                    FROM zen_contacts
                """)
                
                if enriched:
                    e = enriched[0]
                    stats['enriched_total'] = e['total']
                    stats['enriched_success'] = e['success']
                    stats['enriched_failed'] = e['failed']
                    stats['enriched_pending'] = e['pending']
                    stats['total_emails'] = int(e['emails'])
                    stats['total_phones'] = int(e['phones'])
                    stats['total_whatsapp'] = int(e['whatsapp'])
                    stats['rows_with_email'] = e['with_email']
                    stats['rows_with_phone'] = e['with_phone']
                    stats['rows_with_whatsapp'] = e['with_whatsapp']
                    stats['countries_processed'] = e['countries']
                    stats['processed_24h'] = e['last_24h']
                    stats['processed_1h'] = e['last_1h']
            
            # Calculate pending (source - enriched)
            if has_results and has_zen_contacts:
                pending = db.execute_scalar("""
                    SELECT COUNT(*) FROM results r
                    WHERE NOT EXISTS (
                        SELECT 1 FROM zen_contacts zc WHERE zc.source_link = r.data->>'link'
                    )
                """)
                stats['pending_total'] = pending or 0
            
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
            if has_gmpas_job:
                jobs = db.execute_query("""
                    SELECT 
                        COUNT(*) AS total,
                        COUNT(*) FILTER (WHERE status = 'running') AS running
                    FROM gmpas_job
                """)
                if jobs:
                    stats['gmpas_jobs_total'] = jobs[0]['total']
                    stats['gmpas_jobs_running'] = jobs[0]['running']
                    
        except Exception as e:
            logger.error(f"Error fetching overview stats: {e}")
        
        return stats
    
    @staticmethod
    def get_countries() -> List[Dict[str, Any]]:
        """Get progress per country."""
        db = get_database()
        
        if not db.table_exists('zen_contacts'):
            return []
        
        has_results = db.table_exists('results')
        
        try:
            if has_results:
                query = """
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
                ORDER BY COALESCE(s.source_count, 0) DESC
                """
            else:
                query = """
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
                ORDER BY enriched_total DESC
                """
            
            return db.execute_query(query)
            
        except Exception as e:
            logger.error(f"Error fetching countries: {e}")
            return []
    
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
                server_region,
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
                last_activity
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
    def get_gmpas_jobs() -> List[Dict[str, Any]]:
        """Get GMPAS job status (if table exists)."""
        db = get_database()
        
        if not db.table_exists('gmpas_job'):
            return []
        
        try:
            query = """
            SELECT *
            FROM gmpas_job
            ORDER BY created_at DESC
            LIMIT 100
            """
            return db.execute_query(query)
            
        except Exception as e:
            logger.error(f"Error fetching GMPAS jobs: {e}")
            return []
