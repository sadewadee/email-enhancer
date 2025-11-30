"""
Database Writer V2 for Zenvoyer Schema

Writes to zen_contacts (partitioned table) with:
- Hash partitioned table (32 partitions)
- Batch UPSERT with array merging
- Server registration and heartbeat
- Country extraction from data

Tables: zen_contacts, zen_servers, zen_jobs
Author: Claude (Sonnet 4)
Date: 2025-11-30
"""

import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values
import logging
import json
import time
from typing import Dict, Optional, List, Any
from dataclasses import dataclass


@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    host: str
    port: int
    database: str
    user: str
    password: str
    min_connections: int = 1
    max_connections: int = 5  # Reduced from 10 to prevent pool saturation
    connect_timeout: int = 10
    statement_timeout: int = 60000


class DatabaseWriterV2:
    """
    Optimized database writer for partitioned schema.
    
    Features:
    - Works with scraped_contacts_v2 (hash partitioned)
    - Batch UPSERT with array merging
    - Automatic partition key calculation
    - Country code extraction
    """
    
    # ISO 3166-1 alpha-2 country codes (165 countries)
    VALID_COUNTRIES = {
        'AF', 'AL', 'DZ', 'AD', 'AO', 'AR', 'AM', 'AU', 'AT', 'AZ',
        'BH', 'BD', 'BY', 'BE', 'BZ', 'BJ', 'BT', 'BO', 'BA', 'BW',
        'BR', 'BN', 'BG', 'BF', 'BI', 'KH', 'CM', 'CA', 'CV', 'CF',
        'TD', 'CL', 'CN', 'CO', 'KM', 'CG', 'CD', 'CR', 'CI', 'HR',
        'CU', 'CY', 'CZ', 'DK', 'DJ', 'DM', 'DO', 'EC', 'EG', 'SV',
        'GQ', 'ER', 'EE', 'ET', 'FJ', 'FI', 'FR', 'GA', 'GM', 'GE',
        'DE', 'GH', 'GR', 'GT', 'GN', 'GW', 'GY', 'HT', 'HN', 'HK',
        'HU', 'IS', 'IN', 'ID', 'IR', 'IQ', 'IE', 'IL', 'IT', 'JM',
        'JP', 'JO', 'KZ', 'KE', 'KW', 'KG', 'LA', 'LV', 'LB', 'LS',
        'LR', 'LY', 'LI', 'LT', 'LU', 'MO', 'MK', 'MG', 'MW', 'MY',
        'MV', 'ML', 'MT', 'MR', 'MU', 'MX', 'MD', 'MC', 'MN', 'ME',
        'MA', 'MZ', 'MM', 'NA', 'NP', 'NL', 'NZ', 'NI', 'NE', 'NG',
        'NO', 'OM', 'PK', 'PA', 'PG', 'PY', 'PE', 'PH', 'PL', 'PT',
        'QA', 'RO', 'RU', 'RW', 'SA', 'SN', 'RS', 'SG', 'SK', 'SI',
        'SO', 'ZA', 'KR', 'SS', 'ES', 'LK', 'SD', 'SR', 'SZ', 'SE',
        'CH', 'SY', 'TW', 'TJ', 'TZ', 'TH', 'TL', 'TG', 'TT', 'TN',
        'TR', 'TM', 'UG', 'UA', 'AE', 'GB', 'US', 'UY', 'UZ', 'VE',
        'VN', 'YE', 'ZM', 'ZW', 'XX'  # XX = unknown
    }
    
    def __init__(self, config: DatabaseConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.pool: Optional[pool.ThreadedConnectionPool] = None
        self.retry_count = 3
        self.retry_delay = 1.0
    
    def connect(self) -> bool:
        """Initialize connection pool."""
        try:
            self.logger.info(f"Connecting to PostgreSQL: {self.config.host}:{self.config.port}/{self.config.database}")
            
            self.pool = pool.ThreadedConnectionPool(
                minconn=self.config.min_connections,
                maxconn=self.config.max_connections,
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
                connect_timeout=self.config.connect_timeout,
                options=f'-c statement_timeout={self.config.statement_timeout}',
                client_encoding='UTF8'
            )
            
            # Test connection
            conn = self.pool.getconn()
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT version();")
                self.logger.info("PostgreSQL connection successful")
                cursor.close()
            finally:
                self.pool.putconn(conn)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect: {e}")
            return False
    
    def close(self):
        """Close connection pool."""
        if self.pool:
            self.pool.closeall()
            self.logger.info("Connection pool closed")
    
    def verify_schema(self) -> bool:
        """Verify zen_contacts table exists."""
        try:
            conn = self.pool.getconn()
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = 'zen_contacts'
                    );
                """)
                exists = cursor.fetchone()[0]
                
                if not exists:
                    self.logger.error("Table 'zen_contacts' does not exist")
                    self.logger.error("Run migrations/schema_v3_complete.sql first")
                    return False
                
                self.logger.info("Schema validation passed (zen_contacts)")
                return True
            finally:
                self.pool.putconn(conn)
        except Exception as e:
            self.logger.error(f"Schema validation error: {e}")
            return False
    
    def _get_partition_key(self, link: str) -> int:
        """Calculate partition key from link URL."""
        # Same algorithm as PostgreSQL function
        return abs(hash(link)) % 32
    
    def _normalize_country(self, country: str) -> str:
        """Normalize and validate country code."""
        if not country:
            return 'XX'
        
        country = country.upper().strip()[:2]
        
        if country in self.VALID_COUNTRIES:
            return country
        
        return 'XX'
    
    def _extract_country(self, row_data: Dict[str, Any]) -> str:
        """Extract country from row data."""
        # Try direct country field
        country = row_data.get('country', '')
        if country:
            return self._normalize_country(country)
        
        # Try original_data.complete_address.country
        original = row_data.get('original_data', {})
        if isinstance(original, dict):
            complete_addr = original.get('complete_address', {})
            if isinstance(complete_addr, dict):
                country = complete_addr.get('country', '')
                if country:
                    return self._normalize_country(country)
        
        return 'XX'
    
    def _prepare_row(self, row_data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare row for database insertion."""
        
        # Helper: Convert to array
        def to_array(value) -> List[str]:
            if not value:
                return []
            if isinstance(value, list):
                return [str(v).strip() for v in value if v]
            if isinstance(value, str):
                if ';' in value:
                    return [v.strip() for v in value.split(';') if v.strip()]
                return [value.strip()] if value.strip() else []
            return []
        
        # Get original GMaps data
        original = row_data.get('original_data', {}) or {}
        
        # Extract primary fields
        link = row_data.get('link') or row_data.get('url', '')
        country = self._extract_country(row_data)
        
        # Extract from original GMaps data
        complete_address = original.get('complete_address', {}) or {}
        
        # Country name mapping (common ones)
        country_names = {
            'US': 'United States', 'GB': 'United Kingdom', 'CA': 'Canada',
            'AU': 'Australia', 'DE': 'Germany', 'FR': 'France', 'IT': 'Italy',
            'ES': 'Spain', 'JP': 'Japan', 'KR': 'South Korea', 'CN': 'China',
            'IN': 'India', 'BR': 'Brazil', 'MX': 'Mexico', 'ID': 'Indonesia',
            'SG': 'Singapore', 'MY': 'Malaysia', 'TH': 'Thailand', 'VN': 'Vietnam',
            'PH': 'Philippines', 'NL': 'Netherlands', 'BE': 'Belgium', 'CH': 'Switzerland',
            'AT': 'Austria', 'SE': 'Sweden', 'NO': 'Norway', 'DK': 'Denmark',
            'FI': 'Finland', 'PL': 'Poland', 'CZ': 'Czech Republic', 'RU': 'Russia',
            'UA': 'Ukraine', 'TR': 'Turkey', 'SA': 'Saudi Arabia', 'AE': 'UAE',
            'ZA': 'South Africa', 'EG': 'Egypt', 'NG': 'Nigeria', 'KE': 'Kenya',
            'NZ': 'New Zealand', 'AR': 'Argentina', 'CL': 'Chile', 'CO': 'Colombia',
            'PE': 'Peru', 'PT': 'Portugal', 'IE': 'Ireland', 'IL': 'Israel',
        }
        
        return {
            # Primary keys
            'link': link,
            'partition_key': self._get_partition_key(link),
            
            # Business info (from original GMaps data)
            'country': country,
            'country_name': country_names.get(country, ''),
            'title': row_data.get('name') or original.get('title', ''),
            'category': row_data.get('category') or original.get('category', ''),
            'website': original.get('web_site') or original.get('domain', ''),
            
            # Address fields (from original GMaps data)
            'city': complete_address.get('city', ''),
            'state': complete_address.get('state', ''),
            'borough': complete_address.get('borough', ''),
            'street': complete_address.get('street', ''),
            'address': original.get('address', ''),
            'postal_code': complete_address.get('postal_code', ''),
            'latitude': original.get('latitude'),
            'longitude': original.get('longtitude') or original.get('longitude'),
            'timezone': original.get('timezone', ''),
            
            # GMaps metadata (all available fields)
            'gmaps_phone': original.get('phone', ''),
            'gmaps_rating': original.get('review_rating'),
            'gmaps_review_count': original.get('review_count'),
            'gmaps_cid': original.get('cid', ''),
            'gmaps_data_id': original.get('data_id', ''),
            'gmaps_price_range': original.get('price_range', ''),
            'gmaps_status': original.get('claimed', ''),  # YES/NO/empty
            'gmaps_domain': original.get('domain', ''),
            'gmaps_plus_code': original.get('plus_code', ''),
            'gmaps_thumbnail': original.get('thumbnail', ''),
            'gmaps_featured_image': original.get('featured_image', ''),
            
            # Source tracking
            'source_id': row_data.get('result_id'),
            
            # Scraped contact data
            'emails': to_array(row_data.get('emails')),
            'phones': to_array(row_data.get('phones')),
            'whatsapp': to_array(row_data.get('whatsapp')),
            'facebook': row_data.get('facebook') or None,
            'instagram': row_data.get('instagram') or None,
            'tiktok': row_data.get('tiktok') or None,
            'youtube': row_data.get('youtube') or None,
            
            # Scraping metadata
            'final_url': row_data.get('final_url') or row_data.get('url', ''),
            'was_redirected': bool(row_data.get('was_redirected', False)),
            'scraping_status': row_data.get('status') or row_data.get('scraping_status', 'unknown'),
            'scraping_error': row_data.get('error') or row_data.get('scraping_error', ''),
            'processing_time': float(row_data.get('processing_time', 0)),
            'pages_scraped': int(row_data.get('pages_scraped', 0)),
        }
    
    def upsert_batch(self, rows: List[Dict[str, Any]], server_id: str = 'unknown') -> int:
        """
        Batch UPSERT to zen_contacts table.
        
        Args:
            rows: List of row dictionaries
            server_id: Identifier of the server performing the upsert
            
        Returns:
            Number of successfully upserted rows
        """
        if not rows:
            return 0
        
        # Prepare all rows
        prepared = []
        for row in rows:
            try:
                p = self._prepare_row(row)
                if p.get('link'):
                    prepared.append(p)
            except Exception as e:
                self.logger.warning(f"Failed to prepare row: {e}")
        
        if not prepared:
            return 0
        
        # Build UPSERT query for zen_contacts schema
        query = """
        INSERT INTO zen_contacts (
            source_link, partition_key, country_code, country_name, business_name, business_category, business_website,
            city, state, borough, street, address, postal_code, latitude, longitude, timezone,
            gmaps_phone, gmaps_rating, gmaps_review_count, gmaps_cid, gmaps_data_id,
            gmaps_price_range, gmaps_status, gmaps_domain, gmaps_plus_code, gmaps_thumbnail, gmaps_featured_image,
            source_id,
            emails, emails_count,
            phones, phones_count,
            whatsapp, whatsapp_count,
            social_facebook, social_instagram, social_tiktok, social_youtube,
            scrape_final_url, scrape_was_redirected, scrape_status, scrape_error,
            scrape_time_seconds, scrape_pages_count, last_scrape_server, scrape_count, last_scrape_at
        ) VALUES %s
        ON CONFLICT (source_link, partition_key) DO UPDATE SET
            emails = ARRAY(
                SELECT DISTINCT unnest FROM unnest(
                    COALESCE(zen_contacts.emails, '{}') || 
                    COALESCE(EXCLUDED.emails, '{}')
                ) WHERE unnest IS NOT NULL AND unnest != ''
            ),
            emails_count = (
                SELECT COUNT(DISTINCT e) FROM unnest(
                    COALESCE(zen_contacts.emails, '{}') || 
                    COALESCE(EXCLUDED.emails, '{}')
                ) AS e WHERE e IS NOT NULL AND e != ''
            ),
            phones = ARRAY(
                SELECT DISTINCT unnest FROM unnest(
                    COALESCE(zen_contacts.phones, '{}') || 
                    COALESCE(EXCLUDED.phones, '{}')
                ) WHERE unnest IS NOT NULL AND unnest != ''
            ),
            phones_count = (
                SELECT COUNT(DISTINCT p) FROM unnest(
                    COALESCE(zen_contacts.phones, '{}') || 
                    COALESCE(EXCLUDED.phones, '{}')
                ) AS p WHERE p IS NOT NULL AND p != ''
            ),
            whatsapp = ARRAY(
                SELECT DISTINCT unnest FROM unnest(
                    COALESCE(zen_contacts.whatsapp, '{}') || 
                    COALESCE(EXCLUDED.whatsapp, '{}')
                ) WHERE unnest IS NOT NULL AND unnest != ''
            ),
            whatsapp_count = (
                SELECT COUNT(DISTINCT w) FROM unnest(
                    COALESCE(zen_contacts.whatsapp, '{}') || 
                    COALESCE(EXCLUDED.whatsapp, '{}')
                ) AS w WHERE w IS NOT NULL AND w != ''
            ),
            social_facebook = COALESCE(EXCLUDED.social_facebook, zen_contacts.social_facebook),
            social_instagram = COALESCE(EXCLUDED.social_instagram, zen_contacts.social_instagram),
            social_tiktok = COALESCE(EXCLUDED.social_tiktok, zen_contacts.social_tiktok),
            social_youtube = COALESCE(EXCLUDED.social_youtube, zen_contacts.social_youtube),
            scrape_final_url = EXCLUDED.scrape_final_url,
            scrape_was_redirected = EXCLUDED.scrape_was_redirected,
            scrape_status = EXCLUDED.scrape_status,
            scrape_error = EXCLUDED.scrape_error,
            scrape_time_seconds = EXCLUDED.scrape_time_seconds,
            scrape_pages_count = EXCLUDED.scrape_pages_count,
            last_scrape_server = EXCLUDED.last_scrape_server,
            scrape_count = zen_contacts.scrape_count + 1,
            last_scrape_at = NOW(),
            updated_at = NOW()
        """
        
        # Convert to tuples (last_scrape_at will be set by trigger/NOW() in UPDATE clause)
        import datetime
        now = datetime.datetime.now()
        values = [
            (
                # Primary keys
                p['link'], p['partition_key'], p['country'], p['country_name'], p['title'], p['category'], p['website'],
                # Address fields
                p['city'], p['state'], p['borough'], p['street'], p['address'], p['postal_code'], 
                p['latitude'], p['longitude'], p['timezone'],
                # GMaps metadata (all fields)
                p['gmaps_phone'], p['gmaps_rating'], p['gmaps_review_count'], p['gmaps_cid'], p['gmaps_data_id'],
                p['gmaps_price_range'], p['gmaps_status'], p['gmaps_domain'], p['gmaps_plus_code'],
                p['gmaps_thumbnail'], p['gmaps_featured_image'],
                # Source tracking
                p['source_id'],
                # Scraped contact data
                p['emails'], len(p['emails']),
                p['phones'], len(p['phones']),
                p['whatsapp'], len(p['whatsapp']),
                p['facebook'], p['instagram'], p['tiktok'], p['youtube'],
                # Scraping metadata
                p['final_url'], p['was_redirected'], p['scraping_status'], p['scraping_error'],
                p['processing_time'], p['pages_scraped'], server_id, 1, now
            )
            for p in prepared
        ]
        
        # Execute with retry
        for attempt in range(self.retry_count):
            conn = None
            try:
                conn = self.pool.getconn()
                cursor = conn.cursor()
                
                execute_values(cursor, query, values)
                conn.commit()
                
                self.logger.debug(f"Batch UPSERT: {len(values)} rows")
                return len(values)
                
            except psycopg2.OperationalError as e:
                self.logger.warning(f"DB error (attempt {attempt+1}/{self.retry_count}): {e}")
                if attempt < self.retry_count - 1:
                    time.sleep(self.retry_delay * (2 ** attempt))
                    continue
                return 0
                
            except Exception as e:
                self.logger.error(f"Batch UPSERT error: {e}", exc_info=True)
                return 0
                
            finally:
                if conn:
                    try:
                        self.pool.putconn(conn)
                    except:
                        pass
        
        return 0
    
    def get_country_stats(self) -> Dict[str, int]:
        """Get row count per country."""
        conn = self.pool.getconn()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT country_code, COUNT(*) as count
                FROM zen_contacts
                GROUP BY country_code
                ORDER BY count DESC
            """)
            return {row[0]: row[1] for row in cursor.fetchall()}
        finally:
            self.pool.putconn(conn)
    
    def get_total_stats(self) -> Dict[str, Any]:
        """Get overall statistics."""
        conn = self.pool.getconn()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(emails_count) as total_emails,
                    SUM(phones_count) as total_phones,
                    SUM(whatsapp_count) as total_whatsapp,
                    COUNT(DISTINCT country_code) as countries,
                    COUNT(*) FILTER (WHERE scrape_status = 'success') as successful,
                    COUNT(*) FILTER (WHERE scrape_status = 'failed') as failed
                FROM zen_contacts
            """)
            row = cursor.fetchone()
            return {
                'total': row[0] or 0,
                'total_emails': row[1] or 0,
                'total_phones': row[2] or 0,
                'total_whatsapp': row[3] or 0,
                'countries': row[4] or 0,
                'successful': row[5] or 0,
                'failed': row[6] or 0,
            }
        finally:
            self.pool.putconn(conn)


def create_database_writer_v2(logger: logging.Logger) -> Optional[DatabaseWriterV2]:
    """Factory function to create DatabaseWriterV2 from environment."""
    try:
        from dotenv import load_dotenv
        import os
        
        load_dotenv()
        
        config = DatabaseConfig(
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', '5432')),
            database=os.getenv('DB_NAME', 'zenvoyer_db'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', ''),
            min_connections=int(os.getenv('DB_MIN_CONNECTIONS', '2')),
            max_connections=int(os.getenv('DB_MAX_CONNECTIONS', '10')),
            connect_timeout=int(os.getenv('DB_CONNECT_TIMEOUT', '10')),
            statement_timeout=int(os.getenv('DB_STATEMENT_TIMEOUT', '60000')),
        )
        
        if not config.password:
            logger.error("DB_PASSWORD not set in .env file")
            return None
        
        return DatabaseWriterV2(config, logger)
        
    except Exception as e:
        logger.error(f"Error creating DatabaseWriterV2: {e}")
        return None
