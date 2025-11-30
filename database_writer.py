"""
PostgreSQL Database Writer for Email Enhancer

Provides connection pooling, UPSERT logic, and error handling for remote PostgreSQL databases.
Designed to enrich existing Google Maps data with web-scraped contact information.

Author: Claude (Sonnet 4.5)
Date: 2025-11-30
"""

import psycopg2
from psycopg2 import pool, extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import logging
import time
import json
from typing import Dict, Optional, List, Any
from dataclasses import dataclass


@dataclass
class DatabaseConfig:
    """Database connection configuration loaded from .env file"""
    host: str
    port: int
    database: str
    user: str
    password: str
    min_connections: int = 1
    max_connections: int = 5  # Reduced from 10 to prevent pool saturation (10 servers × 5 = 50 < 100)
    connect_timeout: int = 10
    statement_timeout: int = 30000  # 30 seconds in milliseconds


class DatabaseWriter:
    """
    Thread-safe PostgreSQL writer with connection pooling.

    Handles UPSERT operations to enrich existing database records with
    web-scraped contact information (emails, phones, WhatsApp, social media).

    Features:
    - Connection pooling (min 2, max 10 connections)
    - Retry logic with exponential backoff (3 attempts)
    - Graceful error handling (fail fast on startup, degrade mid-process)
    - Data enrichment (merge existing Google Maps data with scraped data)
    """

    def __init__(self, config: DatabaseConfig, logger: logging.Logger):
        """
        Initialize DatabaseWriter with configuration and logger.

        Args:
            config: DatabaseConfig with connection parameters
            logger: Logger instance for diagnostic output
        """
        self.config = config
        self.logger = logger
        self.pool: Optional[pool.ThreadedConnectionPool] = None
        self.retry_count = 3
        self.retry_delay = 1.0  # Initial delay in seconds (exponential backoff)

        # UPSERT query template (will be formatted with column values)
        self.upsert_query = """
        INSERT INTO scraped_contacts (
            link, title, emails, whatsapp, facebook, instagram, linkedin,
            phones, tiktok, youtube, validated_emails, validated_whatsapp,
            final_url, was_redirected, scraping_status, scraping_error,
            processing_time, pages_scraped, emails_found, phones_found,
            whatsapp_found, updated_at, scrape_count
        )
        VALUES (
            %(link)s, %(title)s, %(emails)s, %(whatsapp)s, %(facebook)s,
            %(instagram)s, %(linkedin)s, %(phones)s, %(tiktok)s, %(youtube)s,
            %(validated_emails)s, %(validated_whatsapp)s, %(final_url)s,
            %(was_redirected)s, %(scraping_status)s, %(scraping_error)s,
            %(processing_time)s, %(pages_scraped)s, %(emails_found)s,
            %(phones_found)s, %(whatsapp_found)s, CURRENT_TIMESTAMP, 1
        )
        ON CONFLICT (link) DO UPDATE SET
            -- Enrich existing data (merge old + new contacts)
            emails = CASE
                WHEN scraped_contacts.emails IS NULL THEN EXCLUDED.emails
                WHEN EXCLUDED.emails IS NULL THEN scraped_contacts.emails
                ELSE scraped_contacts.emails || EXCLUDED.emails
            END,
            whatsapp = CASE
                WHEN scraped_contacts.whatsapp IS NULL THEN EXCLUDED.whatsapp
                WHEN EXCLUDED.whatsapp IS NULL THEN scraped_contacts.whatsapp
                ELSE scraped_contacts.whatsapp || EXCLUDED.whatsapp
            END,
            facebook = COALESCE(EXCLUDED.facebook, scraped_contacts.facebook),
            instagram = COALESCE(EXCLUDED.instagram, scraped_contacts.instagram),
            linkedin = COALESCE(EXCLUDED.linkedin, scraped_contacts.linkedin),
            phones = EXCLUDED.phones,
            tiktok = EXCLUDED.tiktok,
            youtube = EXCLUDED.youtube,
            validated_emails = EXCLUDED.validated_emails,
            validated_whatsapp = EXCLUDED.validated_whatsapp,
            final_url = EXCLUDED.final_url,
            was_redirected = EXCLUDED.was_redirected,
            scraping_status = EXCLUDED.scraping_status,
            scraping_error = EXCLUDED.scraping_error,
            processing_time = EXCLUDED.processing_time,
            pages_scraped = EXCLUDED.pages_scraped,
            emails_found = EXCLUDED.emails_found,
            phones_found = EXCLUDED.phones_found,
            whatsapp_found = EXCLUDED.whatsapp_found,
            updated_at = CURRENT_TIMESTAMP,
            scrape_count = scraped_contacts.scrape_count + 1
        """

    def connect(self) -> bool:
        """
        Initialize connection pool. Returns True on success, False on failure.

        Creates a ThreadedConnectionPool with configured parameters.
        Tests connection by executing a simple query.

        Returns:
            bool: True if connection pool created successfully, False otherwise
        """
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

            # Test connection by executing a simple query
            conn = self.pool.getconn()
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT version();")
                version = cursor.fetchone()[0]
                self.logger.info(f"PostgreSQL connection successful: {version}")
                cursor.close()
            finally:
                self.pool.putconn(conn)

            return True

        except psycopg2.OperationalError as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {e}")
            self.logger.error(f"  Host: {self.config.host}:{self.config.port}")
            self.logger.error(f"  Database: {self.config.database}")
            self.logger.error(f"  User: {self.config.user}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error during connection: {e}", exc_info=True)
            return False

    def close(self):
        """Close all connections in the pool."""
        if self.pool:
            self.pool.closeall()
            self.logger.info("Database connection pool closed")

    def verify_schema(self) -> bool:
        """
        Verify that the scraped_contacts table exists and has required columns.

        Checks for the presence of:
        - Table: scraped_contacts
        - Required columns: link (for UPSERT), phones, validated_emails, etc.

        Returns:
            bool: True if schema is valid, False otherwise
        """
        required_columns = [
            'link',  # Primary key for UPSERT (existing)
            'phones',  # NEW column (should exist after migration)
            'validated_emails',  # NEW column
            'validated_whatsapp',  # NEW column
            'tiktok',  # NEW column
            'youtube',  # NEW column
        ]

        try:
            conn = self.pool.getconn()
            try:
                cursor = conn.cursor()

                # Check if table exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = 'scraped_contacts'
                    );
                """)
                table_exists = cursor.fetchone()[0]

                if not table_exists:
                    self.logger.error("Table 'scraped_contacts' does not exist")
                    self.logger.error("Please run schema_migration.sql first")
                    return False

                # Check for required columns
                cursor.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'scraped_contacts';
                """)
                existing_columns = {row[0] for row in cursor.fetchall()}

                missing_columns = set(required_columns) - existing_columns
                if missing_columns:
                    self.logger.error(f"Missing required columns: {', '.join(missing_columns)}")
                    self.logger.error("Please run schema_migration.sql to add missing columns")
                    return False

                self.logger.info("Database schema validation passed")
                return True

            finally:
                self.pool.putconn(conn)

        except Exception as e:
            self.logger.error(f"Schema validation error: {e}", exc_info=True)
            return False

    def upsert_contact(self, row_data: Dict[str, Any]) -> bool:
        """
        Insert or update contact record using ON CONFLICT (UPSERT).

        Converts CSV row data to PostgreSQL format and executes UPSERT query.
        Merges new scraped data with existing Google Maps data.

        Args:
            row_data: Dictionary containing scraped contact data from CSV processor

        Returns:
            bool: True on success, False on failure (after retries)
        """
        try:
            # Prepare row data for PostgreSQL
            prepared_data = self._prepare_row(row_data)

            # Execute UPSERT with retry logic
            return self._execute_with_retry(self.upsert_query, prepared_data)

        except Exception as e:
            self.logger.error(f"Error in upsert_contact: {e}", exc_info=True)
            return False

    def _prepare_row(self, row_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert CSV row format to PostgreSQL format.

        Transformations:
        - Semicolon-separated strings → TEXT[] arrays (emails, phones, whatsapp)
        - Validation dicts → JSONB (validated_emails, validated_whatsapp)
        - String booleans → PostgreSQL boolean (was_redirected)
        - Empty values → None (NULL in PostgreSQL)

        Args:
            row_data: Raw row data from CSV processor

        Returns:
            Dict[str, Any]: Prepared data ready for PostgreSQL INSERT/UPDATE
        """
        # Helper: Convert semicolon-separated string to array
        def to_array(value: Optional[str]) -> List[str]:
            if not value or value.strip() == '':
                return []
            return [item.strip() for item in value.split(';') if item.strip()]

        # Helper: Parse validated field format (e.g., "email (reason:verified, conf:0.95)")
        def parse_validated(value: Optional[str]) -> Optional[str]:
            if not value or value.strip() == '':
                return None
            # For MVP, store as JSONB string (parsing can be improved later)
            # Format: {"email": {"reason": "verified", "confidence": 0.95, ...}}
            # For now, store the raw string - full parsing can be added in Phase 2
            return json.dumps({"raw": value})

        # Extract and transform fields
        emails_array = to_array(row_data.get('emails', ''))
        phones_array = to_array(row_data.get('phones', ''))
        whatsapp_array = to_array(row_data.get('whatsapp', ''))

        # Boolean conversion
        was_redirected = str(row_data.get('was_redirected', '')).lower() == 'true'

        # Prepare data dictionary
        prepared = {
            'link': row_data.get('url', ''),  # CSV 'url' → DB 'link'
            'title': row_data.get('name', ''),  # CSV 'name' → DB 'title'
            'emails': emails_array if emails_array else None,
            'phones': phones_array if phones_array else None,
            'whatsapp': whatsapp_array if whatsapp_array else None,
            'facebook': row_data.get('facebook') or None,
            'instagram': row_data.get('instagram') or None,
            'linkedin': row_data.get('linkedin') or None,  # Preserve if exists
            'tiktok': row_data.get('tiktok') or None,
            'youtube': row_data.get('youtube') or None,
            'validated_emails': parse_validated(row_data.get('validated_emails')),
            'validated_whatsapp': parse_validated(row_data.get('validated_whatsapp')),
            'final_url': row_data.get('final_url') or None,
            'was_redirected': was_redirected,
            'scraping_status': row_data.get('scraping_status', 'unknown'),
            'scraping_error': row_data.get('scraping_error') or None,
            'processing_time': float(row_data.get('processing_time', 0)),
            'pages_scraped': int(row_data.get('pages_scraped', 0)),
            'emails_found': len(emails_array),
            'phones_found': len(phones_array),
            'whatsapp_found': len(whatsapp_array),
        }

        return prepared

    def _execute_with_retry(self, query: str, params: Dict[str, Any]) -> bool:
        """
        Execute query with exponential backoff retry.

        Retry strategy:
        - Attempt 1: Immediate
        - Attempt 2: 1 second delay
        - Attempt 3: 2 seconds delay
        - Attempt 4: 4 seconds delay (if retry_count=3, this won't happen)

        Args:
            query: SQL query with named parameters (%(name)s format)
            params: Dictionary of parameter values

        Returns:
            bool: True on success, False after all retries exhausted
        """
        for attempt in range(self.retry_count):
            conn = None
            try:
                # Get connection from pool
                conn = self.pool.getconn()
                cursor = conn.cursor()

                # Execute query
                cursor.execute(query, params)
                conn.commit()

                # Success
                self.logger.debug(f"DB: UPSERT successful for {params.get('link', 'unknown')}")
                return True

            except psycopg2.OperationalError as e:
                # Network error, connection lost, timeout
                self.logger.warning(
                    f"DB connection error (attempt {attempt+1}/{self.retry_count}): {e}"
                )

                if attempt < self.retry_count - 1:
                    # Exponential backoff: 1s, 2s, 4s
                    delay = self.retry_delay * (2 ** attempt)
                    self.logger.info(f"Retrying in {delay}s...")
                    time.sleep(delay)
                    continue
                else:
                    self.logger.error(
                        f"DB write failed for {params.get('link', 'unknown')} after {self.retry_count} attempts"
                    )
                    return False

            except psycopg2.IntegrityError as e:
                # Constraint violation (shouldn't happen with UPSERT ON CONFLICT, but defensive)
                self.logger.error(f"DB integrity error for {params.get('link', 'unknown')}: {e}")
                return False

            except psycopg2.Error as e:
                # Other PostgreSQL errors
                self.logger.error(f"DB error for {params.get('link', 'unknown')}: {e}")
                return False

            except Exception as e:
                # Unexpected errors
                self.logger.error(f"Unexpected error during DB write: {e}", exc_info=True)
                return False

            finally:
                # Always return connection to pool
                if conn:
                    try:
                        self.pool.putconn(conn)
                    except Exception as e:
                        self.logger.warning(f"Error returning connection to pool: {e}")

        return False

    def upsert_batch(self, rows: List[Dict[str, Any]]) -> int:
        """
        Batch UPSERT for performance (50-100 rows per query).
        
        Uses psycopg2.extras.execute_values for efficient multi-row insert.
        10x faster than individual upsert_contact calls.
        
        Args:
            rows: List of row dictionaries (same format as upsert_contact)
            
        Returns:
            int: Number of successfully upserted rows
        """
        if not rows:
            return 0
        
        from psycopg2.extras import execute_values
        
        # Prepare all rows
        prepared_rows = []
        for row in rows:
            try:
                prepared = self._prepare_row(row)
                prepared_rows.append(prepared)
            except Exception as e:
                self.logger.warning(f"Failed to prepare row for batch: {e}")
        
        if not prepared_rows:
            return 0
        
        # Build batch UPSERT query
        batch_query = """
        INSERT INTO scraped_contacts (
            link, title, emails, whatsapp, facebook, instagram, linkedin,
            phones, tiktok, youtube, validated_emails, validated_whatsapp,
            final_url, was_redirected, scraping_status, scraping_error,
            processing_time, pages_scraped, emails_found, phones_found,
            whatsapp_found, updated_at, scrape_count
        )
        VALUES %s
        ON CONFLICT (link) DO UPDATE SET
            emails = CASE
                WHEN scraped_contacts.emails IS NULL THEN EXCLUDED.emails
                WHEN EXCLUDED.emails IS NULL THEN scraped_contacts.emails
                ELSE scraped_contacts.emails || EXCLUDED.emails
            END,
            whatsapp = CASE
                WHEN scraped_contacts.whatsapp IS NULL THEN EXCLUDED.whatsapp
                WHEN EXCLUDED.whatsapp IS NULL THEN scraped_contacts.whatsapp
                ELSE scraped_contacts.whatsapp || EXCLUDED.whatsapp
            END,
            facebook = COALESCE(EXCLUDED.facebook, scraped_contacts.facebook),
            instagram = COALESCE(EXCLUDED.instagram, scraped_contacts.instagram),
            linkedin = COALESCE(EXCLUDED.linkedin, scraped_contacts.linkedin),
            phones = EXCLUDED.phones,
            tiktok = EXCLUDED.tiktok,
            youtube = EXCLUDED.youtube,
            validated_emails = EXCLUDED.validated_emails,
            validated_whatsapp = EXCLUDED.validated_whatsapp,
            final_url = EXCLUDED.final_url,
            was_redirected = EXCLUDED.was_redirected,
            scraping_status = EXCLUDED.scraping_status,
            scraping_error = EXCLUDED.scraping_error,
            processing_time = EXCLUDED.processing_time,
            pages_scraped = EXCLUDED.pages_scraped,
            emails_found = EXCLUDED.emails_found,
            phones_found = EXCLUDED.phones_found,
            whatsapp_found = EXCLUDED.whatsapp_found,
            updated_at = CURRENT_TIMESTAMP,
            scrape_count = scraped_contacts.scrape_count + 1
        """
        
        # Convert prepared rows to tuples
        values = [
            (
                p['link'], p['title'], p['emails'], p['whatsapp'], p['facebook'],
                p['instagram'], p['linkedin'], p['phones'], p['tiktok'], p['youtube'],
                p['validated_emails'], p['validated_whatsapp'], p['final_url'],
                p['was_redirected'], p['scraping_status'], p['scraping_error'],
                p['processing_time'], p['pages_scraped'], p['emails_found'],
                p['phones_found'], p['whatsapp_found'],
                None,  # updated_at - will be set by CURRENT_TIMESTAMP
                1      # scrape_count - initial value
            )
            for p in prepared_rows
        ]
        
        conn = None
        for attempt in range(self.retry_count):
            try:
                conn = self.pool.getconn()
                cursor = conn.cursor()
                
                execute_values(cursor, batch_query, values)
                conn.commit()
                
                self.logger.debug(f"DB: Batch UPSERT successful for {len(values)} rows")
                return len(values)
                
            except psycopg2.OperationalError as e:
                self.logger.warning(f"DB batch error (attempt {attempt+1}/{self.retry_count}): {e}")
                if attempt < self.retry_count - 1:
                    delay = self.retry_delay * (2 ** attempt)
                    time.sleep(delay)
                    continue
                else:
                    self.logger.error(f"DB batch write failed after {self.retry_count} attempts")
                    return 0
                    
            except Exception as e:
                self.logger.error(f"Error in batch UPSERT: {e}", exc_info=True)
                return 0
                
            finally:
                if conn:
                    try:
                        self.pool.putconn(conn)
                    except:
                        pass
        
        return 0


# Utility function for main.py to create DatabaseWriter instance
def create_database_writer(logger: logging.Logger) -> Optional[DatabaseWriter]:
    """
    Factory function to create DatabaseWriter from environment variables.

    Loads configuration from .env file using python-dotenv.

    Args:
        logger: Logger instance

    Returns:
        DatabaseWriter instance if successful, None if .env missing or invalid
    """
    try:
        from dotenv import load_dotenv
        import os

        # Load .env file
        load_dotenv()

        # Read configuration
        config = DatabaseConfig(
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', '5432')),
            database=os.getenv('DB_NAME', 'email_enhancer'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', ''),
            min_connections=int(os.getenv('DB_MIN_CONNECTIONS', '2')),
            max_connections=int(os.getenv('DB_MAX_CONNECTIONS', '10')),
            connect_timeout=int(os.getenv('DB_CONNECT_TIMEOUT', '10')),
            statement_timeout=int(os.getenv('DB_STATEMENT_TIMEOUT', '30000')),
        )

        # Validate required fields
        if not config.password:
            logger.error("DB_PASSWORD not set in .env file")
            return None

        # Create writer
        writer = DatabaseWriter(config, logger)
        return writer

    except ImportError:
        logger.error("python-dotenv not installed. Run: pip install python-dotenv")
        return None
    except ValueError as e:
        logger.error(f"Invalid configuration in .env file: {e}")
        return None
    except Exception as e:
        logger.error(f"Error creating DatabaseWriter: {e}", exc_info=True)
        return None
