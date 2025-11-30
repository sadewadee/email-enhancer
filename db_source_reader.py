"""
Database Source Reader for Multi-Server Enrichment

Reads pending rows from 'result' table (READ-ONLY) and claims them using
PostgreSQL advisory locks to prevent race conditions across 10+ concurrent servers.

Key features:
- READ-ONLY access to result table (no modifications)
- Transaction-level advisory locks (auto-release on commit/rollback)
- scraped_contacts.link serves as implicit "completed" tracker
- Non-blocking: skips rows locked by other servers
- Context manager for safe lock handling

Author: Claude (Sonnet 4)
Date: 2025-11-30
Updated: 2025-11-30 - Fixed advisory lock leakage (session -> transaction level)
"""

import psycopg2
from psycopg2 import pool
import logging
import json
import time
from typing import Dict, Optional, List, Any, Generator
from dataclasses import dataclass
from contextlib import contextmanager


@dataclass
class DBSourceConfig:
    """Database connection configuration"""
    host: str
    port: int
    database: str
    user: str
    password: str
    min_connections: int = 1
    max_connections: int = 5  # Reduced from 6 to prevent pool saturation (10 servers × 5 = 50 < 100 limit)
    connect_timeout: int = 10
    statement_timeout: int = 60000  # 60 seconds


class DBSourceReader:
    """
    Reads from result table with advisory locks for multi-server concurrency.
    
    Uses PostgreSQL advisory locks to ensure 10+ servers can run concurrently
    without processing the same rows. The scraped_contacts.link column serves
    as an implicit "completed" tracker - if a link exists, it's already enriched.
    """
    
    def __init__(self, config: DBSourceConfig, server_id: str, logger: logging.Logger):
        """
        Initialize DB source reader.
        
        Args:
            config: Database connection configuration
            server_id: Unique identifier for this server (e.g., 'sg-01')
            logger: Logger instance
        """
        self.config = config
        self.server_id = server_id
        self.logger = logger
        self.pool: Optional[pool.ThreadedConnectionPool] = None
        self._claimed_ids: List[int] = []  # Track claimed result.id for lock release
    
    def connect(self) -> bool:
        """
        Initialize connection pool.
        
        Returns:
            bool: True if connection successful
        """
        try:
            self.logger.info(f"[{self.server_id}] Connecting to PostgreSQL: {self.config.host}:{self.config.port}/{self.config.database}")
            
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
                version = cursor.fetchone()[0]
                self.logger.info(f"[{self.server_id}] PostgreSQL connection successful")
                self.logger.debug(f"[{self.server_id}] Database version: {version}")
                cursor.close()
            finally:
                self.pool.putconn(conn)
            
            return True
            
        except psycopg2.OperationalError as e:
            self.logger.error(f"[{self.server_id}] Failed to connect to PostgreSQL: {e}")
            return False
        except Exception as e:
            self.logger.error(f"[{self.server_id}] Unexpected connection error: {e}", exc_info=True)
            return False
    
    def close(self):
        """Close connection pool and release all locks."""
        if self._claimed_ids:
            self.release_locks(self._claimed_ids)
        if self.pool:
            self.pool.closeall()
            self.logger.info(f"[{self.server_id}] Connection pool closed")
    
    def claim_batch(self, batch_size: int = 100) -> List[Dict[str, Any]]:
        """
        Claim batch of unprocessed rows using transaction-level advisory locks.
        
        IMPORTANT: Uses pg_try_advisory_xact_lock which auto-releases on commit/rollback.
        For safe processing, use claim_batch_safe() context manager instead.
        
        Args:
            batch_size: Maximum rows to claim
            
        Returns:
            List of row dictionaries with parsed data
            
        Note:
            Locks are released when the connection is returned to pool (commit).
            Process rows quickly or use claim_batch_safe() for long processing.
        """
        query = """
        SELECT r.id, r.data
        FROM results r
        WHERE NOT EXISTS (
            SELECT 1 FROM scraped_contacts sc 
            WHERE sc.link = r.data->>'link'
        )
        AND r.data->>'web_site' IS NOT NULL
        AND r.data->>'web_site' != ''
        AND pg_try_advisory_xact_lock(r.id)
        ORDER BY r.id
        LIMIT %s;
        """
        
        conn = self.pool.getconn()
        try:
            cursor = conn.cursor()
            cursor.execute(query, (batch_size,))
            rows = cursor.fetchall()
            
            # Parse rows and track claimed IDs
            claimed = []
            for row in rows:
                try:
                    parsed = self._parse_row(row)
                    if parsed:
                        claimed.append(parsed)
                        self._claimed_ids.append(parsed['result_id'])
                except Exception as e:
                    self.logger.warning(f"[{self.server_id}] Failed to parse row {row[0]}: {e}")
                    # No need to manually release - xact locks auto-release on commit/rollback
            
            # NOTE: Don't commit here - keep transaction open to hold locks
            # Locks will be released when connection is returned to pool
            
            if claimed:
                self.logger.info(f"[{self.server_id}] Claimed {len(claimed)} rows (IDs: {[r['result_id'] for r in claimed[:5]]}{'...' if len(claimed) > 5 else ''})")
            
            return claimed
            
        except Exception as e:
            self.logger.error(f"[{self.server_id}] Error claiming batch: {e}", exc_info=True)
            if conn:
                conn.rollback()  # Release any locks on error
            return []
        finally:
            self.pool.putconn(conn)
    
    @contextmanager
    def claim_batch_safe(self, batch_size: int = 100) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Context manager for safely claiming and processing a batch.
        
        Locks are held for the duration of the 'with' block and automatically
        released on exit (commit on success, rollback on exception).
        
        Usage:
            with reader.claim_batch_safe(100) as rows:
                for row in rows:
                    process(row)
            # Locks automatically released here
        
        Args:
            batch_size: Maximum rows to claim
            
        Yields:
            List of row dictionaries with parsed data
        """
        query = """
        SELECT r.id, r.data
        FROM results r
        WHERE NOT EXISTS (
            SELECT 1 FROM scraped_contacts sc 
            WHERE sc.link = r.data->>'link'
        )
        AND r.data->>'web_site' IS NOT NULL
        AND r.data->>'web_site' != ''
        AND pg_try_advisory_xact_lock(r.id)
        ORDER BY r.id
        LIMIT %s;
        """
        
        conn = self.pool.getconn()
        claimed = []
        try:
            cursor = conn.cursor()
            cursor.execute(query, (batch_size,))
            rows = cursor.fetchall()
            
            for row in rows:
                try:
                    parsed = self._parse_row(row)
                    if parsed:
                        claimed.append(parsed)
                except Exception as e:
                    self.logger.warning(f"[{self.server_id}] Failed to parse row {row[0]}: {e}")
            
            if claimed:
                self.logger.info(f"[{self.server_id}] Claimed {len(claimed)} rows (holding locks)")
            
            yield claimed  # Process rows while locks are held
            
            # Success - commit to release locks
            conn.commit()
            self.logger.debug(f"[{self.server_id}] Released {len(claimed)} locks (commit)")
            
        except Exception as e:
            self.logger.error(f"[{self.server_id}] Error in claim_batch_safe: {e}")
            conn.rollback()  # Release locks on error
            self.logger.debug(f"[{self.server_id}] Released locks (rollback)")
            raise
        finally:
            self.pool.putconn(conn)
    
    def release_locks(self, result_ids: List[int] = None):
        """
        Release advisory locks for processed rows.
        
        DEPRECATED: With pg_try_advisory_xact_lock, locks auto-release on commit/rollback.
        This method is kept for backward compatibility but is now a no-op.
        Use claim_batch_safe() context manager for automatic lock management.
        
        Args:
            result_ids: List of result.id to unlock. If None, releases all claimed.
        """
        # With xact locks, manual release is not needed - locks auto-release on commit/rollback
        # Just clear the tracking list
        if result_ids:
            self._claimed_ids = [x for x in self._claimed_ids if x not in result_ids]
        else:
            self._claimed_ids = []
        
        self.logger.debug(f"[{self.server_id}] release_locks called (no-op with xact locks)")
    
    def _parse_row(self, row: tuple) -> Optional[Dict[str, Any]]:
        """
        Parse result table row into dictionary.
        
        Extracts fields from JSON data column:
        - web_site: URL to scrape
        - link: Google Maps link (UPSERT key for scraped_contacts)
        - title: Business name
        - complete_address.country: Country code
        - And other business metadata
        
        Args:
            row: Tuple of (id, data)
            
        Returns:
            Parsed dictionary or None if invalid
        """
        result_id, data = row
        
        # Parse JSON if string
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except json.JSONDecodeError as e:
                self.logger.warning(f"[{self.server_id}] Invalid JSON for row {result_id}: {e}")
                return None
        
        # Extract web_site URL (required for scraping)
        web_site = data.get('web_site', '') or ''
        if not web_site.strip():
            self.logger.debug(f"[{self.server_id}] Row {result_id} has no web_site, skipping")
            return None
        
        # Extract Google Maps link (required for UPSERT key)
        link = data.get('link', '') or ''
        if not link.strip():
            self.logger.warning(f"[{self.server_id}] Row {result_id} has no link field")
            return None
        
        # Extract country from complete_address
        complete_address = data.get('complete_address', {}) or {}
        country = complete_address.get('country', '') or ''
        
        return {
            'result_id': result_id,
            'url': web_site.strip(),           # URL to scrape
            'link': link.strip(),              # UPSERT key for scraped_contacts
            'name': (data.get('title', '') or '').strip(),
            'country': country.strip(),
            'phone': (data.get('phone', '') or '').strip(),
            'category': (data.get('category', '') or '').strip(),
            'address': (data.get('address', '') or '').strip(),
            'latitude': data.get('latitude'),
            'longitude': data.get('longtitude') or data.get('longitude'),  # Note: source has typo 'longtitude'
            'review_count': data.get('review_count'),
            'review_rating': data.get('review_rating'),
            'original_data': data,             # Preserve all original fields
        }
    
    def get_pending_count(self) -> int:
        """
        Count rows pending enrichment.
        
        Returns:
            Number of rows in result not yet in scraped_contacts
        """
        query = """
        SELECT COUNT(*) FROM results r
        WHERE NOT EXISTS (
            SELECT 1 FROM scraped_contacts sc 
            WHERE sc.link = r.data->>'link'
        )
        AND r.data->>'web_site' IS NOT NULL
        AND r.data->>'web_site' != '';
        """
        
        conn = self.pool.getconn()
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            count = cursor.fetchone()[0]
            return count
        except Exception as e:
            self.logger.error(f"[{self.server_id}] Error getting pending count: {e}")
            return -1
        finally:
            self.pool.putconn(conn)
    
    def get_total_count(self) -> int:
        """
        Get total row count in result table.
        
        Returns:
            Total number of rows in result table
        """
        query = "SELECT COUNT(*) FROM results;"
        
        conn = self.pool.getconn()
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            count = cursor.fetchone()[0]
            return count
        except Exception as e:
            self.logger.error(f"[{self.server_id}] Error getting total count: {e}")
            return -1
        finally:
            self.pool.putconn(conn)
    
    def get_completed_count(self) -> int:
        """
        Count rows already enriched (exist in scraped_contacts).
        
        Returns:
            Number of completed rows
        """
        query = """
        SELECT COUNT(*) FROM results r
        WHERE EXISTS (
            SELECT 1 FROM scraped_contacts sc 
            WHERE sc.link = r.data->>'link'
        );
        """
        
        conn = self.pool.getconn()
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            count = cursor.fetchone()[0]
            return count
        except Exception as e:
            self.logger.error(f"[{self.server_id}] Error getting completed count: {e}")
            return -1
        finally:
            self.pool.putconn(conn)


def create_db_source_reader(server_id: str, logger: logging.Logger) -> Optional[DBSourceReader]:
    """
    Factory function to create DBSourceReader from environment variables.
    
    Args:
        server_id: Unique server identifier
        logger: Logger instance
        
    Returns:
        DBSourceReader instance or None if configuration failed
    """
    try:
        from dotenv import load_dotenv
        import os
        
        load_dotenv()
        
        config = DBSourceConfig(
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', '5432')),
            database=os.getenv('DB_NAME', 'zenvoyer_db'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', ''),
            min_connections=int(os.getenv('DB_MIN_CONNECTIONS', '1')),
            max_connections=min(int(os.getenv('DB_MAX_CONNECTIONS', '5')), 5),  # Capped at 5 (10 servers × 5 = 50 < 100 limit)
            connect_timeout=int(os.getenv('DB_CONNECT_TIMEOUT', '10')),
            statement_timeout=int(os.getenv('DB_STATEMENT_TIMEOUT', '60000')),
        )
        
        if not config.password:
            logger.error("DB_PASSWORD not set in .env file")
            return None
        
        reader = DBSourceReader(config, server_id, logger)
        return reader
        
    except ImportError:
        logger.error("python-dotenv not installed. Run: pip install python-dotenv")
        return None
    except ValueError as e:
        logger.error(f"Invalid configuration in .env file: {e}")
        return None
    except Exception as e:
        logger.error(f"Error creating DBSourceReader: {e}", exc_info=True)
        return None
