"""
Database Source Reader V2 for Zenvoyer Schema

Reads from results table and checks completion against zen_contacts.
Optimized for multi-server concurrent processing with transaction-level advisory locks.

Tables: results (source), zen_contacts (target)
Author: Claude (Sonnet 4)
Date: 2025-11-30
Updated: 2025-11-30 - Fixed advisory lock leakage (session -> transaction level)
"""

import psycopg2
from psycopg2 import pool
import logging
import json
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
    max_connections: int = 5  # Reduced: 10 servers × 5 = 50 < 100 limit
    connect_timeout: int = 10
    statement_timeout: int = 60000


class DBSourceReaderV2:
    """
    Reads from results table with advisory locks for multi-server concurrency.
    
    Checks completion against zen_contacts (partitioned table).
    Uses partition key calculation for efficient lookups.
    """
    
    def __init__(self, config: DBSourceConfig, server_id: str, logger: logging.Logger):
        self.config = config
        self.server_id = server_id
        self.logger = logger
        self.pool: Optional[pool.ThreadedConnectionPool] = None
        self._claimed_ids: List[int] = []
    
    def connect(self) -> bool:
        """Initialize connection pool."""
        try:
            self.logger.info(f"[{self.server_id}] Connecting to PostgreSQL...")
            
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
                cursor.execute("SELECT 1")
                self.logger.info(f"[{self.server_id}] Connection successful")
            finally:
                self.pool.putconn(conn)
            
            return True
            
        except Exception as e:
            self.logger.error(f"[{self.server_id}] Connection failed: {e}")
            return False
    
    def close(self):
        """Close connections and release locks."""
        if self._claimed_ids:
            self.release_locks(self._claimed_ids)
        if self.pool:
            self.pool.closeall()
            self.logger.info(f"[{self.server_id}] Connection closed")
    
    def claim_batch(self, batch_size: int = 100, country_filter: str = None) -> List[Dict[str, Any]]:
        """
        Claim batch of unprocessed rows using transaction-level advisory locks.
        
        Uses pg_try_advisory_xact_lock which auto-releases on commit/rollback.
        For safe processing with guaranteed lock release, use claim_batch_safe().
        
        Args:
            batch_size: Number of rows to claim
            country_filter: Optional ISO country code to filter (e.g., 'US', 'ID')
            
        Returns:
            List of parsed row dictionaries
        """
        # Build query with optional country filter
        country_clause = ""
        if country_filter:
            country_filter = country_filter.upper()[:2]
            country_clause = f"AND (r.data->'complete_address'->>'country')::VARCHAR(2) = '{country_filter}'"
        
        query = f"""
        SELECT r.id, r.data
        FROM results r
        WHERE NOT EXISTS (
            SELECT 1 FROM zen_contacts sc 
            WHERE sc.source_link = r.data->>'link'
            AND sc.partition_key = ABS(hashtext(r.data->>'link')) % 32
        )
        AND r.data->>'web_site' IS NOT NULL
        AND r.data->>'web_site' != ''
        {country_clause}
        AND pg_try_advisory_xact_lock(r.id)
        ORDER BY r.id
        LIMIT %s;
        """
        
        conn = self.pool.getconn()
        try:
            cursor = conn.cursor()
            cursor.execute(query, (batch_size,))
            rows = cursor.fetchall()
            
            claimed = []
            for row in rows:
                try:
                    parsed = self._parse_row(row)
                    if parsed:
                        claimed.append(parsed)
                        self._claimed_ids.append(parsed['result_id'])
                except Exception as e:
                    self.logger.warning(f"[{self.server_id}] Parse error for row {row[0]}: {e}")
                    # No manual release needed - xact locks auto-release
            
            # Don't commit - keep transaction open to hold locks
            
            if claimed:
                self.logger.info(f"[{self.server_id}] Claimed {len(claimed)} rows")
            
            return claimed
            
        except Exception as e:
            self.logger.error(f"[{self.server_id}] Claim error: {e}")
            if conn:
                conn.rollback()
            return []
        finally:
            self.pool.putconn(conn)
    
    @contextmanager
    def claim_batch_safe(self, batch_size: int = 100, country_filter: str = None) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Context manager for safely claiming and processing a batch.
        
        Locks are held for the duration of the 'with' block and automatically
        released on exit (commit on success, rollback on exception).
        
        Usage:
            with reader.claim_batch_safe(100, 'ID') as rows:
                for row in rows:
                    process(row)
            # Locks automatically released here
        
        Args:
            batch_size: Number of rows to claim
            country_filter: Optional ISO country code
            
        Yields:
            List of parsed row dictionaries
        """
        country_clause = ""
        if country_filter:
            country_filter = country_filter.upper()[:2]
            country_clause = f"AND (r.data->'complete_address'->>'country')::VARCHAR(2) = '{country_filter}'"
        
        query = f"""
        SELECT r.id, r.data
        FROM results r
        WHERE NOT EXISTS (
            SELECT 1 FROM zen_contacts sc 
            WHERE sc.source_link = r.data->>'link'
            AND sc.partition_key = ABS(hashtext(r.data->>'link')) % 32
        )
        AND r.data->>'web_site' IS NOT NULL
        AND r.data->>'web_site' != ''
        {country_clause}
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
                    self.logger.warning(f"[{self.server_id}] Parse error for row {row[0]}: {e}")
            
            if claimed:
                self.logger.info(f"[{self.server_id}] Claimed {len(claimed)} rows (holding locks)")
            
            yield claimed  # Process while locks held
            
            conn.commit()  # Release locks on success
            self.logger.debug(f"[{self.server_id}] Released {len(claimed)} locks (commit)")
            
        except Exception as e:
            self.logger.error(f"[{self.server_id}] Error in claim_batch_safe: {e}")
            conn.rollback()  # Release locks on error
            raise
        finally:
            self.pool.putconn(conn)
    
    def release_locks(self, result_ids: List[int] = None):
        """
        Release advisory locks.
        
        DEPRECATED: With pg_try_advisory_xact_lock, locks auto-release on commit/rollback.
        This method is kept for backward compatibility but is now a no-op.
        """
        # With xact locks, manual release is not needed
        if result_ids:
            self._claimed_ids = [x for x in self._claimed_ids if x not in result_ids]
        else:
            self._claimed_ids = []
        
        self.logger.debug(f"[{self.server_id}] release_locks called (no-op with xact locks)")
    
    def _parse_row(self, row: tuple) -> Optional[Dict[str, Any]]:
        """Parse results row into dictionary."""
        result_id, data = row
        
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except json.JSONDecodeError:
                return None
        
        web_site = data.get('web_site', '') or ''
        if not web_site.strip():
            return None
        
        link = data.get('link', '') or ''
        if not link.strip():
            return None
        
        # Extract country
        complete_address = data.get('complete_address', {}) or {}
        country = complete_address.get('country', '') or ''
        
        return {
            'result_id': result_id,
            'url': web_site.strip(),
            'link': link.strip(),
            'name': (data.get('title', '') or '').strip(),
            'country': country.upper()[:2] if country else 'XX',
            'phone': (data.get('phone', '') or '').strip(),
            'category': (data.get('category', '') or '').strip(),
            'address': (data.get('address', '') or '').strip(),
            'latitude': data.get('latitude'),
            'longitude': data.get('longtitude') or data.get('longitude'),
            'review_count': data.get('review_count'),
            'review_rating': data.get('review_rating'),
            'original_data': data,
        }
    
    def get_pending_count(self, country_filter: str = None) -> int:
        """Count pending rows."""
        country_clause = ""
        if country_filter:
            country_filter = country_filter.upper()[:2]
            country_clause = f"AND (r.data->'complete_address'->>'country')::VARCHAR(2) = '{country_filter}'"
        
        query = f"""
        SELECT COUNT(*) FROM results r
        WHERE NOT EXISTS (
            SELECT 1 FROM zen_contacts sc 
            WHERE sc.source_link = r.data->>'link'
            AND sc.partition_key = ABS(hashtext(r.data->>'link')) % 32
        )
        AND r.data->>'web_site' IS NOT NULL
        AND r.data->>'web_site' != ''
        {country_clause};
        """
        
        conn = self.pool.getconn()
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            return cursor.fetchone()[0]
        except Exception as e:
            self.logger.error(f"[{self.server_id}] Count error: {e}")
            return -1
        finally:
            self.pool.putconn(conn)
    
    def get_total_count(self) -> int:
        """Get total rows in results."""
        conn = self.pool.getconn()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM results")
            return cursor.fetchone()[0]
        except Exception as e:
            self.logger.error(f"[{self.server_id}] Total count error: {e}")
            return -1
        finally:
            self.pool.putconn(conn)
    
    def get_completed_count(self) -> int:
        """Count completed rows (exist in zen_contacts)."""
        query = """
        SELECT COUNT(*) FROM results r
        WHERE EXISTS (
            SELECT 1 FROM zen_contacts sc 
            WHERE sc.source_link = r.data->>'link'
            AND sc.partition_key = ABS(hashtext(r.data->>'link')) % 32
        );
        """
        
        conn = self.pool.getconn()
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            return cursor.fetchone()[0]
        except Exception as e:
            self.logger.error(f"[{self.server_id}] Completed count error: {e}")
            return -1
        finally:
            self.pool.putconn(conn)
    
    def get_country_pending_counts(self) -> Dict[str, int]:
        """Get pending count per country."""
        query = """
        SELECT 
            UPPER(LEFT(COALESCE(r.data->'complete_address'->>'country', 'XX'), 2)) as country,
            COUNT(*) as pending
        FROM results r
        WHERE NOT EXISTS (
            SELECT 1 FROM zen_contacts sc 
            WHERE sc.source_link = r.data->>'link'
            AND sc.partition_key = ABS(hashtext(r.data->>'link')) % 32
        )
        AND r.data->>'web_site' IS NOT NULL
        AND r.data->>'web_site' != ''
        GROUP BY 1
        ORDER BY pending DESC;
        """
        
        conn = self.pool.getconn()
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            return {row[0]: row[1] for row in cursor.fetchall()}
        except Exception as e:
            self.logger.error(f"[{self.server_id}] Country counts error: {e}")
            return {}
        finally:
            self.pool.putconn(conn)


def create_db_source_reader_v2(server_id: str, logger: logging.Logger) -> Optional[DBSourceReaderV2]:
    """Factory function to create DBSourceReaderV2."""
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
            max_connections=min(int(os.getenv('DB_MAX_CONNECTIONS', '5')), 5),  # Capped at 5
            connect_timeout=int(os.getenv('DB_CONNECT_TIMEOUT', '10')),
            statement_timeout=int(os.getenv('DB_STATEMENT_TIMEOUT', '60000')),
        )
        
        if not config.password:
            logger.error("DB_PASSWORD not set")
            return None
        
        return DBSourceReaderV2(config, server_id, logger)
        
    except Exception as e:
        logger.error(f"Error creating reader: {e}")
        return None
