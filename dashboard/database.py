"""Database connection for dashboard (read-only)."""

import psycopg2
from psycopg2 import pool, OperationalError
from contextlib import contextmanager
from typing import Optional, Any, Dict, List
import logging
import time

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_DELAY = 0.5


class Database:
    """Read-only database connection pool for dashboard."""
    
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.pool: Optional[pool.ThreadedConnectionPool] = None
    
    def connect(self) -> bool:
        """Initialize connection pool."""
        try:
            self.pool = pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=5,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                connect_timeout=10,
                options='-c statement_timeout=60000',
                client_encoding='UTF8'
            )
            
            # Test connection
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
            
            logger.info("Dashboard database connected")
            return True
            
        except Exception as e:
            logger.error(f"Dashboard database connection failed: {e}")
            return False
    
    def _reconnect(self) -> bool:
        """Reconnect to database."""
        try:
            if self.pool:
                try:
                    self.pool.closeall()
                except Exception:
                    pass
            return self.connect()
        except Exception as e:
            logger.error(f"Reconnection failed: {e}")
            return False
    
    def close(self):
        """Close all connections."""
        if self.pool:
            self.pool.closeall()
            logger.info("Dashboard database closed")
    
    def _is_connection_valid(self, conn) -> bool:
        """Check if connection is still valid."""
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
        except Exception:
            return False
    
    @contextmanager
    def get_connection(self):
        """Get connection from pool with validation."""
        conn = None
        try:
            conn = self.pool.getconn()
            if not self._is_connection_valid(conn):
                self.pool.putconn(conn, close=True)
                conn = self.pool.getconn()
            yield conn
        except OperationalError:
            if conn:
                try:
                    self.pool.putconn(conn, close=True)
                except Exception:
                    pass
            raise
        finally:
            if conn:
                try:
                    self.pool.putconn(conn)
                except Exception:
                    pass
    
    def execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """Execute SELECT query and return results as list of dicts with retry."""
        last_error = None
        for attempt in range(MAX_RETRIES):
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(query, params)
                    columns = [desc[0] for desc in cursor.description]
                    return [dict(zip(columns, row)) for row in cursor.fetchall()]
            except OperationalError as e:
                last_error = e
                logger.warning(f"DB query failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    self._reconnect()
        raise last_error
    
    def execute_scalar(self, query: str, params: tuple = None) -> Any:
        """Execute query and return single value with retry."""
        last_error = None
        for attempt in range(MAX_RETRIES):
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(query, params)
                    result = cursor.fetchone()
                    return result[0] if result else None
            except OperationalError as e:
                last_error = e
                logger.warning(f"DB scalar query failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    self._reconnect()
        raise last_error
    
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists."""
        query = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = %s
        )
        """
        return self.execute_scalar(query, (table_name,))


# Global database instance
_db: Optional[Database] = None


def init_database(config) -> Database:
    """Initialize global database instance."""
    global _db
    _db = Database(
        host=config.db_host,
        port=config.db_port,
        database=config.db_name,
        user=config.db_user,
        password=config.db_password
    )
    if not _db.connect():
        raise RuntimeError(f"Failed to connect to database at {config.db_host}:{config.db_port}")
    return _db


def get_database() -> Database:
    """Get global database instance."""
    if _db is None:
        raise RuntimeError("Database not initialized")
    return _db
