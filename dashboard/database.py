"""Database connection for dashboard (read-only)."""

import psycopg2
from psycopg2 import pool
from contextlib import contextmanager
from typing import Optional, Any, Dict, List
import logging

logger = logging.getLogger(__name__)


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
    
    def close(self):
        """Close all connections."""
        if self.pool:
            self.pool.closeall()
            logger.info("Dashboard database closed")
    
    @contextmanager
    def get_connection(self):
        """Get connection from pool."""
        conn = None
        try:
            conn = self.pool.getconn()
            yield conn
        finally:
            if conn:
                self.pool.putconn(conn)
    
    def execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """Execute SELECT query and return results as list of dicts."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    def execute_scalar(self, query: str, params: tuple = None) -> Any:
        """Execute query and return single value."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            result = cursor.fetchone()
            return result[0] if result else None
    
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
    _db.connect()
    return _db


def get_database() -> Database:
    """Get global database instance."""
    if _db is None:
        raise RuntimeError("Database not initialized")
    return _db
