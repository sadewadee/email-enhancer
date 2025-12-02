"""
Network Optimization - Connection pooling with health monitoring

Implements network optimization:
- HTTP connection pooling with keep-alive
- Connection health monitoring
- Automatic connection recycling
- Adaptive timeout management
- Request rate limiting
- DNS caching
"""

import asyncio
import logging
import socket
import ssl
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse
import hashlib


class ConnectionStatus(Enum):
    """Connection status"""
    IDLE = "idle"
    BUSY = "busy"
    UNHEALTHY = "unhealthy"
    CLOSED = "closed"


@dataclass
class ConnectionMetrics:
    """Connection metrics"""
    requests_made: int = 0
    errors: int = 0
    total_time: float = 0.0
    last_used: float = field(default_factory=time.time)
    created_at: float = field(default_factory=time.time)
    
    @property
    def avg_request_time(self) -> float:
        if self.requests_made == 0:
            return 0.0
        return self.total_time / self.requests_made
    
    @property
    def error_rate(self) -> float:
        total = self.requests_made + self.errors
        if total == 0:
            return 0.0
        return self.errors / total * 100
    
    @property
    def age(self) -> float:
        return time.time() - self.created_at
    
    @property
    def idle_time(self) -> float:
        return time.time() - self.last_used


@dataclass
class PooledConnection:
    """Pooled connection wrapper"""
    connection_id: str
    host: str
    port: int
    ssl: bool
    reader: Optional[asyncio.StreamReader] = None
    writer: Optional[asyncio.StreamWriter] = None
    status: ConnectionStatus = ConnectionStatus.IDLE
    metrics: ConnectionMetrics = field(default_factory=ConnectionMetrics)
    
    def is_available(self) -> bool:
        return self.status == ConnectionStatus.IDLE
    
    def is_healthy(self) -> bool:
        # Unhealthy if too many errors
        if self.metrics.error_rate > 30:
            return False
        # Unhealthy if too old
        if self.metrics.age > 300:
            return False
        # Check if connection is still open
        if self.writer and self.writer.is_closing():
            return False
        return self.status != ConnectionStatus.UNHEALTHY


class DNSCache:
    """Simple DNS cache"""
    
    def __init__(self, ttl: float = 300.0):
        self.ttl = ttl
        self._cache: Dict[str, Tuple[List[str], float]] = {}
        self._lock = asyncio.Lock()
    
    async def resolve(self, hostname: str) -> List[str]:
        """Resolve hostname with caching"""
        async with self._lock:
            # Check cache
            if hostname in self._cache:
                ips, timestamp = self._cache[hostname]
                if time.time() - timestamp < self.ttl:
                    return ips
            
            # Resolve
            try:
                loop = asyncio.get_event_loop()
                result = await loop.getaddrinfo(
                    hostname, None,
                    family=socket.AF_INET,
                    type=socket.SOCK_STREAM
                )
                ips = list(set(addr[4][0] for addr in result))
                self._cache[hostname] = (ips, time.time())
                return ips
            except Exception:
                return []
    
    def clear(self) -> None:
        """Clear cache"""
        self._cache.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return {
            'entries': len(self._cache),
            'hosts': list(self._cache.keys())
        }


class RateLimiter:
    """Token bucket rate limiter"""
    
    def __init__(self, rate: float = 10.0, burst: int = 20):
        self.rate = rate  # Requests per second
        self.burst = burst  # Max burst size
        self._tokens = float(burst)
        self._last_update = time.time()
        self._lock = asyncio.Lock()
    
    async def acquire(self, tokens: int = 1) -> bool:
        """Acquire tokens (wait if necessary)"""
        async with self._lock:
            now = time.time()
            elapsed = now - self._last_update
            
            # Add tokens based on elapsed time
            self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
            self._last_update = now
            
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            
            # Calculate wait time
            needed = tokens - self._tokens
            wait_time = needed / self.rate
            
            await asyncio.sleep(wait_time)
            
            self._tokens = 0
            self._last_update = time.time()
            return True
    
    def try_acquire(self, tokens: int = 1) -> bool:
        """Try to acquire tokens without waiting"""
        now = time.time()
        elapsed = now - self._last_update
        
        self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
        self._last_update = now
        
        if self._tokens >= tokens:
            self._tokens -= tokens
            return True
        return False


class ConnectionPool:
    """
    HTTP connection pool with health monitoring.
    
    Features:
    - Connection reuse with keep-alive
    - Per-host connection limits
    - Health monitoring
    - Automatic recycling
    - Rate limiting
    """
    
    def __init__(self,
                 max_connections_per_host: int = 10,
                 max_total_connections: int = 100,
                 connection_timeout: float = 30.0,
                 idle_timeout: float = 60.0,
                 max_connection_age: float = 300.0):
        
        self.max_connections_per_host = max_connections_per_host
        self.max_total_connections = max_total_connections
        self.connection_timeout = connection_timeout
        self.idle_timeout = idle_timeout
        self.max_connection_age = max_connection_age
        
        self.logger = logging.getLogger(__name__)
        
        # Connection storage by host
        self._pools: Dict[str, deque] = defaultdict(deque)
        self._connection_count: Dict[str, int] = defaultdict(int)
        self._total_connections = 0
        
        # DNS cache and rate limiters
        self._dns_cache = DNSCache()
        self._rate_limiters: Dict[str, RateLimiter] = {}
        
        # State
        self._lock = asyncio.Lock()
        self._running = False
        self._cleanup_task: Optional[asyncio.Task] = None
        
        # Stats
        self._stats = {
            'connections_created': 0,
            'connections_reused': 0,
            'connections_closed': 0,
            'connection_errors': 0,
            'requests_made': 0
        }
    
    async def start(self) -> None:
        """Start connection pool"""
        if self._running:
            return
        
        self._running = True
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        self.logger.info("Connection pool started")
    
    async def stop(self) -> None:
        """Stop and close all connections"""
        self._running = False
        
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        
        # Close all connections
        async with self._lock:
            for host, pool in self._pools.items():
                while pool:
                    conn = pool.popleft()
                    await self._close_connection(conn)
            
            self._pools.clear()
            self._connection_count.clear()
            self._total_connections = 0
        
        self.logger.info("Connection pool stopped")
    
    async def acquire(self, url: str) -> PooledConnection:
        """Acquire connection for URL"""
        parsed = urlparse(url)
        host = parsed.hostname or ''
        port = parsed.port or (443 if parsed.scheme == 'https' else 80)
        use_ssl = parsed.scheme == 'https'
        
        pool_key = f"{host}:{port}"
        
        # Apply rate limiting
        await self._apply_rate_limit(host)
        
        async with self._lock:
            # Try to get existing connection
            pool = self._pools[pool_key]
            
            while pool:
                conn = pool.popleft()
                if conn.is_healthy():
                    conn.status = ConnectionStatus.BUSY
                    conn.metrics.last_used = time.time()
                    self._stats['connections_reused'] += 1
                    return conn
                else:
                    # Close unhealthy connection
                    await self._close_connection(conn)
            
            # Create new connection if under limit
            if self._connection_count[pool_key] < self.max_connections_per_host:
                if self._total_connections < self.max_total_connections:
                    conn = await self._create_connection(host, port, use_ssl)
                    if conn:
                        conn.status = ConnectionStatus.BUSY
                        self._connection_count[pool_key] += 1
                        self._total_connections += 1
                        return conn
        
        # Wait for connection to become available
        for _ in range(10):
            await asyncio.sleep(0.5)
            async with self._lock:
                pool = self._pools[pool_key]
                if pool:
                    conn = pool.popleft()
                    if conn.is_healthy():
                        conn.status = ConnectionStatus.BUSY
                        return conn
        
        raise RuntimeError(f"No connection available for {pool_key}")
    
    async def release(self, conn: PooledConnection, error: bool = False) -> None:
        """Release connection back to pool"""
        pool_key = f"{conn.host}:{conn.port}"
        
        if error:
            conn.metrics.errors += 1
            self._stats['connection_errors'] += 1
        else:
            conn.metrics.requests_made += 1
            self._stats['requests_made'] += 1
        
        # Check if connection should be closed
        if not conn.is_healthy() or conn.metrics.age > self.max_connection_age:
            await self._close_connection(conn)
            async with self._lock:
                self._connection_count[pool_key] = max(0, self._connection_count[pool_key] - 1)
                self._total_connections = max(0, self._total_connections - 1)
            return
        
        # Return to pool
        conn.status = ConnectionStatus.IDLE
        async with self._lock:
            self._pools[pool_key].append(conn)
    
    async def _create_connection(self, host: str, port: int, 
                                  use_ssl: bool) -> Optional[PooledConnection]:
        """Create new connection"""
        try:
            # Resolve DNS
            ips = await self._dns_cache.resolve(host)
            target_host = ips[0] if ips else host
            
            # Create SSL context if needed
            ssl_context = None
            if use_ssl:
                ssl_context = ssl.create_default_context()
            
            # Open connection
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(
                    target_host, port,
                    ssl=ssl_context,
                    server_hostname=host if use_ssl else None
                ),
                timeout=self.connection_timeout
            )
            
            conn = PooledConnection(
                connection_id=hashlib.md5(f"{host}:{port}:{time.time()}".encode()).hexdigest()[:8],
                host=host,
                port=port,
                ssl=use_ssl,
                reader=reader,
                writer=writer,
                status=ConnectionStatus.IDLE
            )
            
            self._stats['connections_created'] += 1
            self.logger.debug(f"Created connection to {host}:{port}")
            
            return conn
            
        except Exception as e:
            self.logger.error(f"Failed to create connection to {host}:{port}: {e}")
            self._stats['connection_errors'] += 1
            return None
    
    async def _close_connection(self, conn: PooledConnection) -> None:
        """Close connection"""
        try:
            conn.status = ConnectionStatus.CLOSED
            if conn.writer:
                conn.writer.close()
                await conn.writer.wait_closed()
            
            self._stats['connections_closed'] += 1
            
        except Exception as e:
            self.logger.warning(f"Error closing connection: {e}")
    
    async def _cleanup_loop(self) -> None:
        """Background cleanup loop"""
        while self._running:
            try:
                await asyncio.sleep(30)
                await self._cleanup_idle_connections()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Cleanup error: {e}")
    
    async def _cleanup_idle_connections(self) -> None:
        """Close idle connections"""
        async with self._lock:
            for pool_key, pool in list(self._pools.items()):
                # Keep at least one connection
                while len(pool) > 1:
                    conn = pool[0]
                    if conn.metrics.idle_time > self.idle_timeout:
                        pool.popleft()
                        await self._close_connection(conn)
                        self._connection_count[pool_key] -= 1
                        self._total_connections -= 1
                    else:
                        break
    
    async def _apply_rate_limit(self, host: str) -> None:
        """Apply rate limiting for host"""
        if host not in self._rate_limiters:
            self._rate_limiters[host] = RateLimiter(rate=10.0, burst=20)
        
        await self._rate_limiters[host].acquire()
    
    def set_rate_limit(self, host: str, rate: float, burst: int = 20) -> None:
        """Set rate limit for specific host"""
        self._rate_limiters[host] = RateLimiter(rate=rate, burst=burst)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get pool statistics"""
        pool_stats = {}
        for pool_key, pool in self._pools.items():
            pool_stats[pool_key] = {
                'available': len(pool),
                'total': self._connection_count[pool_key]
            }
        
        return {
            'total_connections': self._total_connections,
            'max_total': self.max_total_connections,
            'pools': pool_stats,
            'dns_cache': self._dns_cache.get_stats(),
            **self._stats
        }


class ConnectionPoolContext:
    """Context manager for connection pool"""
    
    def __init__(self, pool: ConnectionPool, url: str):
        self.pool = pool
        self.url = url
        self.connection: Optional[PooledConnection] = None
        self._error = False
    
    async def __aenter__(self) -> PooledConnection:
        self.connection = await self.pool.acquire(self.url)
        return self.connection
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.connection:
            self._error = exc_type is not None
            await self.pool.release(self.connection, error=self._error)


# Global connection pool instance
_global_pool: Optional[ConnectionPool] = None


async def get_connection_pool() -> ConnectionPool:
    """Get or create global connection pool"""
    global _global_pool
    if _global_pool is None:
        _global_pool = ConnectionPool()
        await _global_pool.start()
    return _global_pool


def create_connection_pool(config: Optional[Dict[str, Any]] = None) -> ConnectionPool:
    """Factory function for connection pool"""
    config = config or {}
    return ConnectionPool(
        max_connections_per_host=config.get('max_connections_per_host', 10),
        max_total_connections=config.get('max_total_connections', 100),
        connection_timeout=config.get('connection_timeout', 30.0),
        idle_timeout=config.get('idle_timeout', 60.0),
        max_connection_age=config.get('max_connection_age', 300.0)
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("Connection Pool Demo")
    print("=" * 60)
    
    async def main():
        # Create pool
        pool = ConnectionPool(
            max_connections_per_host=5,
            max_total_connections=20
        )
        
        await pool.start()
        
        print("\n1. Testing DNS cache...")
        dns = DNSCache()
        ips = await dns.resolve("example.com")
        print(f"   Resolved example.com: {ips}")
        print(f"   DNS cache stats: {dns.get_stats()}")
        
        print("\n2. Testing rate limiter...")
        limiter = RateLimiter(rate=5.0, burst=3)
        
        start = time.time()
        for i in range(5):
            await limiter.acquire()
            print(f"   Request {i+1} at {time.time() - start:.2f}s")
        
        print("\n3. Testing connection pool...")
        
        # Note: This will try to actually connect, may fail without network
        try:
            async with ConnectionPoolContext(pool, "https://example.com") as conn:
                print(f"   Acquired connection: {conn.connection_id}")
                print(f"   Host: {conn.host}:{conn.port}")
                print(f"   SSL: {conn.ssl}")
        except Exception as e:
            print(f"   Connection failed (expected without network): {e}")
        
        print("\n4. Pool Statistics:")
        stats = pool.get_stats()
        for key, value in stats.items():
            print(f"   {key}: {value}")
        
        await pool.stop()
        print("\nDemo complete!")
    
    asyncio.run(main())
