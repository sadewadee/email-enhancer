"""
Advanced Caching - Intelligent cache with TTL, invalidation strategies, and persistence

Implements advanced caching capabilities:
- Multi-tier caching (memory, disk)
- TTL-based expiration with lazy/eager cleanup
- Intelligent invalidation strategies
- Cache warming and prefetching
- Statistics and hit rate tracking
- Thread-safe operations
- Serialization support
"""

import asyncio
import hashlib
import json
import logging
import os
import pickle
import sqlite3
import threading
import time
from abc import ABC, abstractmethod
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, Generic, List, Optional, Set, Tuple, TypeVar, Union
from functools import wraps


T = TypeVar('T')


class CacheStrategy(Enum):
    """Cache eviction strategies"""
    LRU = "lru"      # Least Recently Used
    LFU = "lfu"      # Least Frequently Used
    FIFO = "fifo"    # First In First Out
    TTL = "ttl"      # Time To Live only


class InvalidationStrategy(Enum):
    """Cache invalidation strategies"""
    LAZY = "lazy"           # Check on access
    EAGER = "eager"         # Background cleanup
    WRITE_THROUGH = "write_through"  # Invalidate on write
    WRITE_BEHIND = "write_behind"    # Async invalidation


@dataclass
class CacheEntry(Generic[T]):
    """Cache entry with metadata"""
    key: str
    value: T
    created_at: float = field(default_factory=time.time)
    expires_at: Optional[float] = None
    last_accessed: float = field(default_factory=time.time)
    access_count: int = 0
    size_bytes: int = 0
    tags: Set[str] = field(default_factory=set)
    
    @property
    def is_expired(self) -> bool:
        """Check if entry is expired"""
        if self.expires_at is None:
            return False
        return time.time() > self.expires_at
    
    @property
    def ttl_remaining(self) -> Optional[float]:
        """Get remaining TTL in seconds"""
        if self.expires_at is None:
            return None
        return max(0, self.expires_at - time.time())
    
    def touch(self) -> None:
        """Update access metadata"""
        self.last_accessed = time.time()
        self.access_count += 1


@dataclass
class CacheStats:
    """Cache statistics"""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    expirations: int = 0
    size: int = 0
    max_size: int = 0
    memory_bytes: int = 0
    
    @property
    def hit_rate(self) -> float:
        """Calculate hit rate percentage"""
        total = self.hits + self.misses
        return (self.hits / total * 100) if total > 0 else 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'hits': self.hits,
            'misses': self.misses,
            'hit_rate': f"{self.hit_rate:.1f}%",
            'evictions': self.evictions,
            'expirations': self.expirations,
            'size': self.size,
            'max_size': self.max_size,
            'memory_bytes': self.memory_bytes
        }


class CacheBackend(ABC, Generic[T]):
    """Abstract cache backend"""
    
    @abstractmethod
    def get(self, key: str) -> Optional[CacheEntry[T]]:
        """Get entry from cache"""
        pass
    
    @abstractmethod
    def set(self, entry: CacheEntry[T]) -> bool:
        """Set entry in cache"""
        pass
    
    @abstractmethod
    def delete(self, key: str) -> bool:
        """Delete entry from cache"""
        pass
    
    @abstractmethod
    def clear(self) -> None:
        """Clear all entries"""
        pass
    
    @abstractmethod
    def keys(self) -> List[str]:
        """Get all keys"""
        pass
    
    @abstractmethod
    def size(self) -> int:
        """Get number of entries"""
        pass


class MemoryBackend(CacheBackend[T]):
    """In-memory cache backend with LRU eviction"""
    
    def __init__(self, max_size: int = 10000):
        self.max_size = max_size
        self._cache: OrderedDict[str, CacheEntry[T]] = OrderedDict()
        self._lock = threading.RLock()
    
    def get(self, key: str) -> Optional[CacheEntry[T]]:
        with self._lock:
            if key in self._cache:
                # Move to end (most recently used)
                self._cache.move_to_end(key)
                entry = self._cache[key]
                entry.touch()
                return entry
            return None
    
    def set(self, entry: CacheEntry[T]) -> bool:
        with self._lock:
            # Evict if at capacity
            while len(self._cache) >= self.max_size:
                # Remove oldest (LRU)
                self._cache.popitem(last=False)
            
            self._cache[entry.key] = entry
            self._cache.move_to_end(entry.key)
            return True
    
    def delete(self, key: str) -> bool:
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False
    
    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
    
    def keys(self) -> List[str]:
        with self._lock:
            return list(self._cache.keys())
    
    def size(self) -> int:
        with self._lock:
            return len(self._cache)


class DiskBackend(CacheBackend[T]):
    """SQLite-backed disk cache"""
    
    def __init__(self, db_path: str = "cache.db", max_size: int = 100000):
        self.db_path = db_path
        self.max_size = max_size
        self._lock = threading.RLock()
        self._init_db()
    
    def _init_db(self) -> None:
        """Initialize database schema"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS cache (
                    key TEXT PRIMARY KEY,
                    value BLOB NOT NULL,
                    created_at REAL,
                    expires_at REAL,
                    last_accessed REAL,
                    access_count INTEGER DEFAULT 0,
                    size_bytes INTEGER DEFAULT 0,
                    tags TEXT
                )
            ''')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_expires ON cache(expires_at)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_accessed ON cache(last_accessed)')
            conn.commit()
    
    def get(self, key: str) -> Optional[CacheEntry[T]]:
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.execute(
                        'SELECT * FROM cache WHERE key = ?', (key,)
                    )
                    row = cursor.fetchone()
                    
                    if row:
                        # Update access stats
                        conn.execute('''
                            UPDATE cache SET last_accessed = ?, access_count = access_count + 1
                            WHERE key = ?
                        ''', (time.time(), key))
                        conn.commit()
                        
                        return CacheEntry(
                            key=row['key'],
                            value=pickle.loads(row['value']),
                            created_at=row['created_at'],
                            expires_at=row['expires_at'],
                            last_accessed=time.time(),
                            access_count=row['access_count'] + 1,
                            size_bytes=row['size_bytes'],
                            tags=set(json.loads(row['tags'])) if row['tags'] else set()
                        )
                    return None
            except Exception as e:
                logging.error(f"Cache get error: {e}")
                return None
    
    def set(self, entry: CacheEntry[T]) -> bool:
        with self._lock:
            try:
                serialized = pickle.dumps(entry.value)
                entry.size_bytes = len(serialized)
                
                with sqlite3.connect(self.db_path) as conn:
                    # Check size limit
                    cursor = conn.execute('SELECT COUNT(*) FROM cache')
                    current_size = cursor.fetchone()[0]
                    
                    if current_size >= self.max_size:
                        # Evict oldest entries
                        conn.execute('''
                            DELETE FROM cache WHERE key IN (
                                SELECT key FROM cache ORDER BY last_accessed ASC LIMIT ?
                            )
                        ''', (current_size - self.max_size + 1,))
                    
                    conn.execute('''
                        INSERT OR REPLACE INTO cache 
                        (key, value, created_at, expires_at, last_accessed, access_count, size_bytes, tags)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        entry.key, serialized, entry.created_at, entry.expires_at,
                        entry.last_accessed, entry.access_count, entry.size_bytes,
                        json.dumps(list(entry.tags))
                    ))
                    conn.commit()
                return True
            except Exception as e:
                logging.error(f"Cache set error: {e}")
                return False
    
    def delete(self, key: str) -> bool:
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute('DELETE FROM cache WHERE key = ?', (key,))
                    conn.commit()
                return True
            except Exception as e:
                logging.error(f"Cache delete error: {e}")
                return False
    
    def clear(self) -> None:
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute('DELETE FROM cache')
                    conn.commit()
            except Exception as e:
                logging.error(f"Cache clear error: {e}")
    
    def keys(self) -> List[str]:
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.execute('SELECT key FROM cache')
                    return [row[0] for row in cursor.fetchall()]
            except Exception as e:
                logging.error(f"Cache keys error: {e}")
                return []
    
    def size(self) -> int:
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.execute('SELECT COUNT(*) FROM cache')
                    return cursor.fetchone()[0]
            except Exception as e:
                logging.error(f"Cache size error: {e}")
                return 0
    
    def cleanup_expired(self) -> int:
        """Remove expired entries"""
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.execute(
                        'DELETE FROM cache WHERE expires_at IS NOT NULL AND expires_at < ?',
                        (time.time(),)
                    )
                    conn.commit()
                    return cursor.rowcount
            except Exception as e:
                logging.error(f"Cache cleanup error: {e}")
                return 0


class AdvancedCache(Generic[T]):
    """
    Advanced caching system with multi-tier storage and intelligent invalidation.
    
    Features:
    - Multi-tier caching (memory + disk)
    - TTL-based expiration
    - Multiple eviction strategies
    - Cache warming and prefetching
    - Tag-based invalidation
    - Statistics tracking
    - Thread-safe operations
    """
    
    def __init__(self,
                 memory_size: int = 10000,
                 disk_size: int = 100000,
                 default_ttl: Optional[float] = 3600.0,  # 1 hour
                 enable_disk: bool = False,
                 disk_path: str = "cache.db",
                 strategy: CacheStrategy = CacheStrategy.LRU,
                 invalidation: InvalidationStrategy = InvalidationStrategy.LAZY):
        
        self.logger = logging.getLogger(__name__)
        self.default_ttl = default_ttl
        self.strategy = strategy
        self.invalidation = invalidation
        
        # Initialize backends
        self._memory = MemoryBackend[T](max_size=memory_size)
        self._disk: Optional[DiskBackend[T]] = None
        
        if enable_disk:
            self._disk = DiskBackend[T](db_path=disk_path, max_size=disk_size)
        
        # Statistics
        self._stats = CacheStats(max_size=memory_size + (disk_size if enable_disk else 0))
        
        # Background cleanup
        self._cleanup_thread: Optional[threading.Thread] = None
        self._running = False
        
        # Start eager cleanup if configured
        if invalidation == InvalidationStrategy.EAGER:
            self._start_cleanup()
    
    def get(self, key: str) -> Optional[T]:
        """Get value from cache"""
        # Try memory first
        entry = self._memory.get(key)
        
        if entry is None and self._disk:
            # Try disk
            entry = self._disk.get(key)
            if entry and not entry.is_expired:
                # Promote to memory
                self._memory.set(entry)
        
        if entry:
            if entry.is_expired:
                # Lazy expiration
                self.delete(key)
                self._stats.expirations += 1
                self._stats.misses += 1
                return None
            
            self._stats.hits += 1
            return entry.value
        
        self._stats.misses += 1
        return None
    
    def set(self, key: str, value: T, ttl: Optional[float] = None, tags: Optional[Set[str]] = None) -> bool:
        """Set value in cache"""
        actual_ttl = ttl if ttl is not None else self.default_ttl
        expires_at = time.time() + actual_ttl if actual_ttl else None
        
        entry = CacheEntry(
            key=key,
            value=value,
            expires_at=expires_at,
            tags=tags or set()
        )
        
        # Try to estimate size
        try:
            entry.size_bytes = len(pickle.dumps(value))
        except Exception:
            entry.size_bytes = 0
        
        # Set in memory
        success = self._memory.set(entry)
        
        # Set in disk if enabled
        if self._disk:
            self._disk.set(entry)
        
        self._stats.size = self._memory.size()
        return success
    
    def delete(self, key: str) -> bool:
        """Delete value from cache"""
        success = self._memory.delete(key)
        
        if self._disk:
            self._disk.delete(key)
        
        self._stats.size = self._memory.size()
        return success
    
    def exists(self, key: str) -> bool:
        """Check if key exists and is not expired"""
        return self.get(key) is not None
    
    def get_or_set(self, key: str, factory: Callable[[], T], 
                   ttl: Optional[float] = None, tags: Optional[Set[str]] = None) -> T:
        """Get value or compute and cache it"""
        value = self.get(key)
        
        if value is None:
            value = factory()
            self.set(key, value, ttl=ttl, tags=tags)
        
        return value
    
    def invalidate_by_tag(self, tag: str) -> int:
        """Invalidate all entries with given tag"""
        count = 0
        
        for key in self._memory.keys():
            entry = self._memory.get(key)
            if entry and tag in entry.tags:
                self.delete(key)
                count += 1
        
        return count
    
    def invalidate_by_prefix(self, prefix: str) -> int:
        """Invalidate all entries with key prefix"""
        count = 0
        
        for key in self._memory.keys():
            if key.startswith(prefix):
                self.delete(key)
                count += 1
        
        return count
    
    def invalidate_by_pattern(self, pattern: str) -> int:
        """Invalidate entries matching pattern (simple glob)"""
        import fnmatch
        count = 0
        
        for key in self._memory.keys():
            if fnmatch.fnmatch(key, pattern):
                self.delete(key)
                count += 1
        
        return count
    
    def clear(self) -> None:
        """Clear all cache entries"""
        self._memory.clear()
        if self._disk:
            self._disk.clear()
        self._stats.size = 0
    
    def warm(self, items: Dict[str, T], ttl: Optional[float] = None) -> int:
        """Warm cache with items"""
        count = 0
        for key, value in items.items():
            if self.set(key, value, ttl=ttl):
                count += 1
        return count
    
    def prefetch(self, keys: List[str], factory: Callable[[str], T],
                 ttl: Optional[float] = None) -> int:
        """Prefetch and cache values for keys"""
        count = 0
        for key in keys:
            if not self.exists(key):
                try:
                    value = factory(key)
                    if self.set(key, value, ttl=ttl):
                        count += 1
                except Exception as e:
                    self.logger.warning(f"Prefetch failed for {key}: {e}")
        return count
    
    def get_stats(self) -> CacheStats:
        """Get cache statistics"""
        self._stats.size = self._memory.size()
        if self._disk:
            self._stats.size += self._disk.size()
        return self._stats
    
    def cleanup_expired(self) -> int:
        """Cleanup expired entries"""
        count = 0
        
        # Memory cleanup
        for key in list(self._memory.keys()):
            entry = self._memory._cache.get(key)
            if entry and entry.is_expired:
                self._memory.delete(key)
                count += 1
        
        # Disk cleanup
        if self._disk:
            count += self._disk.cleanup_expired()
        
        self._stats.expirations += count
        return count
    
    def _start_cleanup(self, interval: float = 60.0) -> None:
        """Start background cleanup thread"""
        if self._running:
            return
        
        self._running = True
        
        def cleanup_loop():
            while self._running:
                try:
                    self.cleanup_expired()
                except Exception as e:
                    self.logger.error(f"Cleanup error: {e}")
                time.sleep(interval)
        
        self._cleanup_thread = threading.Thread(target=cleanup_loop, daemon=True)
        self._cleanup_thread.start()
    
    def _stop_cleanup(self) -> None:
        """Stop background cleanup"""
        self._running = False
        if self._cleanup_thread:
            self._cleanup_thread.join(timeout=5.0)
    
    def close(self) -> None:
        """Close cache and cleanup resources"""
        self._stop_cleanup()


# Decorator for caching function results
def cached(cache: Optional[AdvancedCache] = None,
           ttl: Optional[float] = 3600.0,
           key_prefix: str = "",
           key_func: Optional[Callable] = None,
           tags: Optional[Set[str]] = None):
    """Decorator to cache function results"""
    def decorator(func: Callable) -> Callable:
        nonlocal cache
        if cache is None:
            cache = get_default_cache()
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Generate cache key
            if key_func:
                cache_key = key_func(*args, **kwargs)
            else:
                # Default key generation
                key_parts = [key_prefix or func.__name__]
                key_parts.extend(str(arg) for arg in args)
                key_parts.extend(f"{k}={v}" for k, v in sorted(kwargs.items()))
                cache_key = hashlib.md5(":".join(key_parts).encode()).hexdigest()
            
            # Try to get from cache
            result = cache.get(cache_key)
            if result is not None:
                return result
            
            # Compute and cache
            result = func(*args, **kwargs)
            cache.set(cache_key, result, ttl=ttl, tags=tags)
            return result
        
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            # Generate cache key
            if key_func:
                cache_key = key_func(*args, **kwargs)
            else:
                key_parts = [key_prefix or func.__name__]
                key_parts.extend(str(arg) for arg in args)
                key_parts.extend(f"{k}={v}" for k, v in sorted(kwargs.items()))
                cache_key = hashlib.md5(":".join(key_parts).encode()).hexdigest()
            
            # Try to get from cache
            result = cache.get(cache_key)
            if result is not None:
                return result
            
            # Compute and cache
            result = await func(*args, **kwargs)
            cache.set(cache_key, result, ttl=ttl, tags=tags)
            return result
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return wrapper
    
    return decorator


# URL-specific cache for web scraping
class URLCache(AdvancedCache[Dict[str, Any]]):
    """Specialized cache for URL scraping results"""
    
    def __init__(self, **kwargs):
        kwargs.setdefault('default_ttl', 86400.0)  # 24 hours
        super().__init__(**kwargs)
    
    def get_url(self, url: str) -> Optional[Dict[str, Any]]:
        """Get cached URL result"""
        key = self._url_key(url)
        return self.get(key)
    
    def set_url(self, url: str, result: Dict[str, Any], 
                ttl: Optional[float] = None) -> bool:
        """Cache URL result"""
        key = self._url_key(url)
        domain = self._extract_domain(url)
        return self.set(key, result, ttl=ttl, tags={domain, 'url'})
    
    def invalidate_domain(self, domain: str) -> int:
        """Invalidate all cached results for domain"""
        return self.invalidate_by_tag(domain)
    
    def _url_key(self, url: str) -> str:
        """Generate cache key for URL"""
        return f"url:{hashlib.md5(url.encode()).hexdigest()}"
    
    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL"""
        from urllib.parse import urlparse
        parsed = urlparse(url)
        return parsed.netloc or 'unknown'


# Global cache instance
_default_cache: Optional[AdvancedCache] = None


def get_default_cache() -> AdvancedCache:
    """Get or create default cache instance"""
    global _default_cache
    if _default_cache is None:
        _default_cache = AdvancedCache(
            memory_size=10000,
            default_ttl=3600.0
        )
    return _default_cache


def set_default_cache(cache: AdvancedCache) -> None:
    """Set default cache instance"""
    global _default_cache
    _default_cache = cache


def create_cache(config: Optional[Dict[str, Any]] = None) -> AdvancedCache:
    """Factory function to create cache with configuration"""
    config = config or {}
    return AdvancedCache(
        memory_size=config.get('memory_size', 10000),
        disk_size=config.get('disk_size', 100000),
        default_ttl=config.get('default_ttl', 3600.0),
        enable_disk=config.get('enable_disk', False),
        disk_path=config.get('disk_path', 'cache.db'),
        strategy=CacheStrategy(config.get('strategy', 'lru')),
        invalidation=InvalidationStrategy(config.get('invalidation', 'lazy'))
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("Advanced Cache Demo")
    print("=" * 60)
    
    # Create cache
    cache = AdvancedCache[str](
        memory_size=100,
        default_ttl=5.0,  # 5 seconds for demo
        enable_disk=False
    )
    
    # Basic operations
    print("\n1. Basic Operations:")
    cache.set("key1", "value1")
    cache.set("key2", "value2", ttl=10.0)
    cache.set("key3", "value3", tags={"group1"})
    
    print(f"   key1: {cache.get('key1')}")
    print(f"   key2: {cache.get('key2')}")
    print(f"   key3: {cache.get('key3')}")
    print(f"   exists key1: {cache.exists('key1')}")
    
    # Get or set
    print("\n2. Get or Set:")
    result = cache.get_or_set("computed", lambda: "computed_value")
    print(f"   computed: {result}")
    
    # Stats
    print("\n3. Statistics:")
    stats = cache.get_stats()
    print(f"   {stats.to_dict()}")
    
    # Invalidation
    print("\n4. Invalidation:")
    cache.set("prefix:a", "a")
    cache.set("prefix:b", "b")
    cache.set("prefix:c", "c")
    count = cache.invalidate_by_prefix("prefix:")
    print(f"   Invalidated {count} entries by prefix")
    
    # Tag-based invalidation
    cache.set("tagged1", "v1", tags={"group2"})
    cache.set("tagged2", "v2", tags={"group2"})
    count = cache.invalidate_by_tag("group2")
    print(f"   Invalidated {count} entries by tag")
    
    # Cache warming
    print("\n5. Cache Warming:")
    warm_data = {f"warm:{i}": f"value{i}" for i in range(10)}
    count = cache.warm(warm_data, ttl=60.0)
    print(f"   Warmed {count} entries")
    
    # TTL expiration
    print("\n6. TTL Expiration:")
    cache.set("expires", "will_expire", ttl=1.0)
    print(f"   Before expiry: {cache.get('expires')}")
    time.sleep(1.5)
    print(f"   After expiry: {cache.get('expires')}")
    
    # Decorator usage
    print("\n7. Decorator Usage:")
    
    @cached(cache=cache, ttl=10.0)
    def expensive_computation(x: int) -> int:
        time.sleep(0.1)  # Simulate work
        return x * 2
    
    start = time.time()
    result1 = expensive_computation(5)
    time1 = time.time() - start
    
    start = time.time()
    result2 = expensive_computation(5)
    time2 = time.time() - start
    
    print(f"   First call: {result1} ({time1:.3f}s)")
    print(f"   Cached call: {result2} ({time2:.3f}s)")
    
    # Final stats
    print("\n8. Final Statistics:")
    stats = cache.get_stats()
    print(f"   {stats.to_dict()}")
    
    # URL Cache demo
    print("\n9. URL Cache Demo:")
    url_cache = URLCache(memory_size=100)
    url_cache.set_url("https://example.com/page1", {"status": "ok", "emails": ["test@example.com"]})
    result = url_cache.get_url("https://example.com/page1")
    print(f"   Cached URL result: {result}")
    
    cache.close()
    print("\nDemo complete!")
