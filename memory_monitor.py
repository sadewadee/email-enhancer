"""
Memory Management - Real-time memory monitoring with backpressure control

Implements comprehensive memory management:
- Real-time memory usage tracking
- Backpressure control for overload protection
- Automatic garbage collection triggers
- Memory pool for frequently allocated objects
- Streaming CSV processing support
- Integration with metrics and alerting
"""

import asyncio
import gc
import logging
import os
import psutil
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple


class MemoryPressureLevel(Enum):
    """Memory pressure levels"""
    LOW = "low"           # < 60% - Normal operation
    MODERATE = "moderate" # 60-75% - Start conservation
    HIGH = "high"         # 75-85% - Aggressive conservation
    CRITICAL = "critical" # > 85% - Emergency measures


@dataclass
class MemoryMetrics:
    """Memory usage metrics"""
    timestamp: float
    total_mb: float
    available_mb: float
    used_mb: float
    percent: float
    process_mb: float
    process_percent: float
    swap_used_mb: float
    swap_percent: float
    
    @property
    def pressure_level(self) -> MemoryPressureLevel:
        """Determine memory pressure level"""
        if self.percent < 60:
            return MemoryPressureLevel.LOW
        elif self.percent < 75:
            return MemoryPressureLevel.MODERATE
        elif self.percent < 85:
            return MemoryPressureLevel.HIGH
        else:
            return MemoryPressureLevel.CRITICAL
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'timestamp': self.timestamp,
            'total_mb': round(self.total_mb, 2),
            'available_mb': round(self.available_mb, 2),
            'used_mb': round(self.used_mb, 2),
            'percent': round(self.percent, 1),
            'process_mb': round(self.process_mb, 2),
            'process_percent': round(self.process_percent, 2),
            'swap_used_mb': round(self.swap_used_mb, 2),
            'swap_percent': round(self.swap_percent, 1),
            'pressure_level': self.pressure_level.value
        }


class BackpressureController:
    """
    Controls system backpressure based on memory usage.
    
    Features:
    - Automatic throttling when memory is high
    - Configurable thresholds
    - Gradual pressure increase/decrease
    - Callback notifications
    """
    
    def __init__(self,
                 low_threshold: float = 60.0,
                 moderate_threshold: float = 75.0,
                 high_threshold: float = 85.0,
                 critical_threshold: float = 92.0):
        
        self.low_threshold = low_threshold
        self.moderate_threshold = moderate_threshold
        self.high_threshold = high_threshold
        self.critical_threshold = critical_threshold
        
        self._active = False
        self._current_level = MemoryPressureLevel.LOW
        self._callbacks: List[Callable[[MemoryPressureLevel], None]] = []
        self._lock = threading.Lock()
        
        # Throttling factors by level
        self._throttle_factors = {
            MemoryPressureLevel.LOW: 1.0,       # No throttling
            MemoryPressureLevel.MODERATE: 0.7,  # 30% reduction
            MemoryPressureLevel.HIGH: 0.4,      # 60% reduction
            MemoryPressureLevel.CRITICAL: 0.1   # 90% reduction
        }
    
    def update(self, memory_percent: float) -> MemoryPressureLevel:
        """Update pressure level based on memory percentage"""
        with self._lock:
            old_level = self._current_level
            
            if memory_percent >= self.critical_threshold:
                self._current_level = MemoryPressureLevel.CRITICAL
                self._active = True
            elif memory_percent >= self.high_threshold:
                self._current_level = MemoryPressureLevel.HIGH
                self._active = True
            elif memory_percent >= self.moderate_threshold:
                self._current_level = MemoryPressureLevel.MODERATE
                self._active = True
            else:
                self._current_level = MemoryPressureLevel.LOW
                self._active = False
            
            # Notify callbacks on level change
            if old_level != self._current_level:
                for callback in self._callbacks:
                    try:
                        callback(self._current_level)
                    except Exception:
                        pass
            
            return self._current_level
    
    def is_active(self) -> bool:
        """Check if backpressure is active"""
        return self._active
    
    def get_throttle_factor(self) -> float:
        """Get current throttle factor (0.0-1.0)"""
        return self._throttle_factors[self._current_level]
    
    def get_recommended_concurrency(self, base_concurrency: int) -> int:
        """Get recommended concurrency based on pressure"""
        factor = self.get_throttle_factor()
        return max(1, int(base_concurrency * factor))
    
    def get_recommended_batch_size(self, base_size: int) -> int:
        """Get recommended batch size based on pressure"""
        factor = self.get_throttle_factor()
        return max(10, int(base_size * factor))
    
    def should_pause(self) -> bool:
        """Check if processing should pause"""
        return self._current_level == MemoryPressureLevel.CRITICAL
    
    def on_pressure_change(self, callback: Callable[[MemoryPressureLevel], None]) -> None:
        """Register callback for pressure level changes"""
        self._callbacks.append(callback)
    
    @property
    def current_level(self) -> MemoryPressureLevel:
        return self._current_level


class MemoryPool:
    """
    Object pool for reducing allocation overhead.
    
    Reuses objects to reduce garbage collection pressure.
    """
    
    def __init__(self, factory: Callable[[], Any], max_size: int = 100):
        self.factory = factory
        self.max_size = max_size
        self._pool: deque = deque(maxlen=max_size)
        self._lock = threading.Lock()
        self._stats = {
            'created': 0,
            'reused': 0,
            'returned': 0
        }
    
    def acquire(self) -> Any:
        """Acquire object from pool or create new"""
        with self._lock:
            if self._pool:
                self._stats['reused'] += 1
                return self._pool.pop()
            else:
                self._stats['created'] += 1
                return self.factory()
    
    def release(self, obj: Any) -> None:
        """Return object to pool"""
        with self._lock:
            if len(self._pool) < self.max_size:
                # Reset object if it has reset method
                if hasattr(obj, 'reset'):
                    obj.reset()
                elif hasattr(obj, 'clear'):
                    obj.clear()
                
                self._pool.append(obj)
                self._stats['returned'] += 1
    
    def clear(self) -> None:
        """Clear pool"""
        with self._lock:
            self._pool.clear()
    
    def get_stats(self) -> Dict[str, int]:
        """Get pool statistics"""
        with self._lock:
            return {
                **self._stats,
                'pool_size': len(self._pool),
                'max_size': self.max_size
            }


class MemoryMonitor:
    """
    Real-time memory monitoring with automatic backpressure control.
    
    Features:
    - Continuous memory tracking
    - Backpressure control integration
    - Automatic garbage collection
    - Memory usage history
    - Alerting integration
    """
    
    def __init__(self,
                 check_interval: float = 5.0,
                 history_size: int = 100,
                 gc_threshold: float = 80.0,
                 process_limit_mb: Optional[float] = None):
        
        self.logger = logging.getLogger(__name__)
        self.check_interval = check_interval
        self.history_size = history_size
        self.gc_threshold = gc_threshold
        self.process_limit_mb = process_limit_mb
        
        # Components
        self.backpressure = BackpressureController()
        
        # State
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._history: deque = deque(maxlen=history_size)
        self._current_metrics: Optional[MemoryMetrics] = None
        self._lock = threading.RLock()
        
        # Process handle
        self._process = psutil.Process(os.getpid())
        
        # Callbacks
        self._alert_callbacks: List[Callable[[MemoryMetrics], None]] = []
        
        # Stats
        self._stats = {
            'gc_triggered': 0,
            'alerts_sent': 0,
            'checks_performed': 0
        }
    
    def start(self) -> None:
        """Start monitoring"""
        if self._running:
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()
        self.logger.info("Memory monitor started")
    
    def stop(self) -> None:
        """Stop monitoring"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5.0)
        self.logger.info("Memory monitor stopped")
    
    def _monitor_loop(self) -> None:
        """Main monitoring loop"""
        while self._running:
            try:
                metrics = self._collect_metrics()
                
                with self._lock:
                    self._current_metrics = metrics
                    self._history.append(metrics)
                    self._stats['checks_performed'] += 1
                
                # Update backpressure
                self.backpressure.update(metrics.percent)
                
                # Check if GC needed
                if metrics.percent >= self.gc_threshold:
                    self._trigger_gc()
                
                # Check process limit
                if self.process_limit_mb and metrics.process_mb > self.process_limit_mb:
                    self._handle_process_limit_exceeded(metrics)
                
                # Check for alerts
                if metrics.pressure_level in (MemoryPressureLevel.HIGH, MemoryPressureLevel.CRITICAL):
                    self._send_alert(metrics)
                
                time.sleep(self.check_interval)
                
            except Exception as e:
                self.logger.error(f"Monitor error: {e}")
                time.sleep(self.check_interval)
    
    def _collect_metrics(self) -> MemoryMetrics:
        """Collect current memory metrics"""
        # System memory
        mem = psutil.virtual_memory()
        swap = psutil.swap_memory()
        
        # Process memory
        proc_mem = self._process.memory_info()
        proc_percent = self._process.memory_percent()
        
        return MemoryMetrics(
            timestamp=time.time(),
            total_mb=mem.total / 1024 / 1024,
            available_mb=mem.available / 1024 / 1024,
            used_mb=mem.used / 1024 / 1024,
            percent=mem.percent,
            process_mb=proc_mem.rss / 1024 / 1024,
            process_percent=proc_percent,
            swap_used_mb=swap.used / 1024 / 1024,
            swap_percent=swap.percent
        )
    
    def _trigger_gc(self) -> None:
        """Trigger garbage collection"""
        collected = gc.collect()
        self._stats['gc_triggered'] += 1
        self.logger.debug(f"GC triggered, collected {collected} objects")
    
    def _handle_process_limit_exceeded(self, metrics: MemoryMetrics) -> None:
        """Handle process memory limit exceeded"""
        self.logger.warning(
            f"Process memory limit exceeded: {metrics.process_mb:.0f}MB > {self.process_limit_mb}MB"
        )
        self._trigger_gc()
    
    def _send_alert(self, metrics: MemoryMetrics) -> None:
        """Send memory alert"""
        self._stats['alerts_sent'] += 1
        
        for callback in self._alert_callbacks:
            try:
                callback(metrics)
            except Exception as e:
                self.logger.error(f"Alert callback error: {e}")
    
    def on_alert(self, callback: Callable[[MemoryMetrics], None]) -> None:
        """Register alert callback"""
        self._alert_callbacks.append(callback)
    
    def get_current_metrics(self) -> Optional[MemoryMetrics]:
        """Get current memory metrics"""
        with self._lock:
            return self._current_metrics
    
    def get_history(self, limit: int = 100) -> List[MemoryMetrics]:
        """Get memory history"""
        with self._lock:
            return list(self._history)[-limit:]
    
    def get_usage_percentage(self) -> float:
        """Get current memory usage percentage"""
        metrics = self.get_current_metrics()
        return metrics.percent if metrics else 0.0
    
    def is_backpressure_active(self) -> bool:
        """Check if backpressure is active"""
        return self.backpressure.is_active()
    
    def get_recommended_concurrency(self, base: int) -> int:
        """Get recommended concurrency level"""
        return self.backpressure.get_recommended_concurrency(base)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get monitor statistics"""
        with self._lock:
            current = self._current_metrics
            
            return {
                'running': self._running,
                'current': current.to_dict() if current else None,
                'backpressure_active': self.backpressure.is_active(),
                'pressure_level': self.backpressure.current_level.value,
                'throttle_factor': self.backpressure.get_throttle_factor(),
                'history_size': len(self._history),
                **self._stats
            }
    
    def force_gc(self) -> int:
        """Force garbage collection and return objects collected"""
        collected = gc.collect()
        self._stats['gc_triggered'] += 1
        return collected


class StreamingCSVProcessor:
    """
    Memory-efficient CSV processing with streaming.
    
    Processes CSV files in chunks to minimize memory usage.
    """
    
    def __init__(self,
                 chunk_size: int = 1000,
                 memory_monitor: Optional[MemoryMonitor] = None):
        self.chunk_size = chunk_size
        self.memory_monitor = memory_monitor
        self.logger = logging.getLogger(__name__)
    
    def get_adaptive_chunk_size(self) -> int:
        """Get chunk size based on memory pressure"""
        if self.memory_monitor:
            return self.memory_monitor.backpressure.get_recommended_batch_size(self.chunk_size)
        return self.chunk_size
    
    def stream_csv(self, filepath: str, encoding: str = 'utf-8'):
        """Stream CSV file in chunks"""
        import csv
        
        chunk_size = self.get_adaptive_chunk_size()
        chunk = []
        row_count = 0
        
        try:
            with open(filepath, 'r', encoding=encoding, errors='replace') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    chunk.append(row)
                    row_count += 1
                    
                    if len(chunk) >= chunk_size:
                        yield chunk
                        chunk = []
                        
                        # Update chunk size based on memory
                        chunk_size = self.get_adaptive_chunk_size()
                        
                        # Pause if critical memory
                        if self.memory_monitor and self.memory_monitor.backpressure.should_pause():
                            self.logger.warning("Pausing due to critical memory pressure")
                            self.memory_monitor.force_gc()
                            time.sleep(5)
                
                # Yield remaining rows
                if chunk:
                    yield chunk
                    
        except Exception as e:
            self.logger.error(f"Error streaming CSV: {e}")
            raise
    
    async def stream_csv_async(self, filepath: str, encoding: str = 'utf-8'):
        """Async stream CSV file in chunks"""
        import csv
        
        chunk_size = self.get_adaptive_chunk_size()
        chunk = []
        
        try:
            with open(filepath, 'r', encoding=encoding, errors='replace') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    chunk.append(row)
                    
                    if len(chunk) >= chunk_size:
                        yield chunk
                        chunk = []
                        chunk_size = self.get_adaptive_chunk_size()
                        
                        # Allow other tasks to run
                        await asyncio.sleep(0)
                        
                        if self.memory_monitor and self.memory_monitor.backpressure.should_pause():
                            self.logger.warning("Pausing due to critical memory")
                            self.memory_monitor.force_gc()
                            await asyncio.sleep(5)
                
                if chunk:
                    yield chunk
                    
        except Exception as e:
            self.logger.error(f"Error streaming CSV: {e}")
            raise


# Global memory monitor instance
_global_monitor: Optional[MemoryMonitor] = None


def get_memory_monitor() -> MemoryMonitor:
    """Get or create global memory monitor"""
    global _global_monitor
    if _global_monitor is None:
        _global_monitor = MemoryMonitor()
    return _global_monitor


def set_memory_monitor(monitor: MemoryMonitor) -> None:
    """Set global memory monitor"""
    global _global_monitor
    _global_monitor = monitor


def create_memory_monitor(config: Optional[Dict[str, Any]] = None) -> MemoryMonitor:
    """Factory function for memory monitor"""
    config = config or {}
    return MemoryMonitor(
        check_interval=config.get('check_interval', 5.0),
        history_size=config.get('history_size', 100),
        gc_threshold=config.get('gc_threshold', 80.0),
        process_limit_mb=config.get('process_limit_mb')
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("Memory Monitor Demo")
    print("=" * 60)
    
    # Create monitor
    monitor = MemoryMonitor(check_interval=1.0)
    
    # Register alert callback
    def on_memory_alert(metrics: MemoryMetrics):
        print(f"   ALERT: Memory at {metrics.percent:.1f}%")
    
    monitor.on_alert(on_memory_alert)
    
    # Register backpressure callback
    def on_pressure_change(level: MemoryPressureLevel):
        print(f"   Pressure level changed to: {level.value}")
    
    monitor.backpressure.on_pressure_change(on_pressure_change)
    
    # Start monitoring
    monitor.start()
    
    print("\n1. Current Memory Status:")
    time.sleep(2)  # Wait for first collection
    
    metrics = monitor.get_current_metrics()
    if metrics:
        for key, value in metrics.to_dict().items():
            print(f"   {key}: {value}")
    
    print("\n2. Backpressure Status:")
    print(f"   Active: {monitor.is_backpressure_active()}")
    print(f"   Level: {monitor.backpressure.current_level.value}")
    print(f"   Throttle Factor: {monitor.backpressure.get_throttle_factor()}")
    print(f"   Recommended Concurrency (base=10): {monitor.get_recommended_concurrency(10)}")
    
    print("\n3. Memory Pool Demo:")
    pool = MemoryPool(factory=list, max_size=10)
    
    # Acquire and release objects
    obj1 = pool.acquire()
    obj1.extend([1, 2, 3])
    pool.release(obj1)
    
    obj2 = pool.acquire()  # Should reuse obj1
    print(f"   Pool stats: {pool.get_stats()}")
    
    print("\n4. Streaming CSV Demo:")
    # Create test CSV
    import csv
    import tempfile
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        writer = csv.DictWriter(f, fieldnames=['url', 'name'])
        writer.writeheader()
        for i in range(25):
            writer.writerow({'url': f'https://example{i}.com', 'name': f'Test {i}'})
        test_csv = f.name
    
    processor = StreamingCSVProcessor(chunk_size=10, memory_monitor=monitor)
    
    chunk_count = 0
    for chunk in processor.stream_csv(test_csv):
        chunk_count += 1
        print(f"   Chunk {chunk_count}: {len(chunk)} rows")
    
    # Cleanup
    os.unlink(test_csv)
    
    print("\n5. Monitor Stats:")
    stats = monitor.get_stats()
    for key, value in stats.items():
        if key != 'current':
            print(f"   {key}: {value}")
    
    # Stop monitor
    monitor.stop()
    
    print("\nDemo complete!")
