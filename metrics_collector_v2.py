"""
Metrics Collection Service v2 - Enhanced metrics tracking with Prometheus-compatible format

Implements comprehensive metrics collection and management:
- Counter, gauge, histogram, and timing metrics
- Prometheus-compatible output for system integration
- Thread-safe operations with concurrent access
- Performance optimized with minimal overhead
- Automatic metric categorization and tagging
- Export capabilities in multiple formats
- Integration with structured logging
"""

import json
import logging
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Union
from functools import wraps
import statistics


class MetricType(Enum):
    """Metric type enumeration"""
    COUNTER = "counter"
    GAUGE = "gauge"
    TIMER = "timer"
    HISTOGRAM = "histogram"


@dataclass
class MetricPoint:
    """Single data point from metric collection"""
    timestamp: float
    value: Union[int, float]
    tags: Dict[str, str] = field(default_factory=dict)
    
    def __post_init__(self):
        if self.timestamp == 0:
            self.timestamp = time.time()


@dataclass
class HistogramBucket:
    """Histogram bucket configuration"""
    le: float  # less than or equal
    count: int = 0


class Counter:
    """Thread-safe counter metric"""
    
    def __init__(self, name: str, description: str = "", labels: Optional[List[str]] = None):
        self.name = name
        self.description = description
        self.labels = labels or []
        self._value: float = 0.0
        self._lock = threading.Lock()
        self._label_values: Dict[str, float] = {}
    
    def inc(self, value: float = 1.0, **label_kwargs) -> None:
        """Increment counter"""
        with self._lock:
            if label_kwargs:
                key = self._label_key(label_kwargs)
                self._label_values[key] = self._label_values.get(key, 0.0) + value
            else:
                self._value += value
    
    def get(self, **label_kwargs) -> float:
        """Get current counter value"""
        with self._lock:
            if label_kwargs:
                key = self._label_key(label_kwargs)
                return self._label_values.get(key, 0.0)
            return self._value
    
    def reset(self) -> None:
        """Reset counter to zero"""
        with self._lock:
            self._value = 0.0
            self._label_values.clear()
    
    def _label_key(self, label_kwargs: Dict[str, str]) -> str:
        """Generate key from labels"""
        return ','.join(f"{k}={v}" for k, v in sorted(label_kwargs.items()))
    
    def collect(self) -> Dict[str, Any]:
        """Collect counter data for export"""
        with self._lock:
            data = {
                'name': self.name,
                'type': 'counter',
                'description': self.description,
                'value': self._value
            }
            if self._label_values:
                data['labeled_values'] = dict(self._label_values)
            return data


class Gauge:
    """Thread-safe gauge metric"""
    
    def __init__(self, name: str, description: str = "", labels: Optional[List[str]] = None):
        self.name = name
        self.description = description
        self.labels = labels or []
        self._value: float = 0.0
        self._lock = threading.Lock()
        self._label_values: Dict[str, float] = {}
    
    def set(self, value: float, **label_kwargs) -> None:
        """Set gauge value"""
        with self._lock:
            if label_kwargs:
                key = self._label_key(label_kwargs)
                self._label_values[key] = value
            else:
                self._value = value
    
    def inc(self, value: float = 1.0, **label_kwargs) -> None:
        """Increment gauge"""
        with self._lock:
            if label_kwargs:
                key = self._label_key(label_kwargs)
                self._label_values[key] = self._label_values.get(key, 0.0) + value
            else:
                self._value += value
    
    def dec(self, value: float = 1.0, **label_kwargs) -> None:
        """Decrement gauge"""
        self.inc(-value, **label_kwargs)
    
    def get(self, **label_kwargs) -> float:
        """Get current gauge value"""
        with self._lock:
            if label_kwargs:
                key = self._label_key(label_kwargs)
                return self._label_values.get(key, 0.0)
            return self._value
    
    def _label_key(self, label_kwargs: Dict[str, str]) -> str:
        """Generate key from labels"""
        return ','.join(f"{k}={v}" for k, v in sorted(label_kwargs.items()))
    
    def collect(self) -> Dict[str, Any]:
        """Collect gauge data for export"""
        with self._lock:
            data = {
                'name': self.name,
                'type': 'gauge',
                'description': self.description,
                'value': self._value
            }
            if self._label_values:
                data['labeled_values'] = dict(self._label_values)
            return data


class Histogram:
    """Thread-safe histogram metric with configurable buckets"""
    
    DEFAULT_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, float('inf'))
    
    def __init__(self, name: str, description: str = "", 
                 buckets: Optional[tuple] = None, labels: Optional[List[str]] = None):
        self.name = name
        self.description = description
        self.labels = labels or []
        self._buckets = buckets or self.DEFAULT_BUCKETS
        self._lock = threading.Lock()
        
        # Initialize bucket counters
        self._bucket_counts: Dict[float, int] = {b: 0 for b in self._buckets}
        self._sum: float = 0.0
        self._count: int = 0
        self._values: List[float] = []  # For percentile calculations
        self._max_values = 10000  # Limit stored values
    
    def observe(self, value: float) -> None:
        """Observe a value and update histogram"""
        with self._lock:
            self._sum += value
            self._count += 1
            
            # Update bucket counts
            for bucket_le in self._buckets:
                if value <= bucket_le:
                    self._bucket_counts[bucket_le] += 1
            
            # Store value for percentile calculations
            self._values.append(value)
            if len(self._values) > self._max_values:
                self._values = self._values[-self._max_values:]
    
    def get_percentile(self, percentile: float) -> float:
        """Get percentile value (0-100)"""
        with self._lock:
            if not self._values:
                return 0.0
            sorted_values = sorted(self._values)
            index = int(len(sorted_values) * percentile / 100)
            return sorted_values[min(index, len(sorted_values) - 1)]
    
    def get_stats(self) -> Dict[str, float]:
        """Get histogram statistics"""
        with self._lock:
            if not self._values:
                return {'count': 0, 'sum': 0, 'avg': 0, 'min': 0, 'max': 0, 'p50': 0, 'p95': 0, 'p99': 0}
            
            return {
                'count': self._count,
                'sum': self._sum,
                'avg': self._sum / self._count if self._count > 0 else 0,
                'min': min(self._values),
                'max': max(self._values),
                'p50': self.get_percentile(50),
                'p95': self.get_percentile(95),
                'p99': self.get_percentile(99)
            }
    
    def reset(self) -> None:
        """Reset histogram"""
        with self._lock:
            self._bucket_counts = {b: 0 for b in self._buckets}
            self._sum = 0.0
            self._count = 0
            self._values.clear()
    
    def collect(self) -> Dict[str, Any]:
        """Collect histogram data for export"""
        with self._lock:
            return {
                'name': self.name,
                'type': 'histogram',
                'description': self.description,
                'buckets': {str(k): v for k, v in self._bucket_counts.items()},
                'count': self._count,
                'sum': self._sum,
                'stats': self.get_stats()
            }


class Timer:
    """Thread-safe timer metric for measuring durations"""
    
    def __init__(self, name: str, description: str = "", labels: Optional[List[str]] = None):
        self.name = name
        self.description = description
        self.labels = labels or []
        self._histogram = Histogram(f"{name}_seconds", description)
        self._lock = threading.Lock()
    
    def observe(self, duration: float) -> None:
        """Record a duration"""
        self._histogram.observe(duration)
    
    def time(self) -> 'TimerContext':
        """Return context manager for timing"""
        return TimerContext(self)
    
    def get_stats(self) -> Dict[str, float]:
        """Get timer statistics"""
        return self._histogram.get_stats()
    
    def reset(self) -> None:
        """Reset timer"""
        self._histogram.reset()
    
    def collect(self) -> Dict[str, Any]:
        """Collect timer data for export"""
        data = self._histogram.collect()
        data['name'] = self.name
        data['type'] = 'timer'
        return data


class TimerContext:
    """Context manager for timing operations"""
    
    def __init__(self, timer: Timer):
        self.timer = timer
        self.start_time: float = 0
    
    def __enter__(self) -> 'TimerContext':
        self.start_time = time.perf_counter()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        duration = time.perf_counter() - self.start_time
        self.timer.observe(duration)


class MetricsRegistry:
    """Registry for managing all metrics"""
    
    def __init__(self):
        self._lock = threading.Lock()
        self._counters: Dict[str, Counter] = {}
        self._gauges: Dict[str, Gauge] = {}
        self._histograms: Dict[str, Histogram] = {}
        self._timers: Dict[str, Timer] = {}
        self._categories: Dict[str, Set[str]] = defaultdict(set)
    
    def counter(self, name: str, description: str = "", 
                labels: Optional[List[str]] = None, category: str = "default") -> Counter:
        """Get or create counter"""
        with self._lock:
            if name not in self._counters:
                self._counters[name] = Counter(name, description, labels)
                self._categories[category].add(name)
            return self._counters[name]
    
    def gauge(self, name: str, description: str = "", 
              labels: Optional[List[str]] = None, category: str = "default") -> Gauge:
        """Get or create gauge"""
        with self._lock:
            if name not in self._gauges:
                self._gauges[name] = Gauge(name, description, labels)
                self._categories[category].add(name)
            return self._gauges[name]
    
    def histogram(self, name: str, description: str = "", 
                  buckets: Optional[tuple] = None, labels: Optional[List[str]] = None,
                  category: str = "default") -> Histogram:
        """Get or create histogram"""
        with self._lock:
            if name not in self._histograms:
                self._histograms[name] = Histogram(name, description, buckets, labels)
                self._categories[category].add(name)
            return self._histograms[name]
    
    def timer(self, name: str, description: str = "", 
              labels: Optional[List[str]] = None, category: str = "default") -> Timer:
        """Get or create timer"""
        with self._lock:
            if name not in self._timers:
                self._timers[name] = Timer(name, description, labels)
                self._categories[category].add(name)
            return self._timers[name]
    
    def collect_all(self) -> Dict[str, Any]:
        """Collect all metrics data"""
        with self._lock:
            return {
                'timestamp': time.time(),
                'counters': {name: c.collect() for name, c in self._counters.items()},
                'gauges': {name: g.collect() for name, g in self._gauges.items()},
                'histograms': {name: h.collect() for name, h in self._histograms.items()},
                'timers': {name: t.collect() for name, t in self._timers.items()},
                'categories': {cat: list(metrics) for cat, metrics in self._categories.items()}
            }
    
    def reset_all(self) -> None:
        """Reset all metrics"""
        with self._lock:
            for counter in self._counters.values():
                counter.reset()
            for histogram in self._histograms.values():
                histogram.reset()
            for timer in self._timers.values():
                timer.reset()
    
    def get_stats(self) -> Dict[str, int]:
        """Get registry statistics"""
        with self._lock:
            return {
                'counters': len(self._counters),
                'gauges': len(self._gauges),
                'histograms': len(self._histograms),
                'timers': len(self._timers),
                'categories': len(self._categories),
                'total_metrics': len(self._counters) + len(self._gauges) + len(self._histograms) + len(self._timers)
            }


class MetricsCollector:
    """
    Centralized metrics collection and management service.
    
    Features:
    - Multiple metric types: Counters, gauges, histograms, timers
    - Thread-safe concurrent operations
    - Prometheus-compatible export format
    - Statistical analysis (percentiles, averages)
    - Category-based organization
    - Performance optimized for high-frequency operations
    """
    
    def __init__(self, app_name: str = "app"):
        self.app_name = app_name
        self.logger = logging.getLogger(__name__)
        self._registry = MetricsRegistry()
        self._lock = threading.RLock()
        
        # Legacy compatibility
        self._counters: Dict[str, float] = {}
        self._gauges: Dict[str, float] = {}
        self._timers: Dict[str, List[float]] = defaultdict(list)
        self._max_timer_samples = 1000
        
        # Performance tracking
        self._collection_count = 0
        self._last_collection_time = time.time()
    
    # Modern API using registry
    
    def counter(self, name: str, description: str = "", category: str = "default") -> Counter:
        """Get or create counter metric"""
        return self._registry.counter(name, description, category=category)
    
    def gauge(self, name: str, description: str = "", category: str = "default") -> Gauge:
        """Get or create gauge metric"""
        return self._registry.gauge(name, description, category=category)
    
    def histogram(self, name: str, description: str = "", 
                  buckets: Optional[tuple] = None, category: str = "default") -> Histogram:
        """Get or create histogram metric"""
        return self._registry.histogram(name, description, buckets, category=category)
    
    def timer(self, name: str, description: str = "", category: str = "default") -> Timer:
        """Get or create timer metric"""
        return self._registry.timer(name, description, category=category)
    
    # Legacy API for backward compatibility
    
    def increment_counter(self, name: str, value: float = 1.0, **tags) -> None:
        """Increment counter metric (legacy API)"""
        with self._lock:
            self._counters[name] = self._counters.get(name, 0.0) + value
            self._collection_count += 1
    
    def set_gauge(self, name: str, value: float, **tags) -> None:
        """Set gauge metric value (legacy API)"""
        with self._lock:
            self._gauges[name] = value
            self._collection_count += 1
    
    def record_timing(self, name: str, duration: float, **tags) -> None:
        """Record timing metric in seconds (legacy API)"""
        with self._lock:
            self._timers[name].append(duration)
            if len(self._timers[name]) > self._max_timer_samples:
                self._timers[name] = self._timers[name][-self._max_timer_samples:]
            self._collection_count += 1
    
    def record_histogram(self, name: str, value: float, **tags) -> None:
        """Record histogram metric (legacy API)"""
        hist = self._registry.histogram(name)
        hist.observe(value)
        self._collection_count += 1
    
    def get_metric(self, name: str, metric_type: str = "counter") -> Any:
        """Get current value of metric (legacy API)"""
        with self._lock:
            if metric_type == "counter":
                return self._counters.get(name, 0.0)
            elif metric_type == "gauge":
                return self._gauges.get(name, 0.0)
            elif metric_type == "timer":
                timings = self._timers.get(name, [])
                return sum(timings) / len(timings) if timings else 0.0
            return None
    
    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive metrics collection statistics"""
        with self._lock:
            return {
                'app_name': self.app_name,
                'collection_count': self._collection_count,
                'legacy_counters': len(self._counters),
                'legacy_gauges': len(self._gauges),
                'legacy_timers': len(self._timers),
                'registry_stats': self._registry.get_stats(),
                'last_collection_time': self._last_collection_time,
                'timestamp': time.time()
            }
    
    def get_prometheus_metrics(self) -> str:
        """Export metrics in Prometheus format"""
        lines = []
        
        # Registry metrics
        all_metrics = self._registry.collect_all()
        
        # Counters
        for name, data in all_metrics['counters'].items():
            if data.get('description'):
                lines.append(f"# HELP {self.app_name}_{name}_total {data['description']}")
            lines.append(f"# TYPE {self.app_name}_{name}_total counter")
            lines.append(f"{self.app_name}_{name}_total {data['value']}")
            
            for label_key, value in data.get('labeled_values', {}).items():
                lines.append(f"{self.app_name}_{name}_total{{{label_key}}} {value}")
        
        # Gauges
        for name, data in all_metrics['gauges'].items():
            if data.get('description'):
                lines.append(f"# HELP {self.app_name}_{name} {data['description']}")
            lines.append(f"# TYPE {self.app_name}_{name} gauge")
            lines.append(f"{self.app_name}_{name} {data['value']}")
            
            for label_key, value in data.get('labeled_values', {}).items():
                lines.append(f"{self.app_name}_{name}{{{label_key}}} {value}")
        
        # Histograms
        for name, data in all_metrics['histograms'].items():
            if data.get('description'):
                lines.append(f"# HELP {self.app_name}_{name} {data['description']}")
            lines.append(f"# TYPE {self.app_name}_{name} histogram")
            
            for bucket_le, count in data.get('buckets', {}).items():
                lines.append(f"{self.app_name}_{name}_bucket{{le=\"{bucket_le}\"}} {count}")
            
            lines.append(f"{self.app_name}_{name}_sum {data.get('sum', 0)}")
            lines.append(f"{self.app_name}_{name}_count {data.get('count', 0)}")
        
        # Timers (as histograms)
        for name, data in all_metrics['timers'].items():
            if data.get('description'):
                lines.append(f"# HELP {self.app_name}_{name}_seconds {data['description']}")
            lines.append(f"# TYPE {self.app_name}_{name}_seconds histogram")
            
            stats = data.get('stats', {})
            lines.append(f"{self.app_name}_{name}_seconds_sum {stats.get('sum', 0)}")
            lines.append(f"{self.app_name}_{name}_seconds_count {stats.get('count', 0)}")
        
        # Legacy metrics
        with self._lock:
            for name, value in self._counters.items():
                lines.append(f"# TYPE {self.app_name}_legacy_{name}_total counter")
                lines.append(f"{self.app_name}_legacy_{name}_total {value}")
            
            for name, value in self._gauges.items():
                lines.append(f"# TYPE {self.app_name}_legacy_{name} gauge")
                lines.append(f"{self.app_name}_legacy_{name} {value}")
        
        return '\n'.join(lines)
    
    def export_json(self) -> Dict[str, Any]:
        """Export all metrics as JSON"""
        with self._lock:
            legacy = {
                'counters': dict(self._counters),
                'gauges': dict(self._gauges),
                'timers': {name: {'avg': sum(v)/len(v) if v else 0, 'count': len(v)} 
                          for name, v in self._timers.items()}
            }
        
        return {
            'timestamp': time.time(),
            'app_name': self.app_name,
            'registry': self._registry.collect_all(),
            'legacy': legacy,
            'stats': self.get_stats()
        }
    
    def save_json(self, file_path: str) -> bool:
        """Save metrics to JSON file"""
        try:
            with open(file_path, 'w') as f:
                json.dump(self.export_json(), f, indent=2, default=str)
            self.logger.info(f"Metrics exported to {file_path}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to export metrics: {e}")
            return False
    
    def reset_all(self) -> None:
        """Reset all metrics"""
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._timers.clear()
            self._collection_count = 0
        self._registry.reset_all()
        self.logger.info("All metrics reset")
    
    def track_performance(self, category: str, operation: str, duration: float) -> None:
        """Track performance-related metrics"""
        metric_name = f"{category}_{operation}"
        self.record_timing(metric_name, duration)
        self.increment_counter(f"{category}_operations_total")


# Decorator for timing functions
def timed(collector: Optional[MetricsCollector] = None, 
          metric_name: Optional[str] = None,
          category: str = "function"):
    """Decorator to time function execution"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal collector, metric_name
            if collector is None:
                collector = get_metrics_collector()
            if metric_name is None:
                metric_name = f"{category}_{func.__name__}"
            
            start = time.perf_counter()
            try:
                result = func(*args, **kwargs)
                duration = time.perf_counter() - start
                collector.record_timing(metric_name, duration)
                collector.increment_counter(f"{metric_name}_success_total")
                return result
            except Exception as e:
                duration = time.perf_counter() - start
                collector.record_timing(metric_name, duration)
                collector.increment_counter(f"{metric_name}_error_total")
                raise
        
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            nonlocal collector, metric_name
            if collector is None:
                collector = get_metrics_collector()
            if metric_name is None:
                metric_name = f"{category}_{func.__name__}"
            
            start = time.perf_counter()
            try:
                result = await func(*args, **kwargs)
                duration = time.perf_counter() - start
                collector.record_timing(metric_name, duration)
                collector.increment_counter(f"{metric_name}_success_total")
                return result
            except Exception as e:
                duration = time.perf_counter() - start
                collector.record_timing(metric_name, duration)
                collector.increment_counter(f"{metric_name}_error_total")
                raise
        
        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return wrapper
    
    return decorator


# Global metrics collector instance
_global_metrics: Optional[MetricsCollector] = None


def get_metrics_collector() -> MetricsCollector:
    """Get or create global metrics collector instance"""
    global _global_metrics
    if _global_metrics is None:
        _global_metrics = MetricsCollector()
    return _global_metrics


def set_metrics_collector(collector: MetricsCollector) -> None:
    """Set global metrics collector instance"""
    global _global_metrics
    _global_metrics = collector


def create_metrics_collector(config: Optional[Dict[str, Any]] = None) -> MetricsCollector:
    """Factory function for creating metrics collector with DI integration"""
    config = config or {}
    return MetricsCollector(app_name=config.get('app_name', 'app'))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("Metrics Collector v2 Demo")
    print("=" * 50)
    
    # Create collector
    collector = MetricsCollector(app_name="demo")
    set_metrics_collector(collector)
    
    # Modern API usage
    print("\n1. Modern API (Registry-based):")
    
    # Counter
    requests_counter = collector.counter("http_requests", "Total HTTP requests", category="http")
    requests_counter.inc()
    requests_counter.inc(method="GET", path="/api")
    requests_counter.inc(method="POST", path="/api")
    print(f"   Requests total: {requests_counter.get()}")
    
    # Gauge
    memory_gauge = collector.gauge("memory_usage_bytes", "Memory usage in bytes", category="system")
    memory_gauge.set(1024 * 1024 * 500)  # 500MB
    print(f"   Memory usage: {memory_gauge.get() / 1024 / 1024:.0f}MB")
    
    # Histogram
    latency_hist = collector.histogram("request_latency", "Request latency", category="http")
    for i in range(100):
        latency_hist.observe(0.01 + (i % 10) * 0.1)
    print(f"   Latency stats: {latency_hist.get_stats()}")
    
    # Timer
    db_timer = collector.timer("database_query", "Database query time", category="database")
    with db_timer.time():
        time.sleep(0.05)  # Simulate query
    print(f"   DB timer stats: {db_timer.get_stats()}")
    
    # Legacy API usage
    print("\n2. Legacy API (Backward Compatible):")
    collector.increment_counter("legacy_requests")
    collector.set_gauge("legacy_memory", 512.5)
    collector.record_timing("legacy_latency", 0.123)
    print(f"   Legacy requests: {collector.get_metric('legacy_requests')}")
    print(f"   Legacy memory: {collector.get_metric('legacy_memory', 'gauge')}")
    print(f"   Legacy latency: {collector.get_metric('legacy_latency', 'timer'):.3f}s")
    
    # Decorated function
    @timed(collector, "test_function", category="test")
    def slow_function():
        time.sleep(0.1)
        return "done"
    
    print("\n3. Decorated Function:")
    result = slow_function()
    print(f"   Function result: {result}")
    print(f"   Function timing: {collector.get_metric('test_test_function', 'timer'):.3f}s")
    
    # Export formats
    print("\n4. Export Formats:")
    print("   Prometheus format (sample):")
    prometheus = collector.get_prometheus_metrics()
    for line in prometheus.split('\n')[:10]:
        print(f"     {line}")
    print("     ...")
    
    # Stats
    print("\n5. Collector Stats:")
    stats = collector.get_stats()
    for key, value in stats.items():
        print(f"   {key}: {value}")
    
    # Save to file
    collector.save_json("demo_metrics_v2.json")
    print("\n6. Metrics saved to demo_metrics_v2.json")
    
    print("\nDemo complete!")
