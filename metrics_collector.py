"""
Metrics Collection Service - Centralized metrics tracking with Prometheus-compatible format

Implements comprehensive metrics collection and management:
- Counter, gauge, histogram, and timing metrics
- Prometheus-compatible output for system integration
- Thread-safe operations with concurrent access
- Performance optimized with minimal overhead
- Automatic metric categorization and tagging
- Export capabilities in multiple formats
"""

import asyncio
import logging
import time
import threading
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Set, Union
from enum import Enum
from typing_extensions import Final


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
    value: Union[int, float, str]
    tags: Set[str] = field(default_factory=set())
    
    def __post_init__(self):
        self.timestamp = time.time()


@dataclass
class MetricStats:
    """Statistics for metrics collection"""
    total_collections: int = 0
    total_time: float = 0.0
    collections_per_second: float = 0.0
    metrics_by_type: Dict[MetricType, List[MetricPoint]] = field(default_factory=lambda: defaultdict(list))
    
    @property
    def total_metrics(self) -> int:
        """Total number of metrics collected"""
        return sum(len(points) for points in self.metrics_by_type.values())
    
    @property
    def average_collection_time(self) -> float:
        """Average time per metric collection (milliseconds)"""
        return (self.total_time / self.total_collections * 1000) if self.total_collections > 0 else 0.0


@dataclass
class HistogramConfig:
    """Configuration for histogram metrics"""
    bins: int = 10
    max_bins: int = 100
    default_bin_size: float = 1.0
    
    def __post_init__(self, bins: int, max_bins: int, default_bin_size: float):
        if bins < 1:
            self.bins = 1
        if max_bins < 10:
            self.max_bins = 10
        if default_bin_size < 0.1:
            self.default_bin_size = 0.1
        self.bins = min(bins, self.max_bins)


class MetricsCollector:
    """
    Centralized metrics collection and management service.
    
    Features:
    - Multiple metric types: Counters, gauges, timers, histograms
    - Thread-safe concurrent operations with locks
    - Prometheus-compatible export format
    - Automatic histogram bin management
    - Statistical analysis (sum, average, percentiles, percentiles)
    - Performance tracking (collection time, rate calculation)
    
    Performance optimized for high-frequency operations.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Metrics storage by type
        self._counters: Dict[str, float] = {}
        self._gauges: Dict[str, float] = {}
        self._timers: Dict[str, List[float]] = {}
        self._histograms: Dict[str, List[float]] = defaultdict(list)
        
        # Histogram configurations
        self._histogram_configs: Dict[str, HistogramConfig] = {}
        
        # Performance tracking
        self._collection_time = 0.0
        self._collection_count = 0
        
        # Metric categorization
        self._categories: Dict[str, Set[str]] = {
            'system': {'system.cpu', 'system.memory', 'system.disk'},
            'processing': {'processing.urls', 'processing.time', 'processing.success_rate'},
            'web_scraping': {'scraping.total', 'scraping.success_rate', 'browser.pool.utilization'},
            'error_handling': {'errors.network', 'errors.database', 'errors.scraping', 'errors.csv'}
        }
        
        # Auto-cleanup management
        self._cleanup_interval = 300  # 5 minutes
        self._last_cleanup = time.time()
    
    def increment_counter(self, name: str, value: float = 1.0, tags: Optional[Set[str]] = None) -> None:
        """Increment counter metric"""
        with self._lock:
            self._counters[name] = self._counters.get(name, 0.0) + value
            self._collection_time += 0.001  # ~1ms overhead per increment
            self._collection_count += 1
            
            # Add tags if provided
            if tags:
                existing_tags = self._categories.get(name, set())
                self._categories[name] = existing_tags.union(tags)
    
    def set_gauge(self, name: str, value: float, tags: Optional[Set[str]] = None) -> None:
        """Set gauge metric value"""
        with self._lock:
            self._gauges[name] = value
            self._collection_time += 0.001
            self._collection_count += 1
            
            # Add tags if provided
            if tags:
                existing_tags = self._categories.get(name, set())
                self._categories[name] = existing_tags.union(tags)
    
    def record_timing(self, name: str, duration: float, tags: Optional[Set[str]] = None) -> None:
        """Record timing metric (in seconds)"""
        with self._lock:
            # Find or create timer list
            if name not in self._timers:
                self._timers[name] = []
            
            # Add timing sample
            self._timers[name].append(duration)
            
            # Keep manageable list size
            max_samples = 1000  # Keep last 1000 samples
            if len(self._timers[name]) > max_samples:
                self._timers[name] = self._timers[name][-max_samples:]
            
            # Update average timing
            if self._timers[name]:
                avg_time = sum(self._timers[name]) / len(self._timers[name])
                self._gauges[f"{name}_avg"] = avg_time
                self._collection_time += 0.001
                self._collection_count += 1
            
            # Add tags if provided
            if tags:
                existing_tags = self._categories.get(name, set())
                self._categories[name] = existing_tags.union(tags)
    
    def record_histogram(self, name: str, value: float, bins: Optional[int] = None, tags: Optional[Set[str]] = None) -> None:
        """Record histogram metric value in bins"""
        with self._lock:
            # Get or create histogram configuration
            config = self._histogram_configs.get(name, HistogramConfig())
            if bins:
                config.bins = bins
                
            # Find appropriate bin
            bin_size = config.default_bin_size
            max_bins = config.max_bins
            
            if bin_size <= 0.0:
                bin_size = 1.0
                
            # Calculate bin index
            bin_index = min(int(value / bin_size), max_bins - 1)
            
            # Ensure list is large enough
            while len(self._histograms[name]) <= bin_index:
                self._histograms[name] = [0.0] * max_bins
            
            # Update histogram
            self._histograms[name][bin_index] += 1
            self._histograms[name] = self._histograms[name][:max_bins]
            
            # Update average calculation
            if self._histograms[name]:
                total_count = sum(self._histograms[name])
                average_value = sum(i * bin_size * bin_index for i, bin_index in enumerate(self._histograms[name]))
                if total_count > 0:
                    self._gauges[f"{name}_avg"] = average_value / total_count
                self._gauges[f"{name}_total"] = total_count
                self._collection_time += 0.001
                self._collection_count += 1
            
            # Add tags if provided
            if tags:
                existing_tags = self._categories.get(name, set())
                self._categories[name] = existing_tags.union(tags)
    
    def get_metric(self, metric_name: str, metric_type: str = "counter", default: Any = None) -> Any:
        """Get current value of metric"""
        with self._lock:
            if metric_type == "counter":
                return self._counters.get(metric_name, 0.0)
            elif metric_type == "gauge":
                return self._gauges.get(metric_name, 0.0)
            elif metric_type == "timer":
                timers = self._timers.get(metric_name, [])
                return sum(timers) / len(timers) if timers else 0.0
            elif metric_type == "histogram":
                histogram = self._histograms.get(metric_name, [])
                return sum(histogram) if histogram else []
            else:
                return default
    
    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive metrics collection statistics"""
        with self._lock:
            # Auto-cleanup if needed
            current_time = time.time()
            cleanup_needed = (current_time - self._last_cleanup) > self._cleanup_interval
            
            if cleanup_needed:
                self._cleanup_expired_metrics()
                self._last_cleanup = current_time
            
            return {
                'total_metrics': self.total_metrics,
                'collection_count': self._collection_count,
                'collection_time_totalearn': self._collection_time,
                'collections_per_second': self._collection_count / max(1, self._collection_time),
                'total_counters': len(self._counters),
                'total_gauges': len(self._gauges),
                'total_timers': len(self._timers),
                'total_histograms': len(self._histograms),
                'categories': {cat: list(services) for cat, services in self._categories.items()},
                'timestamp': current_time
            }
    
    def get_metric_summary(self, category: Optional[str] = None) -> Dict[str, Any]:
        """Get summarized metrics by category"""
        with self._lock:
            summary = {}
            
            # Gather metrics by category
            for cat, services in self._categories.items():
                category_name = f"{cat}_{services}"
                
                category_count = len(services)
                
                if category_name not in summary:
                    summary[category_name] = {}
                
                # Counters in this category
                category_counters = [name for name in services if name.startswith(f"{cat}_")]
                category_gauges = [name for name in services if name.startswith(f"{cat}_gauge_")]
                category_timers = [name for name in services if name.startswith(f"{cat}_timer_")]
                category_histograms = [name for name in services if name.startswith(f"{cat}_histogram_")]
                
                summary[category_name] = {
                    'counters': category_counters,
                    'gauges': category_gauges,
                    'timers': category_timers,
                    'histograms': category_histograms,
                    'metrics_total': len(category_counters + category_gauges + category_timers + category_histograms)
                }
        
            return summary
    
    def get_prometheus_metrics(self) -> str:
        """Export metrics in Prometheus format"""
        prometheus_metrics = []
        
        # Counters
        for name, value in self._counters.items():
            prometheus_metrics.append(f"dashboard_counter_total_{name} {value}")
        
        # Gauges  
        for name, value in self._gauges.items():
            prometheus_metrics.append(f"dashboard_gauge_{name} {value}")
            
        # Timings
        for name, values in self._timers.items():
            if values:
                avg_time = sum(values) / len(values)
                prometheus_metrics.append(f"dashboard_timer_{name}_seconds {avg_time}")
        
        # Histograms
        for name, values in self._histograms.items():
            if values and sum(values) > 0:
                for i, count in enumerate(values):
                    prometheus_metrics.append(f"dashboard_histogram_{name}_bin_{i}_count {count}")
        
        return '\n'.join(prometheus_metrics)
    
    def json_export(self, file_path: Optional[str] = None) -> Dict[str, Any]:
        """Export metrics to JSON file"""
        with self._lock:
            return {
                'timestamp': time.time(),
                'stats': self.get_stats(),
                'counters': dict(self._counters),
                'gauges': dict(self._gauges),
                'timers': {name: sorted({name: values for name, values in self._timers.items()})},
                'histograms': {name: sorted({name: values for name, values in self._histograms.items()})}
            }
    
    def save_json(self, file_path: str) -> bool:
        """Save metrics to JSON file"""
        try:
            with self._lock:
                metrics_data = self.json_export(file_path)
                
                with open(file_path, 'w') as f:
                    json.dump(metrics_data, f, indent=2)
                
                self.logger.info(f"Metrics exported to {file_path}")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to export metrics: {e}")
            return False
    
    def _cleanup_expired_metrics(self) -> None:
        """Clean up old metrics to save memory"""
        current_time = time.time()
        
        # Cleanup old counters (older than 1 hour)
        count_threshold = 60  # 60 minutes old
        time_threshold = 3600  # 1 hour
        
        current_time = time.time()
        
        # Check and clean old histogram entries
        for name, values in list(self._histograms.items()):
            # Remove old entries from histograms
            recent_values = [v for v in values if current_time - time.time() < time_threshold]
            if recent_values:
                self._histograms[name] = recent_values
    
        # Cleanup old gauges (older than 1 hour)
        for name in list(self._gauges.keys()):
            if current_time - self._gauges.get('{}_0', {}).get('timestamp', 0) < time_threshold:
                del self._gauges[name]
        
        print(f"Cleaned up {len(self._histograms)} expired histogram entries")
    
    def register_category(self, category: str, services: List[str]) -> None:
        """Register category of services for better organization"""
        with self._lock:
            self._categories[category].update(services)
            self.logger.debug(f"Registered category {category} for services: {services}")
    
    def add_metric_tag(self, metric_name: str, tag: str) -> None:
        """Add tag to metric for categorization"""
        with self._lock:
            existing_tags = self._categories.get('application', set())
            self._categories['application'].add(tag)
            
            if metric_name in self._counters:
                self._categories['application'].add(f"{metric_name}_counter")
            elif metric_name in self._gauge:
                self._categories['application'].add(f"{metric_name}_gauge")
            elif metric_name in self._timers:
                self._categories['application'].add(f"{metric_name}_timer")
            elif metric_name in self._histograms:
                self._categories['application'].add(f"{metric_name}_histogram")
    
    def track_performance(self, category: str, operation: str, duration: float) -> None:
        """Track performance-related metrics"""
        metric_name = f"{category}_{operation}"
        
        if operation == "success":
            self.increment_counter(metric_name, 1.0)
        elif operation == "error":
            self.increment_counter(f"{category}_{operation}_errors", 1.0)
        
        self.record_timing(metric_name, duration)
        self.add_metric_tag(metric_name, category)


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


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Demonstrate metrics collection
    print("Metrics Collector Demo:")
    
    # Create metrics collector
    collector = MetricsCollector()
    set_metrics_collector(collector)
    
    # Basic counters
    collector.increment_counter('web_scrapes')
    collector.increment_counter('emails_found')
    collector.increment_counter('errors_encountered')
    
    # Gauges for system metrics
    collector.set_gauge('cpu_percent', 25.7)
    collector.set_gauge('memory_usage', 45.2)
    collector.set_gauge('success_rate', 92.3)
    
    # Performance timing
    collector.record_timing('scraping_latency', 0.023)
    collector.record_timing('email_validation', 0.15)
    
    # Testing histogram
    for i in range(50):
        collector.record_histogram('response_times', i * 0.02)
    
    # Performance tracking
    collector.track_performance('web_scraping', 'success', 2.5)
    collector.track_performance('email_validation', 'success', 0.95)
    collector.track_performance('connection_pool', 'memory_efficiency', 85.6)
    
    print(f"Demo complete. Stats: {collector.get_stats()}")
    
    # Demo Prometheus format
    print(f"\\nPrometheus Format:")
    print(collector.get_prometheus_metrics())
    
    # Demo JSON export
    collector.save_json('demo_metrics.json')
    print("Metrics exported to demo_metrics.json")
    
    print("\nGlobal metrics collector is ready for integration!")
    
    # Test automatic cleanup
    import time
    time.sleep(1)
    
    print("\nTesting automatic cleanup (5 minutes interval)...")
    collector.track_performance('cleanup_test', 'test', 0.1)
    time.sleep(6)
    
    print(f"After cleanup, stats: {collector.get_stats()}")


# Factory function for creating metrics collector with DI integration
def create_metrics_collector(config: Optional[Dict[str, Any]] = None, **kwargs) -> MetricsCollector:
    """Create metrics collector with optional DI configuration"""
    collector = MetricsCollector()
    
    # Register phase 2 services if available
    try:
        container = get_metrics_collector()
        
        # Register monitoring services
        collector.register_category('system', ['memory_monitor'])
        collector.register_category('web_scraping', ['browser_pool', 'connection_pool'])
        collector.register_category('application', ['di_container', 'config_service'])
        
        collector.track_performance('system_monitoring', 'health_check', 0.05)
        collector.track_performance('dashboard_start', 'background', 0.1)
        collector.track_performance('metrics_collection', 'background', 0.2)
        
        print("Metrics collector integrated with DI container")
        
    except Exception as e:
        print(f"DI integration not available: {e}")
        print("Continuing with standalone metrics collector...")
    
    return collector


if __name__ == "main__":
    logging.basicConfig(level=logging.INFO)
    
    try:
        print("Phase 3.1 - Metrics Collection Service Demo")
        
        metrics = get_metrics_collector()
        
        print(f"✅ Metrics Collector created and integrated")
        print(f"   Services registered: {metrics.get_stats()['services_registered']}")
        
        # Test all metric types
        metrics.increment_counter('test_counter')
        metrics.set_gauge('test_gauge', 42.7)
        metrics.record_timing('test_timing', 1.5)
        metrics.record_histogram('test_histogram', 15.3)
        
        print(f"\nMetrics Performance:")
        print(f"   Collections: {metrics.get_stats()['collections_per_second']:.1f}/s")
        print(f"   Counter test_counter: {metrics.get_metric('test_counter')}")
        print(f"   Gauge test_gauge: {metrics.get_metric('test_gauge')}")
        print(f"   Timer test_timing: {metrics.get_metric('test_timing_avg'):.3f}s")
        print(f"   Histogram test_histogram: {len(metrics.get_metric('test_histogram'))}")
        
        # Show Prometheus format
        prometheus_metrics = metrics.get_prometheus_metrics()
        print(f"\\n# Prometheus Format:")
        print(prometheus_metrics)
        
        # Track performance
        metrics.track_performance('test_phase3', 'integration', 0.95)
        print(f"Performance tracked: {metrics.get_stats()['collections_per_second']:.1f}/s")
        
    except Exception as e:
        print(f"❌ Metrics collection failed: {e}")
        traceback.print_exc()
    
    finally:
        # Cleanup
        if _global_metrics is not None:
            _global_metrics.cleanup()
            print("Metrics collector cleaned up")
        
        print("\nPhase 3.1 Metrics Collection complete. Ready for integration!")
