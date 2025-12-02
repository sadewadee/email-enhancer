"""
Adaptive Timeout Manager - Dynamic timeout adjustment based on performance

Implements adaptive timeout management:
- Success rate tracking per domain
- Dynamic timeout adjustment
- Percentile-based calculations
- Timeout bounds enforcement
- Historical trend analysis
"""

import logging
import statistics
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


class TimeoutCategory(Enum):
    """Timeout categories"""
    CONNECT = "connect"
    READ = "read"
    WRITE = "write"
    TOTAL = "total"


@dataclass
class TimeoutMetrics:
    """Timeout metrics for a domain"""
    samples: deque = field(default_factory=lambda: deque(maxlen=100))
    success_count: int = 0
    failure_count: int = 0
    timeout_count: int = 0
    last_updated: float = field(default_factory=time.time)
    
    @property
    def total_requests(self) -> int:
        return self.success_count + self.failure_count + self.timeout_count
    
    @property
    def success_rate(self) -> float:
        if self.total_requests == 0:
            return 100.0
        return self.success_count / self.total_requests * 100
    
    @property
    def timeout_rate(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return self.timeout_count / self.total_requests * 100
    
    def add_sample(self, duration: float, success: bool, timeout: bool = False) -> None:
        """Add request sample"""
        self.samples.append(duration)
        if timeout:
            self.timeout_count += 1
        elif success:
            self.success_count += 1
        else:
            self.failure_count += 1
        self.last_updated = time.time()
    
    def get_percentile(self, percentile: float) -> float:
        """Get percentile of response times"""
        if not self.samples:
            return 0.0
        sorted_samples = sorted(self.samples)
        index = int(len(sorted_samples) * percentile / 100)
        return sorted_samples[min(index, len(sorted_samples) - 1)]
    
    def get_stats(self) -> Dict[str, Any]:
        """Get timeout statistics"""
        if not self.samples:
            return {
                'samples': 0,
                'success_rate': 0,
                'timeout_rate': 0
            }
        
        return {
            'samples': len(self.samples),
            'success_rate': f"{self.success_rate:.1f}%",
            'timeout_rate': f"{self.timeout_rate:.1f}%",
            'min': f"{min(self.samples):.2f}s",
            'max': f"{max(self.samples):.2f}s",
            'avg': f"{statistics.mean(self.samples):.2f}s",
            'median': f"{statistics.median(self.samples):.2f}s",
            'p90': f"{self.get_percentile(90):.2f}s",
            'p95': f"{self.get_percentile(95):.2f}s",
            'p99': f"{self.get_percentile(99):.2f}s"
        }


@dataclass
class TimeoutConfig:
    """Timeout configuration"""
    connect_timeout: float = 10.0
    read_timeout: float = 30.0
    write_timeout: float = 30.0
    total_timeout: float = 60.0
    
    # Bounds
    min_timeout: float = 5.0
    max_timeout: float = 300.0
    
    # Adaptation parameters
    adaptation_rate: float = 0.1  # How quickly to adjust
    target_success_rate: float = 90.0
    percentile_target: float = 95.0  # Use p95 for timeout
    
    def to_dict(self) -> Dict[str, float]:
        return {
            'connect': self.connect_timeout,
            'read': self.read_timeout,
            'write': self.write_timeout,
            'total': self.total_timeout
        }


class AdaptiveTimeoutManager:
    """
    Adaptive timeout manager that adjusts timeouts based on performance.
    
    Features:
    - Per-domain timeout tracking
    - Dynamic adjustment based on success rates
    - Percentile-based timeout calculation
    - Timeout bounds enforcement
    - Trend analysis for proactive adjustment
    """
    
    def __init__(self,
                 default_config: Optional[TimeoutConfig] = None,
                 min_samples: int = 10,
                 adaptation_interval: float = 60.0):
        
        self.default_config = default_config or TimeoutConfig()
        self.min_samples = min_samples
        self.adaptation_interval = adaptation_interval
        
        self.logger = logging.getLogger(__name__)
        
        # Per-domain metrics and configs
        self._metrics: Dict[str, TimeoutMetrics] = defaultdict(TimeoutMetrics)
        self._configs: Dict[str, TimeoutConfig] = {}
        
        # Global metrics
        self._global_metrics = TimeoutMetrics()
        
        # Threading
        self._lock = threading.RLock()
        
        # Adaptation history
        self._adaptation_history: Dict[str, List[Tuple[float, float]]] = defaultdict(list)
    
    def get_timeouts(self, domain: str) -> Dict[str, float]:
        """Get timeout configuration for domain"""
        with self._lock:
            config = self._configs.get(domain, self.default_config)
            return config.to_dict()
    
    def get_timeout(self, domain: str, category: TimeoutCategory) -> float:
        """Get specific timeout for domain"""
        with self._lock:
            config = self._configs.get(domain, self.default_config)
            
            if category == TimeoutCategory.CONNECT:
                return config.connect_timeout
            elif category == TimeoutCategory.READ:
                return config.read_timeout
            elif category == TimeoutCategory.WRITE:
                return config.write_timeout
            else:
                return config.total_timeout
    
    def record_request(self, domain: str, duration: float, 
                       success: bool, timeout: bool = False) -> None:
        """Record request result for learning"""
        with self._lock:
            # Update domain metrics
            self._metrics[domain].add_sample(duration, success, timeout)
            
            # Update global metrics
            self._global_metrics.add_sample(duration, success, timeout)
            
            # Check if adaptation needed
            metrics = self._metrics[domain]
            if metrics.total_requests >= self.min_samples:
                self._adapt_timeout(domain)
    
    def _adapt_timeout(self, domain: str) -> None:
        """Adapt timeout based on metrics"""
        metrics = self._metrics[domain]
        
        if domain not in self._configs:
            self._configs[domain] = TimeoutConfig(
                connect_timeout=self.default_config.connect_timeout,
                read_timeout=self.default_config.read_timeout,
                write_timeout=self.default_config.write_timeout,
                total_timeout=self.default_config.total_timeout,
                adaptation_rate=self.default_config.adaptation_rate,
                target_success_rate=self.default_config.target_success_rate
            )
        
        config = self._configs[domain]
        
        # Calculate optimal timeout based on percentile
        p95 = metrics.get_percentile(config.percentile_target)
        
        if p95 > 0:
            # Set timeout to 1.5x the p95 latency
            optimal_total = p95 * 1.5
            
            # Apply adaptation rate (smooth transition)
            current = config.total_timeout
            new_timeout = current + (optimal_total - current) * config.adaptation_rate
            
            # Enforce bounds
            new_timeout = max(config.min_timeout, min(config.max_timeout, new_timeout))
            
            # Adjust based on success rate
            if metrics.success_rate < config.target_success_rate:
                # Increase timeout if success rate is low
                new_timeout *= 1.2
            elif metrics.success_rate > 98:
                # Can decrease timeout if very successful
                new_timeout *= 0.9
            
            # Enforce bounds again
            new_timeout = max(config.min_timeout, min(config.max_timeout, new_timeout))
            
            # Update config
            old_timeout = config.total_timeout
            config.total_timeout = new_timeout
            
            # Update other timeouts proportionally
            ratio = new_timeout / old_timeout if old_timeout > 0 else 1.0
            config.connect_timeout = max(5, min(60, config.connect_timeout * ratio))
            config.read_timeout = max(10, min(120, config.read_timeout * ratio))
            
            # Record adaptation
            self._adaptation_history[domain].append((time.time(), new_timeout))
            
            # Keep history limited
            if len(self._adaptation_history[domain]) > 100:
                self._adaptation_history[domain] = self._adaptation_history[domain][-100:]
            
            self.logger.debug(
                f"Adapted timeout for {domain}: {old_timeout:.1f}s -> {new_timeout:.1f}s "
                f"(success rate: {metrics.success_rate:.1f}%)"
            )
    
    def set_timeout(self, domain: str, category: TimeoutCategory, value: float) -> None:
        """Manually set timeout for domain"""
        with self._lock:
            if domain not in self._configs:
                self._configs[domain] = TimeoutConfig()
            
            config = self._configs[domain]
            value = max(config.min_timeout, min(config.max_timeout, value))
            
            if category == TimeoutCategory.CONNECT:
                config.connect_timeout = value
            elif category == TimeoutCategory.READ:
                config.read_timeout = value
            elif category == TimeoutCategory.WRITE:
                config.write_timeout = value
            else:
                config.total_timeout = value
    
    def set_bounds(self, domain: str, min_timeout: float, max_timeout: float) -> None:
        """Set timeout bounds for domain"""
        with self._lock:
            if domain not in self._configs:
                self._configs[domain] = TimeoutConfig()
            
            self._configs[domain].min_timeout = min_timeout
            self._configs[domain].max_timeout = max_timeout
    
    def get_domain_stats(self, domain: str) -> Dict[str, Any]:
        """Get statistics for domain"""
        with self._lock:
            metrics = self._metrics.get(domain)
            config = self._configs.get(domain, self.default_config)
            
            return {
                'domain': domain,
                'current_timeouts': config.to_dict(),
                'metrics': metrics.get_stats() if metrics else None,
                'adaptations': len(self._adaptation_history.get(domain, []))
            }
    
    def get_global_stats(self) -> Dict[str, Any]:
        """Get global statistics"""
        with self._lock:
            return {
                'domains_tracked': len(self._metrics),
                'total_requests': self._global_metrics.total_requests,
                'global_success_rate': f"{self._global_metrics.success_rate:.1f}%",
                'global_timeout_rate': f"{self._global_metrics.timeout_rate:.1f}%",
                'global_metrics': self._global_metrics.get_stats()
            }
    
    def get_all_domains(self) -> List[str]:
        """Get all tracked domains"""
        with self._lock:
            return list(self._metrics.keys())
    
    def reset_domain(self, domain: str) -> None:
        """Reset metrics and config for domain"""
        with self._lock:
            if domain in self._metrics:
                del self._metrics[domain]
            if domain in self._configs:
                del self._configs[domain]
            if domain in self._adaptation_history:
                del self._adaptation_history[domain]
    
    def get_recommended_timeout(self, domain: str) -> float:
        """Get recommended timeout based on current metrics"""
        with self._lock:
            metrics = self._metrics.get(domain)
            config = self._configs.get(domain, self.default_config)
            
            if not metrics or len(metrics.samples) < self.min_samples:
                return config.total_timeout
            
            # Use p95 * 1.5 as recommendation
            p95 = metrics.get_percentile(95)
            recommended = max(config.min_timeout, min(config.max_timeout, p95 * 1.5))
            
            return recommended


class TimeoutContext:
    """Context manager for tracking request timeouts"""
    
    def __init__(self, manager: AdaptiveTimeoutManager, domain: str):
        self.manager = manager
        self.domain = domain
        self.start_time: float = 0
        self._success = True
        self._timeout = False
    
    def __enter__(self) -> 'TimeoutContext':
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        duration = time.time() - self.start_time
        
        # Check if it was a timeout
        if exc_type is not None:
            if 'timeout' in str(exc_type).lower() or 'timeout' in str(exc_val).lower():
                self._timeout = True
            self._success = False
        
        self.manager.record_request(
            self.domain,
            duration,
            success=self._success,
            timeout=self._timeout
        )
    
    def mark_success(self) -> None:
        """Explicitly mark as success"""
        self._success = True
    
    def mark_failure(self, is_timeout: bool = False) -> None:
        """Explicitly mark as failure"""
        self._success = False
        self._timeout = is_timeout


# Global timeout manager instance
_global_timeout_manager: Optional[AdaptiveTimeoutManager] = None


def get_timeout_manager() -> AdaptiveTimeoutManager:
    """Get or create global timeout manager"""
    global _global_timeout_manager
    if _global_timeout_manager is None:
        _global_timeout_manager = AdaptiveTimeoutManager()
    return _global_timeout_manager


def set_timeout_manager(manager: AdaptiveTimeoutManager) -> None:
    """Set global timeout manager"""
    global _global_timeout_manager
    _global_timeout_manager = manager


def create_timeout_manager(config: Optional[Dict[str, Any]] = None) -> AdaptiveTimeoutManager:
    """Factory function for timeout manager"""
    config = config or {}
    
    default_config = TimeoutConfig(
        connect_timeout=config.get('connect_timeout', 10.0),
        read_timeout=config.get('read_timeout', 30.0),
        total_timeout=config.get('total_timeout', 60.0),
        min_timeout=config.get('min_timeout', 5.0),
        max_timeout=config.get('max_timeout', 300.0)
    )
    
    return AdaptiveTimeoutManager(
        default_config=default_config,
        min_samples=config.get('min_samples', 10),
        adaptation_interval=config.get('adaptation_interval', 60.0)
    )


if __name__ == "__main__":
    import random
    
    logging.basicConfig(level=logging.INFO)
    
    print("Adaptive Timeout Manager Demo")
    print("=" * 60)
    
    # Create manager
    manager = AdaptiveTimeoutManager(min_samples=5)
    
    # Simulate requests for different domains
    print("\n1. Simulating requests...")
    
    domains = {
        'fast-site.com': (0.1, 0.5, 0.98),      # Fast, high success
        'slow-site.com': (2.0, 5.0, 0.90),       # Slow, good success
        'unstable-site.com': (0.5, 10.0, 0.60),  # Variable, low success
    }
    
    for domain, (min_time, max_time, success_rate) in domains.items():
        for i in range(20):
            duration = random.uniform(min_time, max_time)
            success = random.random() < success_rate
            timeout = not success and random.random() < 0.3
            
            manager.record_request(domain, duration, success, timeout)
    
    # Show results
    print("\n2. Domain Statistics:")
    
    for domain in domains.keys():
        stats = manager.get_domain_stats(domain)
        print(f"\n   {domain}:")
        print(f"     Timeouts: {stats['current_timeouts']}")
        if stats['metrics']:
            print(f"     Success Rate: {stats['metrics']['success_rate']}")
            print(f"     P95 Latency: {stats['metrics']['p95']}")
            print(f"     Adaptations: {stats['adaptations']}")
    
    print("\n3. Recommended Timeouts:")
    for domain in domains.keys():
        recommended = manager.get_recommended_timeout(domain)
        current = manager.get_timeout(domain, TimeoutCategory.TOTAL)
        print(f"   {domain}: current={current:.1f}s, recommended={recommended:.1f}s")
    
    print("\n4. Global Statistics:")
    global_stats = manager.get_global_stats()
    for key, value in global_stats.items():
        if key != 'global_metrics':
            print(f"   {key}: {value}")
    
    print("\n5. Using TimeoutContext:")
    
    with TimeoutContext(manager, "test-domain.com") as ctx:
        time.sleep(0.1)  # Simulate request
        ctx.mark_success()
    
    stats = manager.get_domain_stats("test-domain.com")
    print(f"   test-domain.com recorded: {stats['metrics']}")
    
    print("\nDemo complete!")
