"""
Phase 2 Integration Module

Integrates Phase 2 performance optimization modules into the main codebase.
Provides a clean interface for enabling/disabling Phase 2 features.
"""

import logging
import threading
import time
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class Phase2Manager:
    """
    Manages Phase 2 optimization modules integration.
    
    Features:
    - Memory monitoring with backpressure
    - Adaptive timeout management
    - Connection pooling
    - Work stealing for load balancing
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self._initialized = True
        self._enabled = False
        
        # Phase 2 components (lazy loaded)
        self._memory_monitor = None
        self._timeout_manager = None
        self._metrics_collector = None
        
        # Configuration
        self._config: Dict[str, Any] = {}
        
        logger.info("Phase2Manager initialized")
    
    def enable(self, config: Optional[Dict[str, Any]] = None) -> bool:
        """Enable Phase 2 optimizations"""
        if self._enabled:
            return True
        
        self._config = config or {}
        
        try:
            # Initialize memory monitor
            if self._config.get('memory_monitor', True):
                self._init_memory_monitor()
            
            # Initialize timeout manager
            if self._config.get('adaptive_timeout', True):
                self._init_timeout_manager()
            
            # Initialize metrics
            if self._config.get('metrics', True):
                self._init_metrics()
            
            self._enabled = True
            logger.info("Phase 2 optimizations enabled")
            return True
            
        except Exception as e:
            logger.error(f"Failed to enable Phase 2: {e}")
            return False
    
    def disable(self) -> None:
        """Disable Phase 2 optimizations"""
        if not self._enabled:
            return
        
        # Stop memory monitor
        if self._memory_monitor:
            try:
                self._memory_monitor.stop()
            except Exception:
                pass
            self._memory_monitor = None
        
        self._enabled = False
        logger.info("Phase 2 optimizations disabled")
    
    def _init_memory_monitor(self) -> None:
        """Initialize memory monitor"""
        try:
            from memory_monitor import MemoryMonitor
            
            # Convert 0-1 ratio to percentage (0-100)
            warning_pct = self._config.get('memory_warning', 0.75) * 100
            critical_pct = self._config.get('memory_critical', 0.90) * 100
            
            self._memory_monitor = MemoryMonitor(
                check_interval=self._config.get('memory_interval', 5.0),
                gc_threshold=warning_pct
            )
            self._memory_monitor.start()
            logger.debug("Memory monitor initialized")
            
        except ImportError:
            logger.warning("memory_monitor module not available")
        except Exception as e:
            logger.warning(f"Memory monitor init failed: {e}")
    
    def _init_timeout_manager(self) -> None:
        """Initialize adaptive timeout manager"""
        try:
            from adaptive_timeout import AdaptiveTimeoutManager, TimeoutConfig
            
            config = TimeoutConfig(
                connect_timeout=self._config.get('connect_timeout', 10.0),
                read_timeout=self._config.get('read_timeout', 30.0),
                total_timeout=self._config.get('total_timeout', 90.0),
                min_timeout=self._config.get('min_timeout', 5.0),
                max_timeout=self._config.get('max_timeout', 300.0)
            )
            
            self._timeout_manager = AdaptiveTimeoutManager(
                default_config=config,
                min_samples=self._config.get('timeout_samples', 10)
            )
            logger.debug("Adaptive timeout manager initialized")
            
        except ImportError:
            logger.warning("adaptive_timeout module not available")
        except Exception as e:
            logger.warning(f"Timeout manager init failed: {e}")
    
    def _init_metrics(self) -> None:
        """Initialize metrics collector"""
        try:
            from metrics_collector import MetricsCollector
            self._metrics_collector = MetricsCollector()
            logger.debug("Metrics collector initialized")
        except ImportError:
            logger.warning("metrics_collector module not available")
        except Exception as e:
            logger.warning(f"Metrics init failed: {e}")
    
    @property
    def enabled(self) -> bool:
        return self._enabled
    
    @property
    def memory_monitor(self):
        return self._memory_monitor
    
    @property
    def timeout_manager(self):
        return self._timeout_manager
    
    @property
    def metrics(self):
        return self._metrics_collector
    
    def check_memory_pressure(self) -> bool:
        """Check if system is under memory pressure"""
        if not self._memory_monitor:
            return False
        
        try:
            usage = self._memory_monitor.get_usage_percentage()
            return usage > 75.0  # Warning threshold
        except Exception:
            return False
    
    def should_apply_backpressure(self) -> bool:
        """Check if backpressure should be applied"""
        if not self._memory_monitor:
            return False
        
        try:
            usage = self._memory_monitor.get_usage_percentage()
            return usage > 85.0  # High threshold
        except Exception:
            return False
    
    def get_adaptive_timeout(self, domain: str) -> float:
        """Get adaptive timeout for domain"""
        if not self._timeout_manager:
            return 90.0  # Default
        
        try:
            timeouts = self._timeout_manager.get_timeouts(domain)
            return timeouts.get('total', 90.0)
        except Exception:
            return 90.0
    
    def record_request(self, domain: str, duration: float, 
                       success: bool, timeout: bool = False) -> None:
        """Record request for adaptive learning"""
        if not self._timeout_manager:
            return
        
        try:
            self._timeout_manager.record_request(domain, duration, success, timeout)
        except Exception:
            pass
    
    def record_metric(self, name: str, value: float, metric_type: str = 'counter') -> None:
        """Record metric"""
        if not self._metrics_collector:
            return
        
        try:
            if metric_type == 'counter':
                self._metrics_collector.increment_counter(name, value)
            elif metric_type == 'gauge':
                self._metrics_collector.set_gauge(name, value)
            elif metric_type == 'timing':
                self._metrics_collector.record_timing(name, value)
        except Exception:
            pass
    
    def get_stats(self) -> Dict[str, Any]:
        """Get Phase 2 statistics"""
        stats = {
            'enabled': self._enabled,
            'memory_monitor': None,
            'timeout_manager': None,
            'metrics': None
        }
        
        if self._memory_monitor:
            try:
                stats['memory_monitor'] = self._memory_monitor.get_memory_status()
            except Exception:
                pass
        
        if self._timeout_manager:
            try:
                stats['timeout_manager'] = self._timeout_manager.get_global_stats()
            except Exception:
                pass
        
        if self._metrics_collector:
            try:
                stats['metrics'] = self._metrics_collector.get_stats()
            except Exception:
                pass
        
        return stats


# Global instance
_phase2_manager: Optional[Phase2Manager] = None


def get_phase2_manager() -> Phase2Manager:
    """Get or create Phase2Manager instance"""
    global _phase2_manager
    if _phase2_manager is None:
        _phase2_manager = Phase2Manager()
    return _phase2_manager


def enable_phase2(config: Optional[Dict[str, Any]] = None) -> bool:
    """Enable Phase 2 optimizations"""
    return get_phase2_manager().enable(config)


def disable_phase2() -> None:
    """Disable Phase 2 optimizations"""
    get_phase2_manager().disable()


def is_phase2_enabled() -> bool:
    """Check if Phase 2 is enabled"""
    return get_phase2_manager().enabled


# Convenience functions
def check_memory() -> bool:
    """Check memory pressure"""
    return get_phase2_manager().check_memory_pressure()


def should_throttle() -> bool:
    """Check if throttling needed"""
    return get_phase2_manager().should_apply_backpressure()


def get_timeout(domain: str) -> float:
    """Get adaptive timeout for domain"""
    return get_phase2_manager().get_adaptive_timeout(domain)


def record_request(domain: str, duration: float, success: bool, timeout: bool = False) -> None:
    """Record request for learning"""
    get_phase2_manager().record_request(domain, duration, success, timeout)


def record_metric(name: str, value: float, metric_type: str = 'counter') -> None:
    """Record a metric"""
    get_phase2_manager().record_metric(name, value, metric_type)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("Phase 2 Integration Test")
    print("=" * 60)
    
    # Enable Phase 2
    print("\n1. Enabling Phase 2...")
    success = enable_phase2({
        'memory_monitor': True,
        'adaptive_timeout': True,
        'metrics': True,
        'memory_warning': 0.75,
        'memory_critical': 0.90
    })
    print(f"   Enabled: {success}")
    
    # Check status
    print("\n2. Phase 2 Status:")
    manager = get_phase2_manager()
    stats = manager.get_stats()
    for key, value in stats.items():
        print(f"   {key}: {value}")
    
    # Test memory check
    print("\n3. Memory Check:")
    print(f"   Under pressure: {check_memory()}")
    print(f"   Should throttle: {should_throttle()}")
    
    # Test timeout
    print("\n4. Adaptive Timeout:")
    print(f"   example.com: {get_timeout('example.com')}s")
    
    # Simulate requests
    print("\n5. Simulating requests...")
    for i in range(5):
        record_request('test.com', 0.5 + i * 0.1, True)
    print(f"   Recorded 5 requests")
    print(f"   New timeout for test.com: {get_timeout('test.com')}s")
    
    # Record metrics
    print("\n6. Recording metrics...")
    record_metric('urls_processed', 100)
    record_metric('success_rate', 0.95, 'gauge')
    print("   Metrics recorded")
    
    # Final stats
    print("\n7. Final Statistics:")
    stats = manager.get_stats()
    if stats.get('metrics'):
        print(f"   Metrics: {stats['metrics']}")
    
    # Disable
    print("\n8. Disabling Phase 2...")
    disable_phase2()
    print(f"   Enabled: {is_phase2_enabled()}")
    
    print("\nTest complete!")
