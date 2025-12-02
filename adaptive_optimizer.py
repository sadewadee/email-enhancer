"""
Adaptive Performance Optimizer - ML-based optimization for scraping parameters

Implements adaptive optimization:
- Online learning from success/failure patterns
- Dynamic parameter tuning (timeouts, concurrency, delays)
- Domain-specific optimization profiles
- Performance prediction models
- Resource utilization optimization
- Automatic strategy selection
"""

import json
import logging
import math
import random
import statistics
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple


class OptimizationStrategy(Enum):
    """Optimization strategy types"""
    CONSERVATIVE = "conservative"  # Prioritize reliability
    BALANCED = "balanced"          # Balance speed and reliability
    AGGRESSIVE = "aggressive"      # Prioritize speed
    ADAPTIVE = "adaptive"          # Learn and adapt


@dataclass
class PerformanceMetrics:
    """Performance metrics for a domain/request"""
    success_count: int = 0
    failure_count: int = 0
    timeout_count: int = 0
    total_time: float = 0.0
    min_time: float = float('inf')
    max_time: float = 0.0
    times: List[float] = field(default_factory=list)
    last_updated: float = field(default_factory=time.time)
    
    @property
    def total_requests(self) -> int:
        return self.success_count + self.failure_count + self.timeout_count
    
    @property
    def success_rate(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return self.success_count / self.total_requests * 100
    
    @property
    def avg_time(self) -> float:
        if not self.times:
            return 0.0
        return statistics.mean(self.times)
    
    @property
    def median_time(self) -> float:
        if not self.times:
            return 0.0
        return statistics.median(self.times)
    
    @property
    def p95_time(self) -> float:
        if len(self.times) < 20:
            return self.max_time
        sorted_times = sorted(self.times)
        index = int(len(sorted_times) * 0.95)
        return sorted_times[index]
    
    def record(self, success: bool, duration: float, timeout: bool = False) -> None:
        """Record a request result"""
        if timeout:
            self.timeout_count += 1
        elif success:
            self.success_count += 1
        else:
            self.failure_count += 1
        
        self.total_time += duration
        self.min_time = min(self.min_time, duration)
        self.max_time = max(self.max_time, duration)
        
        # Keep last 100 times for percentile calculations
        self.times.append(duration)
        if len(self.times) > 100:
            self.times = self.times[-100:]
        
        self.last_updated = time.time()


@dataclass
class OptimizationProfile:
    """Optimization profile for a domain or context"""
    domain: str
    timeout: float = 120.0
    concurrency: int = 3
    delay_min: float = 0.5
    delay_max: float = 2.0
    retry_count: int = 3
    use_proxy: bool = False
    block_resources: bool = True
    wait_for_network_idle: bool = False
    
    # Learned parameters
    optimal_timeout: Optional[float] = None
    optimal_concurrency: Optional[int] = None
    optimal_delay: Optional[float] = None
    
    # Confidence scores (0-1)
    timeout_confidence: float = 0.0
    concurrency_confidence: float = 0.0
    delay_confidence: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'domain': self.domain,
            'timeout': self.optimal_timeout or self.timeout,
            'concurrency': self.optimal_concurrency or self.concurrency,
            'delay_min': self.delay_min,
            'delay_max': self.delay_max,
            'optimal_delay': self.optimal_delay,
            'retry_count': self.retry_count,
            'use_proxy': self.use_proxy,
            'block_resources': self.block_resources,
            'wait_for_network_idle': self.wait_for_network_idle,
            'confidence': {
                'timeout': self.timeout_confidence,
                'concurrency': self.concurrency_confidence,
                'delay': self.delay_confidence
            }
        }


class ExponentialMovingAverage:
    """Exponential moving average for online learning"""
    
    def __init__(self, alpha: float = 0.1):
        self.alpha = alpha
        self.value: Optional[float] = None
    
    def update(self, new_value: float) -> float:
        """Update EMA with new value"""
        if self.value is None:
            self.value = new_value
        else:
            self.value = self.alpha * new_value + (1 - self.alpha) * self.value
        return self.value
    
    def get(self) -> Optional[float]:
        """Get current EMA value"""
        return self.value


class ThompsonSampling:
    """Thompson Sampling for exploration/exploitation"""
    
    def __init__(self):
        self.successes: Dict[str, int] = defaultdict(int)
        self.failures: Dict[str, int] = defaultdict(int)
    
    def record(self, option: str, success: bool) -> None:
        """Record outcome for an option"""
        if success:
            self.successes[option] += 1
        else:
            self.failures[option] += 1
    
    def sample(self, options: List[str]) -> str:
        """Sample best option using Thompson Sampling"""
        samples = {}
        
        for option in options:
            alpha = self.successes[option] + 1
            beta = self.failures[option] + 1
            samples[option] = random.betavariate(alpha, beta)
        
        return max(samples, key=samples.get)
    
    def get_stats(self) -> Dict[str, Dict[str, int]]:
        """Get sampling statistics"""
        return {
            option: {
                'successes': self.successes[option],
                'failures': self.failures[option],
                'total': self.successes[option] + self.failures[option]
            }
            for option in set(self.successes.keys()) | set(self.failures.keys())
        }


class AdaptiveOptimizer:
    """
    Adaptive performance optimizer using online learning.
    
    Features:
    - Domain-specific performance tracking
    - Automatic timeout/concurrency tuning
    - Thompson Sampling for strategy selection
    - Exponential moving averages for trend detection
    - Resource utilization optimization
    """
    
    def __init__(self,
                 strategy: OptimizationStrategy = OptimizationStrategy.ADAPTIVE,
                 learning_rate: float = 0.1,
                 min_samples: int = 10,
                 persistence_path: Optional[str] = None):
        
        self.logger = logging.getLogger(__name__)
        self.strategy = strategy
        self.learning_rate = learning_rate
        self.min_samples = min_samples
        self.persistence_path = persistence_path
        
        # Performance tracking by domain
        self._metrics: Dict[str, PerformanceMetrics] = defaultdict(PerformanceMetrics)
        self._profiles: Dict[str, OptimizationProfile] = {}
        
        # Learning components
        self._success_rate_ema: Dict[str, ExponentialMovingAverage] = {}
        self._latency_ema: Dict[str, ExponentialMovingAverage] = {}
        self._strategy_sampler = ThompsonSampling()
        
        # Global metrics
        self._global_metrics = PerformanceMetrics()
        self._global_success_ema = ExponentialMovingAverage(alpha=learning_rate)
        
        # Threading
        self._lock = threading.RLock()
        
        # Load persisted data
        if persistence_path:
            self._load()
    
    def record_result(self, domain: str, success: bool, duration: float,
                      timeout: bool = False, params: Optional[Dict[str, Any]] = None) -> None:
        """Record a scraping result for learning"""
        with self._lock:
            # Update domain metrics
            if domain not in self._metrics:
                self._metrics[domain] = PerformanceMetrics()
            
            self._metrics[domain].record(success, duration, timeout)
            self._global_metrics.record(success, duration, timeout)
            
            # Update EMAs
            if domain not in self._success_rate_ema:
                self._success_rate_ema[domain] = ExponentialMovingAverage(self.learning_rate)
                self._latency_ema[domain] = ExponentialMovingAverage(self.learning_rate)
            
            self._success_rate_ema[domain].update(1.0 if success else 0.0)
            self._latency_ema[domain].update(duration)
            self._global_success_ema.update(1.0 if success else 0.0)
            
            # Update strategy sampler
            if params:
                strategy_key = self._params_to_strategy_key(params)
                self._strategy_sampler.record(strategy_key, success)
            
            # Trigger optimization if enough samples
            metrics = self._metrics[domain]
            if metrics.total_requests >= self.min_samples:
                self._optimize_domain(domain)
    
    def get_optimal_params(self, domain: str) -> Dict[str, Any]:
        """Get optimized parameters for domain"""
        with self._lock:
            profile = self._get_or_create_profile(domain)
            metrics = self._metrics.get(domain)
            
            # Use learned values if confident enough
            params = {
                'timeout': profile.optimal_timeout if profile.timeout_confidence > 0.5 else profile.timeout,
                'concurrency': profile.optimal_concurrency if profile.concurrency_confidence > 0.5 else profile.concurrency,
                'delay': profile.optimal_delay if profile.delay_confidence > 0.5 else (profile.delay_min + profile.delay_max) / 2,
                'retry_count': profile.retry_count,
                'use_proxy': profile.use_proxy,
                'block_resources': profile.block_resources,
                'wait_for_network_idle': profile.wait_for_network_idle
            }
            
            # Apply strategy modifiers
            params = self._apply_strategy(params, domain)
            
            return params
    
    def get_recommended_delay(self, domain: str) -> float:
        """Get recommended delay between requests"""
        with self._lock:
            profile = self._get_or_create_profile(domain)
            metrics = self._metrics.get(domain)
            
            if metrics and metrics.total_requests >= self.min_samples:
                # Base delay on success rate
                success_rate = metrics.success_rate
                
                if success_rate >= 90:
                    # High success rate - can be more aggressive
                    return profile.delay_min
                elif success_rate >= 70:
                    # Moderate success - use balanced delay
                    return (profile.delay_min + profile.delay_max) / 2
                else:
                    # Low success rate - be more conservative
                    return profile.delay_max * 1.5
            
            # Default
            return (profile.delay_min + profile.delay_max) / 2
    
    def get_recommended_concurrency(self, domain: str) -> int:
        """Get recommended concurrency level"""
        with self._lock:
            profile = self._get_or_create_profile(domain)
            metrics = self._metrics.get(domain)
            
            if metrics and metrics.total_requests >= self.min_samples:
                success_rate = metrics.success_rate
                
                if success_rate >= 90:
                    # Can increase concurrency
                    return min(profile.concurrency + 1, 10)
                elif success_rate >= 70:
                    return profile.concurrency
                else:
                    # Should decrease concurrency
                    return max(profile.concurrency - 1, 1)
            
            return profile.concurrency
    
    def should_use_proxy(self, domain: str) -> bool:
        """Determine if proxy should be used for domain"""
        with self._lock:
            metrics = self._metrics.get(domain)
            
            if metrics and metrics.total_requests >= self.min_samples:
                # Use proxy if high failure rate
                if metrics.success_rate < 50:
                    return True
                # Use proxy if many timeouts
                if metrics.timeout_count > metrics.total_requests * 0.2:
                    return True
            
            return False
    
    def _get_or_create_profile(self, domain: str) -> OptimizationProfile:
        """Get or create optimization profile for domain"""
        if domain not in self._profiles:
            self._profiles[domain] = OptimizationProfile(domain=domain)
        return self._profiles[domain]
    
    def _optimize_domain(self, domain: str) -> None:
        """Optimize parameters for domain based on metrics"""
        metrics = self._metrics[domain]
        profile = self._get_or_create_profile(domain)
        
        # Optimize timeout based on p95 latency
        if metrics.p95_time > 0:
            # Set timeout to 1.5x p95 latency, with bounds
            optimal_timeout = min(max(metrics.p95_time * 1.5, 30), 300)
            profile.optimal_timeout = optimal_timeout
            profile.timeout_confidence = min(metrics.total_requests / 100, 1.0)
        
        # Optimize concurrency based on success rate trend
        success_ema = self._success_rate_ema.get(domain)
        if success_ema and success_ema.value is not None:
            if success_ema.value >= 0.9:
                profile.optimal_concurrency = min(profile.concurrency + 1, 10)
            elif success_ema.value < 0.7:
                profile.optimal_concurrency = max(profile.concurrency - 1, 1)
            else:
                profile.optimal_concurrency = profile.concurrency
            
            profile.concurrency_confidence = min(metrics.total_requests / 50, 1.0)
        
        # Optimize delay based on failure patterns
        if metrics.failure_count > 0:
            failure_rate = metrics.failure_count / metrics.total_requests
            if failure_rate > 0.3:
                # Increase delay if high failure rate
                profile.optimal_delay = profile.delay_max * 1.5
            elif failure_rate < 0.1:
                # Decrease delay if low failure rate
                profile.optimal_delay = profile.delay_min
            else:
                profile.optimal_delay = (profile.delay_min + profile.delay_max) / 2
            
            profile.delay_confidence = min(metrics.total_requests / 30, 1.0)
        
        # Update proxy recommendation
        profile.use_proxy = self.should_use_proxy(domain)
        
        self.logger.debug(f"Optimized profile for {domain}: {profile.to_dict()}")
    
    def _apply_strategy(self, params: Dict[str, Any], domain: str) -> Dict[str, Any]:
        """Apply strategy modifiers to parameters"""
        if self.strategy == OptimizationStrategy.CONSERVATIVE:
            params['timeout'] = params['timeout'] * 1.5
            params['delay'] = params['delay'] * 1.5
            params['concurrency'] = max(params['concurrency'] - 1, 1)
        
        elif self.strategy == OptimizationStrategy.AGGRESSIVE:
            params['timeout'] = params['timeout'] * 0.8
            params['delay'] = params['delay'] * 0.5
            params['concurrency'] = min(params['concurrency'] + 2, 10)
        
        elif self.strategy == OptimizationStrategy.ADAPTIVE:
            # Use Thompson Sampling to select strategy
            strategies = ['conservative', 'balanced', 'aggressive']
            selected = self._strategy_sampler.sample(strategies)
            
            if selected == 'conservative':
                params['delay'] = params['delay'] * 1.2
            elif selected == 'aggressive':
                params['delay'] = params['delay'] * 0.8
        
        return params
    
    def _params_to_strategy_key(self, params: Dict[str, Any]) -> str:
        """Convert parameters to strategy key for sampling"""
        concurrency = params.get('concurrency', 3)
        delay = params.get('delay', 1.0)
        
        if concurrency <= 2 and delay >= 2.0:
            return 'conservative'
        elif concurrency >= 5 and delay <= 0.5:
            return 'aggressive'
        else:
            return 'balanced'
    
    def get_stats(self) -> Dict[str, Any]:
        """Get optimizer statistics"""
        with self._lock:
            domain_stats = {}
            for domain, metrics in self._metrics.items():
                domain_stats[domain] = {
                    'total_requests': metrics.total_requests,
                    'success_rate': f"{metrics.success_rate:.1f}%",
                    'avg_time': f"{metrics.avg_time:.2f}s",
                    'p95_time': f"{metrics.p95_time:.2f}s",
                    'timeout_rate': f"{metrics.timeout_count / max(1, metrics.total_requests) * 100:.1f}%"
                }
            
            return {
                'strategy': self.strategy.value,
                'domains_tracked': len(self._metrics),
                'global_success_rate': f"{self._global_metrics.success_rate:.1f}%",
                'global_avg_time': f"{self._global_metrics.avg_time:.2f}s",
                'domain_stats': domain_stats,
                'strategy_sampling': self._strategy_sampler.get_stats()
            }
    
    def get_domain_report(self, domain: str) -> Dict[str, Any]:
        """Get detailed report for domain"""
        with self._lock:
            metrics = self._metrics.get(domain)
            profile = self._profiles.get(domain)
            
            if not metrics:
                return {'domain': domain, 'status': 'no_data'}
            
            return {
                'domain': domain,
                'metrics': {
                    'total_requests': metrics.total_requests,
                    'success_count': metrics.success_count,
                    'failure_count': metrics.failure_count,
                    'timeout_count': metrics.timeout_count,
                    'success_rate': f"{metrics.success_rate:.1f}%",
                    'avg_time': f"{metrics.avg_time:.2f}s",
                    'median_time': f"{metrics.median_time:.2f}s",
                    'p95_time': f"{metrics.p95_time:.2f}s",
                    'min_time': f"{metrics.min_time:.2f}s",
                    'max_time': f"{metrics.max_time:.2f}s"
                },
                'profile': profile.to_dict() if profile else None,
                'recommendations': self.get_optimal_params(domain)
            }
    
    def save(self) -> bool:
        """Save optimizer state to file"""
        if not self.persistence_path:
            return False
        
        try:
            state = {
                'metrics': {
                    domain: {
                        'success_count': m.success_count,
                        'failure_count': m.failure_count,
                        'timeout_count': m.timeout_count,
                        'total_time': m.total_time,
                        'times': m.times[-100:]  # Keep last 100
                    }
                    for domain, m in self._metrics.items()
                },
                'profiles': {
                    domain: p.to_dict()
                    for domain, p in self._profiles.items()
                },
                'strategy_sampling': {
                    'successes': dict(self._strategy_sampler.successes),
                    'failures': dict(self._strategy_sampler.failures)
                }
            }
            
            with open(self.persistence_path, 'w') as f:
                json.dump(state, f, indent=2)
            
            self.logger.info(f"Optimizer state saved to {self.persistence_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save optimizer state: {e}")
            return False
    
    def _load(self) -> bool:
        """Load optimizer state from file"""
        if not self.persistence_path or not Path(self.persistence_path).exists():
            return False
        
        try:
            with open(self.persistence_path, 'r') as f:
                state = json.load(f)
            
            # Restore metrics
            for domain, data in state.get('metrics', {}).items():
                metrics = PerformanceMetrics()
                metrics.success_count = data.get('success_count', 0)
                metrics.failure_count = data.get('failure_count', 0)
                metrics.timeout_count = data.get('timeout_count', 0)
                metrics.total_time = data.get('total_time', 0)
                metrics.times = data.get('times', [])
                self._metrics[domain] = metrics
            
            # Restore strategy sampling
            sampling = state.get('strategy_sampling', {})
            self._strategy_sampler.successes = defaultdict(int, sampling.get('successes', {}))
            self._strategy_sampler.failures = defaultdict(int, sampling.get('failures', {}))
            
            self.logger.info(f"Optimizer state loaded from {self.persistence_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to load optimizer state: {e}")
            return False


# Global optimizer instance
_global_optimizer: Optional[AdaptiveOptimizer] = None


def get_optimizer() -> AdaptiveOptimizer:
    """Get or create global optimizer instance"""
    global _global_optimizer
    if _global_optimizer is None:
        _global_optimizer = AdaptiveOptimizer()
    return _global_optimizer


def set_optimizer(optimizer: AdaptiveOptimizer) -> None:
    """Set global optimizer instance"""
    global _global_optimizer
    _global_optimizer = optimizer


def create_optimizer(config: Optional[Dict[str, Any]] = None) -> AdaptiveOptimizer:
    """Factory function to create optimizer with configuration"""
    config = config or {}
    return AdaptiveOptimizer(
        strategy=OptimizationStrategy(config.get('strategy', 'adaptive')),
        learning_rate=config.get('learning_rate', 0.1),
        min_samples=config.get('min_samples', 10),
        persistence_path=config.get('persistence_path')
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("Adaptive Optimizer Demo")
    print("=" * 60)
    
    # Create optimizer
    optimizer = AdaptiveOptimizer(
        strategy=OptimizationStrategy.ADAPTIVE,
        learning_rate=0.2,
        min_samples=5
    )
    
    # Simulate scraping results for different domains
    print("\n1. Simulating scraping results...")
    
    # Domain 1: Fast and reliable
    for i in range(20):
        success = random.random() > 0.05  # 95% success
        duration = random.uniform(0.5, 2.0)
        optimizer.record_result("fast-domain.com", success, duration)
    
    # Domain 2: Slow but reliable
    for i in range(20):
        success = random.random() > 0.1  # 90% success
        duration = random.uniform(3.0, 8.0)
        optimizer.record_result("slow-domain.com", success, duration)
    
    # Domain 3: Problematic
    for i in range(20):
        success = random.random() > 0.4  # 60% success
        duration = random.uniform(1.0, 15.0)
        timeout = random.random() > 0.7
        optimizer.record_result("problem-domain.com", success, duration, timeout)
    
    # Get recommendations
    print("\n2. Optimization Recommendations:")
    
    for domain in ["fast-domain.com", "slow-domain.com", "problem-domain.com"]:
        params = optimizer.get_optimal_params(domain)
        report = optimizer.get_domain_report(domain)
        
        print(f"\n   {domain}:")
        print(f"     Success Rate: {report['metrics']['success_rate']}")
        print(f"     Avg Time: {report['metrics']['avg_time']}")
        print(f"     P95 Time: {report['metrics']['p95_time']}")
        print(f"     Recommended Timeout: {params['timeout']:.1f}s")
        print(f"     Recommended Concurrency: {params['concurrency']}")
        print(f"     Recommended Delay: {params['delay']:.2f}s")
        print(f"     Use Proxy: {params['use_proxy']}")
    
    # Show global stats
    print("\n3. Global Statistics:")
    stats = optimizer.get_stats()
    print(f"   Strategy: {stats['strategy']}")
    print(f"   Domains Tracked: {stats['domains_tracked']}")
    print(f"   Global Success Rate: {stats['global_success_rate']}")
    print(f"   Global Avg Time: {stats['global_avg_time']}")
    
    print("\n4. Strategy Sampling Stats:")
    for strategy, data in stats['strategy_sampling'].items():
        if data['total'] > 0:
            print(f"   {strategy}: {data['successes']}/{data['total']} "
                  f"({data['successes']/data['total']*100:.0f}%)")
    
    print("\nDemo complete!")
