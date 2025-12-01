"""
Memory Monitoring Service - Real-time memory usage tracking and backpressure control

Implements system resource monitoring with:
- Real-time memory usage tracking
- CPU and network monitoring
- Backpressure control mechanisms
- Adaptive resource allocation
- Performance metrics collection
- Alert system for resource thresholds
"""

import logging
import os
import psutil
import time
import gc
import threading
from collections import deque
from dataclasses import dataclass
from typing import Dict, List, Optional, Callable, Any
import asyncio
from enum import Enum


class ResourceType(Enum):
    """Resource types that can be monitored"""
    MEMORY = "memory"
    CPU = "cpu"
    DISK = "disk"
    NETWORK = "network"


@dataclass
class ResourceMetrics:
    """Resource usage metrics snapshot"""
    timestamp: float
    cpu_percent: float
    memory_mb: float
    memory_percent: float
    disk_usage_mb: float
    disk_percent: float
    network_sent_mb: float
    network_recv_mb: float
    process_count: int
    load_average: List[float]  # 1min, 5min, 15min
    
    @classmethod
    def capture(cls) -> 'ResourceMetrics':
        """Capture current system metrics"""
        # System metrics
        cpu_percent = psutil.cpu_percent(interval=0.1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        # Network metrics
        network = psutil.net_io_counters()
        
        # Process count and load average
        process_count = len(psutil.pids())
        load_avg = list(os.getloadavg()) if hasattr(os, 'getloadavg') else [0.0, 0.0, 0.0]
        
        return cls(
            timestamp=time.time(),
            cpu_percent=cpu_percent,
            memory_mb=memory.used / 1024 / 1024,
            memory_percent=memory.percent,
            disk_usage_mb=disk.used / 1024 / 1024,
            disk_percent=disk.percent,
            network_sent_mb=network.bytes_sent / 1024 / 1024,
            network_recv_mb=network.bytes_recv / 1024 / 1024,
            process_count=process_count,
            load_average=load_avg
        )


@dataclass
class ResourceThresholds:
    """Resource usage thresholds for alerting and backpressure"""
    memory_warning_percent: float = 75.0
    memory_critical_percent: float = 85.0
    cpu_warning_percent: float = 80.0
    cpu_critical_percent: float = 90.0
    disk_warning_percent: float = 80.0
    disk_critical_percent: float = 90.0
    max_memory_mb: float = 4096.0  # 4GB default
    
    def should_trigger_warning(self, resource: ResourceType, value: float) -> bool:
        """Check if value triggers warning threshold"""
        thresholds = {
            ResourceType.MEMORY: self.memory_warning_percent,
            ResourceType.CPU: self.cpu_warning_percent,
            ResourceType.DISK: self.disk_warning_percent
        }
        return value >= thresholds.get(resource, 100.0)
    
    def should_trigger_critical(self, resource: ResourceType, value: float) -> bool:
        """Check if value triggers critical threshold"""
        thresholds = {
            ResourceType.MEMORY: self.memory_critical_percent,
            ResourceType.CPU: self.cpu_critical_percent,
            ResourceType.DISK: self.disk_critical_percent
        }
        return value >= thresholds.get(resource, 100.0)


class MemoryMonitor:
    """
    Real-time memory usage monitoring and backpressure control.
    
    Features:
    - Memory usage tracking with historical data
    - Automatic garbage collection triggering
    - Backpressure control for production systems
    - Alert system for resource threshold violations
    """
    
    def __init__(self, thresholds: Optional[ResourceThresholds] = None):
        self.thresholds = thresholds or ResourceThresholds()
        self.process = psutil.Process()
        self.logger = logging.getLogger(__name__)
        
        # Historical data tracking
        self.metrics_history: deque[ResourceMetrics] = deque(maxlen=1000)  # Store last 1000 snapshots
        self.alert_history: deque[Dict] = deque(maxlen=100)  # Store last 100 alerts
        
        # Backpressure control
        self.backpressure_active = False
        self.last_gc_time = 0
        self.gc_cooldown = 30.0  # Seconds between GC triggers
        
        # Monitoring thread
        self._monitoring = False
        self._monitor_thread: Optional[threading.Thread] = None
        self._monitor_interval = 5.0  # Seconds between metrics capture
        
        # Alert callbacks
        self.warning_callbacks: List[Callable] = []
        self.critical_callbacks: List[Callable] = []
    
    def start_monitoring(self, interval: float = 5.0):
        """Start continuous monitoring in background thread"""
        if self._monitoring:
            return
        
        self._monitor_interval = interval
        self._monitoring = True
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()
        self.logger.info(f"Memory monitoring started (interval: {interval}s)")
    
    def stop_monitoring(self):
        """Stop continuous monitoring"""
        self._monitoring = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5.0)
        self.logger.info("Memory monitoring stopped")
    
    def _monitor_loop(self):
        """Main monitoring loop running in background thread"""
        while self._monitoring:
            try:
                metrics = ResourceMetrics.capture()
                self._process_metrics(metrics)
                time.sleep(self._monitor_interval)
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                time.sleep(self._monitor_interval)
    
    def _process_metrics(self, metrics: ResourceMetrics):
        """Process captured metrics and trigger appropriate actions"""
        # Store in history
        self.metrics_history.append(metrics)
        
        # Check thresholds and trigger alerts
        self._check_thresholds(metrics)
        
        # Trigger backpressure if needed
        self._update_backpressure(metrics)
        
        # Automatic garbage collection
        self._trigger_gc_if_needed(metrics)
    
    def _check_thresholds(self, metrics: ResourceMetrics):
        """Check resource thresholds and trigger alerts"""
        alerts = []
        
        # Memory thresholds
        if self.thresholds.should_trigger_critical(ResourceType.MEMORY, metrics.memory_percent):
            alerts.append({
                'type': 'critical',
                'resource': ResourceType.MEMORY,
                'message': f"Critical memory usage: {metrics.memory_percent:.1f}% ({metrics.memory_mb:.1f}MB)",
                'timestamp': metrics.timestamp,
                'value': metrics.memory_percent
            })
        elif self.thresholds.should_trigger_warning(ResourceType.MEMORY, metrics.memory_percent):
            alerts.append({
                'type': 'warning',
                'resource': ResourceType.MEMORY,
                'message': f"High memory usage: {metrics.memory_percent:.1f}% ({metrics.memory_mb:.1f}MB)",
                'timestamp': metrics.timestamp,
                'value': metrics.memory_percent
            })
        
        # CPU thresholds
        if self.thresholds.should_trigger_critical(ResourceType.CPU, metrics.cpu_percent):
            alerts.append({
                'type': 'critical',
                'resource': ResourceType.CPU,
                'message': f"Critical CPU usage: {metrics.cpu_percent:.1f}%",
                'timestamp': metrics.timestamp,
                'value': metrics.cpu_percent
            })
        elif self.thresholds.should_trigger_warning(ResourceType.CPU, metrics.cpu_percent):
            alerts.append({
                'type': 'warning',
                'resource': ResourceType.CPU,
                'message': f"High CPU usage: {metrics.cpu_percent:.1f}%",
                'timestamp': metrics.timestamp,
                'value': metrics.cpu_percent
            })
        
        # Process alerts
        for alert in alerts:
            self.alert_history.append(alert)
            self._trigger_alert_callbacks(alert)
    
    def _trigger_alert_callbacks(self, alert: Dict):
        """Trigger appropriate callbacks based on alert type"""
        if alert['type'] == 'critical':
            for callback in self.critical_callbacks:
                try:
                    callback(alert)
                except Exception as e:
                    self.logger.error(f"Critical alert callback failed: {e}")
        elif alert['type'] == 'warning':
            for callback in self.warning_callbacks:
                try:
                    callback(alert)
                except Exception as e:
                    self.logger.error(f"Warning alert callback failed: {e}")
    
    def _update_backpressure(self, metrics: ResourceMetrics):
        """Update backpressure state based on resource usage"""
        was_active = self.backpressure_active
        
        # Activate backpressure on critical usage
        critical_thresholds = [
            (ResourceType.MEMORY, self.thresholds.memory_critical_percent),
            (ResourceType.CPU, self.thresholds.cpu_critical_percent)
        ]
        
        self.backpressure_active = any(
            getattr(metrics, f"{resource.value}_percent") >= threshold
            for resource, threshold in critical_thresholds
        )
        
        # Log state changes
        if not was_active and self.backpressure_active:
            self.logger.warning(f"Backpressure activated due to high resource usage")
        elif was_active and not self.backpressure_active:
            self.logger.info("Backpressure deactivated - resource usage normal")
    
    def _trigger_gc_if_needed(self, metrics: ResourceMetrics):
        """Trigger garbage collection if memory usage is high and cooldown passed"""
        current_time = time.time()
        
        if (metrics.memory_percent >= self.thresholds.memory_warning_percent and
            current_time - self.last_gc_time > self.gc_cooldown):
            
            self.logger.info(f"Triggering garbage collection (memory: {metrics.memory_percent:.1f}%)")
            
            before_mb = metrics.memory_mb
            collected = gc.collect()
            after_mb = psutil.virtual_memory().used / 1024 / 1024
            
            freed_mb = before_mb - after_mb
            self.logger.info(f"GC completed: {collected} objects collected, {freed_mb:.1f}MB freed")
            
            self.last_gc_time = current_time
            return freed_mb
        
        return 0
    
    def usage_percentage(self) -> float:
        """Get current memory usage as percentage of threshold"""
        metrics = ResourceMetrics.capture()
        return metrics.memory_percent
    
    def memory_usage_mb(self) -> float:
        """Get current memory usage in MB"""
        metrics = ResourceMetrics.capture()
        return metrics.memory_mb
    
    def should_trigger_gc(self) -> bool:
        """Check if garbage collection should be triggered"""
        return (
            self.usage_percentage() >= self.thresholds.memory_warning_percent and
            time.time() - self.last_gc_time > self.gc_cooldown
        )
    
    def get_recent_metrics(self, count: int = 10) -> List[ResourceMetrics]:
        """Get recent metrics from history"""
        return list(self.metrics_history)[-count:]
    
    def get_average_metrics(self, minutes: int = 30) -> Optional[ResourceMetrics]:
        """Get average metrics over the last N minutes"""
        cutoff_time = time.time() - (minutes * 60)
        recent_metrics = [
            m for m in self.metrics_history
            if m.timestamp >= cutoff_time
        ]
        
        if not recent_metrics:
            return None
        
        # Calculate averages
        avg_metrics = ResourceMetrics(
            timestamp=time.time(),
            cpu_percent=sum(m.cpu_percent for m in recent_metrics) / len(recent_metrics),
            memory_mb=sum(m.memory_mb for m in recent_metrics) / len(recent_metrics),
            memory_percent=sum(m.memory_percent for m in recent_metrics) / len(recent_metrics),
            disk_usage_mb=sum(m.disk_usage_mb for m in recent_metrics) / len(recent_metrics),
            disk_percent=sum(m.disk_percent for m in recent_metrics) / len(recent_metrics),
            network_sent_mb=sum(m.network_sent_mb for m in recent_metrics) / len(recent_metrics),
            network_recv_mb=sum(m.network_recv_mb for m in recent_metrics) / len(recent_metrics),
            process_count=sum(m.process_count for m in recent_metrics) / len(recent_metrics),
            load_average=[
                sum(m.load_average[i] for m in recent_metrics) / len(recent_metrics)
                for i in range(3)
            ]
        )
        
        return avg_metrics
    
    def add_warning_callback(self, callback: Callable[[Dict], None]):
        """Add callback for warning alerts"""
        self.warning_callbacks.append(callback)
    
    def add_critical_callback(self, callback: Callable[[Dict], None]):
        """Add callback for critical alerts"""
        self.critical_callbacks.append(callback)
    
    def is_backpressure_active(self) -> bool:
        """Check if backpressure control is currently active"""
        return self.backpressure_active
    
    def get_stats(self) -> Dict[str, Any]:
        """Get monitoring statistics"""
        current_metrics = ResourceMetrics.capture() if self.metrics_history else None
        recent_metrics = self.get_recent_metrics(10)
        
        return {
            'monitoring_active': self._monitoring,
            'backpressure_active': self.backpressure_active,
            'current_metrics': current_metrics.__dict__ if current_metrics else None,
            'recent_average': self.get_average_metrics(10).__dict__ if recent_metrics else None,
            'alert_count': len(self.alert_history),
            'warning_count': len([a for a in self.alert_history if a['type'] == 'warning']),
            'critical_count': len([a for a in self.alert_history if a['type'] == 'critical']),
            'metrics_history_size': len(self.metrics_history),
            'last_gc_time': self.last_gc_time,
            'thresholds': self.thresholds.__dict__
        }


class BackpressureController:
    """
    Backpressure control for production systems.
    
    Controls throughput based on system resource availability:
    - Pauses processing when resources are constrained
    - Resumes automatically when resources normalize
    - Provides wait() method for blocking operations
    """
    
    def __init__(self, memory_monitor: MemoryMonitor):
        self.memory_monitor = memory_monitor
        self.logger = logging.getLogger(__name__)
        self._wait_backoff = 1.0  # Initial wait time
        self._max_backoff = 30.0  # Maximum wait time
    
    def wait(self, max_wait: float = 10.0):
        """
        Wait if backpressure is active.
        
        Args:
            max_wait: Maximum time to wait (seconds)
        
        Returns:
            float: Actual time waited in seconds
        """
        if not self.memory_monitor.is_backpressure_active():
            if self._wait_backoff > 1.0:
                self._wait_backoff = max(1.0, self._wait_backoff / 2)  # Reduce backoff
            return 0.0
        
        # Use exponential backoff
        wait_time = min(self._wait_backoff, max_wait, self._max_backoff)
        self._wait_backoff = min(self._wait_backoff * 1.5, self._max_backoff)
        
        if self.logger.isEnabledFor(logging.DEBUG):
            metrics = self.memory_monitor.get_recent_metrics(1)
            if metrics:
                latest = metrics[0]
                self.logger.debug(
                    f"Backpressure wait: {wait_time:.1f}s "
                    f"(mem: {latest.memory_percent:.1f}%, cpu: {latest.cpu_percent:.1f}%)"
                )
        
        time.sleep(wait_time)
        return wait_time
    
    def should_throttle(self) -> bool:
        """Check if processing should be throttled"""
        return self.memory_monitor.is_backpressure_active()
    
    def get_throttling_factor(self) -> float:
        """
        Get throttling factor (0.0 to 1.0).
        
        Returns:
            float: 1.0 = full speed, 0.0 = fully throttled
        """
        if not self.memory_monitor.is_backpressure_active():
            return 1.0
        
        metrics = self.memory_monitor.get_recent_metrics(1)
        if not metrics:
            return 0.5  # Default throttling
        
        latest = metrics[0]
        
        # Calculate throttling based on resource pressure
        memory_pressure = max(0, (latest.memory_percent - 75) / 10)  # 75-85% = 0-1
        cpu_pressure = max(0, (latest.cpu_percent - 80) / 10)  # 80-90% = 0-1
        
        # Use maximum pressure for throttling
        pressure = max(memory_pressure, cpu_pressure)
        throttling_factor = max(0.1, 1.0 - pressure)  # Minimum 10% throughput
        
        return throttling_factor


# Global memory monitor instance (singleton pattern)
_global_memory_monitor: Optional[MemoryMonitor] = None


def get_memory_monitor() -> MemoryMonitor:
    """Get or create global memory monitor instance"""
    global _global_memory_monitor
    if _global_memory_monitor is None:
        _global_memory_monitor = MemoryMonitor()
        _global_memory_monitor.start_monitoring()
    return _global_memory_monitor


def stop_memory_monitor():
    """Stop global memory monitor"""
    global _global_memory_monitor
    if _global_memory_monitor is not None:
        _global_memory_monitor.stop_monitoring()
        _global_memory_monitor = None
