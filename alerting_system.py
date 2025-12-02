"""
Alerting System - Rule-based alerting with multiple notification channels

Implements comprehensive alerting capabilities:
- Rule engine for alert conditions
- Multiple notification channels (webhook, email, file)
- Alert deduplication and suppression
- Escalation policies
- Alert history and management
- Integration with metrics and logging
"""

import asyncio
import hashlib
import json
import logging
import os
import threading
import time
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Union
from functools import wraps


class AlertSeverity(Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertState(Enum):
    """Alert state"""
    PENDING = "pending"
    FIRING = "firing"
    RESOLVED = "resolved"
    SUPPRESSED = "suppressed"


@dataclass
class Alert:
    """Alert data structure"""
    alert_id: str
    name: str
    severity: AlertSeverity
    state: AlertState
    message: str
    source: str
    timestamp: float
    labels: Dict[str, str] = field(default_factory=dict)
    annotations: Dict[str, str] = field(default_factory=dict)
    metric_value: Optional[float] = None
    threshold: Optional[float] = None
    resolved_at: Optional[float] = None
    notification_sent: bool = False
    suppressed_until: Optional[float] = None
    escalation_level: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        data = asdict(self)
        data['severity'] = self.severity.value
        data['state'] = self.state.value
        return data
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict(), default=str)
    
    @property
    def fingerprint(self) -> str:
        """Generate unique fingerprint for deduplication"""
        key = f"{self.name}:{self.source}:{sorted(self.labels.items())}"
        return hashlib.md5(key.encode()).hexdigest()[:16]
    
    @property
    def duration(self) -> float:
        """Get alert duration in seconds"""
        end_time = self.resolved_at or time.time()
        return end_time - self.timestamp
    
    def resolve(self) -> None:
        """Mark alert as resolved"""
        self.state = AlertState.RESOLVED
        self.resolved_at = time.time()


@dataclass
class AlertRule:
    """Alert rule definition"""
    name: str
    condition: Callable[[Dict[str, Any]], bool]
    severity: AlertSeverity
    message_template: str
    source: str = "metrics"
    labels: Dict[str, str] = field(default_factory=dict)
    annotations: Dict[str, str] = field(default_factory=dict)
    for_duration: float = 0.0  # Alert must be true for this duration
    cooldown: float = 300.0  # Minimum time between repeat alerts
    enabled: bool = True
    
    # State tracking
    _last_fired: float = 0.0
    _condition_true_since: Optional[float] = None
    
    def evaluate(self, metrics: Dict[str, Any]) -> Optional[Alert]:
        """Evaluate rule against metrics"""
        if not self.enabled:
            return None
        
        current_time = time.time()
        condition_result = False
        
        try:
            condition_result = self.condition(metrics)
        except Exception as e:
            logging.getLogger(__name__).error(f"Error evaluating rule {self.name}: {e}")
            return None
        
        if condition_result:
            # Track when condition first became true
            if self._condition_true_since is None:
                self._condition_true_since = current_time
            
            # Check if condition has been true long enough
            condition_duration = current_time - self._condition_true_since
            if condition_duration >= self.for_duration:
                # Check cooldown
                if current_time - self._last_fired >= self.cooldown:
                    self._last_fired = current_time
                    
                    # Format message with metrics
                    try:
                        message = self.message_template.format(**metrics)
                    except KeyError:
                        message = self.message_template
                    
                    return Alert(
                        alert_id=f"{self.name}_{int(current_time)}",
                        name=self.name,
                        severity=self.severity,
                        state=AlertState.FIRING,
                        message=message,
                        source=self.source,
                        timestamp=current_time,
                        labels=dict(self.labels),
                        annotations=dict(self.annotations),
                        metric_value=metrics.get('value'),
                        threshold=metrics.get('threshold')
                    )
        else:
            # Reset condition tracking
            self._condition_true_since = None
        
        return None


class NotificationChannel(ABC):
    """Abstract base class for notification channels"""
    
    @abstractmethod
    async def send(self, alert: Alert) -> bool:
        """Send alert notification"""
        pass
    
    @abstractmethod
    def test_connection(self) -> bool:
        """Test channel connection"""
        pass


class WebhookChannel(NotificationChannel):
    """Webhook notification channel"""
    
    def __init__(self, url: str, method: str = "POST", 
                 headers: Optional[Dict[str, str]] = None,
                 timeout: float = 10.0):
        self.url = url
        self.method = method
        self.headers = headers or {"Content-Type": "application/json"}
        self.timeout = timeout
        self.logger = logging.getLogger(__name__)
    
    async def send(self, alert: Alert) -> bool:
        """Send alert via webhook"""
        try:
            import aiohttp
            
            payload = alert.to_dict()
            
            async with aiohttp.ClientSession() as session:
                async with session.request(
                    self.method,
                    self.url,
                    json=payload,
                    headers=self.headers,
                    timeout=aiohttp.ClientTimeout(total=self.timeout)
                ) as response:
                    success = 200 <= response.status < 300
                    if not success:
                        self.logger.warning(f"Webhook failed: HTTP {response.status}")
                    return success
                    
        except ImportError:
            self.logger.warning("aiohttp not available, falling back to sync request")
            return self._send_sync(alert)
        except Exception as e:
            self.logger.error(f"Webhook error: {e}")
            return False
    
    def _send_sync(self, alert: Alert) -> bool:
        """Sync fallback for webhook"""
        try:
            import urllib.request
            
            data = json.dumps(alert.to_dict()).encode()
            req = urllib.request.Request(
                self.url,
                data=data,
                headers=self.headers,
                method=self.method
            )
            
            with urllib.request.urlopen(req, timeout=self.timeout) as response:
                return 200 <= response.status < 300
                
        except Exception as e:
            self.logger.error(f"Sync webhook error: {e}")
            return False
    
    def test_connection(self) -> bool:
        """Test webhook connection"""
        try:
            import urllib.request
            req = urllib.request.Request(self.url, method="HEAD")
            with urllib.request.urlopen(req, timeout=5) as response:
                return response.status < 500
        except Exception:
            return False


class FileChannel(NotificationChannel):
    """File-based notification channel for logging alerts"""
    
    def __init__(self, file_path: str, append: bool = True):
        self.file_path = file_path
        self.append = append
        self.logger = logging.getLogger(__name__)
        self._lock = threading.Lock()
    
    async def send(self, alert: Alert) -> bool:
        """Write alert to file"""
        try:
            mode = "a" if self.append else "w"
            with self._lock:
                with open(self.file_path, mode) as f:
                    f.write(alert.to_json() + "\n")
            return True
        except Exception as e:
            self.logger.error(f"File write error: {e}")
            return False
    
    def test_connection(self) -> bool:
        """Test file write permission"""
        try:
            with open(self.file_path, "a") as f:
                pass
            return True
        except Exception:
            return False


class ConsoleChannel(NotificationChannel):
    """Console notification channel for development"""
    
    COLORS = {
        AlertSeverity.INFO: '\033[36m',      # Cyan
        AlertSeverity.WARNING: '\033[33m',   # Yellow
        AlertSeverity.ERROR: '\033[31m',     # Red
        AlertSeverity.CRITICAL: '\033[35m',  # Magenta
    }
    RESET = '\033[0m'
    
    def __init__(self, use_colors: bool = True):
        self.use_colors = use_colors
    
    async def send(self, alert: Alert) -> bool:
        """Print alert to console"""
        try:
            timestamp = datetime.fromtimestamp(alert.timestamp).strftime('%Y-%m-%d %H:%M:%S')
            
            if self.use_colors:
                color = self.COLORS.get(alert.severity, '')
                severity = f"{color}[{alert.severity.value.upper()}]{self.RESET}"
            else:
                severity = f"[{alert.severity.value.upper()}]"
            
            print(f"{timestamp} {severity} {alert.name}: {alert.message}")
            return True
        except Exception:
            return False
    
    def test_connection(self) -> bool:
        """Console is always available"""
        return True


class AlertManager:
    """
    Centralized alert management with rule engine and notification routing.
    
    Features:
    - Rule-based alert evaluation
    - Multiple notification channels
    - Alert deduplication and suppression
    - Escalation policies
    - Alert history with cleanup
    - Thread-safe operations
    """
    
    def __init__(self, max_history: int = 1000, 
                 evaluation_interval: float = 10.0,
                 suppression_window: float = 300.0):
        self.logger = logging.getLogger(__name__)
        self.max_history = max_history
        self.evaluation_interval = evaluation_interval
        self.suppression_window = suppression_window
        
        # Rules and channels
        self._rules: Dict[str, AlertRule] = {}
        self._channels: Dict[str, NotificationChannel] = {}
        self._routing: Dict[AlertSeverity, List[str]] = defaultdict(list)
        
        # Alert state
        self._active_alerts: Dict[str, Alert] = {}
        self._alert_history: deque = deque(maxlen=max_history)
        self._suppressed_fingerprints: Dict[str, float] = {}
        
        # Threading
        self._lock = threading.RLock()
        self._running = False
        self._evaluation_thread: Optional[threading.Thread] = None
        
        # Metrics
        self._metrics: Dict[str, Any] = {}
        
        # Stats
        self._stats = {
            'total_alerts': 0,
            'alerts_by_severity': defaultdict(int),
            'notifications_sent': 0,
            'notifications_failed': 0,
            'suppressed_alerts': 0
        }
    
    # Rule management
    
    def add_rule(self, rule: AlertRule) -> None:
        """Add alert rule"""
        with self._lock:
            self._rules[rule.name] = rule
            self.logger.info(f"Added alert rule: {rule.name}")
    
    def remove_rule(self, name: str) -> bool:
        """Remove alert rule"""
        with self._lock:
            if name in self._rules:
                del self._rules[name]
                self.logger.info(f"Removed alert rule: {name}")
                return True
            return False
    
    def enable_rule(self, name: str) -> bool:
        """Enable alert rule"""
        with self._lock:
            if name in self._rules:
                self._rules[name].enabled = True
                return True
            return False
    
    def disable_rule(self, name: str) -> bool:
        """Disable alert rule"""
        with self._lock:
            if name in self._rules:
                self._rules[name].enabled = False
                return True
            return False
    
    def get_rules(self) -> List[Dict[str, Any]]:
        """Get all rules"""
        with self._lock:
            return [
                {
                    'name': rule.name,
                    'severity': rule.severity.value,
                    'enabled': rule.enabled,
                    'source': rule.source,
                    'for_duration': rule.for_duration,
                    'cooldown': rule.cooldown
                }
                for rule in self._rules.values()
            ]
    
    # Channel management
    
    def add_channel(self, name: str, channel: NotificationChannel) -> None:
        """Add notification channel"""
        with self._lock:
            self._channels[name] = channel
            self.logger.info(f"Added notification channel: {name}")
    
    def remove_channel(self, name: str) -> bool:
        """Remove notification channel"""
        with self._lock:
            if name in self._channels:
                del self._channels[name]
                return True
            return False
    
    def route_severity(self, severity: AlertSeverity, channel_names: List[str]) -> None:
        """Configure routing for severity level"""
        with self._lock:
            self._routing[severity] = channel_names
    
    def test_channels(self) -> Dict[str, bool]:
        """Test all notification channels"""
        results = {}
        with self._lock:
            for name, channel in self._channels.items():
                try:
                    results[name] = channel.test_connection()
                except Exception:
                    results[name] = False
        return results
    
    # Metrics integration
    
    def update_metrics(self, metrics: Dict[str, Any]) -> None:
        """Update metrics for rule evaluation"""
        with self._lock:
            self._metrics.update(metrics)
    
    def set_metric(self, name: str, value: Any) -> None:
        """Set single metric value"""
        with self._lock:
            self._metrics[name] = value
    
    # Alert lifecycle
    
    def evaluate_rules(self) -> List[Alert]:
        """Evaluate all rules against current metrics"""
        alerts = []
        
        with self._lock:
            for rule in self._rules.values():
                alert = rule.evaluate(self._metrics)
                if alert:
                    alerts.append(alert)
        
        return alerts
    
    async def fire_alert(self, alert: Alert) -> bool:
        """Fire alert and send notifications"""
        with self._lock:
            # Check suppression
            if self._is_suppressed(alert):
                self._stats['suppressed_alerts'] += 1
                alert.state = AlertState.SUPPRESSED
                return False
            
            # Deduplicate
            fingerprint = alert.fingerprint
            if fingerprint in self._active_alerts:
                existing = self._active_alerts[fingerprint]
                if existing.state == AlertState.FIRING:
                    return False
            
            # Add to active alerts
            self._active_alerts[fingerprint] = alert
            self._alert_history.append(alert)
            
            self._stats['total_alerts'] += 1
            self._stats['alerts_by_severity'][alert.severity.value] += 1
        
        # Send notifications
        success = await self._send_notifications(alert)
        
        if success:
            alert.notification_sent = True
            self._stats['notifications_sent'] += 1
        else:
            self._stats['notifications_failed'] += 1
        
        self.logger.info(f"Alert fired: {alert.name} [{alert.severity.value}]")
        return success
    
    def resolve_alert(self, fingerprint: str) -> bool:
        """Resolve an active alert"""
        with self._lock:
            if fingerprint in self._active_alerts:
                alert = self._active_alerts[fingerprint]
                alert.resolve()
                self.logger.info(f"Alert resolved: {alert.name}")
                return True
            return False
    
    def suppress_alert(self, fingerprint: str, duration: float) -> bool:
        """Suppress alert for specified duration"""
        with self._lock:
            self._suppressed_fingerprints[fingerprint] = time.time() + duration
            if fingerprint in self._active_alerts:
                self._active_alerts[fingerprint].suppressed_until = time.time() + duration
            return True
    
    def _is_suppressed(self, alert: Alert) -> bool:
        """Check if alert should be suppressed"""
        fingerprint = alert.fingerprint
        current_time = time.time()
        
        # Check explicit suppression
        if fingerprint in self._suppressed_fingerprints:
            if current_time < self._suppressed_fingerprints[fingerprint]:
                return True
            else:
                del self._suppressed_fingerprints[fingerprint]
        
        # Check suppression window
        if fingerprint in self._active_alerts:
            existing = self._active_alerts[fingerprint]
            if current_time - existing.timestamp < self.suppression_window:
                return True
        
        return False
    
    async def _send_notifications(self, alert: Alert) -> bool:
        """Send alert to configured channels"""
        # Get channels for this severity
        channel_names = self._routing.get(alert.severity, [])
        
        # Fall back to all channels if no routing configured
        if not channel_names:
            channel_names = list(self._channels.keys())
        
        success = False
        for channel_name in channel_names:
            channel = self._channels.get(channel_name)
            if channel:
                try:
                    result = await channel.send(alert)
                    success = success or result
                except Exception as e:
                    self.logger.error(f"Channel {channel_name} error: {e}")
        
        return success
    
    # Background evaluation
    
    def start(self) -> None:
        """Start background evaluation loop"""
        if self._running:
            return
        
        self._running = True
        self._evaluation_thread = threading.Thread(
            target=self._evaluation_loop,
            daemon=True
        )
        self._evaluation_thread.start()
        self.logger.info("Alert manager started")
    
    def stop(self) -> None:
        """Stop background evaluation"""
        self._running = False
        if self._evaluation_thread:
            self._evaluation_thread.join(timeout=5.0)
        self.logger.info("Alert manager stopped")
    
    def _evaluation_loop(self) -> None:
        """Background evaluation loop"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        while self._running:
            try:
                # Evaluate rules
                alerts = self.evaluate_rules()
                
                # Fire alerts
                for alert in alerts:
                    loop.run_until_complete(self.fire_alert(alert))
                
                # Cleanup expired suppressions
                self._cleanup_suppressions()
                
                time.sleep(self.evaluation_interval)
                
            except Exception as e:
                self.logger.error(f"Evaluation loop error: {e}")
                time.sleep(self.evaluation_interval)
        
        loop.close()
    
    def _cleanup_suppressions(self) -> None:
        """Remove expired suppressions"""
        current_time = time.time()
        with self._lock:
            expired = [fp for fp, until in self._suppressed_fingerprints.items() 
                      if current_time >= until]
            for fp in expired:
                del self._suppressed_fingerprints[fp]
    
    # Queries
    
    def get_active_alerts(self) -> List[Dict[str, Any]]:
        """Get all active (firing) alerts"""
        with self._lock:
            return [
                alert.to_dict()
                for alert in self._active_alerts.values()
                if alert.state == AlertState.FIRING
            ]
    
    def get_alert_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent alert history"""
        with self._lock:
            alerts = list(self._alert_history)[-limit:]
            return [alert.to_dict() for alert in alerts]
    
    def get_stats(self) -> Dict[str, Any]:
        """Get alerting statistics"""
        with self._lock:
            return {
                **self._stats,
                'active_alerts': len([a for a in self._active_alerts.values() 
                                     if a.state == AlertState.FIRING]),
                'total_rules': len(self._rules),
                'enabled_rules': len([r for r in self._rules.values() if r.enabled]),
                'total_channels': len(self._channels),
                'suppressed_count': len(self._suppressed_fingerprints),
                'running': self._running
            }
    
    def clear_alerts(self) -> None:
        """Clear all alerts and history"""
        with self._lock:
            self._active_alerts.clear()
            self._alert_history.clear()
            self._suppressed_fingerprints.clear()
        self.logger.info("All alerts cleared")


# Predefined alert rules for common scenarios

class CommonAlertRules:
    """Factory for common alert rules"""
    
    @staticmethod
    def high_memory_usage(threshold: float = 85.0) -> AlertRule:
        """Alert when memory usage exceeds threshold"""
        return AlertRule(
            name="high_memory_usage",
            condition=lambda m: m.get('memory_percent', 0) > threshold,
            severity=AlertSeverity.WARNING,
            message_template=f"Memory usage is {{memory_percent:.1f}}% (threshold: {threshold}%)",
            source="system",
            labels={"type": "resource"},
            for_duration=60.0,
            cooldown=300.0
        )
    
    @staticmethod
    def critical_memory_usage(threshold: float = 95.0) -> AlertRule:
        """Alert when memory usage is critical"""
        return AlertRule(
            name="critical_memory_usage",
            condition=lambda m: m.get('memory_percent', 0) > threshold,
            severity=AlertSeverity.CRITICAL,
            message_template=f"CRITICAL: Memory usage is {{memory_percent:.1f}}% (threshold: {threshold}%)",
            source="system",
            labels={"type": "resource"},
            for_duration=30.0,
            cooldown=60.0
        )
    
    @staticmethod
    def high_error_rate(threshold: float = 10.0) -> AlertRule:
        """Alert when error rate exceeds threshold"""
        return AlertRule(
            name="high_error_rate",
            condition=lambda m: m.get('error_rate', 0) > threshold,
            severity=AlertSeverity.ERROR,
            message_template=f"Error rate is {{error_rate:.1f}}% (threshold: {threshold}%)",
            source="processing",
            labels={"type": "quality"},
            for_duration=120.0,
            cooldown=300.0
        )
    
    @staticmethod
    def low_success_rate(threshold: float = 80.0) -> AlertRule:
        """Alert when success rate falls below threshold"""
        return AlertRule(
            name="low_success_rate",
            condition=lambda m: m.get('success_rate', 100) < threshold,
            severity=AlertSeverity.WARNING,
            message_template=f"Success rate is {{success_rate:.1f}}% (threshold: {threshold}%)",
            source="processing",
            labels={"type": "quality"},
            for_duration=180.0,
            cooldown=600.0
        )
    
    @staticmethod
    def processing_stalled(timeout: float = 300.0) -> AlertRule:
        """Alert when processing appears stalled"""
        return AlertRule(
            name="processing_stalled",
            condition=lambda m: (time.time() - m.get('last_activity', time.time())) > timeout,
            severity=AlertSeverity.ERROR,
            message_template=f"No processing activity for {timeout}s",
            source="processing",
            labels={"type": "health"},
            for_duration=0.0,
            cooldown=300.0
        )
    
    @staticmethod
    def browser_pool_exhausted() -> AlertRule:
        """Alert when browser pool is exhausted"""
        return AlertRule(
            name="browser_pool_exhausted",
            condition=lambda m: m.get('browser_pool_available', 1) == 0,
            severity=AlertSeverity.WARNING,
            message_template="Browser pool exhausted - no browsers available",
            source="browser",
            labels={"type": "resource"},
            for_duration=30.0,
            cooldown=120.0
        )


# Global alert manager instance
_global_alert_manager: Optional[AlertManager] = None


def get_alert_manager() -> AlertManager:
    """Get or create global alert manager instance"""
    global _global_alert_manager
    if _global_alert_manager is None:
        _global_alert_manager = AlertManager()
    return _global_alert_manager


def set_alert_manager(manager: AlertManager) -> None:
    """Set global alert manager instance"""
    global _global_alert_manager
    _global_alert_manager = manager


def create_alert_manager(config: Optional[Dict[str, Any]] = None) -> AlertManager:
    """Factory function for creating alert manager with DI integration"""
    config = config or {}
    return AlertManager(
        max_history=config.get('max_history', 1000),
        evaluation_interval=config.get('evaluation_interval', 10.0),
        suppression_window=config.get('suppression_window', 300.0)
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("Alerting System Demo")
    print("=" * 50)
    
    # Create alert manager
    manager = AlertManager(evaluation_interval=5.0)
    
    # Add notification channels
    manager.add_channel("console", ConsoleChannel(use_colors=True))
    manager.add_channel("alerts_log", FileChannel("logs/alerts.jsonl"))
    
    # Configure routing
    manager.route_severity(AlertSeverity.INFO, ["console"])
    manager.route_severity(AlertSeverity.WARNING, ["console", "alerts_log"])
    manager.route_severity(AlertSeverity.ERROR, ["console", "alerts_log"])
    manager.route_severity(AlertSeverity.CRITICAL, ["console", "alerts_log"])
    
    # Add common rules
    manager.add_rule(CommonAlertRules.high_memory_usage(threshold=50.0))  # Low threshold for demo
    manager.add_rule(CommonAlertRules.low_success_rate(threshold=90.0))
    
    # Add custom rule
    custom_rule = AlertRule(
        name="test_alert",
        condition=lambda m: m.get('test_value', 0) > 5,
        severity=AlertSeverity.INFO,
        message_template="Test value is {test_value}",
        source="test",
        for_duration=0.0,
        cooldown=10.0
    )
    manager.add_rule(custom_rule)
    
    print("\nRules configured:")
    for rule in manager.get_rules():
        print(f"  - {rule['name']} [{rule['severity']}] enabled={rule['enabled']}")
    
    print("\nChannels configured:")
    for name, status in manager.test_channels().items():
        print(f"  - {name}: {'OK' if status else 'FAILED'}")
    
    # Simulate metrics updates
    print("\nSimulating metrics updates...")
    
    async def demo():
        # Update metrics to trigger alerts
        manager.update_metrics({
            'memory_percent': 75.0,
            'success_rate': 85.0,
            'test_value': 10
        })
        
        # Evaluate and fire alerts
        alerts = manager.evaluate_rules()
        print(f"\nAlerts triggered: {len(alerts)}")
        
        for alert in alerts:
            print(f"\n  Firing alert: {alert.name}")
            await manager.fire_alert(alert)
        
        # Show active alerts
        print("\nActive alerts:")
        for alert in manager.get_active_alerts():
            print(f"  - {alert['name']}: {alert['message']}")
        
        # Show stats
        print("\nStats:")
        for key, value in manager.get_stats().items():
            print(f"  {key}: {value}")
    
    # Run demo
    asyncio.run(demo())
    
    print("\nDemo complete!")
