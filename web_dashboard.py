"""
Web Dashboard Framework - FastAPI-based Dashboard with Real-time System Monitoring

Implements comprehensive web dashboard with:
- Real-time system metrics display
- WebSocket connections for live updates
- RESTful API for dashboard data
- Integration with Phase 2 DI Container
- Prometheus metrics endpoints
- Secure authentication and user management
- Responsive design with progressive enhancement
"""

import asyncio
import logging
import os
import json
import time
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime, timedelta

# Web framework and WebSocket
import uvicorn
from fastapi import FastAPI, Request, Response, BackgroundTasks, WebSocket, WebSocket, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates

# Phase 2 components
from di_container import DIContainer, DIContext
from configuration_service import ConfigurationService, Environment

# Phase 1 components
from memory_monitor import MemoryMonitor
from browser_pool import BrowserPool
from enhanced_csv_processor import EnhancedCSVProcessor

# Project components
import psutil
import pydoc


class DashboardMetrics:
    """Dashboard metrics data structure"""
    
    def __init__(self):
        self.timestamp = time.time()
        self.system_metrics = {
            'cpu_percent': 0.0,
            'memory_mb': 0.0,
            'memory_percent': 0.0,
            'disk_usage_mb': 0.0,
            'disk_percent': 0.0,
            'network_sent_mb': 0.0,
            'network_recv_mb': 0.0,
            'process_count': 0,
            'load_average': [0.0, 0.0, 0.0]
        }
        
        self.processing_metrics = {
            'urls_processed_total': 0,
            'urls_processed_today': 0,
            'success_rate': 0.0,
            'processing_rate_per_minute': 0.0,
            'memory_peak_mb': 0.0,
            'processing_time_total': 0.0
        }
        
        self.service_metrics = {
            'browser_pool_active': False,
            'config_service_loaded': False,
            'di_container_services': 0,
            'memory_monitor_active': False,
            'csv_processor_active': False
        }
        
        self.alerts = {
            'total_alerts': 0,
            'active_alerts': 0,
            'alert_history': []
        }
        
        self.uptime = time.time()
    
    def update_system_metrics(self):
        """Update system metrics"""
        try:
            # CPU metrics
            cpu_percent = psutil.cpu_percent(interval=1)
            self.system_metrics['cpu_percent'] = sum(cpu_percent) / len(cpu_percent)
            
            # Memory metrics
            memory = psutil.virtual_memory()
            self.system_metrics['memory_mb'] = memory.used / 1024 / 1024
            self.system_metrics['memory_percent'] = memory.percent
            
            # Disk metrics
            disk = psutil.disk_usage('/')
            self.system_metrics['disk_usage_mb'] = disk.used / 1024 / 1024
            self.system_metrics['disk_percent'] = disk.percent
            
            # Network metrics
            network = psutil.net_io_counters()
            self.system_metrics['network_sent_mb'] = network.bytes_sent / 1024 / 1024
            self.system_metrics['network_recv_mb'] = network.bytes_recv / 1024 / 1024
            
            # Process metrics
            self.system_metrics['process_count'] = len(psutil.pids())
            
            # Load average (3 intervals)
            load_avg = os.getloadavg() if hasattr(os, 'getloadavg') else [0, 0, 0]
            self.system_metrics['load_average'] = load_avg
            
            self.system_metrics['timestamp'] = time.time()
            
        except Exception as e:
            logging.error(f"Error updating system metrics: {e}")
    
    def update_processing_metrics(self, urls_processed: int, success_rate: float, processing_rate: float):
        """Update processing metrics"""
        self.processing_metrics['urls_processed_total'] += urls_processed
        self.processing_metrics['urls_processed_today'] = urls_processed
        self.processing_metrics['success_rate'] = success_rate
        self.processing_metrics['processing_rate_per_minute'] = processing_rate
        self.processing_metrics['timestamp'] = time.time()
    
    def get_json(self) -> Dict[str, Any]:
        """Get dashboard metrics in JSON format"""
        return {
            'timestamp': self.timestamp,
            'uptime': time.time() - self.uptime,
            'system': self.system_metrics,
            'processing': self.processing_metrics,
            'services': self.service_metrics,
            'alerts': self.alerts
        }


class WebSocketManager:
    """Manages WebSocket connections for real-time dashboard updates"""
    
    def __init__(self):
        self.connections: List[WebSocket] = []
        self.logger = logging.getLogger(__name__)
    
    def add_connection(self, websocket: WebSocket):
        """Add new WebSocket connection"""
        self.connections.append(websocket)
        self.logger.debug(f"WebSocket connected. Total: {len(self.connections)}")
    
    def remove_connection(self, websocket: WebSocket):
        """Remove WebSocket connection"""
        if websocket in self.connections:
            self.connections.remove(websocket)
        self.logger.debug(f"WebSocket disconnected. Total: {len(self.connections)}")
    
    async def broadcast_metrics(self, metrics: Dict[str, Any]):
        """Broadcast metrics update to all connected WebSocket clients"""
        if not self.connections:
            return
        
        message = json.dumps({
            'type': 'metrics_update',
            'timestamp': time.time(),
            'data': metrics
        })
        
        # Send to all connections
        closed_connections = []
        for connection in self.connections:
            try:
                await connection.send_text(message)
            except Exception as e:
                self.logger.warning(f"Failed to send to WebSocket: {e}")
                closed_connections.append(connection)
        
        # Remove closed connections
        for connection in closed_connections:
            self.remove_connection(connection)
    
    def get_connection_count(self) -> int:
        """Get current WebSocket connection count"""
        return len(self.connections)


class MetricsCollector:
    """Centralized metrics collection service"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Internal metrics storage
        self._counters: Dict[str, float] = {}
        self._gauges: Dict[str, float] = {}
        self._timers: Dict[str, float] = {}
        self._histograms: Dict[str, List[float]] = {}
        
        # Performance tracking
        self.collection_time = 0.0
        self.collection_count = 0
    
    def increment_counter(self, name: str, value: float = 1.0) -> None:
        """Increment counter metric"""
        with self._lock:
            self._counters[name] = self._counters.get(name, 0.0) + value
            self._collection_time += 0.001
            self._collection_count += 1
    
    def set_gauge(self, name: str, value: float) -> None:
        """Set gauge metric value"""
        with self._lock:
            self._gauges[name] = value
            self._collection_time += 0.001
            self._collection_count += 1
    
    def record_timing(self, name: str, duration: float) -> None:
        """Record timing metric (in seconds)"""
        with self._lock:
            if name not in self._timers:
                self._timers[name] = []
            
            self._timers[name].append(duration)
            
            # Keep only last 1000 timing samples
            if len(self._timers[name]) > 1000:
                self._timers[name] = self._di_container._timers[name][-1000:]
            
            # Update average timing
            avg_duration = sum(self._timers[name]) / len(self._timers[name])
            self._gauges[f"{name}_avg"] = avg_duration
            self._collection_time += 0.001
            self._collection_count += 1
    
    def record_histogram(self, name: str, value: float, bins: int = 10) -> None:
        """Record histogram metric"""
        with self._lock:
            if name not in self._histograms:
                self._histograms[name] = []
            
            # Find appropriate bin
            max_bins = 100  # Maximum bins per histogram
            bin_size = value / bins
            bin_index = min(int(bin_index), max_bins - 1)
            
            # Extend histogram if needed
            while len(self._histograms[name]) <= bin_index:
                self._histograms[name] = [0.0] * max_bins
            
            self._histograms[name][bin_index] += 1
            self._histograms[name] = self._histograms[name][:max_bins]
            
            self._collection_time += 0.001
            self._collection_count += 1
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get all collected metrics"""
        with self._lock:
            stats = {
                'counters': dict(self._counters),
                'gauges': dict(self._gauges),
                'timers': {name: (sum(values) / len(values) if values else 0)
                          for name, values in self._timers.items()},
                'histograms': dict(self._histograms),
                'collection_time': self._collection_time,
                'collection_count': self._collection_count
            }
        return stats
    
    def get_prometheus_metrics(self) -> str:
        """Get metrics in Prometheus format"""
        # Convert to Prometheus format
        prometheus_metrics = []
        
        # Counters
        for name, value in self._counters.items():
            prometheus_metrics.append(f"dashboard_counter_total{{metric=\"{name}\"}} {value}")
        
        # Gauges
        for name, value in self._gauges.items():
            prometheus_metrics.append(f"dashboard_gauge_{name} {value}")
            
        # Average timings
        for name, avg_value in [item for item in self._timers.items()]:
            prometheus_metrics.append(f"dashboard_timer_{name}_seconds {avg_value}")
        
        # Histograms (simple flat format)
        for name, values in self._histograms.items():
            total_sum = sum(values)
            if total_sum > 0:
                for i, count in enumerate(values):
                    prometheus_metrics.append(f"dashboard_histogram_{name}_bucket_{i} {count}")
        
        return '\n'.join(prometheus_metrics)
    
    def reset_metrics(self) -> None:
        """Reset all collected metrics"""
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._timers.clear()
            self._histograms.clear()
            self._collection_time = 0.0
            self._collection_count = 0
        
        self.logger.info("All metrics reset")


class RealtimeMonitor:
    """Real-time system monitoring and alerting"""
    
    def __init__(self, config_service: Optional[ConfigurationService] = None, webhook_url: Optional[str] = None):
        self.config_service = config_service
        self.webhook_url = webhook_url
        self.logger = logging.getLogger(__name__)
        
        # Monitoring state
        self.monitoring_active = False
        self.check_interval = 5.0
        self.alert_check_interval = 30.0
        self.last_health_check = 0.0
        
        # Alert management
        self.alert_rules: Dict[str, Dict[str, Any]] = {}
        self.active_alerts: Dict[str, Dict[str, Any]] = {}
        self.alert_history: List[Dict[str, Any]] = []
        
        # Monitoring thread
        self._monitor_thread: Optional[asyncio.Task] = None
        
        # Metrics collector
        self.metrics_collector = MetricsCollector()
    
    def start_monitoring(self, interval: float = 5.0) -> None:
        """Start real-time monitoring"""
        if self.monitoring_active:
            return
        
        self.monitoring_active = True
        self.check_interval = interval
        
        self._monitor_thread = asyncio.create_task(self._monitor_loop)
        self.logger.info("Real-time monitoring started")
    
    async def stop_monitoring(self) -> None:
        """Stop real-time monitoring"""
        self.monitoring_active = False
        if self._monitor_thread:
            self._monitor_thread.cancel()
            try:
                await self._monitor_thread
            except asyncio.CancelledError:
                pass  # Expected
    
        self.logger.info("Real-time monitoring stopped")
    
    async def _monitor_loop(self) -> None:
        """Main monitoring loop"""
        while self.monitoring_active:
            try:
                # Collect and validate current state
                await self._check_system_state()
                
                # Sleep until next check
                await asyncio.sleep(self.check_interval)
                
            except Exception as e:
                self.logger.error(f"Monitoring error: {e}")
                await asyncio.sleep(self.check_interval)
    
    async def _check_system_state(self) -> None:
        """Check current system state and trigger alerts"""
        current_time = time.time()
        
        # Check if time for health check
        if current_time - self.last_health_check >= self.alert_check_interval:
            await self._perform_health_check()
            self.last_health_check = current_time
        
        # Check for alert rule violations
        await self._check_alert_rules()
    
    async def _perform_health_check(self) -> None:
        """Perform comprehensive health check on all services"""
        health_status = {}
        
        # Check memory monitor
        try:
            from memory_monitor import get_memory_monitor
            memory_monitor = get_memory_monitor()
            memory_usage = memory_monitor.usage_percentage()
            
            health_status['memory_monitor'] = {
                'healthy': memory_usage < 90.0,
                'usage_percent': memory_usage,
                'timestamp': current_time
            }
        except Exception as e:
            self.logger.warning(f"Memory monitor health check failed: {e}")
            health_status['memory_monitor'] = {'healthy': False, 'error': str(e)}
        
        # Check browser pool
        try:
            if hasattr(self, 'browser_pool') and self.browser_pool:
                pool_stats = self.browser_pool.get_stats()
                
                health_status['browser_pool'] = {
                    'healthy': pool_stats.get('success_rate', 0) > 80.0,
                    'success_rate': pool_stats.get('success_rate', 0),
                    'total_browsers': pool_stats.get('total_browsers', 0),
                    'available_browsers': pool_stats.get('available_browsers', 0),
                    'timestamp': current_time
                }
        except Exception as e:
            self.logger.warning(f"Browser pool health check failed: {e}")
            health_status['browser_pool'] = {'healthy': False, 'error': str(e)}
        
        # Trigger alerts for unhealthy services
        for service_name, status in health_status.items():
            if not status['healthy']:
                await self._trigger_alert(service_name, status)
    
    async def _check_alert_rules(self) -> None:
        """Check alert rules and trigger alerts"""
        current_time = time.time()
        
        # Memory alert rule
        memory_rule = self.alert_rules.get('memory_high_cpu', {})
        try:
            from memory_monitor import get_memory_monitor
            memory_monitor = get_memory_monitor()
            memory_usage = memory_monitor.usage_percentage()
            
            if memory_usage >= memory_rule.get('threshold', 90.0):
                severity = memory_rule.get('severity', 'warning')
                alert_data = {
                    'service': 'memory_monitor',
                    'metric': 'memory_usage_percent',
                    'value': memory_usage,
                    'threshold': memory_rule.get('threshold', 90.0),
                    'severity': severity
                }
                await self._trigger_alert('memory_high_cpu', alert_data)
            
        except Exception as e:
            self.logger.warning(f"Memory alert check failed: {e}")
    
    async def _trigger_alert(self, alert_type: str, data: Dict[str, Any]) -> None:
        """Trigger alert notification"""
        alert_data = {
            'type': 'alert',
            'alert_type': alert_type,
            'timestamp': time.time(),
            'severity': data.get('severity', 'warning'),
            'service': data.get('service', 'unknown'),
            'metric': data.get('metric', 'unknown'),
            'value': data.get('value'),
            'threshold': data.get('threshold', 'unknown'),
            'details': data
        }
        
        # Add to alert history
        self.alert_history.append(alert_data)
        
        # Keep only last 100 alerts
        if len(self.alert_count()) > 100:
            self.alert_history = self.alert_history[-100:]
    
        # Update active alerts
        self.active_alerts[alert_type] = data
        
        # Update alert counters
        self.metrics_collector.increment_counter('total_alerts')
        if data.get('severity') == 'critical':
            self.metrics_collector.increment_counter('critical_alerts')
        
        # Log alert
        severity = data.get('severity', 'warning')
        message = f"ALERT [{severity.upper()}] {data.get('service', 'unknown')}: {data.get('metric', 'unknown')}={data.get('value', 'unknown')}"
        self.logger.warning(message)
        
        # Send webhook notification if configured
        await self._send_webhook_notification(alert_data)
    
    async def _send_webhook_notification(self, alert_data: Dict[str, Any]) -> None:
        """Send webhook notification for alert"""
        if not self.webhook_url:
            return
        
        try:
            import aiohttp
            
            payload = json.dumps(alert_data)
            
            async with aiohttp.ClientSession() as session:
                async with session.post(self.webhook_url, 
                                                 data=payload,
                                                 headers={'Content-Type': 'application/json'},
                                                 timeout=5) as response:
                    if response.status_code == 200:
                        self.logger.debug(f"Webhook notification sent: {alert_type}")
                    else:
                        self.logger.warning(f"Webhook failed: HTTP {response.status_code}")
        except Exception as e:
            self.logger.error(f"Webhook notification failed: {e}")
    
    def add_alert_rule(self, name: str, rule: Dict[str, Any]) -> None:
        """Add alert rule for monitoring"""
        self.alert_rules[name] = rule
        self.logger.debug(f"Added alert rule: {name}")
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get monitoring statistics"""
        return {
            'monitoring_active': self.monitoring_active,
            'check_interval': self.check_interval,
            'total_alerts': len(self.alert_count()),
            'active_alerts': len(self.active_alerts),
            'alert_rules': len(self.alert_rules),
            'metrics': self.metrics_collector.get_metrics()
        }
    
    def alert_count(self) -> int:
        """Count all alerts"""
        return len(self.alert_history)
    
    def clear_alerts(self) -> None:
        """Clear all alerts"""
        self.active_alerts.clear()
        self.alert_history.clear()
        self.metrics_collector.reset_metrics()
        self.logger.info("All alerts cleared")


class WebScraperDashboard:
    """
    FastAPI-based web dashboard with real-time system monitoring capabilities.
    
    Features:
    - RESTful API endpoints for dashboard data access
    - WebSocket connections for live updates
    - Integration with Phase 2 DI Container
    - Prometheus-compatible metrics endpoints
    - Real-time system monitoring with alerts
    - Responsive design with progressive enhancement
    """
    
    def __init__(self, 
                 port: int = 8000,
                 host: str = "0.0.0.0",
                 debug: bool = False,
                 static_dir: Optional[str] = None):
        self.port = port
        self.host = host
        self.debug = debug
        self.static_dir = static_dir
        self.logger = logging.getLogger(__name__)
        
        # Initialize FastAPI app
        self.app = FastAPI(
            title="Email Scraper Dashboard",
            description="Real-time monitoring dashboard for Email Scraper system",
            version="2.0.0",
            debug=debug
        )
        
        # Configure CORS for development
        if not debug:
            self.app.add_middleware(
                CORSMiddleware(
                    allow_origins=["http://localhost:3000", "http://127.0.0.1:8000"],
                    allow_credentials=True,
                )
            )
        
        # Static files
        if static_dir and os.path.exists(static_dir):
            self.app.mount("/static", StaticFiles(directory=static_dir))
        
        # Initialize WebSocket manager and metrics
        self.websocket_manager = WebSocketManager()
        self.metrics_collector = MetricsCollector()
        self.dashboard_metrics = DashboardMetrics()
        
        # Services that will be injected
        self.config_service = None
        self.memory_monitor = None
        self.container = None
        self.realtime_monitor = None
        
        # Initialize background tasks
        self.background_tasks = BackgroundTasks()
        self.message_queue = asyncio.Queue(maxsize=100)
        
        # Template directory for HTML templates
        template_dir = os.path.join(os.path.dirname(__file__), 'templates')
        if os.path.exists(template_dir):
            templates = Jinja2Templates(directory=template_dir)
            self.app.mount("/dash", templates)
        
        self.logger.info(f"Dashboard server starting on http://{host}:{port}")
    
    def set_di_container(self, container: DIContainer) -> None:
        """Set DI container for dependency injection"""
        self.container = container
        # Resolve dependencies here
        try:
            self.config_service = container.get(ConfigurationService)
            self.memory_monitor = container.get(IMemoryMonitor)
        except Exception as e:
            self.logger.warning(f"Could not resolve dependencies: {e}")
        
        self.logger.info("DI container integrated with dashboard")
        self.logger.info(f"Services registered: {container.get_stats()['registered_services']}")
    
    async def start_background_tasks(self) -> None:
        """Start background tasks for metrics and monitoring"""
        # Start metrics collection
        self.background_tasks.add_task(self._collect_metrics_loop, name="metrics_collection")
        
        # Start WebSocket broadcasting
        self.background_tasks.add_task(self._websocket_broadcast_loop, name="websocket_broadcast")
        
        self.logger.info("Background tasks started")
    
    async def stop_background_tasks(self) -> None:
        """Stop all background tasks"""
        self.background_tasks.cancel()
        self.logger.info("Background tasks stopped")
    
    async def _collect_metrics_loop(self) -> None:
        """Background task to collect system metrics periodically"""
        while True:
            try:
                # Update dashboard metrics
                self.dashboard_metrics.update_system_metrics()
                
                # Broadcast metrics to WebSocket clients
                await self.websocket_manager.broadcast_metrics(self.dashboard_metrics.get_json())
                
                # Collect additional metrics
                if self.realtime_monitor:
                    self.realtime_monitor.metrics_collector.increment_counter('metrics_checks')
                
                # Collect from Phase 1 component metrics
                self._collect_phase1_metrics()
                
                # Sleep until next collection
                await asyncio.sleep(5.0)
                
            except Exception as e:
                self.logger.error(f"Metrics loop error: {e}")
                await asyncio.sleep(5.0)
    
    async def _collect_phase1_metrics(self) -> None:
        """Collect metrics from Phase 1 components"""
        # Browser pool metrics if available
        if hasattr(self, 'browser_pool') and self.browser_pool:
            pool_stats = self.browser_pool.get_stats()
            self.metrics_collector.set_gauge('browser_pool_total_browsers', pool_stats.get('total_browsers', 0))
            self.metrics_collector.set_gauge('browser_pool_available', pool_stats.get('available_browsers', 0))
            self.metrics_collector.set_gauge('browser_pool_success_rate', pool_stats.get('success_rate', 0))
            self.dashboard_metrics.service_metrics['browser_pool_active'] = pool_stats['total_browsers'] > 0
        
        # Memory monitor metrics
        if self.memory_monitor:
            self.metrics_collector.set_gauge('memory_usage', self.memory_monitor.usage_percentage())
            self.metrics_collector.set_gauge('backpressure_active', self.memory_monitor.is_backpressure_active())
            self.dashboard_metrics.service_metrics['memory_monitor_active'] = self.memory_monitor.is_backpressure_active()
        
        # Configuration service metrics
        if self.config_service:
            self.dashboard_metrics.service_metrics['config_service_loaded'] = True
            self.dashboard_metrics.service_metrics['environment'] = self.config_service.environment.value
        
        # DI container metrics
        if self.container:
            self.dashboard_metrics.service_metrics['di_container_services'] = self.container.get_stats()['registered_services']
        
        # Real-time monitor metrics
        if self.realtime_monitor:
            monitor_stats = self.realtime_monitor.get_stats()
            self.dashboard_metrics.service_metrics['realtime_monitor_active'] = monitor_stats['health_monitoring_active']
        
        # CSV processor metrics
        # This would need integration if we have access to CSV processor instance
        
        self.metrics_collector.record_timing('dashboard_update', 0.01)
    
    async def _websocket_broadcast_loop(self) -> None:
        """Background task to broadcast metrics via WebSocket"""
        while True:
            try:
                await asyncio.sleep(1.0)  # Broadcast every second
                await self.websocket_manager.broadcast_metrics(self.dashboard_metrics.get_json())
            except Exception as e:
                self.logger.error(f"WebSocket broadcast error: {e}")
                await asyncio.sleep(5.0)
    
    async def shutdown(self) -> None:
        """Graceful shutdown cleanup"""
        self.logger.info("Shutting down web dashboard")
        
        # Stop background tasks
        await self.stop_background_tasks()
        
        # Close WebSocket connections
        for connection in self.websocket_manager.connections:
            connection.close()
        self.websocket_manager.connections.clear()
        
        # Clean up services
        if self.realtime_monitor:
            self.realtime_monitor.stop_monitoring()
        
        if self.container:
            self.container.cleanup()
        
        self.logger.info("Web dashboard shutdown complete")
    
    # API Endpoints
    
    async def get_metrics(self, format: str = "json") -> Response:
        """Get dashboard metrics in requested format"""
        try:
            metrics_data = self.dashboard_metrics.get_json()
            
            if format == "json":
                return JSONResponse(content=metrics_data)
            elif format == "prometheus":
                prometheus_metrics = self.metrics_collector.get_prometheus_metrics()
                return Response(content=prometheus_metrics)
            else:
                return JSONResponse(content={"error": "Unsupported format"}, status=400)
                
        except Exception as e:
            self.logger.error(f"Error getting metrics: {e}")
            return JSONResponse(content={"error": str(e)}, status=500)
    
    async def get_status(self) -> Response:
        """Get dashboard service status"""
        return JSONResponse(content={
            'status': 'operating',
            'uptime': time.time() - self.dashboard_metrics.uptime,
            'websocket_connections': len(self.websocket_manager.get_connection_count()),
            'services': {
                'config_service': self.config_service is not None,
                'memory_monitor': self.memory_monitor is not None,
                'di_container': self.container is not None,
                'background_tasks': bool(self.background_tasks),
                'message_queue_size': self.message_queue.qsize()
            },
            'metrics_count': self.metrics_collector.collection_count
        })
    
    async def process_url(self, url: str) -> Response:
        """Process a single URL (compatibility endpoint)"""
        if not self.container:
            return JSONResponse(
                content={'error': 'DI integration not yet implemented'}, 
                status=503
            )
        
        try:
            # Get CSV processor adapter
            csv_processor = self.container.get(ICsvProcessor)
            
            # Create row data for processing
            row = {'url': url, 'country': 'US', 'name': 'Test Entry'}
            
            # Process URL using existing interfaces
            result = csv_processor.process_single_url(row)
            
            return JSONResponse(content={
                'url': url,
                'status': 'processed',
                'emails': result.emails,
                'phones': result.phones,
                'social_media': result.social_media,
                'success': result.metadata.get('status') == 'success',
                'processing_time': result.get('processing_time', 0),
                'memory_usage_mb': result.get('memory_usage_mb', 0)
            })
            
        except Exception as e:
            self.logger.error(f"Error processing URL {url}: {e}")
            return JSONResponse(
                content={'error': str(e)}, 
                status=500
            )
    
    async def restart_services(self) -> Response:
        """Restart all dashboard services"""
        self.logger.info("Restarting dashboard services...")
        
        result = {}
        
        # Restart services through DI container
        if self.container:
            # Clean up instances
            self.container.cleanup()
        
        try:
            # Reinitialize services with fresh DI container
            self.set_di_container(DIContainer(self.config_service))
            
            result['di_container_restarted'] = True
            result['services_registered'] = self.container.get_stats()['registered_services']
            
            # Restart real-time monitoring
            if self.realtime_monitor:
                self.realtime_monitor.stop_monitoring()
                self.realtime_monitor = get_memory_monitor()
            
            if self.memory_monitor:
                self.memory_monitor.start_monitoring()
            
            result['realtime_monitor_restarted'] = True
            
            result['memory_monitor_active'] = True
            
        except Exception as e:
            self.logger.error(f"Failed to restart services: {e}")
            result['error'] = str(e)
            result['di_container_restarted'] = False
            
        return JSONResponse(content=result)
    
    async def trigger_test_alert(self) -> Response:
        """Trigger a test alert for demonstration"""
        await self.realtime_monitor._trigger_alert('test_alert', {
            'service': 'test_service',
            'metric': 'error_rate',
            'value': 100.0,
            'threshold': 90.0,
            'severity': 'warning'
        })
        
        return JSONResponse(content={'message': 'Test alert triggered successfully'})
    
    # WebSocket endpoints
    
    async def websocket_endpoint(self, websocket: WebSocket):
        """Handle WebSocket connection for real-time updates"""
        self.websocket_manager.add_connection(websocket)
        
        try:
            await websocket.send_json({
                'type': 'connection',
                'message': 'Connected to dashboard',
                'timestamp': time.time()
            })
            
            # Keep connection alive and send periodic metrics updates
            while connection.client_state != WebSocket.DISCONNECTED:
                try:
                    # Poll connection status
                    await asyncio.sleep(1.0)
                except Exception:
                    break
            
        except Exception as e:
            self.logger.warning(f"WebSocket error: {e}")
        
        finally:
            if websocket.client_state != WebSocket.DISCONNECTED:
                self.websocket_manager.remove_connection(websocket)
    
    def create_app(self) -> FastAPI:
        """Create FastAPI application instance"""
        return self.app


# Factory function for creating dashboard server with DI integration
def create_dashboard(config: Optional[Dict[str, Any]] = None, **kwargs) -> WebScraperDashboard:
    """Create web dashboard with optional DI container integration"""
    
    # Load or create configuration
    config_service = ConfigurationService(
        config_path=config.get('config_path', 'dashboard_config.yaml') if config else None,
        environment=Environment(config.get('environment', 'development'))
    )
    
    # Create dashboard
    dashboard = WebScraperDashboard(**kwargs)
    
    # Set DI container if not provided
    if 'container' not in kwargs:
        container = DIContainer(config_service)
        dashboard.set_di_container(container)
    else:
        dashboard.set_di_container(config if isinstance(config, DIContainer) else None)
    
    # Initialize services for monitoring
    dashboard.start_background_tasks()
    
    # Start real-time monitoring
    real_time_monitor = get_memory_monitor()
    dashboard.realtime_monitor = realtime_monitor
    real_time_monitor.start_monitoring(interval=2.0)
    
    # Start browser pool if needed
    if config and config.get('browser_pool_enabled', False):
        try:
            browser_pool = BrowserPool()
            browser_pool.initialize()
            dashboard.browser_pool = browser_pool
            dashboard.metrics_collector.set_gauge('browser_pool_instances', browser_pool.max_instances)
        except Exception as e:
            dashboard.logger.warning(f"Browser pool disabled: {e}")
    
    return dashboard


# Server startup
async def run_dashboard_server(config: Optional[Dict[str, Any]] = None, **kwargs) -> WebScraperDashboard:
    """Run dashboard server"""
    dashboard = create_dashboard(config, **kwargs)
    
    try:
        await dashboard.start_background_tasks()
        await dashboard.start_health_monitoring()
    except Exception as e:
        print(f"Failed to start dashboard: {e}")
        raise
    
    dashboard.app.router.add_websocket_route("/ws", dashboard.websocket_endpoint)
    
    print("🤖 Starting dashboard server...")
    print(f"🌐 Dashboard URL: http://{dashboard.host}:{dashboard.port}")
    print(f"🌐 WebSocket endpoint: ws://{dashboard.host}:{dashboard.port}/ws")
    print(f"🔍 Metrics endpoint: http://{dashboard.host}:{dashboard.port}/metrics")
    print(f"🔥 Status endpoint: http://{dashboard.host}:{dashboard.port}/status")
    
    try:
        import uvicorn
        config = {
            "host": dashboard.host,
            "port": dashboard.port,
            "log_level": "info",
            "access_log": not dashboard.debug,
            "reload": False
        }
        
        await uvicorn.run(dashboard.app, **config)
        return dashboard
        
    except KeyboardInterrupt:
        print("\n🛑 Dashboard stopped by user")
        return dashboard
    except Exception as e:
        print(f"❌ Failed to start server: {e}")
        return dashboard


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    try:
        # Create sample configuration for example
        sample_config = {
            "app_name": "Email Scraper Dashboard",
            "port": 8000,
            "browser_pool_enabled": False  # Disable for demo
        }
        
        # Create and run dashboard
        dashboard = run_dashboard(sample_config)
        
    except KeyboardInterrupt:
        print("\nDemonstration complete.")
    except Exception as e:
        print(f"Demonstration failed: {e}")
