"""
Browser Pool Service - Efficient Browser Instance Management

Implements browser instance pooling with:
- Pre-warmed instances (2-10 browsers)
- Health checks and automatic recycling
- Adaptive scaling based on load
- State isolation between tasks
- Memory and performance monitoring
"""

import asyncio
import logging
import time
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from queue import Queue, Empty
from typing import Dict, List, Optional, Set, Callable, Any
import psutil
from playwright.async_api import async_playwright, Browser, BrowserContext, Page


@dataclass
class BrowserMetrics:
    """Metrics for browser instance health monitoring"""
    pid: int
    memory_mb: float
    page_count: int
    last_used: float
    error_count: int
    startup_time: float
    
    def is_healthy(self, memory_limit_mb: float = 500, max_pages: int = 10) -> bool:
        """Check if browser instance is healthy"""
        age_seconds = time.time() - self.startup_time
        return (
            self.memory_mb < memory_limit_mb and
            self.page_count <= max_pages and
            self.error_count < 5 and
            age_seconds < 3600  # Max 1 hour lifetime
        )


@dataclass
class BrowserHandle:
    """Wrapper for browser instance with state management"""
    browser: Browser
    context: BrowserContext
    metrics: BrowserMetrics
    lock: threading.Lock
    
    def __init__(self, browser: Browser, context: BrowserContext):
        self.browser = browser
        self.context = context
        self.lock = threading.Lock()
        self.metrics = BrowserMetrics(
            pid=browser.process.pid,
            memory_mb=0,
            page_count=0,
            last_used=time.time(),
            error_count=0,
            startup_time=time.time()
        )
    
    async def cleanup_state(self):
        """Reset browser state between tasks"""
        try:
            # Clear cookies and localStorage
            await self.context.clear_cookies()
            await self.context.clear_permissions()
            
            # Close all pages except one
            pages = self.context.pages
            while len(pages) > 1:
                await pages[-1].close()
                pages.pop()
            
            # Reset page for next task
            if pages:
                await pages[0].goto('about:blank')
            
            # Update metrics
            self.metrics.page_count = len(pages)
            self.metrics.last_used = time.time()
            
        except Exception as e:
            self.metrics.error_count += 1

    async def update_metrics(self):
        """Update browser health metrics"""
        try:
            process = psutil.Process(self.metrics.pid)
            self.metrics.memory_mb = process.memory_info().rss / 1024 / 1024
            self.metrics.page_count = len(self.context.pages)
        except Exception:
            self.metrics.error_count += 1


class BrowserHealthChecker:
    """Browser health monitoring and automatic replacement"""
    
    def __init__(self, check_interval: float = 60.0):
        self.check_interval = check_interval
        self.health_history: Dict[int, List] = {}
        self.logger = logging.getLogger(__name__)
    
    async def is_healthy(self, handle: BrowserHandle) -> bool:
        """Check browser health with multiple metrics"""
        try:
            # Update metrics first
            await handle.update_metrics()
            
            # Check basic health criteria
            is_healthy = handle.metrics.is_healthy()
            
            # Track health history
            pid = handle.metrics.pid
            if pid not in self.health_history:
                self.health_history[pid] = deque(maxlen=10)
            
            self.health_history[pid].append({
                'timestamp': time.time(),
                'healthy': is_healthy,
                'memory_mb': handle.metrics.memory_mb,
                'page_count': handle.metrics.page_count
            })
            
            return is_healthy
            
        except Exception as e:
            self.logger.warning(f"Health check failed for browser {handle.metrics.pid}: {e}")
            return False
    
    def get_health_trend(self, pid: int) -> float:
        """Get health trend (0-1, where 1 = perfect health)"""
        if pid not in self.health_history or len(self.health_history[pid]) < 3:
            return 1.0
        
        recent_checks = list(self.health_history[pid])[-5:]
        healthy_count = sum(1 for check in recent_checks if check['healthy'])
        return healthy_count / len(recent_checks)


class BrowserPool:
    """
    Efficient browser pool with adaptive scaling and health monitoring.
    
    Features:
    - Pre-warmed browser instances
    - Automatic scaling (2-10 instances)
    - Health monitoring and replacement
    - State isolation and cleanup
    - Performance metrics tracking
    """
    
    def __init__(self, 
                 min_instances: int = 2, 
                 max_instances: int = 10,
                 health_check_interval: float = 60.0,
                 browser_type: str = 'chromium'):
        
        self.min_instances = min_instances
        self.max_instances = max_instances
        self.health_check_interval = health_check_interval
        self.browser_type = browser_type
        
        self.available_browsers: Queue = Queue(maxsize=max_instances)
        self.busy_browsers: Set[BrowserHandle] = set()
        self.all_browsers: Set[BrowserHandle] = set()
        
        # Services
        self.health_checker = BrowserHealthChecker(health_check_interval)
        self.logger = logging.getLogger(__name__)
        
        # Scaling and monitoring
        self._scale_lock = threading.Lock()
        self._monitor_task: Optional[asyncio.Task] = None
        self._playwright_stop_event = asyncio.Event()
        
        # Statistics
        self.stats = {
            'browsers_created': 0,
            'browsers_replaced': 0,
            'browsers_retired': 0,
            'total_requests': 0,
            'healthy_requests': 0,
            'failed_requests': 0
        }
    
    async def initialize(self):
        """Initialize browser pool with pre-warmed instances"""
        self.logger.info(f"Initializing browser pool: {self.min_instances} pre-warmed instances")
        
        try:
            self.playwright = await async_playwright().start()
            
            # Pre-warm minimum browsers
            for i in range(self.min_instances):
                handle = await self._create_browser()
                if handle:
                    self.available_browsers.put(handle)
                    self.all_browsers.add(handle)
                    self.stats['browsers_created'] += 1
            
            # Start health monitoring
            self._monitor_task = asyncio.create_task(self._health_monitor_loop())
            
            self.logger.info(f"Browser pool initialized: {len(self.all_browsers)} instances ready")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize browser pool: {e}")
            raise
    
    async def _create_browser(self) -> Optional[BrowserHandle]:
        """Create new browser instance with setup"""
        try:
            with self._scale_lock:
                # Check scaling limits
                if len(self.all_browsers) >= self.max_instances:
                    return None
                
                # Launch browser with stealth options
                launch_options = {
                    'headless': True,
                    'args': [
                        '--no-sandbox',
                        '--disable-dev-shm-usage',
                        '--disable-web-security',
                        '--disable-features=VizDisplayCompositor',
                        '--disable-background-timer-throttling',
                        '--disable-renderer-backgrounding',
                        '--disable-backgrounding-occluded-windows',
                        '--disable-ipc-flooding-protection'
                    ]
                }
                
                browser = await getattr(self.playwright, self.browser_type).launch(**launch_options)
                context = await browser.new_context(
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    viewport={'width': 1920, 'height': 1080},
                    java_script_enabled=True
                )
                
                handle = BrowserHandle(browser, context)
                
                # Pre-warm with a blank page
                page = await context.new_page()
                await page.goto('about:blank', timeout=5000)
                await page.close()
                
                handle.metrics.startup_time = time.time()
                
                self.logger.debug(f"Created browser {handle.metrics.pid}")
                return handle
                
        except Exception as e:
            self.logger.error(f"Failed to create browser: {e}")
            return None
    
    async def get_browser(self, timeout: float = 30.0) -> BrowserHandle:
        """Get browser from pool with automatic scaling"""
        self.stats['total_requests'] += 1
        
        # Check if need to scale up
        if self.available_browsers.empty() and len(self.busy_browsers) < self.max_instances:
            await self._scale_up()
        
        try:
            # Get browser or timeout
            handle = self.available_browsers.get(timeout=timeout)
            
            # Health check before returning
            if not await self.health_checker.is_healthy(handle):
                self.logger.warning(f"Browser {handle.metrics.pid} unhealthy, replacing")
                await self._replace_browser(handle)
                return await self.get_browser(timeout)
            
            # Mark as busy and cleanup state
            self.busy_browsers.add(handle)
            await handle.cleanup_state()
            
            handle.metrics.last_used = time.time()
            self.stats['healthy_requests'] += 1
            
            return handle
            
        except Empty:
            self.stats['failed_requests'] += 1
            raise TimeoutError(f"No available browser after {timeout}s")
    
    async def return_browser(self, handle: BrowserHandle):
        """Return browser to pool with cleanup and state reset"""
        if handle not in self.busy_browsers:
            return  # Already returned or not from pool
        
        # Clean up browser state
        await handle.cleanup_state()
        
        # Return to available pool
        self.busy_browsers.remove(handle)
        self.available_browsers.put(handle)
        
        # Check if we should scale down
        if self.available_browsers.qsize() > self.min_instances:
            await self._scale_down()
    
    async def _replace_browser(self, handle: BrowserHandle):
        """Replace unhealthy browser with new instance"""
        if handle in self.all_browsers:
            self.all_browsers.remove(handle)
        
        if handle in self.busy_browsers:
            self.busy_browsers.remove(handle)
        
        # Close old browser
        try:
            await handle.context.close()
            await handle.browser.close()
        except Exception as e:
            self.logger.warning(f"Error closing browser {handle.metrics.pid}: {e}")
        
        # Create replacement
        new_handle = await self._create_browser()
        if new_handle:
            self.all_browsers.add(new_handle)
            self.available_browsers.put(new_handle)
            self.stats['browsers_replaced'] += 1
            self.logger.info(f"Replaced browser {handle.metrics.pid} with {new_handle.metrics.pid}")
        else:
            self.logger.error(f"Failed to create replacement for browser {handle.metrics.pid}")
    
    async def _scale_up(self):
        """Scale up pool by creating new browser instance"""
        with self._scale_lock:
            if len(self.all_browsers) < self.max_instances:
                handle = await self._create_browser()
                if handle:
                    self.all_browsers.add(handle)
                    self.available_browsers.put(handle)
                    self.stats['browsers_created'] += 1
                    self.logger.info(f"Scaled up browser pool: {len(self.all_browsers)} instances")
    
    async def _scale_down(self):
        """Scale down pool by retiring idle browser"""
        with self._scale_lock:
            if len(self.all_browsers) > self.min_instances:
                try:
                    handle = self.available_browsers.get_nowait()
                    if handle in self.all_browsers:
                        self.all_browsers.remove(handle)
                    
                    # Close retired browser
                    await handle.context.close()
                    await handle.browser.close()
                    
                    self.stats['browsers_retired'] += 1
                    self.logger.info(f"Scaled down browser pool: {len(self.all_browsers)} instances")
                    
                except Empty:
                    pass
    
    async def _health_monitor_loop(self):
        """Continuous health monitoring with automatic replacement"""
        self.logger.info("Starting browser health monitoring")
        
        while not self._playwright_stop_event.is_set():
            try:
                await asyncio.sleep(self.health_check_interval)
                
                unhealthy_browsers = []
                for handle in list(self.all_browsers):
                    if not await self.health_checker.is_healthy(handle):
                        unhealthy_browsers.append(handle)
                
                # Replace unhealthy browsers
                for handle in unhealthy_browsers:
                    self.logger.warning(f"Replacing unhealthy browser {handle.metrics.pid}")
                    await self._replace_browser(handle)
                
                # Log pool status
                if self.logger.isEnabledFor(logging.DEBUG):
                    self.logger.debug(
                        f"Pool status: {len(self.all_browsers)} total, "
                        f"{self.available_browsers.qsize()} available, "
                        f"{len(self.busy_browsers)} busy"
                    )
                    
            except Exception as e:
                self.logger.error(f"Health monitoring error: {e}")
    
    async def close(self):
        """Close all browser instances and cleanup resources"""
        self.logger.info("Closing browser pool")
        
        # Stop health monitoring
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        
        # Close all browsers
        for handle in list(self.all_browsers):
            try:
                await handle.context.close()
                await handle.browser.close()
            except Exception as e:
                self.logger.warning(f"Error closing browser {handle.metrics.pid}: {e}")
        
        self.all_browsers.clear()
        self.busy_browsers.clear()
        
        # Clear queue
        while True:
            try:
                self.available_browsers.get_nowait()
            except Empty:
                break
        
        # Stop playwright
        try:
            await self.playwright.stop()
        except Exception as e:
            self.logger.warning(f"Error stopping playwright: {e}")
        
        self._playwright_stop_event.set()
        self.logger.info("Browser pool closed")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get pool statistics"""
        return {
            **self.stats,
            'total_browsers': len(self.all_browsers),
            'available_browsers': self.available_browsers.qsize(),
            'busy_browsers': len(self.busy_browsers),
            'success_rate': (
                self.stats['healthy_requests'] / max(1, self.stats['total_requests']) * 100
            )
        }
    
    def get_memory_usage_mb(self) -> float:
        """Get total memory usage of all browsers"""
        total_mb = 0
        for handle in self.all_browsers:
            total_mb += handle.metrics.memory_mb
        return total_mb


# Context manager for browser pool usage
async def with_browser_pool(pool: BrowserPool, timeout: float = 30.0):
    """Context manager for browser pool usage"""
    handle = await pool.get_browser(timeout)
    try:
        yield handle
    finally:
        await pool.return_browser(handle)


# Backward compatibility adapter for existing WebScraper
class BackwardCompatibleBrowserPool:
    """
    Browser pool adapter that maintains existing WebScraper interface
    while using the new browser pool internally.
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.pool = BrowserPool(
            min_instances=config.get('min_browsers', 2),
            max_instances=config.get('max_browsers', 10),
            browser_type=config.get('browser_type', 'chromium')
        )
        self.logger = logging.getLogger(__name__)
    
    async def initialize(self):
        """Initialize the browser pool"""
        await self.pool.initialize()
    
    def scrape_url(self, url: str, **kwargs) -> Dict[str, Any]:
        """
        Backward compatible scrape_url method (sync interface).
        Uses browser pool internally while maintaining existing API.
        """
        async def _async_scrape():
            async with with_browser_pool(self.pool) as handle:
                try:
                    # Reuse existing scraping logic but with pooled browser
                    page = await handle.context.new_page()
                    
                    # Set timeout and other options from kwargs
                    timeout = kwargs.get('timeout', 30000)
                    page.set_default_timeout(timeout)
                    
                    # Navigate to URL
                    response = await page.goto(url, waitUntil='networkidle')
                    
                    # Extract basic info (compatible with existing API)
                    html_content = await page.content()
                    title = await page.title()
                    final_url = page.url
                    
                    # Close page
                    await page.close()
                    
                    return {
                        'status': response.status if response else 0,
                        'html': html_content,
                        'title': title,
                        'final_url': final_url,
                        'error': None,
                        'load_time': time.time(),
                        'proxy_used': False,
                        'pages_scraped': 1
                    }
                    
                except Exception as e:
                    self.logger.error(f"Scraping failed for {url}: {e}")
                    return {
                        'status': 0,
                        'html': '',
                        'title': '',
                        'final_url': url,
                        'error': str(e),
                        'load_time': time.time(),
                        'proxy_used': False,
                        'pages_scraped': 0
                    }
        
        # Run async function in sync context
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # Already in async context, create new loop
                return asyncio.run(_async_scrape())
            else:
                # Use existing loop
                return loop.run_until_complete(_async_scrape())
        except Exception as e:
            self.logger.error(f"Failed to execute async scraping: {e}")
            # Fallback result
            return {
                'status': 0,
                'html': '',
                'title': '',
                'final_url': url,
                'error': f"Async execution failed: {str(e)}",
                'load_time': time.time(),
                'proxy_used': False,
                'pages_scraped': 0
            }
    
    def close(self):
        """Close browser pool (sync interface)"""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # Already in async context, create new loop
                asyncio.run(self.pool.close())
            else:
                # Use existing loop
                loop.run_until_complete(self.pool.close())
        except Exception as e:
            self.logger.error(f"Failed to close browser pool: {e}")
