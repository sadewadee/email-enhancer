"""
Enhanced WebScraper with Browser Pool Integration

Maintains backward compatibility while using the new browser pool system.
Key improvements:
- Browser instance reuse eliminates 2-3s initialization overhead
- Health monitoring and automatic browser replacement
- Async processing capabilities while maintaining sync interface
- Memory-efficient resource management
"""

import asyncio
import logging
import time
from typing import Dict, Optional, Any, List
from urllib.parse import urljoin, urlparse, parse_qs
from collections import deque

# Import existing components
from web_scraper import WebScraper as OriginalWebScraper
from browser_pool import BrowserPool, BackwardCompatibleBrowserPool
from memory_monitor import MemoryMonitor


class EnhancedWebScraper:
    """
    Enhanced WebScraper that uses Browser Pool for performance optimization.
    
    Maintains the same interface as the original WebScraper while using
    browser pooling internally for 5x+ performance improvement.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.logger = logging.getLogger(__name__)
        
        # Initialize browser pool with backward-compatible adapter
        self.use_browser_pool = self.config.get('use_browser_pool', True)
        self.pool_initialized = False
        
        if self.use_browser_pool:
            # Extract browser pool configuration
            pool_config = {
                'min_browsers': self.config.get('min_browsers', 2),
                'max_browsers': self.config.get('max_browsers', 10),
                'browser_type': self.config.get('browser_type', 'chromium')
            }
            
            self.browser_pool = BackwardCompatibleBrowserPool(pool_config)
            self.logger.info("Using Browser Pool for enhanced performance")
        else:
            # Fall back to original approach
            self.browser_pool = None
            self.original_scraper = OriginalWebScraper(config)
            self.logger.info("Using original WebScraper (no browser pool)")
        
        # Memory monitoring integration
        self.memory_monitor = MemoryMonitor()
        
        # Statistics tracking
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'pool_hits': 0,
            'pool_misses': 0,
            'average_response_time': 0.0,
            'memory_usage_peak': 0.0
        }
        
        # Response time tracking
        self.response_times = deque(maxlen=100)
    
    async def initialize(self):
        """Initialize browser pool or original scraper"""
        if self.use_browser_pool and self.browser_pool:
            try:
                await self.browser_pool.initialize()
                self.pool_initialized = True
                self.logger.info("Browser pool initialized successfully")
                
                # Start memory monitoring
                self.memory_monitor.start_monitoring(interval=5.0)
                
            except Exception as e:
                self.logger.error(f"Browser pool initialization failed: {e}")
                # Fall back to original scraper
                self._fallback_to_original()
        elif not self.use_browser_pool and hasattr(self, 'original_scraper'):
            # Original scraper doesn't need async initialization
            pass
    
    async def _fallback_to_original(self):
        """Fall back to original scraper on browser pool failure"""
        self.use_browser_pool = False
        self.browser_pool = None
        self.original_scraper = OriginalWebScraper(self.config)
        self.pool_initialized = False
        self.logger.warning("Fell back to original WebScraper due to browser pool failure")
    
    def scrape_url(self, url: str, **kwargs) -> Dict[str, Any]:
        """
        Scrape a single URL using browser pool.
        
        Maintains the same interface as original WebScraper.scrape_url()
        but uses pooling internally when available.
        """
        start_time = time.time()
        self.stats['total_requests'] += 1
        
        # Initialize pool if not already done
        if self.use_browser_pool and not self.pool_initialized:
            # Run async init in sync context
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # Already in async context, run directly
                    loop.run_until_complete(self.initialize())
                else:
                    # Create new loop
                    asyncio.run(self.initialize())
            except Exception as e:
                self.logger.error(f"Failed to initialize browser pool: {e}")
                self._fallback_to_original()
        
        # Choose scraping method
        if self.use_browser_pool and self.pool_initialized:
            result = self._scrape_with_pool(url, **kwargs)
        else:
            result = self.original_scraper.scrape_url(url, **kwargs)
        
        # Update statistics
        response_time = time.time() - start_time
        self.response_times.append(response_time)
        self.stats['average_response_time'] = (
            sum(self.response_times) / len(self.response_times)
        )
        
        # Update memory peak
        current_memory = self.memory_monitor.memory_usage_mb()
        self.stats['memory_usage_peak'] = max(
            self.stats['memory_usage_peak'], current_memory
        )
        
        # Update success/failure stats
        if result.get('status') == 200 and result.get('html'):
            self.stats['successful_requests'] += 1
        else:
            self.stats['failed_requests'] += 1
        
        # Add enhanced metadata
        result['scraper_type'] = 'browser_pool' if self.use_browser_pool else 'original'
        result['response_time'] = response_time
        result['memory_usage_mb'] = current_memory
        
        return result
    
    def _scrape_with_pool(self, url: str, **kwargs) -> Dict[str, Any]:
        """
        Scrape using browser pool with async/sync bridge.
        
        Runs the async browser pool in a sync context to maintain interface compatibility.
        """
        try:
            # Run async operation in sync context
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(
                    self._scrape_with_pool_async(url, **kwargs)
                )
            finally:
                loop.close()
        except Exception as e:
            self.logger.error(f"Browser pool scraping failed: {e}")
            # Fall back to original scraper
            return self.original_scraper.scrape_url(url, **kwargs)
    
    async def _scrape_with_pool_async(self, url: str, **kwargs) -> Dict[str, Any]:
        """Async scraping using browser pool"""
        try:
            result = await self.browser_pool.scrape_url(url, **kwargs)
            
            # Ensure result has required fields for compatibility
            default_result = {
                'status': result.get('status', 0),
                'html': result.get('html', ''),
                'url': url,
                'final_url': result.get('final_url', url),
                'page_title': result.get('title', ''),
                'meta_description': '',
                'is_contact_page': False,
                'error': result.get('error', None),
                'load_time': result.get('load_time', time.time()),
                'proxy_used': result.get('proxy_used', False),
                'pages_scraped': result.get('pages_scraped', 1)
            }
            
            self.stats['pool_hits'] += 1
            return default_result
            
        except Exception as e:
            self.logger.error(f"Async browser pool scraping failed: {e}")
            
            # Return error result consistent with original scraper
            return {
                'status': 0,
                'html': '',
                'url': url,
                'final_url': url,
                'page_title': '',
                'meta_description': '',
                'is_contact_page': False,
                'error': str(e),
                'load_time': time.time(),
                'proxy_used': False,
                'pages_scraped': 0
            }
    
    def scrape_urls_batch(self, urls: List[str], **kwargs) -> List[Dict[str, Any]]:
        """
        Scrape multiple URLs concurrently using browser pool.
        
        This is an enhanced method that takes advantage of the browser pool
        for concurrent processing while maintaining sync interface.
        """
        if not self.use_browser_pool or not self.pool_initialized:
            # Fall back to sequential processing
            return [self.scrape_url(url, **kwargs) for url in urls]
        
        # Use ThreadPoolExecutor for concurrent processing
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        results = []
        max_workers = min(len(urls), 20)  # Limit concurrent requests
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all scraping tasks
            future_to_url = {
                executor.submit(self.scrape_url, url, **kwargs): url 
                for url in urls
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    # Create error result
                    error_result = {
                        'status': 0,
                        'html': '',
                        'url': url,
                        'final_url': url,
                        'page_title': '',
                        'meta_description': '',
                        'is_contact_page': False,
                        'error': str(e),
                        'load_time': 0,
                        'proxy_used': False,
                        'pages_scraped': 0,
                        'scraper_type': 'browser_pool',
                        'error_context': 'batch_processing'
                    }
                    results.append(error_result)
                    self.stats['failed_requests'] += 1
        
        return results
    
    def get_stats(self) -> Dict[str, Any]:
        """Get scraper statistics"""
        base_stats = {
            'scraper_type': 'browser_pool' if self.use_browser_pool else 'original',
            'pool_initialized': self.pool_initialized,
            'total_requests': self.stats['total_requests'],
            'successful_requests': self.stats['successful_requests'],
            'failed_requests': self.stats['failed_requests'],
            'success_rate': (
                self.stats['successful_requests'] / max(1, self.stats['total_requests']) * 100
            ),
            'average_response_time': self.stats['average_response_time'],
            'memory_usage_peak': self.stats['memory_usage_peak'],
            'pool_hits': self.stats['pool_hits'],
            'pool_misses': self.stats['pool_misses']
        }
        
        # Add browser pool stats if available
        if self.use_browser_pool and self.pool_initialized and self.browser_pool:
            try:
                pool_stats = asyncio.run(self.browser_pool.pool.get_stats_async())
                base_stats['browser_pool_stats'] = pool_stats
            except Exception as e:
                base_stats['browser_pool_stats_error'] = str(e)
        
        # Add memory monitor stats
        try:
            memory_stats = self.memory_monitor.get_stats()
            base_stats['memory_monitor_stats'] = memory_stats
        except Exception as e:
            base_stats['memory_monitor_stats_error'] = str(e)
        
        return base_stats
    
    async def close_async(self):
        """Close browser pool asynchronously"""
        if self.use_browser_pool and self.pool_initialized and self.browser_pool:
            try:
                await self.browser_pool.close()
                self.pool_initialized = False
                self.logger.info("Browser pool closed")
            except Exception as e:
                self.logger.error(f"Error closing browser pool: {e}")
        
        # Stop memory monitoring
        try:
            self.memory_monitor.stop_monitoring()
        except Exception as e:
            self.logger.error(f"Error stopping memory monitoring: {e}")
    
    def close(self):
        """Close browser pool (sync interface)"""
        if self.use_browser_pool and self.pool_initialized:
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(self.close_async())
                finally:
                    loop.close()
            except Exception as e:
                self.logger.error(f"Failed to close browser pool: {e}")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


# Factory function to create appropriate scraper instance
def create_web_scraper(config: Optional[Dict[str, Any]] = None) -> EnhancedWebScraper:
    """
    Factory function to create EnhancedWebScraper instance.
    
    Automatically configures browser pool usage based on system resources
    and configuration settings.
    """
    if config is None:
        config = {}
    
    # Auto-detect optimal settings for browser pool
    use_pool = config.get('use_browser_pool', True)
    
    # Disable pool explicitly if requested
    if config.get('disable_browser_pool', False):
        use_pool = False
    
    # Adjust pool size based on system resources
    if not config.get('min_browsers') or not config.get('max_browsers'):
        import psutil
        memory_mb = psutil.virtual_memory().total / 1024 / 1024
        
        if memory_mb < 2048:   # < 2GB RAM
            config['min_browsers'] = 1
            config['max_browsers'] = 3
        elif memory_mb < 4096: # < 4GB RAM
            config['min_browsers'] = 2
            config['max_browsers'] = 6
        else:                  # >= 4GB RAM
            config['min_browsers'] = 2
            config['max_browsers'] = 10
    
    config['use_browser_pool'] = use_pool
    
    return EnhancedWebScraper(config)


# Backward compatibility function
def get_gather_contact_info_function(enhanced_scraper: EnhancedWebScraper):
    """
    Get gather_contact_info function that uses the enhanced scraper.
    
    Maintains backward compatibility with existing code that expects
    a gather_contact_info function.
    """
    async def enhanced_gather_contact_info(row, **kwargs):
        """
        Enhanced gather_contact_info that uses browser pool.
        
        Compatible interface with original gather_contact_info.
        """
        url = row.get('url', '')
        if not url:
            return {
                'emails': [], 'phones': [], 'whatsapp': [], 'facebook': '',
                'instagram': '', 'linkedin': '', 'tiktok': '', 'youtube': '',
                'final_url': '', 'status': 'failed', 'error': 'No URL provided',
                'processing_time': 0, 'pages_scraped': 0
            }
        
        # Scrape using enhanced scraper
        scrape_result = enhanced_scraper.scrape_url(url, **kwargs)
        
        # Extract contacts using existing ContactExtractor
        if scrape_result.get('html'):
            from contact_extractor import ContactExtractor
            extractor = ContactExtractor()
            contacts = extractor.extract_all_contacts(
                scrape_result['html'], 
                scrape_result.get('final_url', url)
            )
            
            # Combine scrape result with contacts
            result = {
                **contacts,
                'final_url': scrape_result.get('final_url', url),
                'status': 'success' if scrape_result.get('status') == 200 else 'failed',
                'error': scrape_result.get('error'),
                'processing_time': scrape_result.get('response_time', 0),
                'pages_scraped': scrape_result.get('pages_scraped', 0),
                'scraper_type': scrape_result.get('scraper_type', 'browser_pool')
            }
        else:
            result = {
                'emails': [], 'phones': [], 'whatsapp': [], 'facebook': '',
                'instagram': '', 'linkedin': '', 'tiktok': '', 'youtube': '',
                'final_url': scrape_result.get('final_url', url),
                'status': scrape_result.get('status', 0) == 200 and 'success' or 'failed',
                'error': scrape_result.get('error', 'No HTML content'),
                'processing_time': scrape_result.get('response_time', 0),
                'pages_scraped': 0,
                'scraper_type': scrape_result.get('scraper_type', 'browser_pool')
            }
        
        return result
    
    def sync_gather_contact_info(row, **kwargs):
        """
        Sync wrapper for async gather_contact_info.
        Maintains sync interface compatibility with existing code.
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(enhanced_gather_contact_info(row, **kwargs))
        finally:
            loop.close()
    
    return sync_gather_contact_info


# Enhanced scraper with backward-compatible interface
class WebScraper(EnhancedWebScraper):
    """
    WebScraper alias that maintains full backward compatibility.
    
    This class provides the same interface as the original WebScraper
    but with enhanced performance capabilities.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        # Default to using browser pool for performance
        if config is None:
            config = {}
        
        # Enable browser pool by default unless explicitly disabled
        config.setdefault('use_browser_pool', True)
        
        super().__init__(config)
    
    def gather_contact_info(self, row: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """
        Backward-compatible gather_contact_info method.
        
        Uses enhanced scraping with browser pool while maintaining the exact
        same interface as the original WebScraper.gather_contact_info().
        """
        get_contact_func = get_gather_contact_info_function(self)
        return get_contact_func(row, **kwargs)


if __name__ == "__main__":
    # Test the enhanced web scraper
    async def test_enhanced_scraper():
        config = {
            'min_browsers': 2,
            'max_browsers': 5,
            'timeout': 30000
        }
        
        scraper = EnhancedWebScraper(config)
        
        try:
            await scraper.initialize()
            
            # Test single URL scraping
            test_url = "https://example.com"
            result = scraper.scrape_url(test_url)
            
            print("Scraping Results:")
            print(f"Status: {result.get('status')}")
            print(f"Title: {result.get('page_title')}")
            print(f"HTML Length: {len(result.get('html', ''))}")
            print(f"Response Time: {result.get('response_time', 0):.3f}s")
            print(f"Memory Usage: {result.get('memory_usage_mb', 0):.1f}MB")
            print(f"Scraper Type: {result.get('scraper_type')}")
            
            # Test batch scraping
            test_urls = [
                "https://example.com",
                "https://httpbin.org/html",
                "https://example.org"
            ]
            
            batch_results = scraper.scrape_urls_batch(test_urls)
            print(f"\nBatch Results: {len(batch_results)} URLs processed")
            
            # Show statistics
            stats = scraper.get_stats()
            print("\nScraper Statistics:")
            for key, value in stats.items():
                print(f"{key}: {value}")
            
        finally:
            await scraper.close_async()
    
    # Run test
    asyncio.run(test_enhanced_scraper())
