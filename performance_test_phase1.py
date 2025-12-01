#!/usr/bin/env python3
"""
Performance Testing & Validation Script - Phase 1 Performance Improvements

Tests and validates the performance improvements from Phase 1:
1. Browser Pool Service - efficient browser instance reuse
2. Memory Leak Fixes - bounded collections
3. Streaming CSV Processor - memory-efficient chunked processing
4. Memory Monitoring Service - real-time resource tracking
5. Enhanced WebScraper - browser pool integration
6. Enhanced CSV Processor - streaming integration

Usage:
    python performance_test_phase1.py [--component browser_pool|streaming|memory_monitor|all] [--sample-size 100]
"""

import asyncio
import logging
import os
import sys
import time
import tempfile
import json
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import pandas as pd
import psutil

# Ensure project root is in Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from browser_pool import BrowserPool, BackwardCompatibleBrowserPool
from memory_monitor import MemoryMonitor, BackpressureController
from streaming_csv_processor import StreamingCSVProcessor
from enhanced_web_scraper import EnhancedWebScraper, create_web_scraper
from enhanced_csv_processor import EnhancedCSVProcessor, create_csv_processor


@dataclass
class PerformanceTestResult:
    """Results from performance testing"""
    test_name: str
    component_type: str
    metrics: Dict[str, Any]
    success: bool
    error_message: Optional[str] = None
    duration_seconds: float = 0.0


class PerformanceTester:
    """
    Comprehensive performance testing suite for Phase 1 improvements.
    
    Tests each component individually and shows before/after comparisons.
    """
    
    def __init__(self, sample_size: int = 100):
        self.sample_size = sample_size
        self.logger = self._setup_logging()
        self.test_results: List[PerformanceTestResult] = []
        
        # Test data
        self.test_urls = [
            "https://example.com",
            "https://httpbin.org/html", 
            "https://example.org",
            "https://jsonplaceholder.typicode.com",
            "https://reqres.in"
        ]
        
        # Memory monitoring
        self.memory_monitor = MemoryMonitor()
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging for performance testing"""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        # Console handler
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
    
    def create_test_csv(self, num_rows: int, filename: str) -> str:
        """Create test CSV file for streaming tests"""
        test_data = []
        for i in range(num_rows):
            url = self.test_urls[i % len(self.test_urls)]
            test_data.append({
                'url': url,
                'name': f'Test Company {i}',
                'category': 'test',
                'address': f'Address {i}',
                'phone': f'+12345678{i:04d}',
                'email': f'test{i}@example.com'
            })
        
        df = pd.DataFrame(test_data)
        df.to_csv(filename, index=False)
        return filename
    
    async def test_browser_pool_performance(self) -> PerformanceTestResult:
        """Test Browser Pool performance and memory efficiency"""
        self.logger.info("🧪 Testing Browser Pool Performance...")
        
        config = {
            'min_instances': 2,
            'max_instances': 5,
            'browser_type': 'chromium'
        }
        
        test_name = f"BrowserPool-{self.sample_size}_urls"
        start_time = time.time()
        
        try:
            # Test with browser pool
            pool = BrowserPool(**config)
            await pool.initialize()
            
            # Performance metrics
            start_memory = self.memory_monitor.memory_usage_mb()
            total_scrape_time = 0
            successful_scrapes = 0
            
            # Test URL scraping
            for i, url in enumerate(self.test_urls[:min(10, self.sample_size)]):
                try:
                    scrape_start = time.time()
                    
                    async with pool.get_browser() as handle:
                        page = await handle.context.new_page()
                        await page.goto(url, timeout=5000)
                        content = await page.content()
                        await page.close()
                    
                    scrape_time = time.time() - scrape_start
                    total_scrape_time += scrape_time
                    
                    if content:
                        successful_scrapes += 1
                    
                    self.logger.info(f"  URL {i+1}: {url} ({scrape_time:.2f}s, {len(content)} chars)")
                    
                except Exception as e:
                    self.logger.warning(f"  URL {i+1} failed: {e}")
            
            # Get final metrics
            end_memory = self.memory_monitor.memory_usage_mb()
            pool_stats = pool.get_stats()
            
            await pool.close()
            
            test_duration = time.time() - start_time
            
            metrics = {
                'urls_tested': min(10, self.sample_size),
                'successful_scrapes': successful_scrapes,
                'success_rate': successful_scrapes / min(10, self.sample_size) * 100,
                'avg_scrape_time': total_scrape_time / max(1, successful_scrapes),
                'memory_increase_mb': end_memory - start_memory,
                'pool_stats': pool_stats,
                'throughput_urls_per_min': (min(10, self.sample_size) / test_duration) * 60
            }
            
            result = PerformanceTestResult(
                test_name=test_name,
                component_type="BrowserPool",
                metrics=metrics,
                success=True,
                duration_seconds=test_duration
            )
            
            self.logger.info(f"✅ Browser Pool Test Completed:")
            self.logger.info(f"  Success Rate: {metrics['success_rate']:.1f}%")
            self.logger.info(f"  Avg Scrape Time: {metrics['avg_scrape_time']:.2f}s")
            self.logger.info(f"  Memory Increase: {metrics['memory_increase_mb']:.1f}MB")
            self.logger.info(f"  Throughput: {metrics['throughput_urls_per_min']:.1f} URLs/min")
            
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Browser Pool Test Failed: {e}")
            return PerformanceTestResult(
                test_name=test_name,
                component_type="BrowserPool",
                metrics={},
                success=False,
                error_message=str(e),
                duration_seconds=time.time() - start_time
            )
    
    async def test_enhanced_web_scraper_performance(self) -> PerformanceTestResult:
        """Test Enhanced WebScraper with browser pool"""
        self.logger.info("🧪 Testing Enhanced WebScraper Performance...")
        
        config = {
            'min_browsers': 2,
            'max_browsers': 5,
            'use_browser_pool': True
        }
        
        test_name = f"EnhancedWebScraper-{self.sample_size}_urls"
        start_time = time.time()
        
        try:
            scraper = EnhancedWebScraper(config)
            await scraper.initialize()
            
            start_memory = self.memory_monitor.memory_usage_mb()
            total_response_time = 0
            successful_scrapes = 0
            
            # Test URL scraping
            test_urls = self.test_urls[:min(20, self.sample_size)]
            results = scraper.scrape_urls_batch(test_urls)
            
            for result in results:
                response_time = result.get('response_time', 0)
                total_response_time += response_time
                
                if result.get('status') == 200 and result.get('html'):
                    successful_scrapes += 1
            
            # Get final metrics
            end_memory = self.memory_monitor.memory_usage_mb()
            scraper_stats = scraper.get_stats()
            
            await scraper.close_async()
            
            test_duration = time.time() - start_time
            
            metrics = {
                'urls_tested': len(test_urls),
                'successful_scrapes': successful_scrapes,
                'success_rate': successful_scrapes / len(test_urls) * 100,
                'avg_response_time': total_response_time / len(test_urls),
                'memory_increase_mb': end_memory - start_memory,
                'scraper_stats': scraper_stats,
                'throughput_urls_per_min': (len(test_urls) / test_duration) * 60
            }
            
            result = PerformanceTestResult(
                test_name=test_name,
                component_type="EnhancedWebScraper",
                metrics=metrics,
                success=True,
                duration_seconds=test_duration
            )
            
            self.logger.info(f"✅ Enhanced WebScraper Test Completed:")
            self.logger.info(f"  Success Rate: {metrics['success_rate']:.1f}%")
            self.logger.info(f"  Avg Response Time: {metrics['avg_response_time']:.2f}s")
            self.logger.info(f"  Memory Increase: {metrics['memory_increase_mb']:.1f}MB")
            self.logger.info(f"  Throughput: {metrics['throughput_urls_per_min']:.1f} URLs/min")
            
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Enhanced WebScraper Test Failed: {e}")
            return PerformanceTestResult(
                test_name=test_name,
                component_type="EnhancedWebScraper",
                metrics={},
                success=False,
                error_message=str(e),
                duration_seconds=time.time() - start_time
            )
    
    async def test_streaming_csv_processor(self) -> PerformanceTestResult:
        """Test Streaming CSV Processor performance"""
        self.logger.info("🧪 Testing Streaming CSV Processor Performance...")
        
        test_name = f"StreamingCSVProcessor-{self.sample_size}_rows"
        start_time = time.time()
        
        try:
            # Create test CSV
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
                csv_file = f.name
            
            try:
                self.create_test_csv(self.sample_size, csv_file)
                
                # Initialize streaming processor
                processor = StreamingCSVProcessor(
                    initial_chunk_size=500,
                    max_chunk_size=2000,
                    memory_threshold_mb=512.0  # 512MB for testing
                )
                
                start_memory = self.memory_monitor.memory_usage_mb()
                processed_rows = 0
                chunk_count = 0
                
                # Process CSV
                async for chunk_metrics, chunk_df in processor.process_streaming(
                    csv_file, 
                    processor.stress_test_processor,
                    url_column='url'
                ):
                    processed_rows += chunk_metrics.chunk_size
                    chunk_count += 1
                    
                    self.logger.info(f"  Chunk {chunk_count}: {chunk_metrics.chunk_size} rows "
                                   f"({chunk_metrics.processing_time:.2f}s)")
                
                # Get final metrics
                end_memory = self.memory_monitor.memory_usage_mb()
                processor_stats = processor.get_stats()
                test_duration = time.time() - start_time
                
                metrics = {
                    'total_rows_input': self.sample_size,
                    'total_rows_processed': processed_rows,
                    'chunks_processed': chunk_count,
                    'processing_rate_rows_per_min': (processed_rows / test_duration) * 60,
                    'memory_increase_mb': end_memory - start_memory,
                    'memory_peak_mb': processor_stats['total_stats']['memory_peak_mb'],
                    'average_chunk_size': processor_stats['total_stats']['average_chunk_size'],
                    'backpressure_seconds': processor_stats['total_stats']['backpressure_seconds'],
                    'success_rate': processor_stats['total_stats']['success_rate']
                }
                
                result = PerformanceTestResult(
                    test_name=test_name,
                    component_type="StreamingCSVProcessor",
                    metrics=metrics,
                    success=True,
                    duration_seconds=test_duration
                )
                
                self.logger.info(f"✅ Streaming CSV Processor Test Completed:")
                self.logger.info(f"  Rows Processed: {metrics['total_rows_processed']:,}")
                self.logger.info(f"  Processing Rate: {metrics['processing_rate_rows_per_min']:.0f} rows/min")
                self.logger.info(f"  Memory Peak: {metrics['memory_peak_mb']:.1f}MB")
                self.logger.info(f"  Chunk Success Rate: {metrics['success_rate']:.1f}%")
                
                return result
                
            finally:
                # Clean up test file
                if os.path.exists(csv_file):
                    os.unlink(csv_file)
                
        except Exception as e:
            self.logger.error(f"❌ Streaming CSV Processor Test Failed: {e}")
            return PerformanceTestResult(
                test_name=test_name,
                component_type="StreamingCSVProcessor",
                metrics={},
                success=False,
                error_message=str(e),
                duration_seconds=time.time() - start_time
            )
    
    async def test_memory_monitoring(self) -> PerformanceTestResult:
        """Test Memory Monitoring Service performance"""
        self.logger.info("🧪 Testing Memory Monitor Service...")
        
        test_name = f"MemoryMonitor-{self.sample_size}_cycles"
        start_time = time.time()
        
        try:
            # Test memory monitoring
            monitor = MemoryMonitor()
            monitor.start_monitoring(interval=0.1)  # Fast interval for testing
            
            # Simulate memory usage patterns
            memory_samples = []
            alert_received = False
            
            def alert_callback(alert):
                nonlocal alert_received
                alert_received = True
            
            monitor.add_warning_callback(alert_callback)
            
            # Collect memory samples
            for i in range(min(100, self.sample_size * 2)):
                metrics = monitor.get_recent_metrics(1)
                if metrics:
                    memory_samples.append({
                        'memory_mb': metrics[0].memory_mb,
                        'memory_percent': metrics[0].memory_percent,
                        'timestamp': metrics[0].timestamp
                    })
                
                # Small delay between samples
                await asyncio.sleep(0.01)
            
            # Get final stats
            monitor_stats = monitor.get_stats()
            monitor.stop_monitoring()
            
            test_duration = time.time() - start_time
            
            metrics = {
                'samples_collected': len(memory_samples),
                'avg_memory_mb': sum(s['memory_mb'] for s in memory_samples) / len(memory_samples) if memory_samples else 0,
                'max_memory_mb': max(s['memory_mb'] for s in memory_samples) if memory_samples else 0,
                'min_memory_mb': min(s['memory_mb'] for s in memory_samples) if memory_samples else 0,
                'alerts_received': alert_received,
                'monitoring_active_time': test_duration,
                'samples_per_second': len(memory_samples) / test_duration if test_duration > 0 else 0
            }
            
            result = PerformanceTestResult(
                test_name=test_name,
                component_type="MemoryMonitor",
                metrics=metrics,
                success=True,
                duration_seconds=test_duration
            )
            
            self.logger.info(f"✅ Memory Monitor Test Completed:")
            self.logger.info(f"  Samples Collected: {metrics['samples_collected']}")
            self.logger.info(f"  Avg Memory: {metrics['avg_memory_mb']:.1f}MB")
            self.logger.info(f"  Sample Rate: {metrics['samples_per_second']:.1f} samples/sec")
            self.logger.info(f"  Alerts Working: {metrics['alerts_received']}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Memory Monitor Test Failed: {e}")
            return PerformanceTestResult(
                test_name=test_name,
                component_type="MemoryMonitor",
                metrics={},
                success=False,
                error_message=str(e),
                duration_seconds=time.time() - start_time
            )
    
    async def run_all_tests(self) -> Dict[str, PerformanceTestResult]:
        """Run all performance tests"""
        self.logger.info("🚀 Starting Performance Testing Suite - Phase 1")
        self.logger.info(f"Sample Size: {self.sample_size}")
        self.logger.info(f"System Memory: {psutil.virtual_memory().total / 1024 / 1024:.0f}MB")
        
        # Store system baseline
        baseline_memory = self.memory_monitor.memory_usage_mb()
        
        all_results = {}
        
        # Run tests in sequence to avoid interference
        tests = [
            ('browser_pool', self.test_browser_pool_performance),
            ('enhanced_web_scraper', self.test_enhanced_web_scraper_performance),
            ('streaming_csv_processor', self.test_streaming_csv_processor),
            ('memory_monitor', self.test_memory_monitoring)
        ]
        
        for test_name, test_func in tests:
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f"Testing: {test_name.upper()}")
            self.logger.info(f"{'='*60}")
            
            result = await test_func()
            all_results[test_name] = result
            self.test_results.append(result)
            
            # Small pause between tests
            await asyncio.sleep(2)
        
        # Performance summary
        self.logger.info(f"\n{'='*60}")
        self.logger.info("PERFORMANCE TESTING SUMMARY")
        self.logger.info(f"{'='*60}")
        
        successful_tests = [r for r in self.test_results if r.success]
        failed_tests = [r for r in self.test_results if not r.success]
        
        self.logger.info(f"Tests Run: {len(self.test_results)}")
        self.logger.info(f"Successful: {len(successful_tests)}")
        self.logger.info(f"Failed: {len(failed_tests)}")
        self.logger.info(f"Total Duration: {sum(r.duration_seconds for r in self.test_results):.1f}s")
        
        memory_after = self.memory_monitor.memory_usage_mb()
        total_memory_change = memory_after - baseline_memory
        self.logger.info(f"Memory Change: {total_memory_change:+.1f}MB")
        
        if failed_tests:
            self.logger.info(f"\nFailed Tests:")
            for test in failed_tests:
                self.logger.info(f"  {test.test_name}: {test.error_message}")
        
        # Key performance indicators
        self.logger.info(f"\nKey Performance Indicators:")
        
        browser_pool = all_results.get('browser_pool')
        if browser_pool and browser_pool.success:
            metrics = browser_pool.metrics
            self.logger.info(f"  Browser Pool Throughput: {metrics.get('throughput_urls_per_min', 0):.1f} URLs/min")
            self.logger.info(f"  Browser Pool Success Rate: {metrics.get('success_rate', 0):.1f}%")
        
        streaming_processor = all_results.get('streaming_csv_processor')
        if streaming_processor and streaming_processor.success:
            metrics = streaming_processor.metrics
            self.logger.info(f"  Streaming Rate: {metrics.get('processing_rate_rows_per_min', 0):.0f} rows/min")
            self.logger.info(f"  Streaming Memory Peak: {metrics.get('memory_peak_mb', 0):.1f}MB")
        
        enhanced_scraper = all_results.get('enhanced_web_scraper')
        if enhanced_scraper and enhanced_scraper.success:
            metrics = enhanced_scraper.metrics
            self.logger.info(f"  Enhanced Scraper Throughput: {metrics.get('throughput_urls_per_min', 0):.1f} URLs/min")
            self.logger.info(f"  Enhanced Scraper Success Rate: {metrics.get('success_rate', 0):.1f}%")
        
        return all_results
    
    def save_results(self, results: Dict[str, PerformanceTestResult], filename: str = "performance_test_results.json"):
        """Save test results to JSON file"""
        serializable_results = {}
        
        for key, result in results.items():
            serializable_results[key] = {
                'test_name': result.test_name,
                'component_type': result.component_type,
                'success': result.success,
                'duration_seconds': result.duration_seconds,
                'error_message': result.error_message,
                'metrics': {
                    k: float(v) if isinstance(v, (int, float)) else str(v) 
                    for k, v in result.metrics.items()
                }
            }
        
        with open(filename, 'w') as f:
            json.dump(serializable_results, f, indent=2)
        
        self.logger.info(f"📊 Test results saved to {filename}")


async def main():
    """Main function for performance testing"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Performance Testing Suite - Phase 1")
    parser.add_argument('--component', choices=['browser_pool', 'enhanced_web_scraper', 'streaming_csv_processor', 'memory_monitor', 'all'], 
                       default='all', help='Component to test')
    parser.add_argument('--sample-size', type=int, default=100, help='Sample size for testing')
    parser.add_argument('--save-results', help='Save results to specified file')
    
    args = parser.parse_args()
    
    # Initialize performance tester
    tester = PerformanceTester(sample_size=args.sample_size)
    
    try:
        if args.component == 'all':
            results = await tester.run_all_tests()
        else:
            # Run specific test
            test_methods = {
                'browser_pool': tester.test_browser_pool_performance,
                'enhanced_web_scraper': tester.test_enhanced_web_scraper_performance,
                'streaming_csv_processor': tester.test_streaming_csv_processor,
                'memory_monitor': tester.test_memory_monitoring
            }
            
            if args.component in test_methods:
                result = await test_methods[args.component]()
                await tester.run_all_tests()  # This will show summary
            else:
                print(f"Unknown component: {args.component}")
                return 1
        
        # Save results if requested
        if args.save_results:
            results = await tester.run_all_tests() if args.component == 'all' else {args.component: result}
            tester.save_results(results, args.save_results)
        
        # Return success/failure based on test results
        failed_count = len([r for r in tester.test_results if not r.success])
        return 1 if failed_count > 0 else 0
        
    finally:
        # Clean up memory monitoring
        tester.memory_monitor.stop_monitoring()


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
