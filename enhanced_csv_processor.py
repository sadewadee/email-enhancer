"""
Enhanced CSV Processor with streaming integration.
Maintain backward compatibility while adding streaming capabilities.
"""

import logging
import os
import time
from typing import Dict, List, Optional, Any
import pandas as pd

# Import streaming processor
from streaming_csv_processor import StreamingCSVProcessor, StreamingCSVAdapter
from memory_monitor import MemoryMonitor


class EnhancedCSVProcessor:
    """
    Enhanced CSV processor that uses streaming for memory efficiency.
    
    Maintains backward compatibility with existing CSVProcessor while
    providing significant memory and performance improvements through
    streaming processing.
    """
    
    def __init__(self, 
                 max_workers: int = 3,
                 timeout: int = 300,
                 block_images: bool = True,
                 disable_resources: bool = False,
                 network_idle: bool = False,
                 cf_wait_timeout: int = 30,
                 skip_on_challenge: bool = False,
                 proxy_file: str = 'proxy.txt',
                 max_concurrent_browsers: int = 3,
                 normal_budget: int = 60,
                 challenge_budget: int = 120,
                 dead_site_budget: int = 20,
                 min_retry_threshold: int = 5,
                 fast: bool = False,
                 use_streaming: bool = True,
                 **kwargs):
        
        # Store all configuration parameters
        self.max_workers = max_workers
        self.timeout = timeout
        self.block_images = block_images
        self.disable_resources = disable_resources
        self.network_idle = network_idle
        self.cf_wait_timeout = cf_wait_timeout
        self.skip_on_challenge = skip_on_challenge
        self.proxy_file = proxy_file
        self.max_concurrent_browsers = max_concurrent_browsers
        self.normal_budget = normal_budget
        self.challenge_budget = challenge_budget
        self.dead_site_budget = dead_site_budget
        self.min_retry_threshold = min_retry_threshold
        self.fast = fast
        self.use_streaming = use_streaming
        
        # Initialize logging
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        if use_streaming:
            # Initialize streaming processor
            self.streaming_processor = StreamingCSVProcessor(
                initial_chunk_size=1000,
                min_chunk_size=100,
                max_chunk_size=5000,
                memory_threshold_mb=1024.0  # 1GB default threshold
            )
            self.streaming_adapter = StreamingCSVAdapter(self.streaming_processor)
            self.logger.info("Using Streaming CSV Processor for enhanced memory efficiency")
        else:
            # Initialize original components or fallback
            from csv_processor import CSVProcessor
            self.original_processor = CSVProcessor(
                max_workers=max_workers,
                timeout=timeout,
                block_images=block_images,
                disable_resources=disable_resources,
                network_idle=network_idle,
                cf_wait_timeout=cf_wait_timeout,
                skip_on_challenge=skip_on_challenge,
                proxy_file=proxy_file,
                max_concurrent_browsers=max_concurrent_browsers,
                normal_budget=normal_budget,
                challenge_budget=challenge_budget,
                dead_site_budget=dead_site_budget,
                min_retry_threshold=min_retry_threshold,
                fast=fast,
                **kwargs
            )
            self.logger.info("Using Original CSV Processor (streaming disabled)")
        
        # Memory monitoring
        self.memory_monitor = MemoryMonitor()
        
        # Statistics
        self.stats = {
            'total_files_processed': 0,
            'total_rows_processed': 0,
            'processing_time_total': 0,
            'memory_peak_mb': 0,
            'streaming_used': use_streaming
        }
    
    def process_csv_file(self,
                        input_file: str,
                        output_file: str,
                        batch_size: int = 100,
                        input_chunksize: int = 0,
                        limit_rows: Optional[int] = None,
                        url_column: str = 'url') -> Dict[str, Any]:
        """
        Process CSV file, using streaming when enabled.
        
        Maintains identical interface to original CSVProcessor.process_csv_file()
        but uses streaming internally for better memory efficiency.
        """
        start_time = time.time()
        self.stats['total_files_processed'] += 1
        
        self.logger.info(f"Processing CSV file: {os.path.basename(input_file)}")
        
        try:
            if self.use_streaming:
                result = self._process_with_streaming(
                    input_file, output_file, url_column, limit_rows
                )
            else:
                result = self.original_processor.process_csv_file(
                    input_file, output_file, batch_size, input_chunksize, limit_rows
                )
            
            # Update statistics
            processing_time = time.time() - start_time
            self.stats['processing_time_total'] += processing_time
            self.stats['memory_peak_mb'] = max(
                self.stats['memory_peak_mb'], 
                self.memory_monitor.memory_usage_mb()
            )
            
            # Add processing metadata
            result['streaming_used'] = self.use_streaming
            result['processing_time'] = processing_time
            result['memory_peak_mb'] = self.stats['memory_peak_mb']
            
            return result
            
        except Exception as e:
            self.logger.error(f"CSV processing failed: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'input_file': input_file,
                'output_file': output_file,
                'streaming_used': self.use_streaming
            }
    
    async def _process_with_streaming(self,
                                     input_file: str,
                                     output_file: str,
                                     url_column: str,
                                     limit_rows: Optional[int]) -> Dict[str, Any]:
        """
        Process CSV file using streaming approach.
        """
        async def process_chunk_async(chunk_df):
            """Async chunk processing function for streaming processor."""
            # Initialize web scraper for contact extraction
            from enhanced_web_scraper import EnhancedWebScraper
            
            # Create web scraper configuration
            scraper_config = {
                'max_workers': 1,  # Use single worker per chunk
                'timeout': self.timeout,
                'block_images': self.block_images,
                'disable_resources': self.disable_resources,
                'network_idle': self.network_idle,
                'cf_wait_timeout': self.cf_wait_timeout,
                'skip_on_challenge': self.skip_on_challenge,
                'min_browsers': 1,
                'max_browsers': 2
            }
            
            async with EnhancedWebScraper(scraper_config) as scraper:
                await scraper.initialize()
                
                processed_rows = []
                
                for _, row in chunk_df.iterrows():
                    url = row.get(url_column, '').strip()
                    if not url:
                        continue
                    
                    # Scrape URL using enhanced scraper
                    import asyncio
                    scrape_result = scraper.scrape_url(url)
                    
                    # Extract contacts if successful
                    if scrape_result.get('html'):
                        from contact_extractor import ContactExtractor
                        extractor = ContactExtractor()
                        contacts = extractor.extract_all_contacts(
                            scrape_result['html'], 
                            scrape_result.get('final_url', url)
                        )
                    else:
                        contacts = {
                            'emails': [], 'phones': [], 'whatsapp': [],
                            'facebook': '', 'instagram': '', 'linkedin': '',
                            'tiktok': '', 'youtube': ''
                        }
                    
                    # Combine row data with scraped results
                    processed_row = {
                        **row.to_dict(),  # Original CSV columns
                        'emails': '; '.join(contacts.get('emails', [])),
                        'phones': '; '.join(contacts.get('phones', [])),
                        'whatsapp': '; '.join(contacts.get('whatsapp', [])),
                        'facebook': contacts.get('facebook', ''),
                        'instagram': contacts.get('instagram', ''),
                        'linkedin': contacts.get('linkedin', ''),
                        'tiktok': contacts.get('tiktok', ''),
                        'youtube': contacts.get('youtube', ''),
                        'final_url': scrape_result.get('final_url', url),
                        'scraping_status': scrape_result.get('status', 0),
                        'scraping_error': scrape_result.get('error', ''),
                        'response_time': scrape_result.get('response_time', 0),
                        'pages_scraped': scrape_result.get('pages_scraped', 0)
                    }
                    
                    processed_rows.append(processed_row)
                
                # Return as DataFrame
                return pd.DataFrame(processed_rows)
        
        # Run streaming processing
        return await self.streaming_adapter.process_with_streaming(
            input_file=input_file,
            output_file=output_file,
            processor_func=process_chunk_async,
            url_column=url_column,
            limit_rows=limit_rows
        )
    
    def process_single_url(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single URL for contact extraction.
        
        Compatible interface with original CSVProcessor.process_single_url()
        """
        url = row.get('url', '').strip()
        if not url:
            return {
                'emails': [], 'phones': [], 'whatsapp': [],
                'facebook': '', 'instagram': '', 'linkedin': '',
                'tiktok': '', 'youtube': '',
                'final_url': '', 'status': 'failed', 'error': 'No URL provided',
                'processing_time': 0, 'pages_scraped': 0
            }
        
        # Use enhanced web scraper
        from enhanced_web_scraper import create_web_scraper
        
        scraper_config = {
            'max_workers': 1,
            'timeout': self.timeout,
            'block_images': self.block_images,
            'disable_resources': self.disable_resources,
            'network_idle': self.network_idle,
            'cf_wait_timeout': self.cf_wait_timeout,
            'skip_on_challenge': self.skip_on_challenge,
            'min_browsers': 1,
            'max_browsers': 2,
            'use_browser_pool': True
        }
        
        scraper = create_web_scraper(scraper_config)
        
        try:
            # Initialize scraper if using browser pool
            if isinstance(scraper, EnhancedWebScraper):
                import asyncio
                asyncio.run(scraper.initialize())
            
            # Perform scraping with contact extraction
            scrape_result = scraper.scrape_url(url)
            
            # Extract contacts if scraping successful
            if scrape_result.get('html'):
                from contact_extractor import ContactExtractor
                extractor = ContactExtractor()
                contacts = extractor.extract_all_contacts(
                    scrape_result['html'], 
                    scrape_result.get('final_url', url)
                )
                
                return {
                    **contacts,
                    'final_url': scrape_result.get('final_url', url),
                    'status': 'success' if scrape_result.get('status') == 200 else 'failed',
                    'error': scrape_result.get('error'),
                    'processing_time': scrape_result.get('response_time', 0),
                    'pages_scraped': scrape_result.get('pages_scraped', 0),
                    'scraper_type': scrape_result.get('scraper_type', 'enhanced')
                }
            else:
                return {
                    'emails': [], 'phones': [], 'whatsapp': [],
                    'facebook': '', 'instagram': '', 'linkedin': '',
                    'tiktok': '', 'youtube': '',
                    'final_url': scrape_result.get('final_url', url),
                    'status': 'failed',
                    'error': scrape_result.get('error', 'No HTML content'),
                    'processing_time': scrape_result.get('response_time', 0),
                    'pages_scraped': 0,
                    'scraper_type': scrape_result.get('scraper_type', 'enhanced')
                }
                
        finally:
            # Clean up scraper resources
            if hasattr(scraper, 'close'):
                scraper.close()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get processing statistics"""
        base_stats = {
            'streaming_enabled': self.use_streaming,
            'total_files_processed': self.stats['total_files_processed'],
            'total_rows_processed': self.stats['total_rows_processed'],
            'processing_time_total': self.stats['processing_time_total'],
            'memory_peak_mb': self.stats['memory_peak_mb'],
            'current_memory_usage_mb': self.memory_monitor.memory_usage_mb(),
            'current_memory_percent': self.memory_monitor.usage_percentage()
        }
        
        # Add streaming-specific stats if enabled
        if self.use_streaming:
            streaming_stats = self.streaming_processor.get_stats()
            base_stats['streaming_stats'] = streaming_stats
        
        return base_stats
    
    def close(self):
        """Clean up resources"""
        if self.use_streaming:
            # Clean up streaming processor
            self.memory_monitor.stop_memory_monitor()
        elif hasattr(self, 'original_processor'):
            # Clean up original processor
            self.original_processor.close() if hasattr(self.original_processor, 'close') else None


# Factory function for creating appropriate processor
def create_csv_processor(use_streaming: bool = None, **kwargs) -> 'EnhancedCSVProcessor':
    """
    Factory function to create CSV processor with optimal configuration.
    
    Automatically determines whether to use streaming based on:
    - Explicit use_streaming parameter (if provided)
    - File size and system resources
    - Available memory
    """
    import psutil
    
    # Auto-determine optimal streaming usage if not specified
    if use_streaming is None:
        memory_mb = psutil.virtual_memory().total / 1024 / 1024
        
        # Use streaming for large files or limited memory systems
        use_streaming = memory_mb < 4096  # < 4GB RAM means streaming preferred
    
    return EnhancedCSVProcessor(use_streaming=use_streaming, **kwargs)


# Backward compatibility alias
class CSVProcessor(EnhancedCSVProcessor):
    """
    CSVProcessor alias that maintains full backward compatibility.
    
    This class provides the same interface as the original CSVProcessor
    but with enhanced streaming capabilities by default.
    """
    
    def __init__(self, **kwargs):
        # Enable streaming by default unless explicitly disabled
        kwargs.setdefault('use_streaming', True)
        super().__init__(**kwargs)
