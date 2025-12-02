"""
Adapter Patterns - Bridge existing concrete classes to new service interfaces

Implements adapter pattern to integrate existing concrete classes with new service interfaces:
- Zero breaking changes to existing implementations
- Clean separation between interfaces and implementations
- Gradual migration path to interface-based architecture
- Backward compatibility preservation
- Service registration for dependency injection

Adapters Created:
- EnhancedWebScraperAdapter: bridges EnhancedWebScraper to IWebScraper
- ContactExtractorAdapter: bridges ContactExtractor to IContactExtractor  
- EmailValidatorAdapter: bridges existing email validation to IEmailValidator
- EnhancedCSVProcessorAdapter: bridges EnhancedCSVProcessor to ICsvProcessor
- MemoryMonitorAdapter: bridges MemoryMonitor to IMemoryMonitor
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Tuple, Union, Callable
from dataclasses import dataclass

# Import existing concrete classes
from enhanced_web_scraper import EnhancedWebScraper, create_web_scraper
from contact_extractor import ContactExtractor
from email_validation import EmailValidator
from enhanced_csv_processor import EnhancedCSVProcessor, create_csv_processor
from memory_monitor import MemoryMonitor, get_memory_monitor
from whatsapp_validator import WhatsAppValidator

# Import service interfaces
from service_interfaces import (
    IWebScraper, IContactExtractor, IEmailValidator, ICsvProcessor,
    IMemoryMonitor, IContactValidator, ScrapingResult, ContactInfo,
    ProcessingStats
)


class EnhancedWebScraperAdapter(IWebScraper):
    """Adapter for EnhancedWebScraper to IWebScraper interface"""
    
    def __init__(self, scraper: Optional[EnhancedWebScraper] = None, config: Optional[Dict[str, Any]] = None):
        self.logger = logging.getLogger(__name__)
        
        if scraper:
            self._scraper = scraper
        else:
            self._scraper = create_web_scraper(config or {})
        
        # Initialize if needed
        if hasattr(self._scraper, 'initialize'):
            asyncio.run(self._scraper.initialize())
    
    async def scrape_url_async(self, url: str, **kwargs) -> ScrapingResult:
        """Scrape a single URL asynchronously"""
        result = self._scraper.scrape_url(url, **kwargs)
        
        return ScrapingResult(
            status=result.get('status', 0),
            success=result.get('status', 0) == 200 and bool(result.get('html')),
            url=url,
            final_url=result.get('final_url', url),
            html=result.get('html', ''),
            error=result.get('error'),
            load_time=result.get('load_time', 0),
            page_title=result.get('page_title', ''),
            meta_description=result.get('meta_description', ''),
            proxy_used=result.get('proxy_used', False),
            pages_scraped=result.get('pages_scraped', 1),
            metadata={
                'scraper_type': result.get('scraper_type', 'unknown'),
                'response_time': result.get('response_time', 0),
                'memory_usage_mb': result.get('memory_usage_mb', 0)
            }
        )
    
    def scrape_url(self, url: str, **kwargs) -> ScrapingResult:
        """Scrape a single URL synchronously"""
        # Run async operation in sync context
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(self.scrape_url_async(url, **kwargs))
        finally:
            loop.close()
    
    async def scrape_urls_batch_async(self, urls: List[str], **kwargs) -> List[ScrapingResult]:
        """Scrape multiple URLs asynchronously"""
        # Use existing batch functionality
        results = self._scraper.scrape_urls_batch(urls, **kwargs)
        
        return [await self._convert_result_to_interface(result, url) 
                for result, url in zip(results, urls)]
    
    def scrape_urls_batch(self, urls: List[str], **kwargs) -> List[ScrapingResult]:
        """Scrape multiple URLs synchronously"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(self.scrape_urls_batch_async(urls, **kwargs))
        finally:
            loop.close()
    
    async def _convert_result_to_interface(self, result: Dict[str, Any], url: str) -> ScrapingResult:
        """Convert internal result format to interface format"""
        return ScrapingResult(
            status=result.get('status', 0),
            success=result.get('status', 0) == 200 and bool(result.get('html')),
            url=url,
            final_url=result.get('final_url', url),
            html=result.get('html', ''),
            error=result.get('error'),
            load_time=result.get('response_time', 0),
            page_title=result.get('title', ''),
            meta_description='',
            proxy_used=result.get('proxy_used', False),
            pages_scraped=result.get('pages_scraped', 1),
            metadata={
                'scraper_type': result.get('scraper_type', 'unknown'),
                'memory_usage_mb': result.get('memory_usage_mb', 0)
            }
        )
    
    def get_stats(self) -> Dict[str, Any]:
        """Get scraping statistics and metrics"""
        base_stats = self._scraper.get_stats()
        
        # Convert to interface-friendly format
        return {
            'total_requests': base_stats.get('total_requests', 0),
            'successful_requests': base_stats.get('successful_requests', 0),
            'failed_requests': base_stats.get('failed_requests', 0),
            'success_rate': base_stats.get('success_rate', 0),
            'average_response_time': base_stats.get('average_response_time', 0),
            'memory_usage_peak': base_stats.get('memory_usage_peak', 0),
            'scraper_type': base_stats.get('scraper_type', 'unknown'),
            'pool_stats': base_stats.get('browser_pool_stats', {}),
            'memory_monitor_stats': base_stats.get('memory_monitor_stats', {})
        }
    
    async def close_async(self) -> None:
        """Close resources asynchronously"""
        if hasattr(self._scraper, 'close_async'):
            await self._scraper.close_async()
        else:
            self._scraper.close()
    
    def close(self) -> None:
        """Close resources synchronously"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self.close_async())
        finally:
            loop.close()


class ContactExtractorAdapter(IContactExtractor):
    """Adapter for ContactExtractor to IContactExtractor interface"""
    
    def __init__(self, extractor: Optional[ContactExtractor] = None):
        self._extractor = extractor or ContactExtractor()
    
    def extract_all_contacts(self, html: str, url: str) -> ContactInfo:
        """Extract all contact information from HTML"""
        contacts = self._extractor.extract_all_contacts(html, url)
        
        # Convert to interface format
        return ContactInfo(
            emails=contacts.get('emails', []),
            phones=contacts.get('phones', []),
            whatsapp=contacts.get('whatsapp', []),
            social_media={
                'facebook': contacts.get('facebook', ''),
                'instagram': contacts.get('instagram', ''),
                'linkedin': contacts.get('linkedin', ''),
                'tiktok': contacts.get('tiktok', ''),
                'youtube': contacts.get('youtube', '')
            },
            addresses=contacts.get('addresses', []),
            metadata={
                'extraction_method': contacts.get('extraction_method', 'unknown'),
                'url': url,
                'extraction_time': contacts.get('extraction_time', 0)
            }
        )
    
    def extract_emails(self, html: str) -> List[str]:
        """Extract email addresses from HTML"""
        result = self._extractor.extract_emails(html)
        return result if isinstance(result, list) else []
    
    def extract_phones(self, html: str) -> List[str]:
        """Extract phone numbers from HTML"""
        result = self._extractor.extract_phones(html)
        return result if isinstance(result, list) else []
    
    def extract_whatsapp(self, html: str) -> List[str]:
        """Extract WhatsApp numbers from HTML"""
        result = self._extractor.extract_whatsapp(html)
        return result if isinstance(result, list) else []
    
    def extract_social_media(self, html: str) -> Dict[str, str]:
        """Extract social media profiles from HTML"""
        contacts = self._extractor.extract_all_contacts(html, '')
        
        return {
            'facebook': contacts.get('facebook', ''),
            'instagram': contacts.get('instagram', ''),
            'linkedin': contacts.get('linkedin', ''),
            'tiktok': contacts.get('tiktok', ''),
            'youtube': contacts.get('youtube', ''),
            'twitter': contacts.get('twitter', '')
        }
    
    def extract_addresses(self, html: str) -> List[str]:
        """Extract addresses from HTML"""
        contacts = self._extractor.extract_all_contacts(html, '')
        return contacts.get('addresses', [])


class EmailValidatorAdapter(IEmailValidator):
    """Adapter for EmailValidator to IEmailValidator interface"""
    
    def __init__(self, validator: Optional[EmailValidator] = None):
        self._validator = validator or EmailValidator()
    
    def validate_format(self, email: str) -> bool:
        """Validate email format syntactically"""
        return self._validator.validate_format(email)
    
    async def validate_smtp_async(self, email: str) -> Tuple[bool, str]:
        """Validate email deliverability via SMTP (async)"""
        # Use synchronous validation in async context
        return self.validate_smtp(email)
    
    def validate_smtp(self, email: str) -> Tuple[bool, str]:
        """Validate email deliverability via SMTP (sync)"""
        try:
            is_valid, reason = self._validator.validate_smtp(email)
            return is_valid, reason or ''
        except Exception:
            return False, 'Validation failed'
    
    async def validate_emails_batch_async(self, emails: List[str]) -> List[Tuple[str, bool, str]]:
        """Validate multiple emails via SMTP (async)"""
        return [(email, *self.validate_smtp(email)) for email in emails]
    
    def validate_emails_batch(self, emails: List[str]) -> List[Tuple[str, bool, str]]:
        """Validate multiple emails via SMTP (sync)"""
        results = []
        for email in emails:
            try:
                is_valid, reason = self._validator.validate_smtp(email)
                results.append((email, is_valid, reason or ''))
            except Exception:
                results.append((email, False, 'Validation failed'))
        return results
    
    def get_stats(self) -> Dict[str, Any]:
        """Get validation statistics"""
        # EmailValidator doesn't have built-in stats, return defaults
        return {
            'total_validated': 0,
            'valid_count': 0,
            'invalid_count': 0,
            'success_rate': 0.0
        }


class EnhancedCSVProcessorAdapter(ICsvProcessor):
    """Adapter for EnhancedCSVProcessor to ICsvProcessor interface"""
    
    def __init__(self, processor: Optional[EnhancedCSVProcessor] = None, config: Optional[Dict[str, Any]] = None):
        if processor:
            self._processor = processor
        else:
            self._processor = create_csv_processor(config or {})
    
    async def process_csv_file_async(self, input_file: str, output_file: str, **kwargs) -> ProcessingStats:
        """Process CSV file asynchronously"""
        processing_stats = self._processor.process_csv_file(input_file, output_file, **kwargs)
        
        return ProcessingStats(
            total_urls=processing_stats.get('total_rows_processed', 0),
            successful_urls=processing_stats.get('rows_processed', 0),  # Approximate
            failed_urls=0,  # Not tracked
            processing_time=processing_stats.get('processing_time', 0),
            emails_found=0,  # Not directly tracked
            phones_found=0,  # Not directly tracked
            whatsapp_found=0,  # Not directly tracked
            social_media_found=0,  # Not directly tracked
            memory_peak_mb=processing_stats.get('memory_peak_mb', 0),
            throughput_urls_per_min=processing_stats.get('throughput_urls_per_min', 0)
        )
    
    def process_csv_file(self, input_file: str, output_file: str, **kwargs) -> ProcessingStats:
        """Process CSV file synchronously"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(self.process_csv_file_async(input_file, output_file, **kwargs))
        finally:
            loop.close()
    
    async def process_single_url_async(self, row: Dict[str, Any]) -> ContactInfo:
        """Process single URL row asynchronously"""
        result = self._processor.process_single_url(row)
        
        return ContactInfo(
            emails=result.get('emails', []),
            phones=result.get('phones', []),
            whatsapp=result.get('whatsapp', []),
            social_media={
                'facebook': result.get('facebook', ''),
                'instagram': result.get('instagram', ''),
                'linkedin': result.get('linkedin', ''),
                'tiktok': result.get('tiktok', ''),
                'youtube': result.get('youtube', '')
            },
            addresses=result.get('addresses', []),
            metadata={
                'status': result.get('status', 'unknown'),
                'error': result.get('error', ''),
                'processing_time': result.get('processing_time', 0),
                'pages_scraped': result.get('pages_scraped', 0),
                'scraper_type': result.get('scraper_type', 'unknown')
            }
        )
    
    def process_single_url(self, row: Dict[str, Any]) -> ContactInfo:
        """Process single URL row synchronously"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(self.process_single_url_async(row))
        finally:
            loop.close()
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get processing statistics"""
        return self._processor.get_stats()


class MemoryMonitorAdapter(IMemoryMonitor):
    """Adapter for MemoryMonitor to IMemoryMonitor interface"""
    
    def __init__(self, monitor: Optional[MemoryMonitor] = None):
        self._monitor = monitor or get_memory_monitor()
    
    def start_monitoring(self, interval: float = 5.0) -> None:
        """Start continuous monitoring"""
        self._monitor.start_monitoring(interval)
    
    def stop_monitoring(self) -> None:
        """Stop monitoring"""
        from memory_monitor import stop_memory_monitor
        stop_memory_monitor()
    
    def get_current_metrics(self) -> Dict[str, float]:
        """Get current system metrics"""
        from memory_monitor import ResourceMetrics
        metrics = ResourceMetrics.capture()
        
        return {
            'cpu_percent': metrics.cpu_percent,
            'memory_mb': metrics.memory_mb,
            'memory_percent': metrics.memory_percent,
            'disk_usage_mb': metrics.disk_usage_mb,
            'disk_percent': metrics.disk_percent,
            'network_sent_mb': metrics.network_sent_mb,
            'network_recv_mb': metrics.network_recv_mb,
            'process_count': metrics.process_count
        }
    
    def is_backpressure_active(self) -> bool:
        """Check if backpressure control is active"""
        return self._monitor.is_backpressure_active()
    
    def get_usage_percentage(self) -> float:
        """Get current memory usage percentage"""
        return self._monitor.usage_percentage()


class ContactValidatorAdapter(IContactValidator):
    """Adapter for WhatsAppValidator to IContactValidator interface"""
    
    def __init__(self, validator: Optional[WhatsAppValidator] = None):
        self._validator = validator or WhatsAppValidator()
    
    async def validate_whatsapp_async(self, phone_number: str, country_code: str = None) -> Tuple[bool, str]:
        """Validate WhatsApp number (async)"""
        # WhatsApp validation is synchronous, run in async context
        return self.validate_whatsapp(phone_number, country_code)
    
    def validate_whatsapp(self, phone_number: str, country_code: str = None) -> Tuple[bool, str]:
        """Validate WhatsApp number (sync)"""
        try:
            result = self._validator.check_whatsapp_exists(phone_number, country_code)
            if result:
                return True, "WhatsApp number is valid"
            else:
                return False, "WhatsApp number is not valid"
        except Exception as e:
            return False, f"Validation error: {str(e)}"
    
    async def validate_for_whatsapp_async(self, 
                                         scraped_whatsapp: List[str],
                                         phone_number: str,
                                         country_code: str = None) -> str:
        """Prioritize WhatsApp validation (async)"""
        # Use synchronous version in async context
        return self.validate_for_whatsapp(scraped_whatsapp, phone_number, country_code)
    
    def validate_for_whatsapp(self, 
                              scraped_whatsapp: List[str],
                              phone_number: str,
                              country_code: str = None) -> str:
        """Prioritize WhatsApp validation (sync)"""
        try:
            # Call the actual validation logic from WhatsAppValidator
            return self._validator.validate_for_whatsapp(
                {"whatsapp": scraped_whatsapp, "phone_number": phone_number}, 
                country_code
            )
        except Exception as e:
            self.logger.error(f"WhatsApp validation error: {e}")
            return ""  # Return empty string on error


# Factory functions to create adapters

def create_web_scraper_adapter(config: Optional[Dict[str, Any]] = None) -> IWebScraper:
    """Create web scraper adapter with configuration"""
    return EnhancedWebScraperAdapter(config=config)


def create_contact_extractor_adapter() -> IContactExtractor:
    """Create contact extractor adapter"""
    return ContactExtractorAdapter()


def create_email_validator_adapter() -> IEmailValidator:
    """Create email validator adapter"""
    return EmailValidatorAdapter()


def create_csv_processor_adapter(config: Optional[Dict[str, Any]] = None) -> ICsvProcessor:
    """Create CSV processor adapter with configuration"""
    return EnhancedCSVProcessorAdapter(config=config)


def create_memory_monitor_adapter() -> IMemoryMonitor:
    """Create memory monitor adapter"""
    return MemoryMonitorAdapter()


def create_contact_validator_adapter() -> IContactValidator:
    """Create contact validator adapter"""
    return ContactValidatorAdapter()


# Adapter registration utilities

class AdapterRegistry:
    """Registry for managing service adapters and interface mappings"""
    
    def __init__(self):
        self._adapters: Dict[str, Any] = {}
        self.logger = logging.getLogger(__name__)
    
    def register_adapter(self, interface_name: str, adapter_factory: Callable) -> None:
        """Register adapter factory for interface"""
        self._adapters[interface_name] = adapter_factory
        self.logger.debug(f"Registered adapter for {interface_name}")
    
    def create_adapter(self, interface_name: str, **kwargs) -> Any:
        """Create adapter instance from registration"""
        if interface_name not in self._adapters:
            raise ValueError(f"No adapter registered for {interface_name}")
        
        factory = self._adapters[interface_name]
        return factory(**kwargs)
    
    def list_adapters(self) -> List[str]:
        """List all registered adapters"""
        return list(self._adapters.keys())
    
    def register_default_adapters(self) -> None:
        """Register all default adapters for common interfaces"""
        self.register_adapter('IWebScraper', create_web_scraper_adapter)
        self.register_adapter('IContactExtractor', create_contact_extractor_adapter)
        self.register_adapter('IEmailValidator', create_email_validator_adapter)
        self.register_adapter('ICsvProcessor', create_csv_processor_adapter)
        self.register_adapter('IMemoryMonitor', create_memory_monitor_adapter)
        self.register_adapter('IContactValidator', create_contact_validator_adapter)


# Global adapter registry
_adapter_registry = AdapterRegistry()
_adapter_registry.register_default_adapters()


def get_adapter_registry() -> AdapterRegistry:
    """Get global adapter registry instance"""
    return _adapter_registry


if __name__ == "__main__":
    # Demonstration of adapter patterns
    logging.basicConfig(level=logging.INFO)
    
    print("Adapter Registry Demo:")
    
    # Create adapters using registry
    scraper = get_adapter_registry().create_adapter('IWebScraper', config={'use_browser_pool': False})
    extractor = get_adapter_registry().create_adapter('IContactExtractor')
    validator = get_adapter_registry().create_adapter('IEmailValidator')
    
    print(f"WebScraper adapter type: {type(scraper).__name__}")
    print(f"ContactExtractor adapter type: {type(extractor).__name__}")
    print(f"EmailValidator adapter type: {type(validator).__name__}")
    
    # Test interface methods
    print("\nTesting interface methods:")
    
    # Test web scraper
    try:
        result = scraper.scrape_url("https://example.com")
        print(f"Scraping result: status={result.status}, success={result.success}")
    except Exception as e:
        print(f"Scraping error: {e}")
    
    # Test contact extractor
    try:
        html = "<p>Email: test@example.com</p><p>Phone: +1234567890</p>"
        contacts = extractor.extract_all_contacts(html, "https://example.com")
        print(f"Contact extraction: emails={len(contacts.emails)}, phones={len(contacts.phones)}")
    except Exception as e:
        print(f"Extraction error: {e}")
    
    # Test email validator
    try:
        is_valid = validator.validate_format("test@example.com")
        print(f"Email validation: test@example.com is {is_valid}")
    except Exception as e:
        print(f"Validation error: {e}")
    
    # Show all registered adapters
    print(f"\nRegistered adapters: {get_adapter_registry().list_adapters()}")
    
    # Cleanup
    try:
        scraper.close()
    except:
        pass
