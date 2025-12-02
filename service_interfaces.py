"""
Service Interface Abstractions - Abstract interfaces for key services

Defines clean separation of concerns and enables:
- Interface-based dependency injection
- Component mocking and testing
- Service implementation swapping
- Loose coupling between components
- Standardization of service APIs

Key Services:
- WebScraper: Web content extraction and scraping
- ContactExtractor: Contact information extraction from HTML
- EmailValidator: Email address validation
- CSVProcessor: CSV file processing and management
- ProxyManager: Proxy rotation and management
- MemoryMonitor: System resource monitoring
- DatabaseWriter: Database interactions
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Union, Set, AsyncIterator, Type
from dataclasses import dataclass
from typing_extensions import Literal
import asyncio


# Core result types
@dataclass
class ScrapingResult:
    """Standard result from web scraping operations"""
    status: int  # HTTP status code or custom status
    success: bool
    url: str
    final_url: str
    html: str
    error: Optional[str] = None
    load_time: float = 0.0
    page_title: str = ""
    meta_description: str = ""
    proxy_used: bool = False
    pages_scraped: int = 1
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


@dataclass
class ContactInfo:
    """Standard contact information container"""
    emails: List[str]
    phones: List[str]
    whatsapp: List[str]
    social_media: Dict[str, str]  # platform -> url/profile
    addresses: List[str]
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
    
    def merge_with(self, other: 'ContactInfo') -> 'ContactInfo':
        """Merge contact info with another instance"""
        return ContactInfo(
            emails=list(set(self.emails + other.emails)),
            phones=list(set(self.phones + other.phones)),
            whatsapp=list(set(self.whatsapp + other.whatsapp)),
            social_media={**self.social_media, **other.social_media},
            addresses=list(set(self.addresses + other.addresses)),
            metadata={**self.metadata, **other.metadata}
        )


@dataclass
class ProcessingStats:
    """Processing statistics and metrics"""
    total_urls: int
    successful_urls: int
    failed_urls: int
    processing_time: float
    emails_found: int
    phones_found: int
    whatsapp_found: int
    social_media_found: int
    memory_peak_mb: float
    throughput_urls_per_min: float
    
    @property
    def success_rate(self) -> float:
        return (self.successful_urls / self.total_urls * 100) if self.total_urls > 0 else 0


# Abstract service interfaces

class IWebScraper(ABC):
    """Interface for web scraping services"""
    
    @abstractmethod
    async def scrape_url_async(self, url: str, **kwargs) -> ScrapingResult:
        """Scrape a single URL asynchronously"""
        pass
    
    @abstractmethod
    def scrape_url(self, url: str, **kwargs) -> ScrapingResult:
        """Scrape a single URL synchronously"""
        pass
    
    @abstractmethod
    async def scrape_urls_batch_async(self, urls: List[str], **kwargs) -> List[ScrapingResult]:
        """Scrape multiple URLs asynchronously"""
        pass
    
    @abstractmethod
    def scrape_urls_batch(self, urls: List[str], **kwargs) -> List[ScrapingResult]:
        """Scrape multiple URLs synchronously"""
        pass
    
    @abstractmethod
    def get_stats(self) -> Dict[str, Any]:
        """Get scraping statistics and metrics"""
        pass
    
    @abstractmethod
    async def close_async(self) -> None:
        """Close resources asynchronously"""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close resources synchronously"""
        pass


class IContactExtractor(ABC):
    """Interface for contact information extraction from HTML"""
    
    @abstractmethod
    def extract_all_contacts(self, html: str, url: str) -> ContactInfo:
        """Extract all contact information from HTML"""
        pass
    
    @abstractmethod
    def extract_emails(self, html: str) -> List[str]:
        """Extract email addresses from HTML"""
        pass
    
    @abstractmethod
    def extract_phones(self, html: str) -> List[str]:
        """Extract phone numbers from HTML"""
        pass
    
    @abstractmethod
    def extract_whatsapp(self, html: str) -> List[str]:
        """Extract WhatsApp numbers from HTML"""
        pass
    
    @abstractmethod
    def extract_social_media(self, html: str) -> Dict[str, str]:
        """Extract social media profiles from HTML"""
        pass
    
    @abstractmethod
    def extract_addresses(self, html: str) -> List[str]:
        """Extract addresses from HTML"""
        pass


class IEmailValidator(ABC):
    """Interface for email address validation"""
    
    @abstractmethod
    def validate_format(self, email: str) -> bool:
        """Validate email format syntactically"""
        pass
    
    @abstractmethod
    async def validate_smtp_async(self, email: str) -> tuple[bool, str]:
        """Validate email deliverability via SMTP (async)"""
        pass
    
    @abstractmethod
    def validate_smtp(self, email: str) -> tuple[bool, str]:
        """Validate email deliverability via SMTP (sync)"""
        pass
    
    @abstractmethod
    async def validate_emails_batch_async(self, emails: List[str]) -> List[tuple[str, bool, str]]:
        """Validate multiple emails via SMTP (async)"""
        pass
    
    @abstractmethod
    def validate_emails_batch(self, emails: List[str]) -> List[tuple[str, bool, str]]:
        """Validate multiple emails via SMTP (sync)"""
        pass
    
    @abstractmethod
    def get_stats(self) -> Dict[str, Any]:
        """Get validation statistics"""
        pass


class ICsvProcessor(ABC):
    """Interface for CSV file processing"""
    
    @abstractmethod
    async def process_csv_file_async(self, input_file: str, 
                                    output_file: str, 
                                    **kwargs) -> ProcessingStats:
        """Process CSV file asynchronously"""
        pass
    
    @abstractmethod
    def process_csv_file(self, input_file: str, 
                        output_file: str, 
                        **kwargs) -> ProcessingStats:
        """Process CSV file synchronously"""
        pass
    
    @abstractmethod
    async def process_single_url_async(self, row: Dict[str, Any]) -> ContactInfo:
        """Process single URL row asynchronously"""
        pass
    
    @abstractmethod
    def process_single_url(self, row: Dict[str, Any]) -> ContactInfo:
        """Process single URL row synchronously"""
        pass
    
    @abstractmethod
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get processing statistics"""
        pass


class IProxyManager(ABC):
    """Interface for proxy management and rotation"""
    
    @abstractmethod
    def get_proxy(self) -> Optional[str]:
        """Get next available proxy"""
        pass
    
    @abstractmethod
    def mark_proxy_failed(self, proxy: str, error: str) -> None:
        """Mark proxy as failed with error"""
        pass
    
    @abstractmethod
    def mark_proxy_success(self, proxy: str) -> None:
        """Mark proxy as successful"""
        pass
    
    @abstractmethod
    def get_proxy_stats(self) -> Dict[str, Any]:
        """Get proxy usage statistics"""
        pass
    
    @abstractmethod
    def reload_proxies(self) -> bool:
        """Reload proxy pool from file"""
        pass


class IMemoryMonitor(ABC):
    """Interface for system memory monitoring"""
    
    @abstractmethod
    def start_monitoring(self, interval: float = 5.0) -> None:
        """Start continuous monitoring"""
        pass
    
    @abstractmethod
    def stop_monitoring(self) -> None:
        """Stop monitoring"""
        pass
    
    @abstractmethod
    def get_current_metrics(self) -> Dict[str, float]:
        """Get current system metrics"""
        pass
    
    @abstractmethod
    def is_backpressure_active(self) -> bool:
        """Check if backpressure control is active"""
        pass
    
    @abstractmethod
    def get_usage_percentage(self) -> float:
        """Get current memory usage percentage"""
        pass


class IDatabaseWriter(ABC):
    """Interface for database operations"""
    
    @abstractmethod
    async def write_contacts_async(self, contacts: List[Dict[str, Any]], **kwargs) -> int:
        """Write contacts to database (async)"""
        pass
    
    @abstractmethod
    def write_contacts(self, contacts: List[Dict[str, Any]], **kwargs) -> int:
        """Write contacts to database (sync)"""
        pass
    
    @abstractmethod
    def connect(self) -> bool:
        """Establish database connection"""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close database connection"""
        pass
    
    @abstractmethod
    def get_connection_stats(self) -> Dict[str, Any]:
        """Get connection statistics"""
        pass


class ILogger(ABC):
    """Interface for logging operations"""
    
    @abstractmethod
    def debug(self, message: str, **kwargs) -> None:
        """Log debug message"""
        pass
    
    @abstractmethod
    def info(self, message: str, **kwargs) -> None:
        """Log info message"""
        pass
    
    @abstractmethod
    def warning(self, message: str, **kwargs) -> None:
        """Log warning message"""
        pass
    
    @abstractmethod
    def error(self, message: str, **kwargs) -> None:
        """Log error message"""
        pass
    
    @abstractmethod
    def set_level(self, level: str) -> None:
        """Set logging level"""
        pass


class IWebDriver(ABC):
    """Interface for web driver/browser automation"""
    
    @abstractmethod
    async def goto_async(self, url: str, wait_until: str = 'networkidle') -> Any:
        """Navigate to URL (async)"""
        pass
    
    @abstractmethod
    def goto(self, url: str, wait_until: str = 'networkidle') -> Any:
        """Navigate to URL (sync)"""
        pass
    
    @abstractmethod
    async def get_content_async(self) -> str:
        """Get page content (async)"""
        pass
    
    @abstractmethod
    def get_content(self) -> str:
        """Get page content (sync)"""
        pass
    
    @abstractmethod
    async def wait_for_selector_async(self, selector: str, timeout: int = 30000) -> None:
        """Wait for selector to appear (async)"""
        pass
    
    @abstractmethod
    def wait_for_selector(self, selector: str, timeout: int = 30000) -> None:
        """Wait for selector to appear (sync)"""
        pass


class IContactValidator(ABC):
    """Interface for contact validation and enhancement"""
    
    @abstractmethod
    async def validate_whatsapp_async(self, phone_number: str, country_code: str = None) -> tuple[bool, str]:
        """Validate WhatsApp number (async)"""
        pass
    
    @abstractmethod
    def validate_whatsapp(self, phone_number: str, country_code: str = None) -> tuple[bool, str]:
        """Validate WhatsApp number (sync)"""
        pass
    
    @abstractmethod
    async def validate_for_whatsapp_async(self, 
                                         scraped_whatsapp: List[str],
                                         phone_number: str,
                                         country_code: str = None) -> str:
        """Prioritize WhatsApp validation (async)"""
        pass
    
    @abstractmethod
    def validate_for_whatsapp(self, 
                              scraped_whatsapp: List[str],
                              phone_number: str,
                              country_code: str = None) -> str:
        """Prioritize WhatsApp validation (sync)"""
        pass


class IScraperOrchestrator(ABC):
    """Interface for orchestrating the complete scraping pipeline"""
    
    @abstractmethod
    async def process_csv_async(self, input_file: str, output_file: str, **kwargs) -> ProcessingStats:
        """Process CSV file through complete pipeline (async)"""
        pass
    
    @abstractmethod
    def process_csv(self, input_file: str, output_file: str, **kwargs) -> ProcessingStats:
        """Process CSV file through complete pipeline (sync)"""
        pass
    
    @abstractmethod
    async def process_url_async(self, url: str, **kwargs) -> ContactInfo:
        """Process single URL through complete pipeline (async)"""
        pass
    
    @abstractmethod
    def process_url(self, url: str, **kwargs) -> ContactInfo:
        """Process single URL through complete pipeline (sync)"""
        pass
    
    @abstractmethod
    def get_pipeline_stats(self) -> Dict[str, Any]:
        """Get pipeline processing statistics"""
        pass


# Factory interfaces for creating service instances

class IScraperFactory(ABC):
    """Interface for creating web scraper instances"""
    
    @abstractmethod
    def create_scraper(self, config: Dict[str, Any]) -> IWebScraper:
        """Create configured web scraper"""
        pass


class IProcessorFactory(ABC):
    """Interface for creating processor instances"""
    
    @abstractmethod
    def create_processor(self, config: Dict[str, Any]) -> ICsvProcessor:
        """Create configured CSV processor"""
        pass


# Component configuration interfaces

class IConfigurable(ABC):
    """Interface for components that accept configuration"""
    
    @abstractmethod
    def configure(self, config: Dict[str, Any]) -> None:
        """Configure component with parameters"""
        pass


class IStartable(ABC):
    """Interface for components that require startup"""
    
    @abstractmethod
    async def start_async(self) -> bool:
        """Start component asynchronously"""
        pass
    
    @abstractmethod
    def start(self) -> bool:
        """Start component synchronously"""
        pass
    
    @abstractmethod
    async def stop_async(self) -> None:
        """Stop component asynchronously"""
        pass
    
    @abstractmethod
    def stop(self) -> None:
        """Stop component synchronously"""
        pass


class IHealthCheckable(ABC):
    """Interface for components that support health checks"""
    
    @abstractmethod
    async def is_healthy_async(self) -> bool:
        """Check component health (async)"""
        pass
    
    @abstractmethod
    def is_healthy(self) -> bool:
        """Check component health (sync)"""
        pass
    
    @abstractmethod
    def get_health_details(self) -> Dict[str, Any]:
        """Get detailed health status"""
        pass


# Utility interfaces

class IMetricsCollector(ABC):
    """Interface for collecting and reporting metrics"""
    
    @abstractmethod
    def increment_counter(self, name: str, value: float = 1.0) -> None:
        """Increment counter metric"""
        pass
    
    @abstractmethod
    def set_gauge(self, name: str, value: float) -> None:
        """Set gauge metric"""
        pass
    
    @abstractmethod
    def record_timing(self, name: str, duration: float) -> None:
        """Record timing metric"""
        pass
    
    @abstractmethod
    def get_metrics(self) -> Dict[str, Any]:
        """Get all collected metrics"""
        pass


# Service registry for interface discovery

class ServiceRegistry:
    """Registry for managing service interfaces and implementations"""
    
    def __init__(self):
        self._interfaces: Dict[str, Type] = {}
        self._implementations: Dict[str, Type] = {}
    
    def register_interface(self, interface: Type) -> None:
        """Register service interface"""
        self._interfaces[interface.__name__] = interface
    
    def register_implementation(self, interface_name: str, implementation: Type) -> None:
        """Register implementation for interface"""
        self._implementations[interface_name] = implementation
    
    def get_interface(self, name: str) -> Optional[Type]:
        """Get interface by name"""
        return self._interfaces.get(name)
    
    def get_implementation(self, interface_name: str) -> Optional[Type]:
        """Get implementation by interface name"""
        return self._implementations.get(interface_name)
    
    def list_interfaces(self) -> List[str]:
        """List all registered interface names"""
        return list(self._interfaces.keys())
    
    def list_implementations(self) -> List[str]:
        """List all registered implementation names"""
        return list(self._implementations.keys())


# Global service registry
_service_registry = ServiceRegistry()

# Register all interfaces
for name, cls in list(locals().items()):
    if name.startswith('I') and isinstance(cls, type) and cls.__name__.startswith('I'):
        _service_registry.register_interface(cls)


if __name__ == "__main__":
    # Demonstration of service interfaces
    print("Registered Service Interfaces:")
    for interface_name in _service_registry.list_interfaces():
        interface = _service_registry.get_interface(interface_name)
        methods = [method for method in dir(interface) if not method.startswith('_') and callable(getattr(interface, method))]
        print(f"  {interface_name}: {', '.join(methods)}")
    
    print(f"\nTotal interfaces registered: {len(_service_registry.list_interfaces())}")
    
    # Example interface usage demonstration
    print("\nExample interface method signatures:")
    
    # Show WebScraper interface
    web_scraper = IWebScraper
    print(f"\n{web_scraper.__name__} methods:")
    for method_name in ['scrape_url', 'scrape_urls_batch', 'get_stats', 'close']:
        if hasattr(web_scraper, method_name):
            method = getattr(web_scraper, method_name)
            print(f"  {method_name}{method.__doc__ or ''}")
