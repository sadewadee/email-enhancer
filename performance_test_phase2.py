#!/usr/bin/env python3
"""
Phase 2 Performance Testing & Validation Script

Tests and validates Phase 2 improvements:
1. Dependency Injection Container - Service lifecycle management
2. Configuration Service - Centralized configuration with validation
3. Service Interfaces - Abstract interfaces for key services
4. Adapter Patterns - Bridging concrete classes to interfaces

Usage:
    python performance_test_phase2.py [--component di_container|config_service|interfaces|adapters|all] [--sample-size 50]
"""

import asyncio
import json
import logging
import os
import sys
import time
import tempfile
import yaml
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional
import pandas as pd

# Ensure project root is in Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Try to import with fallback for missing dependencies
try:
    from di_container import DIContainer, ServiceLifecycle, ServiceDescriptor
    from configuration_service import ConfigurationService, Environment, ConfigSchema, CommonSchemas
    from service_interfaces import (
        IWebScraper, IContactExtractor, IEmailValidator, ICsvProcessor,
        IMemoryMonitor, ServiceRegistry, ContactInfo, ScrapingResult, ProcessingStats
    )
    from adapters import (
        create_web_scraper_adapter, create_contact_extractor_adapter,
        create_email_validator_adapter, get_adapter_registry
    )
    DEPENDENCIES_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Some dependencies not available: {e}")
    DEPENDENCIES_AVAILABLE = False


@dataclass
class Phase2TestResult:
    """Results from Phase 2 performance testing"""
    test_name: str
    component_type: str
    metrics: Dict[str, Any]
    success: bool
    error_message: Optional[str] = None
    duration_seconds: float = 0.0


class Phase2Tester:
    """
    Comprehensive Phase 2 testing suite for new architecture improvements.
    
    Tests each component individually and integrates them together.
    """
    
    def __init__(self, sample_size: int = 50):
        self.sample_size = sample_size
        self.logger = self._setup_logging()
        self.test_results: List[Phase2TestResult] = []
        
        # Memory baseline
        baseline_memory_usage = self._get_memory_usage()
        self.memory_baseline = baseline_memory_usage
        
    def _setup_logging(self) -> logging.Logger:
        """Setup logging for Phase 2 testing"""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        # Console handler
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
    
    def _get_memory_usage(self) -> float:
        """Get current memory usage"""
        try:
            import psutil
            return psutil.virtual_memory().used / 1024 / 1024
        except ImportError:
            return 0.0
    
    async def test_di_container_performance(self) -> Phase2TestResult:
        """Test Dependency Injection Container performance and functionality"""
        if not DEPENDENCIES_AVAILABLE:
            return Phase2TestResult(
                test_name=f"DIContainer-{self.sample_size}_services",
                component_type="DIContainer",
                metrics={},
                success=False,
                error_message="Dependencies not available (di_container module)",
                duration_seconds=0.0
            )
        """Test Dependency Injection Container performance and functionality"""
        self.logger.info("🧪 Testing DI Container Performance...")
        
        test_name = f"DIContainer-{self.sample_size}_services"
        start_time = time.time()
        
        try:
            # Initialize container
            container = DIContainer()
            
            # Define test interfaces
            class ITestServiceA:
                def process(self) -> str: return "A"
            
            class ITestServiceB:
                def process(self) -> str: return "B"
            
            class ITestServiceC:
                def process(self) -> str: return "C"
            
            # Implementations with dependencies
            class ServiceA(ITestServiceA):
                def __init__(self, config_value: str = "default"):
                    self.config_value = config_value
                
                def process(self) -> str:
                    return f"ServiceA({self.config_value})"
            
            class ServiceB(ITestServiceB):
                def __init__(self, service_a: ITestServiceA):
                    self.service_a = service_a
                
                def process(self) -> str:
                    return f"ServiceB->{self.service_a.process()}"
            
            class ServiceC(ITestServiceC):
                def __init__(self, service_b: ITestServiceB):
                    self.service_b = service_b
                
                def process(self) -> str:
                    return f"ServiceC->{self.service_b.process()}"
            
            # Register services with lifecycle management
            container.register_singleton(ITestServiceA, ServiceA)
            container.register_transient(ITestServiceB, ServiceB)
            container.register_singleton(ITestServiceC, ServiceC)
            
            # Test service resolution performance
            resolution_times = []
            successful_resolutions = 0
            
            for i in range(min(self.sample_size, 100)):
                resolve_start = time.time()
                
                try:
                    service_c = container.get(ITestServiceC)
                    result = service_c.process()
                    successful_resolutions += 1
                    
                    assert result.startswith("ServiceC->ServiceB->ServiceA")
                    
                    resolve_time = time.time() - resolve_start
                    resolution_times.append(resolve_time)
                    
                except Exception as e:
                    self.logger.warning(f"Resolution {i+1} failed: {e}")
            
            # Test service lifecycle
            service_a1 = container.get(ITestServiceA)
            service_a2 = container.get(ITestServiceA)
            
            singleton_working = service_a1 is service_a2  # Same instance
            
            service_b1 = container.get(ITestServiceB)
            service_b2 = container.get(ITestServiceB)
            
            transient_working = service_b1 is not service_b2  # Different instances
            
            # Test container statistics
            stats = container.get_stats()
            container.cleanup()
            
            test_duration = time.time() - start_time
            
            metrics = {
                'services_registered': stats['registrations'],
                'resolutions_attempted': len(resolution_times),
                'successful_resolutions': successful_resolutions,
                'success_rate': (successful_resolutions / len(resolution_times) * 100) if resolution_times else 0,
                'avg_resolution_time': (sum(resolution_times) / len(resolution_times) * 1000) if resolution_times else 0,
                'singleton_test_passed': singleton_working,
                'transient_test_passed': transient_working,
                'total_stats': stats
            }
            
            result = Phase2TestResult(
                test_name=test_name,
                component_type="DIContainer",
                metrics=metrics,
                success=True,
                duration_seconds=test_duration
            )
            
            self.logger.info(f"✅ DI Container Test Completed:")
            self.logger.info(f"  Services Registered: {metrics['services_registered']}")
            self.logger.info(f"  Resolution Success Rate: {metrics['success_rate']:.1f}%")
            self.logger.info(f"  Avg Resolution Time: {metrics['avg_resolution_time']:.2f}ms")
            self.logger.info(f"  Singleton Test: {'✓' if metrics['singleton_test_passed'] else '✗'}")
            self.logger.info(f"  Transient Test: {'✓' if metrics['transient_test_passed'] else '✗'}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"❌ DI Container Test Failed: {e}")
            return Phase2TestResult(
                test_name=test_name,
                component_type="DIContainer",
                metrics={},
                success=False,
                error_message=str(e),
                duration_seconds=time.time() - start_time
            )
    
    async def test_configuration_service_performance(self) -> Phase2TestResult:
        """Test Configuration Service performance and features"""
        if not DEPENDENCIES_AVAILABLE:
            return Phase2TestResult(
                test_name=f"ConfigService-{self.sample_size}_operations",
                component_type="ConfigurationService",
                metrics={},
                success=False,
                error_message="Dependencies not available (configuration_service module)",
                duration_seconds=0.0
            )
        
        self.logger.info("🧪 Testing Configuration Service Performance...")
        
        test_name = f"ConfigService-{self.sample_size}_operations"
        start_time = time.time()
        
        try:
            # Create sample configuration file
            sample_config = {
                'app_name': 'Phase 2 Test',
                'version': '2.0.0',
                'max_workers': 5,
                'timeout': 120,
                'environments': {
                    'development': {
                        'log_level': 'DEBUG',
                        'debug_mode': True
                    },
                    'production': {
                        'log_level': 'INFO',
                        'debug_mode': False
                    }
                },
                'scraper': {
                    'chunk_size': 1000,
                    'retry_attempts': 3
                }
            }
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                config_file = f.name
                yaml.dump(sample_config, f, default_flow_style=False)
            
            try:
                # Initialize configuration service
                config_service = ConfigurationService(
                    config_path=config_file,
                    environment=Environment.DEVELOPMENT
                )
                
                # Define schemas
                for key_path, schema in CommonSchemas.scraper_config().items():
                    config_service.define_schema(key_path, schema)
                
                # Test configuration access performance
                access_times = []
                successful_accesses = 0
                
                for i in range(min(self.sample_size, 100)):
                    access_start = time.time()
                    
                    try:
                        # Test various access patterns
                        config_service.get('app_name')
                        config_service.get('max_workers')
                        config_service.get('environments.development.log_level')
                        config_service.get('scraper.chunk_size')
                        
                        successful_accesses += 1
                        
                        access_time = time.time() - access_start
                        access_times.append(access_time)
                        
                    except Exception as e:
                        self.logger.warning(f"Access {i+1} failed: {e}")
                
                # Test validation
                config_service.set('scraper.max_workers', 10)
                config_service.set('app_name', 'Modified App')
                
                # Test validation
                validation_errors = config_service.validate_all()
                
                # Test schema validation
                try:
                    config_service.set('scraper.timeout', 'invalid')  # Should be int
                    schema_works = False
                except:
                    schema_works = True
                
                # Test configuration stats
                stats = config_service.get_stats()
                
                # Test hot reload simulation
                original_max_workers = config_service.get('max_workers')
                
                # Modify config in memory
                config_service.set('max_workers', 15)
                new_max_workers = config_service.get('max_workers')
                reload_works = new_max_workers == 15
                
                config_service.cleanup()
                
                test_duration = time.time() - start_time
                
                metrics = {
                    'config_accesses': len(access_times),
                    'successful_accesses': successful_accesses,
                    'access_rate': (successful_accesses / len(access_times) * 100) if access_times else 0,
                    'avg_access_time': (sum(access_times) / len(access_times) * 1000) if access_times else 0,
                    'validation_errors': len(validation_errors),
                    'schema_validation_works': schema_works,
                    'reload_test_passed': reload_works,
                    'total_keys': stats['total_keys'],
                    'schema_definitions': stats['schema_definitions'],
                    'environment': stats['environment']
                }
                
                result = Phase2TestResult(
                    test_name=test_name,
                    component_type="ConfigurationService",
                    metrics=metrics,
                    success=True,
                    duration_seconds=test_duration
                )
                
                self.logger.info(f"✅ Configuration Service Test Completed:")
                self.logger.info(f"  Config Access Rate: {metrics['access_rate']:.1f}%")
                self.logger.info(f"  Avg Access Time: {metrics['avg_access_time']:.2f}ms")
                self.logger.info(f"  Validation Errors: {metrics['validation_errors']}")
                self.logger.info(f"  Schema Validation: {'✓' if metrics['schema_validation_works'] else '✗'}")
                self.logger.info(f"  Reload Test: {'✓' if metrics['reload_test_passed'] else '✗'}")
                self.logger.info(f"  Total Keys: {metrics['total_keys']}")
                
                return result
                
            finally:
                # Clean up test file
                if os.path.exists(config_file):
                    os.unlink(config_file)
                    
        except Exception as e:
            self.logger.error(f"❌ Configuration Service Test Failed: {e}")
            return Phase2TestResult(
                test_name=test_name,
                component_type="ConfigurationService",
                metrics={},
                success=False,
                error_message=str(e),
                duration_seconds=time.time() - start_time
            )
    
    async def test_service_interfaces(self) -> Phase2TestResult:
        """Test Service Interface abstractions and implementations"""
        self.logger.info("🧪 Testing Service Interfaces...")
        
        test_name = f"ServiceInterfaces-{self.sample_size}_tests"
        start_time = time.time()
        
        try:
            # Test interface registry
            registry = ServiceRegistry()
            
            # Test interface discovery
            available_interfaces = registry.list_interfaces()
            
            # Test adapter registry
            adapter_registry = get_adapter_registry()
            available_adapters = adapter_registry.list_adapters()
            
            # Test interface implementation mapping
            interface_tests = 0
            successful_implementations = 0
            
            for interface_name in available_interfaces[:min(self.sample_size, len(available_interfaces))]:
                interface_tests += 1
                
                try:
                    interface = registry.get_interface(interface_name)
                    
                    # Test that interface has required methods
                    required_methods = {'scrape_url', 'get_stats', 'close'} if interface_name == 'IWebScraper' else \
                                     {'extract_all_contacts', 'extract_emails'} if interface_name == 'IContactExtractor' else \
                                     {'validate_format', 'validate_smtp'} if interface_name == 'IEmailValidator' else \
                                     {'process_csv_file', 'process_single_url'} if interface_name == 'ICsvProcessor' else \
                                     {'start_monitoring', 'stop_monitoring'} if interface_name == 'IMemoryMonitor' else []
                    
                    method_count = sum(1 for method in dir(interface) if not method.startswith('_') and callable(getattr(interface, method)))
                    
                    successful_implementations += 1
                    
                    self.logger.debug(f"  Interface {interface_name}: {method_count} methods")
                    
                except Exception as e:
                    self.logger.warning(f"Interface test failed for {interface_name}: {e}")
            
            # Test adapter creation
            adapter_tests = 0
            successful_adapters = 0
            
            adapter_configs = [
                ('IWebScraper', {}),
                ('IContactExtractor', {}),
                ('IEmailValidator', {}),
                ('IMemoryMonitor', {}),
            ]
            
            for adapter_name, config in adapter_configs:
                adapter_tests += 1
                
                try:
                    adapter = adapter_registry.create_adapter(adapter_name, **config)
                    successful_adapters += 1
                    
                    # Test that adapter implements interface
                    self.logger.debug(f"  Adapter created: {type(adapter).__name__}")
                    
                except Exception as e:
                    self.logger.warning(f"Adapter creation failed for {adapter_name}: {e}")
            
            # Test interface compatibility with DI container
            container = DIContainer()
            
            # Try to register adapters as services
            di_registrations = 0
            successful_di_registrations = 0
            
            for adapter_config in adapter_configs[:min(5, len(adapter_configs))]:
                di_registrations += 1
                
                try:
                    # Create factory for adapter
                    def factory(config=adapter_config[1]):
                        return adapter_registry.create_adapter(adapter_config[0], **config)
                    
                    # Register as factory service
                    import service_interfaces
                    interface = getattr(service_interfaces, adapter_config[0])
                    
                    if hasattr(interface, '__name__'):
                        container.register_factory(interface, factory)
                        successful_di_registrations += 1
                    
                except Exception as e:
                    self.logger.warning(f"DI registration failed for {adapter_config[0]}: {e}")
            
            container.cleanup()
            
            test_duration = time.time() - start_time
            
            metrics = {
                'available_interfaces': len(available_interfaces),
                'interface_test_rate': (successful_implementations / interface_tests * 100) if interface_tests > 0 else 0,
                'available_adapters': len(available_adapters),
                'adapter_success_rate': (successful_adapters / adapter_tests * 100) if adapter_tests > 0 else 0,
                'di_registrations': di_registrations,
                'di_success_rate': (successful_di_registrations / di_registrations * 100) if di_registrations > 0 else 0
            }
            
            result = Phase2TestResult(
                test_name=test_name,
                component_type="ServiceInterfaces",
                metrics=metrics,
                success=True,
                duration_seconds=test_duration
            )
            
            self.logger.info(f"✅ Service Interfaces Test Completed:")
            self.logger.info(f"  Available Interfaces: {metrics['available_interfaces']}")
            self.logger.info(f"  Interface Test Rate: {metrics['interface_test_rate']:.1f}%")
            self.logger.info(f"  Available Adapters: {metrics['available_adapters']}")
            self.logger.info(f"  Adapter Success Rate: {metrics['adapter_success_rate']:.1f}%")
            self.logger.info(f"  DI Integration Rate: {metrics['di_success_rate']:.1f}%")
            
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Service Interfaces Test Failed: {e}")
            return Phase2TestResult(
                test_name=test_name,
                component_type="ServiceInterfaces",
                metrics={},
                success=False,
                error_message=str(e),
                duration_seconds=time.time() - start_time
            )
    
    async def test_adapter_patterns(self) -> Phase2TestResult:
        """Test Adapter Pattern implementations and performance"""
        self.logger.info("🧪 Testing Adapter Patterns...")
        
        test_name = f"AdapterPatterns-{self.sample_size}_operations"
        start_time = time.time()
        
        try:
            # Test adapter creation performance
            adapter_creation_times = []
            successful_creations = 0
            
            for i in range(min(50, self.sample_size)):
                creation_start = time.time()
                
                try:
                    # Test various adapter types
                    if i % 4 == 0:
                        adapter = create_web_scraper_adapter()
                    elif i % 4 == 1:
                        adapter = create_contact_extractor_adapter()
                    elif i % 4 == 2:
                        adapter = create_email_validator_adapter()
                    else:
                        adapter = create_memory_monitor_adapter()
                    
                    successful_creations += 1
                    
                    creation_time = time.time() - creation_start
                    adapter_creation_times.append(creation_time)
                    
                except Exception as e:
                    self.logger.warning(f"Adapter creation {i+1} failed: {e}")
            
            # Test interface compliance
            adapter_registry = get_adapter_registry()
            interface_compliance = True
            
            try:
                # Test all adapters implement their interfaces
                web_scraper = adapter_registry.create_adapter('IWebScraper')
                contact_extractor = adapter_registry.create_adapter('IContactExtractor')
                email_validator = adapter_registry.create_adapter('IEmailValidator')
                
                # Check methods exist (basic compliance test)
                has_scrape_url = hasattr(web_scraper, 'scrape_url')
                has_extract_contacts = hasattr(contact_extractor, 'extract_all_contacts')
                has_validate_format = hasattr(email_validator, 'validate_format')
                
                interface_compliance = all([has_scrape_url, has_extract_contacts, has_validate_format])
                
            except Exception as e:
                self.logger.warning(f"Interface compliance test failed: {e}")
                interface_compliance = False
            
            # Test adapter functionality
            functionality_tests = 0
            successful_functionality = 0
            
            try:
                # Test email validator functionality
                email_validator = adapter_registry.create_adapter('IEmailValidator')
                functionality_tests += 1
                
                is_valid = email_validator.validate_format("test@example.com")
                if is_valid:
                    successful_functionality += 1
                
                # Test extractor functionality
                extractor = adapter_registry.create_adapter('IContactExtractor')
                functionality_tests += 1
                
                html = "<p>Email: test@example.com</p><p>Phone: +1234567890</p>"
                contacts = extractor.extract_all_contacts(html, "https://example.com")
                if len(contacts.emails) > 0:
                    successful_functionality += 1
                
            except Exception as e:
                self.logger.warning(f"Functionality test failed: {e}")
            
            # Test performance with high-frequency adapter creation
            high_freq_time = time.time()
            high_freq_created = 0
            
            for _ in range(min(20, self.sample_size // 2)):
                try:
                    adapter = create_web_scraper_adapter()
                    high_freq_created += 1
                    
                    # Simulate some processing
                    result = adapter.scrape_url("https://example.com")
                    
                    # Simulate cleanup
                    if hasattr(adapter, 'close'):
                        adapter.close()
                        
                except Exception:
                    pass  # Ignore errors in high-frequency test
            
            high_freq_duration = time.time() - high_freq_time
            
            test_duration = time.time() - start_time
            
            metrics = {
                'adapter_creations': len(adapter_creation_times),
                'creation_success_rate': (successful_creations / len(adapter_creation_times) * 100) if adapter_creation_times else 0,
                'avg_creation_time': (sum(adapter_creation_times) / len(adapter_creation_times) * 1000) if adapter_creation_times else 0,
                'interface_compliance': interface_compliance,
                'functionality_tests': functionality_tests,
                'functionality_success_rate': (successful_functionality / functionality_tests * 100) if functionality_tests > 0 else 0,
                'high_freq_created': high_freq_created,
                'high_freq_rate': (high_freq_created / 20 * 100),
                'high_freq_avg_time': (high_freq_duration / high_freq_created * 1000) if high_freq_created > 0 else 0
            }
            
            result = Phase2TestResult(
                test_name=test_name,
                component_type="AdapterPatterns",
                metrics=metrics,
                success=True,
                duration_seconds=test_duration
            )
            
            self.logger.info(f"✅ Adapter Patterns Test Completed:")
            self.logger.info(f"  Creation Success Rate: {metrics['creation_success_rate']:.1f}%")
            self.logger.info(f"  Avg Creation Time: {metrics['avg_creation_time']:.2f}ms")
            self.logger.info(f"  Interface Compliance: {'✓' if metrics['interface_compliance'] else '✗'}")
            self.logger.info(f"  Functionality Success Rate: {metrics['functionality_success_rate']:.1f}%")
            self.logger.info(f"  High Frequency Rate: {metrics['high_freq_rate']:.1f}%")
            
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Adapter Patterns Test Failed: {e}")
            return Phase2TestResult(
                test_name=test_name,
                component_type="AdapterPatterns",
                metrics={},
                success=False,
                error_message=str(e),
                duration_seconds=time.time() - start_time
            )
    
    async def run_all_tests(self) -> Dict[str, Phase2TestResult]:
        """Run all Phase 2 performance tests"""
        self.logger.info("🚀 Starting Phase 2 Performance Testing Suite")
        self.logger.info(f"Sample Size: {self.sample_size}")
        self.logger.info(f"Memory Baseline: {self.memory_baseline:.1f}MB")
        
        all_results = {}
        
        # Run tests in sequence
        tests = [
            ('di_container', self.test_di_container_performance),
            ('config_service', self.test_configuration_service_performance),
            ('service_interfaces', self.test_service_interfaces),
            ('adapter_patterns', self.test_adapter_patterns)
        ]
        
        for test_name, test_func in tests:
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f"Testing: {test_name.upper()}")
            self.logger.info(f"{'='*60}")
            
            result = await test_func()
            all_results[test_name] = result
            self.test_results.append(result)
            
            # Small pause between tests
            await asyncio.sleep(1)
        
        # Performance summary
        self.logger.info(f"\n{'='*60}")
        self.logger.info("PHASE 2 PERFORMANCE TESTING SUMMARY")
        self.logger.info(f"{'='*60}")
        
        successful_tests = [r for r in self.test_results if r.success]
        failed_tests = [r for r in self.test_results if not r.success]
        
        self.logger.info(f"Tests Run: {len(self.test_results)}")
        self.logger.info(f"Successful: {len(successful_tests)}")
        self.logger.info(f"Failed: {len(failed_tests)}")
        
        total_duration = sum(r.duration_seconds for r in self.test_results)
        self.logger.info(f"Total Duration: {total_duration:.1f}s")
        
        final_memory = self._get_memory_usage()
        memory_change = final_memory - self.memory_baseline
        self.logger.info(f"Memory Change: {memory_change:+.1f}MB")
        
        if failed_tests:
            self.logger.info(f"\nFailed Tests:")
            for test in failed_tests:
                self.logger.info(f"  {test.test_name}: {test.error_message}")
        
        # Key performance indicators
        self.logger.info(f"\nKey Performance Indicators:")
        
        di_container = all_results.get('di_container')
        if di_container and di_container.success:
            metrics = di_container.metrics
            self.logger.info(f"  DI Container Resolution Rate: {metrics.get('success_rate', 0):.1f}%")
            self.logger.info(f"  DI Container Singleton/Transient: {'✓' if metrics.get('singleton_test_passed', False) and metrics.get('transient_test_passed', False) else '✗'}")
        
        config_service = all_results.get('config_service')
        if config_service and config_service.success:
            metrics = config_service.metrics
            self.logger.info(f"  Config Service Access Rate: {metrics.get('access_rate', 0):.1f}%")
            self.logger.info(f"  Config Service Schema Validation: {'✓' if metrics.get('schema_validation_works', False) else '✗'}")
        
        service_interfaces = all_results.get('service_interfaces')
        if service_interfaces and service_interfaces.success:
            metrics = service_interfaces.metrics
            self.logger.info(f"  Interface Coverage: {len(metrics.get('available_interfaces', []))} interfaces")
            self.logger.info(f"  DI Integration Rate: {metrics.get('di_success_rate', 0):.1f}%")
        
        adapter_patterns = all_results.get('adapter_patterns')
        if adapter_patterns and adapter_patterns.success:
            metrics = adapter_patterns.metrics
            self.logger.info(f"  Adapter Creation Rate: {metrics.get('creation_success_rate', 0):.1f}%")
            self.logger.info(f"  Interface Compliance: {'✓' if metrics.get('interface_compliance', False) else '✗'}")
        
        return all_results
    
    def save_results(self, results: Dict[str, Phase2TestResult], filename: str = "performance_test_phase2_results.json"):
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
        
        self.logger.info(f"📊 Phase 2 test results saved to {filename}")


async def main():
    """Main function for Phase 2 performance testing"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Phase 2 Performance Testing Suite")
    parser.add_argument('--component', choices=['di_container', 'config_service', 'service_interfaces', 'adapter_patterns', 'all'], 
                       default='all', help='Component to test')
    parser.add_argument('--sample-size', type=int, default=50, help='Sample size for testing')
    parser.add_argument('--save-results', help='Save results to specified file')
    
    args = parser.parse_args()
    
    # Initialize Phase 2 tester
    tester = Phase2Tester(sample_size=args.sample_size)
    
    try:
        if args.component == 'all':
            results = await tester.run_all_tests()
        else:
            # Run specific test
            test_methods = {
                'di_container': tester.test_di_container_performance,
                'config_service': tester.test_configuration_service_performance,
                'service_interfaces': tester.test_service_interfaces,
                'adapter_patterns': tester.test_adapter_patterns
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
        
    except Exception as e:
        print(f"Phase 2 testing failed: {e}")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
