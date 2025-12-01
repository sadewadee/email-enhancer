"""
Dependency Injection Container - Lightweight IoC Container with Service Lifecycle Management

Implements minimal dependency injection with:
- Interface-based registration and resolution
- Singleton and transient service lifecycles
- Configuration-driven instantiation
- Circular dependency detection
- Service health monitoring
- Factory function support
"""

import logging
import inspect
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import (
    Any, Dict, Type, TypeVar, Callable, Optional, 
    List, Set, Union, TypeVarTuple, ParamSpec
)
import threading
import time
from collections import defaultdict

# Generic type variables
T = TypeVar('T')
P = ParamSpec('P')


class ServiceLifecycle(Enum):
    """Service lifecycle management options"""
    SINGLETON = "singleton"      # One instance shared across container
    TRANSIENT = "transient"      # New instance per request
    SCOPED = "scoped"           # One instance per scope context
    FACTORY = "factory"        # Factory function creates instances


@dataclass
class ServiceDescriptor:
    """Descriptor for registered service"""
    interface: Type
    implementation: Type
    lifecycle: ServiceLifecycle = ServiceLifecycle.TRANSIENT
    factory: Optional[Callable] = None
    dependencies: List[Type] = field(default_factory=list)
    config_path: Optional[str] = None
    health_check_interval: float = 60.0
    
    def __post_init__(self):
        """Validate descriptor configuration"""
        if self.lifecycle == ServiceLifecycle.FACTORY and self.factory is None:
            raise ValueError("Factory lifecycle requires factory function")
        
        if self.lifecycle != ServiceLifecycle.FACTORY and self.factory is not None:
            raise ValueError("Factory function only allowed with FACTORY lifecycle")


@dataclass
class ServiceMetadata:
    """Metadata about resolved service"""
    instance: Any
    created_at: float
    resolved_count: int = 0
    last_health_check: float = 0.0
    health_status: bool = True
    lifecycle: ServiceLifecycle = ServiceLifecycle.TRANSIENT
    
    def is_healthy(self) -> bool:
        """Check if service instance is healthy"""
        if self.lifecycle == ServiceLifecycle.SINGLETON:
            # For singletons, check if they have health method
            if hasattr(self.instance, 'is_healthy'):
                return self.instance.is_healthy()
        return True


class CircularDependencyError(Exception):
    """Raised when circular dependency is detected during resolution"""
    pass


class ServiceNotRegisteredError(Exception):
    """Raised when attempting to resolve unregistered service"""
    pass


class DIContainer:
    """
    Lightweight Dependency Injection Container with lifecycle management.
    
    Features:
    - Interface-based service registration and resolution
    - Automatic dependency injection with constructor analysis
    - Singleton and transient service lifecycles
    - Circular dependency detection and prevention
    - Service health monitoring and automatic recovery
    - Configuration-driven service instantiation
    """
    
    def __init__(self, config_service: Optional[Any] = None):
        """ Initialize DI container with optional config service """
        self.logger = logging.getLogger(__name__)
        self.config_service = config_service
        
        # Service registry
        self._services: Dict[Type, ServiceDescriptor] = {}
        self._instances: Dict[Type, ServiceMetadata] = {}  # Singleton instances
        self._factories: Dict[Type, Callable] = {}  # Factory functions
        
        # Resolution tracking for circular dependency detection
        self._resolution_stack: List[Type] = []
        self._circular_dependencies: Set[Tuple[Type, Type]] = set()
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Health monitoring
        self._health_monitoring = False
        self._monitor_thread: Optional[threading.Thread] = None
        
        # Statistics
        self._stats = {
            'registrations': 0,
            'resolutions': 0,
            'singletons_created': 0,
            'transients_created': 0,
            'health_checks': 0,
            'health_failures': 0
        }
    
    def register_singleton(self, interface: Type[T], implementation: Type[T]) -> 'DIContainer':
        """Register singleton service implementation"""
        with self._lock:
            descriptor = ServiceDescriptor(
                interface=interface,
                implementation=implementation,
                lifecycle=ServiceLifecycle.SINGLETON
            )
            self._services[interface] = descriptor
            self._stats['registrations'] += 1
            self.logger.debug(f"Registered singleton: {interface.__name__} -> {implementation.__name__}")
            return self
    
    def register_transient(self, interface: Type[T], implementation: Type[T]) -> 'DIContainer':
        """Register transient service implementation"""
        with self._lock:
            descriptor = ServiceDescriptor(
                interface=interface,
                implementation=implementation,
                lifecycle=ServiceLifecycle.TRANSIENT
            )
            self._services[interface] = descriptor
            self._stats['registrations'] += 1
            self.logger.debug(f"Registered transient: {interface.__name__} -> {implementation.__name__}")
            return self
    
    def register_factory(self, interface: Type[T], factory: Callable[..., T]) -> 'DIContainer':
        """Register factory function for service creation"""
        with self._lock:
            descriptor = ServiceDescriptor(
                interface=interface,
                implementation=type(factory.__name__, (), {}),  # Dummy type
                lifecycle=ServiceLifecycle.FACTORY,
                factory=factory
            )
            self._services[interface] = descriptor
            self._factories[interface] = factory
            self._stats['registrations'] += 1
            self.logger.debug(f"Registered factory: {interface.__name__} -> {factory.__name__}")
            return self
    
    def register_with_config(self, interface: Type[T], implementation: Type[T], 
                            config_path: str, lifecycle: ServiceLifecycle = ServiceLifecycle.TRANSIENT) -> 'DIContainer':
        """Register service with configuration path"""
        with self._lock:
            descriptor = ServiceDescriptor(
                interface=interface,
                implementation=implementation,
                lifecycle=lifecycle,
                config_path=config_path
            )
            self._services[interface] = descriptor
            self._stats['registrations'] += 1
            self.logger.debug(f"Registered with config: {interface.__name__} -> {implementation.__name__} (config: {config_path})")
            return self
    
    def get(self, interface: Type[T], **kwargs) -> T:
        """
        Resolve service instance with dependency injection.
        
        Args:
            interface: Service interface type to resolve
            **kwargs: Additional parameters for factory functions
            
        Returns:
            Service instance of type T
            
        Raises:
            ServiceNotRegisteredError: If service is not registered
            CircularDependencyError: If circular dependency detected
        """
        with self._lock:
            # Check if service is registered
            if interface not in self._services:
                raise ServiceNotRegisteredError(f"Service {interface.__name__} is not registered")
            
            # Check for circular dependency
            if interface in self._resolution_stack:
                cycle_path = " -> ".join([cls.__name__ for cls in self._resolution_stack] + [interface.__name__])
                raise CircularDependencyError(f"Circular dependency detected: {cycle_path}")
            
            # Add to resolution stack
            self._resolution_stack.append(interface)
            
            try:
                descriptor = self._services[interface]
                
                # Handle singleton instances
                if descriptor.lifecycle == ServiceLifecycle.SINGLETON:
                    if interface in self._instances:
                        metadata = self._instances[interface]
                        metadata.resolved_count += 1
                        return metadata.instance
                    else:
                        # Create new singleton instance
                        instance = self._create_instance(descriptor, **kwargs)
                        metadata = ServiceMetadata(
                            instance=instance,
                            created_at=time.time(),
                            lifecycle=ServiceLifecycle.SINGLETON
                        )
                        self._instances[interface] = metadata
                        self._stats['singletons_created'] += 1
                        return instance
                
                # Handle transient instances
                elif descriptor.lifecycle == ServiceLifecycle.TRANSIENT:
                    instance = self._create_instance(descriptor, **kwargs)
                    metadata = ServiceMetadata(
                        instance=instance,
                        created_at=time.time(),
                        lifecycle=ServiceLifecycle.TRANSIENT
                    )
                    self._stats['transients_created'] += 1
                    return instance
                
                # Handle factory instances
                elif descriptor.lifecycle == ServiceLifecycle.FACTORY:
                    factory = self._factories[interface]
                    args = self._resolve_dependencies(factory, **kwargs)
                    instance = factory(*args)
                    self._stats['transients_created'] += 1
                    return instance
                
                else:
                    raise ValueError(f"Unsupported lifecycle: {descriptor.lifecycle}")
                    
            finally:
                # Remove from resolution stack
                self._resolution_stack.remove(interface)
                self._stats['resolutions'] += 1
    
    def get_factory(self, interface: Type[T]) -> Callable[..., T]:
        """Get factory function for creating instances with custom parameters"""
        with self._lock:
            if interface in self._factories:
                return self._factories[interface]
            
            # Create factory that resolves with dependencies
            def factory_with_di(**kwargs):
                return self.get(interface, **kwargs)
            
            return factory_with_di
    
    def try_get(self, interface: Type[T], default: Optional[T] = None) -> Optional[T]:
        """Try to resolve service, return None or default if not found"""
        try:
            return self.get(interface)
        except (ServiceNotRegisteredError, CircularDependencyError):
            return default
    
    def _create_instance(self, descriptor: ServiceDescriptor, **kwargs) -> Any:
        """Create service instance with dependency injection"""
        implementation = descriptor.implementation
        
        # Analyze constructor for dependencies
        sig = inspect.signature(implementation.__init__)
        
        # Build arguments for constructor
        constructor_args = {}
        constructor_kwargs = {}
        
        for param_name, param in sig.parameters.items():
            if param_name == 'self':
                continue
            
            # Check for explicit parameter in kwargs
            if param_name in kwargs:
                constructor_kwargs[param_name] = kwargs[param_name]
                continue
            
            # Try to resolve from container
            if param.annotation != inspect.Parameter.empty:
                dependency_type = param.annotation
                
                if self.is_registered(dependency_type):
                    # Resolve dependency
                    constructor_kwargs[param_name] = self.get(dependency_type)
                else:
                    # Check if parameter has default value
                    if param.default != inspect.Parameter.empty:
                        constructor_kwargs[param_name] = param.default
                    else:
                        # Try to get from config service
                        if self.config_service and descriptor.config_path:
                            config_key = f"{descriptor.config_path}.{param_name}"
                            config_value = self.config_service.get(config_key)
                            if config_value is not None:
                                constructor_kwargs[param_name] = config_value
                            else:
                                self.logger.warning(f"Cannot resolve dependency {param_name} for {implementation.__name__}")
                        else:
                            self.logger.warning(f"Cannot resolve dependency {param_name} for {implementation.__name__}")
        
        # Create instance
        try:
            if constructor_args:
                instance = implementation(*constructor_args.values(), **constructor_kwargs)
            else:
                instance = implementation(**constructor_kwargs)
            
            # Set config if available
            if self.config_service and descriptor.config_path:
                self._apply_config(instance, descriptor.config_path)
            
            return instance
            
        except Exception as e:
            self.logger.error(f"Failed to create instance of {implementation.__name__}: {e}")
            raise
    
    def _resolve_dependencies(self, factory: Callable, **kwargs) -> List[Any]:
        """Resolve factory dependencies"""
        sig = inspect.signature(factory)
        dependencies = []
        
        for param_name, param in sig.parameters.items():
            if param_name in kwargs:
                dependencies.append(kwargs[param_name])
            elif param.annotation != inspect.Parameter.empty:
                if self.is_registered(param.annotation):
                    dependencies.append(self.get(param.annotation))
                elif param.default != inspect.Parameter.empty:
                    dependencies.append(param.default)
                else:
                    dependencies.append(None)
        
        return dependencies
    
    def _apply_config(self, instance: Any, config_path: str):
        """Apply configuration to service instance"""
        if not self.config_service:
            return
        
        try:
            # Get all config for this service
            service_config = self.config_service.get(config_path, {})
            
            # Try to apply via config method
            if hasattr(instance, 'configure'):
                instance.configure(service_config)
            # Or set attributes directly
            elif hasattr(instance, 'config'):
                instance.config = service_config
            # Or update via dict-like interface
            elif hasattr(instance, 'update'):
                instance.update(service_config)
            
        except Exception as e:
            self.logger.warning(f"Failed to apply config to {instance.__class__.__name__}: {e}")
    
    def is_registered(self, interface: Type) -> bool:
        """Check if service interface is registered"""
        return interface in self._services
    
    def get_descriptor(self, interface: Type) -> Optional[ServiceDescriptor]:
        """Get service descriptor for interface"""
        return self._services.get(interface)
    
    def start_health_monitoring(self, interval: float = 60.0):
        """Start health monitoring for singleton services"""
        if self._health_monitoring:
            return
        
        self._health_monitoring = True
        self._monitor_thread = threading.Thread(target=self._health_monitor_loop, args=(interval,), daemon=True)
        self._monitor_thread.start()
        self.logger.info("Health monitoring started")
    
    def stop_health_monitoring(self):
        """Stop health monitoring"""
        if not self._health_monitoring:
            return
        
        self._health_monitoring = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5.0)
        
        self.logger.info("Health monitoring stopped")
    
    def _health_monitor_loop(self, interval: float):
        """Health monitoring loop running in background thread"""
        while self._health_monitoring:
            try:
                with self._lock:
                    current_time = time.time()
                    
                    for interface, metadata in list(self._instances.items()):
                        # Check if it's time for health check
                        if current_time - metadata.last_health_check >= interval:
                            try:
                                # Perform health check
                                old_status = metadata.health_status
                                metadata.health_status = metadata.is_healthy()
                                metadata.last_health_check = current_time
                                
                                self._stats['health_checks'] += 1
                                
                                # Log status changes
                                if old_status != metadata.health_status:
                                    status = "healthy" if metadata.health_status else "unhealthy"
                                    self.logger.info(f"Service {interface.__name__} status: {status}")
                                
                                if not metadata.health_status:
                                    self._stats['health_failures'] += 1
                                
                            except Exception as e:
                                self.logger.error(f"Health check failed for {interface.__name__}: {e}")
                                self._stats['health_failures'] += 1
                
                # Wait for next check
                time.sleep(interval)
                
            except Exception as e:
                self.logger.error(f"Health monitoring error: {e}")
                time.sleep(interval)
    
    def health_status(self) -> Dict[Type, bool]:
        """Get health status of all singleton services"""
        with self._lock:
            return {
                interface: metadata.health_status
                for interface, metadata in self._instances.items()
            }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get container statistics"""
        with self._lock:
            return {
                **self._stats,
                'registered_services': len(self._services),
                'singleton_instances': len(self._instances),
                'factory_count': len(self._factories),
                'health_monitoring_active': self._health_monitoring,
                'health_status_summary': {
                    'healthy': sum(1 for status in self.health_status().values() if status),
                    'unhealthy': sum(1 for status in self.health_status().values() if not status)
                }
            }
    
    def list_all_services(self) -> List[str]:
        """Get list of all registered service names"""
        with self._lock:
            return [f"{interface.__name__} -> {desc.implementation.__name__} ({desc.lifecycle.value})"
                   for interface, desc in self._services.items()]
    
    def create_scope(self) -> 'DIScope':
        """Create a new scope for scoped services"""
        return DIScope(self)
    
    def cleanup(self):
        """Clean up container resources"""
        self.stop_health_monitoring()
        
        # Cleanup singleton instances
        with self._lock:
            for interface, metadata in self._instances.items():
                try:
                    if hasattr(metadata.instance, 'close'):
                        metadata.instance.close()
                    elif hasattr(metadata.instance, 'cleanup'):
                        metadata.instance.cleanup()
                except Exception as e:
                    self.logger.warning(f"Error cleaning up {interface.__name__}: {e}")
            
            self._instances.clear()


class DIScope:
    """Dependency injection scope for managing scoped services"""
    
    def __init__(self, container: DIContainer):
        self.container = container
        self._scoped_instances: Dict[Type, Any] = {}
        self._created_at = time.time()
    
    def get(self, interface: Type[T], **kwargs) -> T:
        """Get scoped or singleton service instance"""
        # Return existing scoped instance
        if interface in self._scoped_instances:
            return self._scoped_instances[interface]
        
        # Get singleton from container
        descriptor = self.container.get_descriptor(interface)
        if descriptor and descriptor.lifecycle == ServiceLifecycle.SINGLETON:
            return self.container.get(interface, **kwargs)
        
        # Create scoped instance
        instance = self.container.get(interface, **kwargs)
        self._scoped_instances[interface] = instance
        return instance
    
    def cleanup(self):
        """Clean up scoped instances"""
        for interface, instance in self._scoped_instances.items():
            try:
                if hasattr(instance, 'close'):
                    instance.close()
                elif hasattr(instance, 'cleanup'):
                    instance.cleanup()
            except Exception:
                pass
        self._scoped_instances.clear()


# Global container instance
_global_container: Optional[DIContainer] = None


def get_container() -> DIContainer:
    """Get or create global DI container instance"""
    global _global_container
    if _global_container is None:
        _global_container = DIContainer()
    return _global_container


def set_container(container: DIContainer):
    """Set global DI container instance"""
    global _global_container
    _global_container = container


def cleanup_global_container():
    """Clean up global DI container"""
    global _global_container
    if _global_container is not None:
        _global_container.cleanup()
        _global_container = None


# Decorator for dependency injection
def inject(interface: Type[T]):
    """Decorator for dependency injection"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            if interface.name.lower() not in kwargs:
                container = get_container()
                instance = container.get(interface)
                kwargs[interface.name.lower()] = instance
            return func(*args, **kwargs)
        return wrapper
    return decorator


# Context manager for container lifecycle
class DIContext:
    """Context manager for DI container lifecycle management"""
    
    def __init__(self, config_service: Optional[Any] = None):
        self.container = DIContainer(config_service)
        self.old_global = None
    
    def __enter__(self) -> DIContainer:
        self.old_global = get_container()
        set_container(self.container)
        return self.container
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.container.cleanup()
        set_container(self.old_global)


if __name__ == "__main__":
    # Demonstration of DI container usage
    logging.basicConfig(level=logging.INFO)
    
    # Example interfaces
    class IService(ABC):
        @abstractmethod
        def process(self) -> str:
            pass
    
    class IConfig(ABC):
        @abstractmethod
        def get_setting(self, key: str) -> Any:
            pass
    
    # Example implementations
    class ConfigService(IConfig):
        def __init__(self, setting_value: str = "default"):
            self.setting_value = setting_value
        
        def get_setting(self, key: str) -> Any:
            return f"{key}:{self.setting_value}"
    
    class ProcessingService(IService):
        def __init__(self, config: IConfig):
            self.config = config
        
        def process(self) -> str:
            setting = self.config.get_setting("test")
            return f"Processed with setting: {setting}"
    
    # Demonstration
    with DIContext() as container:
        # Register services
        container.register_singleton(IConfig, ConfigService)
        container.register_transient(IService, ProcessingService)
        
        # Resolve with dependency injection
        service = container.get(IService)
        result = service.process()
        
        print(f"Service result: {result}")
        
        # Show container statistics
        stats = container.get_stats()
        print(f"Container stats: {stats}")
        
        # List registered services
        print("Registered services:")
        for service_desc in container.list_all_services():
            print(f"  {service_desc}")
