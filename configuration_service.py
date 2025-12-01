"""
Configuration Service - Centralized Configuration Management with Validation and Hot-Reload

Implements comprehensive configuration management with:
- Centralized configuration with schema validation
- Environment-specific configurations (dev/staging/production)
- Hot-reload capabilities with atomic operations
- Configuration validation and error handling
- Type conversion and default value support
- File watching and change notifications
- Configuration versioning and rollback
"""

import json
import logging
import os
import threading
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import (
    Any, Dict, List, Optional, Type, TypeVar, Callable, 
    Union, Set, Pattern, TextIO, IO
)
import yaml
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileModifiedEvent
import copy

# Generic type variables
T = TypeVar('T')


class Environment(Enum):
    """Environment types"""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    TESTING = "testing"


class ConfigurationError(Exception):
    """Configuration-related errors"""
    pass


class ValidationError(Exception):
    """Configuration validation errors"""
    pass


@dataclass
class ConfigSchema:
    """Configuration schema definition with validation rules"""
    type: type = str
    required: bool = True
    default: Any = None
    min_value: Optional[Union[int, float]] = None
    max_value: Optional[Union[int, float]] = None
    allowed_values: Optional[List[Any]] = None
    pattern: Optional[str] = None
    description: Optional[str] = None
    env_var: Optional[str] = None  # Environment variable override
    
    def validate(self, value: Any, key: str) -> Any:
        """Validate configuration value against schema rules"""
        # Convert type if needed
        if not isinstance(value, self.type) and value is not None:
            try:
                if self.type == bool:
                    value = str(value).lower() in ('true', '1', 'on', 'yes')
                elif self.type in (int, float):
                    value = self.type(value)
                elif self.type == list:
                    if isinstance(value, str):
                        separator = ',' if ',' in str(value) else ';'
                        value = [item.strip() for item in str(value).split(separator)]
                else:
                    value = self.type(value)
            except (ValueError, TypeError) as e:
                raise ValidationError(f"Invalid type for {key}: expected {self.type.__name__}, got {type(value).__name__}")
        
        # Check if value is required
        if self.required and value is None:
            raise ValidationError(f"Required configuration {key} is missing")
        
        # Skip validation if value is None (and not required)
        if value is None:
            return self.default
        
        # Range validation
        if self.min_value is not None and isinstance(value, (int, float)) and value < self.min_value:
            raise ValidationError(f"{key} must be >= {self.min_value}, got {value}")
        
        if self.max_value is not None and isinstance(value, (int, float)) and value > self.max_value:
            raise ValidationError(f"{key} must be <= {self.max_value}, got {value}")
        
        # Allowed values validation
        if self.allowed_values is not None and value not in self.allowed_values:
            raise ValidationError(f"{key} must be one of {self.allowed_values}, got {value}")
        
        # Pattern validation
        if self.pattern is not None:
            import re
            if not re.match(self.pattern, str(value)):
                raise ValidationError(f"{key} does not match required pattern: {self.pattern}")
        
        return value


@dataclass
class ConfigurationChange:
    """Record of configuration changes"""
    timestamp: float
    key_path: str
    old_value: Any
    new_value: Any
    changed_by: str  # 'file', 'env_var', 'api', etc.


class IConfigurationProvider(ABC):
    """Abstract interface for configuration providers"""
    
    @abstractmethod
    def load_config(self) -> Dict[str, Any]:
        """Load configuration data"""
        pass
    
    @abstractmethod
    def save_config(self, config: Dict[str, Any]) -> bool:
        """Save configuration data"""
        pass
    
    @abstractmethod
    def watch_changes(self, callback: Callable[[str, Any, Any], None]) -> None:
        """Watch for configuration changes"""
        pass


class FileConfigurationProvider(IConfigurationProvider):
    """File-based configuration provider with hot-reload"""
    
    def __init__(self, file_path: str, format: str = 'yaml'):
        self.file_path = Path(file_path)
        self.format = format.lower()
        self.logger = logging.getLogger(__name__)
        self._observer: Optional[Observer] = None
        self._change_callbacks: List[Callable] = []
        
        if not self.file_path.exists():
            self.logger.warning(f"Configuration file not found: {file_path}")
        else:
            self.logger.info(f"Using configuration file: {file_path}")
    
    def load_config(self) -> Dict[str, Any]:
        """Load configuration from file"""
        try:
            if not self.file_path.exists():
                return {}
            
            with open(self.file_path, 'r', encoding='utf-8') as f:
                if self.format == 'yaml':
                    return yaml.safe_load(f) or {}
                elif self.format == 'json':
                    return json.load(f)
                else:
                    raise ConfigurationError(f"Unsupported format: {self.format}")
        
        except Exception as e:
            self.logger.error(f"Failed to load config from {self.file_path}: {e}")
            return {}
    
    def save_config(self, config: Dict[str, Any]) -> bool:
        """Save configuration to file (atomic write)"""
        try:
            # Ensure directory exists
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write to temporary file first
            temp_file = self.file_path.with_suffix('.tmp')
            with open(temp_file, 'w', encoding='utf-8') as f:
                if self.format == 'yaml':
                    yaml.dump(config, f, default_flow_style=False, indent=2)
                elif self.format == 'json':
                    json.dump(config, f, indent=2, sort_keys=True)
            
            # Atomic rename
            temp_file.replace(self.file_path)
            
            self.logger.info(f"Configuration saved to {self.file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save config to {self.file_path}: {e}")
            return False
    
    def watch_changes(self, callback: Callable[[str, Any, Any], None]) -> None:
        """Watch for configuration file changes"""
        self._change_callbacks.append(callback)
        
        if self._observer is not None:
            return  # Already watching
        
        class ConfigFileHandler(FileSystemEventHandler):
            def __init__(self, provider, callbacks):
                super().__init__()
                self.provider = provider
                self.callbacks = callbacks
                self.last_config = None
                
            def on_modified(self, event):
                if not isinstance(event, FileModifiedEvent):
                    return
                if event.src_path != str(self.provider.file_path):
                    return
                
                time.sleep(0.1)  # Wait for file to be fully written
                
                new_config = self.provider.load_config()
                
                if self.last_config is None:
                    self.last_config = new_config
                    return
                
                # Find changed values
                changes = self._find_changes(self.last_config, new_config)
                self.last_config = new_config
                
                # Notify callbacks
                for key_path, (old_val, new_val) in changes.items():
                    for callback in self.callbacks:
                        try:
                            callback(key_path, old_val, new_val)
                        except Exception as e:
                            self.provider.logger.error(f"Change callback error: {e}")
            
            def _find_changes(self, old: Dict, new: Dict, prefix: str = "") -> Dict[str, tuple]:
                """Find all changed configuration values"""
                changes = {}
                
                # Check for changed keys
                all_keys = set(old.keys()) | set(new.keys())
                
                for key in all_keys:
                    full_key = f"{prefix}.{key}" if prefix else key
                    
                    old_val = old.get(key)
                    new_val = new.get(key)
                    
                    if old_val != new_val:
                        changes[full_key] = (old_val, new_val)
                    elif isinstance(old_val, dict) and isinstance(new_val, dict):
                        # Recursively check nested dictionaries
                        nested_changes = self._find_changes(old_val, new_val, full_key)
                        changes.update(nested_changes)
                
                return changes
        
        # Setup file watcher
        handler = ConfigFileHandler(self, self._change_callbacks)
        self._observer = Observer()
        self._observer.schedule(handler, str(self.file_path.parent), recursive=False)
        self._observer.start()
        
        self.logger.info(f"Started watching configuration file: {self.file_path}")
    
    def stop_watching(self):
        """Stop watching for changes"""
        if self._observer:
            self._observer.stop()
            self._observer.join()
            self._observer = None
            self.logger.info("Stopped watching configuration file")


class EnvironmentConfigurationProvider(IConfigurationProvider):
    """Environment variables configuration provider"""
    
    def __init__(self, prefix: str = "APP_"):
        self.prefix = prefix.upper()
        self.logger = logging.getLogger(__name__)
    
    def load_config(self) -> Dict[str, Any]:
        """Load configuration from environment variables"""
        config = {}
        
        for key, value in os.environ.items():
            if key.startswith(self.prefix):
                # Remove prefix and convert to lowercase
                config_key = key[len(self.prefix):].lower()
                
                # Try to parse as JSON first
                try:
                    config[config_key] = json.loads(value)
                except (json.JSONDecodeError, ValueError):
                    # Fall back to string
                    config[config_key] = value
        
        self.logger.debug(f"Loaded {len(config)} environment variables with prefix {self.prefix}")
        return config
    
    def save_config(self, config: Dict[str, Any]) -> bool:
        """Environment variables are read-only"""
        self.logger.warning("Cannot save to environment variables (read-only)")
        return False
    
    def watch_changes(self, callback: Callable[[str, Any, Any], None]) -> None:
        """Environment variable changes require process restart"""
        self.logger.info("Environment variable watching not implemented (requires restart)")


class ConfigurationService:
    """
    Centralized configuration management service with validation and hot-reload.
    
    Features:
    - Multiple configuration providers (file, env vars, etc.)
    - Schema validation with type checking
    - Hot-reload with atomic file operations
    - Environment-specific configurations
    - Configuration change tracking and callbacks
    - Default value resolution and inheritance
    """
    
    def __init__(self, 
                 config_path: str = "config.yaml",
                 environment: Environment = Environment.DEVELOPMENT,
                 providers: Optional[List[IConfigurationProvider]] = None):
        
        self.logger = logging.getLogger(__name__)
        self.environment = environment
        self.config_path = config_path
        
        # Configuration data and schema
        self._config: Dict[str, Any] = {}
        self._schema: Dict[str, ConfigSchema] = {}
        self._defaults: Dict[str, Any] = {}
        
        # Providers (file, env vars, etc.)
        self._providers: List[IConfigurationProvider] = providers or [
            FileConfigurationProvider(config_path),
            EnvironmentConfigurationProvider("APP_")
        ]
        
        # Change tracking
        self._change_history: List[ConfigurationChange] = []
        self._change_callbacks: List[Callable[[ConfigurationChange], None]] = []
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Load initial configuration
        self.reload_config()
    
    def define_schema(self, key_path: str, schema: ConfigSchema) -> None:
        """Define schema for configuration key"""
        with self._lock:
            self._schema[key_path] = schema
            self.logger.debug(f"Schema defined for: {key_path}")
            
            # Validate existing value if present
            current_value = self.get(key_path, None)
            if current_value is not None:
                try:
                    validated_value = schema.validate(current_value, key_path)
                    if validated_value != current_value:
                        self.set(key_path, validated_value)
                except ValidationError as e:
                    self.logger.error(f"Schema validation failed for {key_path}: {e}")
    
    def get(self, key_path: str, default: Any = None, inherit: bool = True) -> Any:
        """Get configuration value with inheritance and validation"""
        with self._lock:
            # Navigate nested dictionary
            keys = key_path.split('.')
            value = self._config
            
            for key in keys:
                if isinstance(value, dict) and key in value:
                    value = value[key]
                else:
                    # Try inheritance if enabled
                    if inherit and hasattr(value, 'get'):
                        value = value.get(key, default)
                    else:
                        value = default
                    break
            
            # Apply schema validation if defined
            if key_path in self._schema:
                try:
                    return self._schema[key_path].validate(value, key_path)
                except ValidationError:
                    return self._schema[key_path].default
            
            return value
    
    def set(self, key_path: str, value: Any) -> None:
        """Set configuration value with validation and change tracking"""
        with self._lock:
            old_value = self.get(key_path)
            
            # Validate with schema if defined
            if key_path in self._schema:
                value = self._schema[key_path].validate(value, key_path)
            
            # Set value in nested dictionary
            keys = key_path.split('.')
            config = self._config
            
            for key in keys[:-1]:
                if key not in config:
                    config[key] = {}
                config = config[key]
            
            config[keys[-1]] = value
            
            # Record change
            change = ConfigurationChange(
                timestamp=time.time(),
                key_path=key_path,
                old_value=old_value,
                new_value=value,
                changed_by='api'
            )
            self._change_history.append(change)
            
            # Notify callbacks
            for callback in self._change_callbacks:
                try:
                    callback(change)
                except Exception as e:
                    self.logger.error(f"Change callback error: {e}")
            
            self.logger.debug(f"Configuration updated: {key_path} = {value}")
    
    def has(self, key_path: str) -> bool:
        """Check if configuration key exists"""
        return self.get(key_path) is not None
    
    def reload_config(self) -> bool:
        """Reload configuration from all providers"""
        with self._lock:
            new_config = {}
            
            # Load from all providers (later providers override earlier ones)
            for provider in self._providers:
                try:
                    provider_config = provider.load_config()
                    new_config.update(provider_config)
                except Exception as e:
                    self.logger.error(f"Failed to load from provider {type(provider).__name__}: {e}")
            
            # Apply environment-specific configuration
            env_key = f"environments.{self.environment.value}"
            env_config = new_config.get(env_key, {})
            base_config = {k: v for k, v in new_config.items() if not k.startswith('environments.')}
            
            # Merge environment config with base (env overrides)
            merged_config = {**base_config, **env_config}
            
            # Apply defaults
            merged_config = {**self._defaults, **merged_config}
            
            # Validate with schema
            validated_config = {}
            for key_path, value in merged_config.items():
                if key_path in self._schema:
                    try:
                        validated_config[key_path] = self._schema[key_path].validate(value, key_path)
                    except ValidationError as e:
                        self.logger.error(f"Validation error for {key_path}: {e}")
                        if self._schema[key_path].default is not None:
                            validated_config[key_path] = self._schema[key_path].default
                else:
                    validated_config[key_path] = value
            
            old_config = self._config
            self._config = validated_config
            
            self.logger.info(f"Configuration reloaded from environment: {self.environment.value}")
            return True
    
    def save_config(self) -> bool:
        """Save configuration to file providers"""
        success = True
        
        for provider in self._providers:
            if isinstance(provider, FileConfigurationProvider):
                success &= provider.save_config(self._config)
        
        return success
    
    def watch_changes(self, callback: Callable[[str, Any, Any], None]) -> None:
        """Watch for configuration changes"""
        # Setup watching on providers that support it
        for provider in self._providers:
            try:
                provider.watch_changes(callback)
            except AttributeError:
                # Provider doesn't support watching
                pass
    
    def add_change_callback(self, callback: Callable[[ConfigurationChange], None]) -> None:
        """Add callback for configuration changes"""
        self._change_callbacks.append(callback)
    
    def get_change_history(self, limit: int = 100) -> List[ConfigurationChange]:
        """Get recent configuration changes"""
        return self._change_history[-limit:] if limit > 0 else self._change_history
    
    def get_schema(self, key_path: str) -> Optional[ConfigSchema]:
        """Get schema for configuration key"""
        return self._schema.get(key_path)
    
    def list_all_keys(self) -> List[str]:
        """List all configuration keys (dot notation)"""
        with self._lock:
            keys = []
            
            def collect_keys(d: Dict[str, Any], prefix: str = ""):
                for key, value in d.items():
                    full_key = f"{prefix}.{key}" if prefix else key
                    
                    if isinstance(value, dict):
                        keys.append(full_key + ".*")
                        collect_keys(value, full_key)
                    else:
                        keys.append(full_key)
            
            collect_keys(self._config)
            return sorted(keys)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get configuration service statistics"""
        with self._lock:
            return {
                'environment': self.environment.value,
                'total_keys': len(self.list_all_keys()),
                'schema_definitions': len(self._schema),
                'change_history_size': len(self._change_history),
                'providers_count': len(self._providers),
                'config_file_path': self.config_path
            }
    
    def validate_all(self) -> List[str]:
        """Validate all configuration values against schema"""
        errors = []
        
        for key_path, schema in self._schema.items():
            try:
                value = self.get(key_path)
                schema.validate(value, key_path)
            except ValidationError as e:
                errors.append(f"{key_path}: {str(e)}")
        
        return errors
    
    def cleanup(self):
        """Clean up configuration service resources"""
        # Stop file watching
        for provider in self._providers:
            if hasattr(provider, 'stop_watching'):
                provider.stop_watching()
        
        self.logger.info("Configuration service cleaned up")


# Configuration schema presets for common application settings
class CommonSchemas:
    """Pre-defined configuration schemas for common application settings"""
    
    @staticmethod
    def database_config() -> Dict[str, ConfigSchema]:
        """Database configuration schema"""
        return {
            'database.host': ConfigSchema(str, required=True, description="Database hostname"),
            'database.port': ConfigSchema(int, required=False, default=5432, min_value=1, max_value=65535),
            'database.name': ConfigSchema(str, required=True, description="Database name"),
            'database.user': ConfigSchema(str, required=False, env_var="DB_USER"),
            'database.password': ConfigSchema(str, required=False, env_var="DB_PASSWORD"),
            'database.pool_size': ConfigSchema(int, required=False, default=5, min_value=1, max_value=50),
            'database.timeout': ConfigSchema(int, required=False, default=30, min_value=1, max_value=300)
        }
    
    @staticmethod
    def logging_config() -> Dict[str, ConfigSchema]:
        """Logging configuration schema"""
        return {
            'logging.level': ConfigSchema(str, required=False, default="INFO", 
                                        allowed_values=["DEBUG", "INFO", "WARNING", "ERROR"]),
            'logging.file_path': ConfigSchema(str, required=False, default="logs/app.log"),
            'logging.max_file_size': ConfigSchema(int, required=False, default=10485760),  # 10MB
            'logging.file_count': ConfigSchema(int, required=False, default=5, min_value=1, max_value=50)
        }
    
    @staticmethod
    def scraper_config() -> Dict[str, ConfigSchema]:
        """Web scraper configuration schema"""
        return {
            'scraper.max_workers': ConfigSchema(int, required=False, default=3, min_value=1, max_value="50"),
            'scraper.timeout': ConfigSchema(int, required=False, default=120, min_value=10, max_value=600),
            'scraper.chunk_size': ConfigSchema(int, required=False, default=1000, min_value=100, max_value=10000),
            'scraper.memory_threshold': ConfigSchema(float, required=False, default=1024.0, min_value=128, max_value=8192),
            'scraper.use_browser_pool': ConfigSchema(bool, required=False, default=True),
            'scraper.min_browsers': ConfigSchema(int, required=False, default=2, min_value=1, max_value=10),
            'scraper.max_browsers': ConfigSchema(int, required=False, default=10, min_value=2, max_value=50)
        }


# Global configuration instance
_global_config: Optional[ConfigurationService] = None


def get_config() -> ConfigurationService:
    """Get or create global configuration service instance"""
    global _global_config
    if _global_config is None:
        _global_config = ConfigurationService()
    return _global_config


def set_config(config: ConfigurationService):
    """Set global configuration service instance"""
    global _global_config
    _global_config = config


if __name__ == "__main__":
    # Demonstration of configuration service usage
    logging.basicConfig(level=logging.INFO)
    
    # Create sample configuration file
    sample_config = {
        'app_name': 'Email Scraper Test',
        'version': '1.0.0',
        'environments': {
            'development': {
                'log_level': 'DEBUG',
                'max_workers': 2
            },
            'production': {
                'log_level': 'INFO',
                'max_workers': 10
            }
        }
    }
    
    # Save sample config
    with open('sample_config.yaml', 'w') as f:
        yaml.dump(sample_config, f, default_flow_style=False)
    
    try:
        # Create configuration service
        config = ConfigurationService(
            config_path='sample_config.yaml',
            environment=Environment.DEVELOPMENT
        )
        
        # Define schemas
        for key_path, schema in CommonSchemas.scraper_config().items():
            config.define_schema(key_path, schema)
        
        # Set configuration
        config.set('scraper.max_workers', 5)
        config.set('scraper.timeout', 90)
        
        # Get configuration with validation
        print(f"App Name: {config.get('app_name')}")
        print(f"Log Level: {config.get('environments.development.log_level')}")
        print(f"Max Workers: {config.get('scraper.max_workers')}")
        print(f"Timeout: {config.get('scraper.timeout')}")
        
        # Show stats
        print(f"Config Stats: {config.get_stats()}")
        
        # List all keys
        print("All Configuration Keys:")
        for key in config.list_all_keys():
            print(f"  {key}")
        
        # Validate all
        errors = config.validate_all()
        if errors:
            print(f"Validation Errors: {errors}")
        else:
            print("All configurations valid!")
        
        # Cleanup
        config.cleanup()
        
    finally:
        # Clean up sample config
        if os.path.exists('sample_config.yaml'):
            os.unlink('sample_config.yaml')
