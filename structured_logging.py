"""
Structured Logging Service - Centralized logging with correlation ID tracking

Implements comprehensive structured logging with:
- JSON-formatted log output for log aggregation systems
- Correlation ID tracking across async operations
- Context enrichment with metadata
- Log level filtering and routing
- Performance-optimized async logging
- Integration with DI container
"""

import asyncio
import contextvars
import json
import logging
import os
import sys
import threading
import time
import traceback
import uuid
from collections import deque
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Union

# Context variable for correlation ID
_correlation_id: contextvars.ContextVar[str] = contextvars.ContextVar('correlation_id', default='')
_request_context: contextvars.ContextVar[Dict[str, Any]] = contextvars.ContextVar('request_context', default={})


class LogLevel(Enum):
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50


@dataclass
class LogEntry:
    """Structured log entry"""
    timestamp: str
    level: str
    message: str
    logger_name: str
    correlation_id: str = ""
    context: Dict[str, Any] = field(default_factory=dict)
    exception: Optional[str] = None
    stack_trace: Optional[str] = None
    duration_ms: Optional[float] = None
    
    # Additional metadata
    module: str = ""
    function: str = ""
    line_number: int = 0
    thread_name: str = ""
    process_id: int = 0
    
    def to_json(self) -> str:
        """Convert log entry to JSON string"""
        data = {k: v for k, v in asdict(self).items() if v is not None and v != "" and v != {}}
        return json.dumps(data, default=str)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert log entry to dictionary"""
        return {k: v for k, v in asdict(self).items() if v is not None and v != "" and v != {}}


class StructuredFormatter(logging.Formatter):
    """Custom formatter for structured JSON logging"""
    
    def __init__(self, include_stack_trace: bool = True, include_context: bool = True):
        super().__init__()
        self.include_stack_trace = include_stack_trace
        self.include_context = include_context
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON"""
        # Get correlation ID from context
        correlation_id = _correlation_id.get('')
        request_context = _request_context.get({}) if self.include_context else {}
        
        # Build log entry
        entry = LogEntry(
            timestamp=datetime.utcnow().isoformat() + 'Z',
            level=record.levelname,
            message=record.getMessage(),
            logger_name=record.name,
            correlation_id=correlation_id,
            context={**request_context, **getattr(record, 'context', {})},
            module=record.module,
            function=record.funcName,
            line_number=record.lineno,
            thread_name=record.threadName,
            process_id=record.process,
        )
        
        # Add exception info if present
        if record.exc_info and self.include_stack_trace:
            entry.exception = str(record.exc_info[1])
            entry.stack_trace = ''.join(traceback.format_exception(*record.exc_info))
        
        # Add duration if present
        if hasattr(record, 'duration_ms'):
            entry.duration_ms = record.duration_ms
        
        return entry.to_json()


class HumanReadableFormatter(logging.Formatter):
    """Human-readable formatter for console output"""
    
    COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[32m',     # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',    # Red
        'CRITICAL': '\033[35m', # Magenta
        'RESET': '\033[0m'
    }
    
    def __init__(self, use_colors: bool = True):
        super().__init__()
        self.use_colors = use_colors and sys.stdout.isatty()
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record for human readability"""
        correlation_id = _correlation_id.get('')
        
        # Build message parts
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        level = record.levelname
        
        if self.use_colors:
            color = self.COLORS.get(level, '')
            reset = self.COLORS['RESET']
            level = f"{color}{level}{reset}"
        
        # Build base message
        parts = [f"{timestamp} {level:8s}"]
        
        if correlation_id:
            parts.append(f"[{correlation_id[:8]}]")
        
        parts.append(f"- {record.getMessage()}")
        
        # Add context if present
        context = getattr(record, 'context', {})
        if context:
            context_str = ' '.join(f"{k}={v}" for k, v in context.items())
            parts.append(f"({context_str})")
        
        message = ' '.join(parts)
        
        # Add exception if present
        if record.exc_info:
            message += '\n' + ''.join(traceback.format_exception(*record.exc_info))
        
        return message


class AsyncLogHandler(logging.Handler):
    """Async log handler with buffering for performance"""
    
    def __init__(self, buffer_size: int = 100, flush_interval: float = 1.0):
        super().__init__()
        self.buffer: deque = deque(maxlen=buffer_size)
        self.flush_interval = flush_interval
        self._lock = threading.Lock()
        self._flush_thread: Optional[threading.Thread] = None
        self._running = False
        self._handlers: List[logging.Handler] = []
    
    def add_target_handler(self, handler: logging.Handler) -> None:
        """Add target handler to flush logs to"""
        self._handlers.append(handler)
    
    def start(self) -> None:
        """Start async flushing"""
        if self._running:
            return
        self._running = True
        self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._flush_thread.start()
    
    def stop(self) -> None:
        """Stop async flushing and flush remaining"""
        self._running = False
        if self._flush_thread:
            self._flush_thread.join(timeout=5.0)
        self.flush()
    
    def emit(self, record: logging.LogRecord) -> None:
        """Buffer log record for async processing"""
        with self._lock:
            self.buffer.append(record)
    
    def flush(self) -> None:
        """Flush buffered records to target handlers"""
        with self._lock:
            records = list(self.buffer)
            self.buffer.clear()
        
        for record in records:
            for handler in self._handlers:
                try:
                    handler.emit(record)
                except Exception:
                    pass
    
    def _flush_loop(self) -> None:
        """Background flush loop"""
        while self._running:
            time.sleep(self.flush_interval)
            self.flush()


class StructuredLogger:
    """
    Structured logger with correlation ID tracking and context enrichment.
    
    Features:
    - JSON-formatted logging for log aggregation
    - Correlation ID propagation across async boundaries
    - Context enrichment with request metadata
    - Performance timing helpers
    - Multiple output handlers (console, file, async)
    """
    
    def __init__(self, 
                 name: str = "app",
                 level: str = "INFO",
                 log_dir: str = "logs",
                 json_output: bool = True,
                 console_output: bool = True,
                 async_logging: bool = False,
                 max_file_size: int = 10 * 1024 * 1024,  # 10MB
                 backup_count: int = 5):
        
        self.name = name
        self.level = getattr(logging, level.upper(), logging.INFO)
        self.log_dir = Path(log_dir)
        self.json_output = json_output
        self.console_output = console_output
        self.async_logging = async_logging
        self.max_file_size = max_file_size
        self.backup_count = backup_count
        
        # Create log directory
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize logger
        self._logger = logging.getLogger(name)
        self._logger.setLevel(self.level)
        self._logger.handlers.clear()
        
        # Async handler (optional)
        self._async_handler: Optional[AsyncLogHandler] = None
        
        # Setup handlers
        self._setup_handlers()
    
    def _setup_handlers(self) -> None:
        """Setup logging handlers"""
        # Console handler
        if self.console_output:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(HumanReadableFormatter(use_colors=True))
            console_handler.setLevel(self.level)
            
            if self.async_logging:
                if not self._async_handler:
                    self._async_handler = AsyncLogHandler()
                    self._async_handler.start()
                self._async_handler.add_target_handler(console_handler)
            else:
                self._logger.addHandler(console_handler)
        
        # File handler with JSON format
        if self.json_output:
            from logging.handlers import RotatingFileHandler
            
            log_file = self.log_dir / f"{self.name}.json.log"
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=self.max_file_size,
                backupCount=self.backup_count
            )
            file_handler.setFormatter(StructuredFormatter())
            file_handler.setLevel(self.level)
            
            if self.async_logging:
                if not self._async_handler:
                    self._async_handler = AsyncLogHandler()
                    self._async_handler.start()
                self._async_handler.add_target_handler(file_handler)
            else:
                self._logger.addHandler(file_handler)
        
        # Add async handler to logger
        if self._async_handler:
            self._logger.addHandler(self._async_handler)
    
    def _log(self, level: int, message: str, context: Optional[Dict[str, Any]] = None, 
             exc_info: bool = False, duration_ms: Optional[float] = None) -> None:
        """Internal log method"""
        extra = {}
        if context:
            extra['context'] = context
        if duration_ms is not None:
            extra['duration_ms'] = duration_ms
        
        self._logger.log(level, message, exc_info=exc_info, extra=extra)
    
    def debug(self, message: str, **context) -> None:
        """Log debug message"""
        self._log(logging.DEBUG, message, context if context else None)
    
    def info(self, message: str, **context) -> None:
        """Log info message"""
        self._log(logging.INFO, message, context if context else None)
    
    def warning(self, message: str, **context) -> None:
        """Log warning message"""
        self._log(logging.WARNING, message, context if context else None)
    
    def error(self, message: str, exc_info: bool = False, **context) -> None:
        """Log error message"""
        self._log(logging.ERROR, message, context if context else None, exc_info=exc_info)
    
    def critical(self, message: str, exc_info: bool = False, **context) -> None:
        """Log critical message"""
        self._log(logging.CRITICAL, message, context if context else None, exc_info=exc_info)
    
    def exception(self, message: str, **context) -> None:
        """Log exception with stack trace"""
        self._log(logging.ERROR, message, context if context else None, exc_info=True)
    
    def log_with_timing(self, message: str, duration_ms: float, **context) -> None:
        """Log message with timing information"""
        self._log(logging.INFO, message, context if context else None, duration_ms=duration_ms)
    
    def set_level(self, level: str) -> None:
        """Set logging level"""
        self.level = getattr(logging, level.upper(), logging.INFO)
        self._logger.setLevel(self.level)
        for handler in self._logger.handlers:
            handler.setLevel(self.level)
    
    def get_child(self, suffix: str) -> 'StructuredLogger':
        """Get child logger with suffix"""
        child = StructuredLogger.__new__(StructuredLogger)
        child.name = f"{self.name}.{suffix}"
        child._logger = self._logger.getChild(suffix)
        child.level = self.level
        return child
    
    def close(self) -> None:
        """Close logger and flush buffers"""
        if self._async_handler:
            self._async_handler.stop()
        
        for handler in self._logger.handlers:
            handler.close()


# Correlation ID management

def set_correlation_id(correlation_id: Optional[str] = None) -> str:
    """Set correlation ID for current context"""
    cid = correlation_id or str(uuid.uuid4())
    _correlation_id.set(cid)
    return cid


def get_correlation_id() -> str:
    """Get correlation ID from current context"""
    return _correlation_id.get('')


def clear_correlation_id() -> None:
    """Clear correlation ID from current context"""
    _correlation_id.set('')


# Context management

def set_request_context(context: Dict[str, Any]) -> None:
    """Set request context for current execution"""
    _request_context.set(context)


def get_request_context() -> Dict[str, Any]:
    """Get request context from current execution"""
    return _request_context.get({})


def add_context(**kwargs) -> None:
    """Add key-value pairs to current context"""
    current = _request_context.get({})
    _request_context.set({**current, **kwargs})


def clear_request_context() -> None:
    """Clear request context"""
    _request_context.set({})


# Decorators for automatic logging

def log_function_call(logger: Optional[StructuredLogger] = None, 
                      level: str = "DEBUG",
                      include_args: bool = False,
                      include_result: bool = False):
    """Decorator to log function calls with timing"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal logger
            if logger is None:
                logger = get_logger()
            
            log_level = getattr(logging, level.upper(), logging.DEBUG)
            func_name = func.__name__
            
            # Build context
            context = {'function': func_name}
            if include_args:
                context['args'] = str(args)[:200]
                context['kwargs'] = str(kwargs)[:200]
            
            # Log entry
            logger._log(log_level, f"Entering {func_name}", context)
            
            # Execute function with timing
            start_time = time.perf_counter()
            try:
                result = func(*args, **kwargs)
                duration_ms = (time.perf_counter() - start_time) * 1000
                
                # Log exit
                exit_context = {**context, 'status': 'success'}
                if include_result:
                    exit_context['result'] = str(result)[:200]
                logger._log(log_level, f"Exiting {func_name}", exit_context, duration_ms=duration_ms)
                
                return result
                
            except Exception as e:
                duration_ms = (time.perf_counter() - start_time) * 1000
                error_context = {**context, 'status': 'error', 'error': str(e)}
                logger._log(logging.ERROR, f"Error in {func_name}", error_context, 
                           exc_info=True, duration_ms=duration_ms)
                raise
        
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            nonlocal logger
            if logger is None:
                logger = get_logger()
            
            log_level = getattr(logging, level.upper(), logging.DEBUG)
            func_name = func.__name__
            
            # Build context
            context = {'function': func_name}
            if include_args:
                context['args'] = str(args)[:200]
                context['kwargs'] = str(kwargs)[:200]
            
            # Log entry
            logger._log(log_level, f"Entering {func_name}", context)
            
            # Execute function with timing
            start_time = time.perf_counter()
            try:
                result = await func(*args, **kwargs)
                duration_ms = (time.perf_counter() - start_time) * 1000
                
                # Log exit
                exit_context = {**context, 'status': 'success'}
                if include_result:
                    exit_context['result'] = str(result)[:200]
                logger._log(log_level, f"Exiting {func_name}", exit_context, duration_ms=duration_ms)
                
                return result
                
            except Exception as e:
                duration_ms = (time.perf_counter() - start_time) * 1000
                error_context = {**context, 'status': 'error', 'error': str(e)}
                logger._log(logging.ERROR, f"Error in {func_name}", error_context, 
                           exc_info=True, duration_ms=duration_ms)
                raise
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return wrapper
    
    return decorator


def with_correlation_id(func: Callable) -> Callable:
    """Decorator to ensure correlation ID exists for function execution"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        if not get_correlation_id():
            set_correlation_id()
        return func(*args, **kwargs)
    
    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        if not get_correlation_id():
            set_correlation_id()
        return await func(*args, **kwargs)
    
    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    return wrapper


# Context manager for timed operations

class TimedOperation:
    """Context manager for timing operations"""
    
    def __init__(self, operation_name: str, logger: Optional[StructuredLogger] = None, 
                 level: str = "INFO", **context):
        self.operation_name = operation_name
        self.logger = logger or get_logger()
        self.level = getattr(logging, level.upper(), logging.INFO)
        self.context = context
        self.start_time: float = 0
        self.duration_ms: float = 0
    
    def __enter__(self) -> 'TimedOperation':
        self.start_time = time.perf_counter()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.duration_ms = (time.perf_counter() - self.start_time) * 1000
        
        if exc_type is None:
            self.logger._log(self.level, f"{self.operation_name} completed", 
                            {**self.context, 'status': 'success'}, 
                            duration_ms=self.duration_ms)
        else:
            self.logger._log(logging.ERROR, f"{self.operation_name} failed", 
                            {**self.context, 'status': 'error', 'error': str(exc_val)}, 
                            exc_info=True, duration_ms=self.duration_ms)
    
    async def __aenter__(self) -> 'TimedOperation':
        self.start_time = time.perf_counter()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.duration_ms = (time.perf_counter() - self.start_time) * 1000
        
        if exc_type is None:
            self.logger._log(self.level, f"{self.operation_name} completed", 
                            {**self.context, 'status': 'success'}, 
                            duration_ms=self.duration_ms)
        else:
            self.logger._log(logging.ERROR, f"{self.operation_name} failed", 
                            {**self.context, 'status': 'error', 'error': str(exc_val)}, 
                            exc_info=True, duration_ms=self.duration_ms)


# Global logger instance
_global_logger: Optional[StructuredLogger] = None


def get_logger(name: Optional[str] = None) -> StructuredLogger:
    """Get or create global structured logger instance"""
    global _global_logger
    if _global_logger is None:
        _global_logger = StructuredLogger(name=name or "app")
    return _global_logger


def set_logger(logger: StructuredLogger) -> None:
    """Set global structured logger instance"""
    global _global_logger
    _global_logger = logger


def configure_logging(name: str = "app",
                      level: str = "INFO",
                      log_dir: str = "logs",
                      json_output: bool = True,
                      console_output: bool = True,
                      async_logging: bool = False) -> StructuredLogger:
    """Configure and return global structured logger"""
    logger = StructuredLogger(
        name=name,
        level=level,
        log_dir=log_dir,
        json_output=json_output,
        console_output=console_output,
        async_logging=async_logging
    )
    set_logger(logger)
    return logger


# Factory function for DI integration
def create_structured_logger(config: Optional[Dict[str, Any]] = None) -> StructuredLogger:
    """Create structured logger with optional configuration"""
    config = config or {}
    return StructuredLogger(
        name=config.get('name', 'app'),
        level=config.get('level', 'INFO'),
        log_dir=config.get('log_dir', 'logs'),
        json_output=config.get('json_output', True),
        console_output=config.get('console_output', True),
        async_logging=config.get('async_logging', False)
    )


if __name__ == "__main__":
    # Demonstration
    print("Structured Logging Demo")
    print("=" * 50)
    
    # Configure logging
    logger = configure_logging(
        name="demo",
        level="DEBUG",
        json_output=True,
        console_output=True
    )
    
    # Set correlation ID
    cid = set_correlation_id()
    print(f"\nCorrelation ID: {cid}")
    
    # Add context
    add_context(user_id="user123", request_id="req456")
    
    # Basic logging
    logger.debug("Debug message", component="test")
    logger.info("Info message", action="demo")
    logger.warning("Warning message", severity="low")
    
    # Timed operation
    with TimedOperation("database_query", logger, url_count=100):
        time.sleep(0.1)  # Simulate work
    
    # Decorated function
    @log_function_call(logger, level="INFO", include_args=True)
    def process_data(items: List[int]) -> int:
        return sum(items)
    
    result = process_data([1, 2, 3, 4, 5])
    print(f"\nFunction result: {result}")
    
    # Error logging
    try:
        raise ValueError("Test error")
    except Exception:
        logger.exception("An error occurred", operation="test")
    
    # Close logger
    logger.close()
    
    print("\nDemo complete. Check logs/ directory for JSON logs.")
