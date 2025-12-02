"""
Real-time Streaming - Event-driven processing with async generators

Implements real-time streaming capabilities:
- Async event streams
- Pub/sub messaging
- Backpressure handling
- Stream transformations
- Progress streaming
- WebSocket-compatible output
"""

import asyncio
import json
import logging
import queue
import threading
import time
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import (
    Any, AsyncGenerator, AsyncIterator, Awaitable, Callable, 
    Coroutine, Dict, Generic, List, Optional, Set, TypeVar, Union
)
from functools import wraps


T = TypeVar('T')


class EventType(Enum):
    """Event types for streaming"""
    DATA = "data"
    PROGRESS = "progress"
    ERROR = "error"
    COMPLETE = "complete"
    HEARTBEAT = "heartbeat"
    METADATA = "metadata"


@dataclass
class StreamEvent(Generic[T]):
    """Event in a stream"""
    event_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    event_type: EventType = EventType.DATA
    data: Optional[T] = None
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'event_id': self.event_id,
            'type': self.event_type.value,
            'data': self.data,
            'timestamp': self.timestamp,
            'metadata': self.metadata
        }
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict(), default=str)
    
    def to_sse(self) -> str:
        """Convert to Server-Sent Events format"""
        lines = [
            f"id: {self.event_id}",
            f"event: {self.event_type.value}",
            f"data: {json.dumps(self.data, default=str)}",
            ""
        ]
        return "\n".join(lines)


@dataclass
class ProgressEvent:
    """Progress update event"""
    current: int
    total: int
    percentage: float
    message: str = ""
    items_per_second: float = 0.0
    eta_seconds: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class AsyncQueue(Generic[T]):
    """Thread-safe async queue with backpressure"""
    
    def __init__(self, maxsize: int = 1000):
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=maxsize)
        self._closed = False
    
    async def put(self, item: T, timeout: Optional[float] = None) -> bool:
        """Put item in queue with optional timeout"""
        if self._closed:
            return False
        
        try:
            if timeout:
                await asyncio.wait_for(self._queue.put(item), timeout=timeout)
            else:
                await self._queue.put(item)
            return True
        except asyncio.TimeoutError:
            return False
    
    async def get(self, timeout: Optional[float] = None) -> Optional[T]:
        """Get item from queue with optional timeout"""
        try:
            if timeout:
                return await asyncio.wait_for(self._queue.get(), timeout=timeout)
            else:
                return await self._queue.get()
        except asyncio.TimeoutError:
            return None
    
    def put_nowait(self, item: T) -> bool:
        """Put item without waiting"""
        if self._closed:
            return False
        try:
            self._queue.put_nowait(item)
            return True
        except asyncio.QueueFull:
            return False
    
    def close(self) -> None:
        """Close queue"""
        self._closed = True
    
    @property
    def size(self) -> int:
        return self._queue.qsize()
    
    @property
    def is_full(self) -> bool:
        return self._queue.full()
    
    @property
    def is_empty(self) -> bool:
        return self._queue.empty()


class EventBus:
    """Pub/sub event bus for decoupled communication"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self._async_subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self._lock = threading.RLock()
    
    def subscribe(self, event_name: str, callback: Callable) -> Callable:
        """Subscribe to event"""
        with self._lock:
            if asyncio.iscoroutinefunction(callback):
                self._async_subscribers[event_name].append(callback)
            else:
                self._subscribers[event_name].append(callback)
        return callback
    
    def unsubscribe(self, event_name: str, callback: Callable) -> bool:
        """Unsubscribe from event"""
        with self._lock:
            if callback in self._subscribers[event_name]:
                self._subscribers[event_name].remove(callback)
                return True
            if callback in self._async_subscribers[event_name]:
                self._async_subscribers[event_name].remove(callback)
                return True
        return False
    
    def publish(self, event_name: str, data: Any = None) -> int:
        """Publish event to sync subscribers"""
        with self._lock:
            callbacks = self._subscribers[event_name].copy()
        
        count = 0
        for callback in callbacks:
            try:
                callback(data)
                count += 1
            except Exception as e:
                self.logger.error(f"Subscriber error for {event_name}: {e}")
        
        return count
    
    async def publish_async(self, event_name: str, data: Any = None) -> int:
        """Publish event to all subscribers (sync + async)"""
        # Sync subscribers
        sync_count = self.publish(event_name, data)
        
        # Async subscribers
        with self._lock:
            async_callbacks = self._async_subscribers[event_name].copy()
        
        async_count = 0
        for callback in async_callbacks:
            try:
                await callback(data)
                async_count += 1
            except Exception as e:
                self.logger.error(f"Async subscriber error for {event_name}: {e}")
        
        return sync_count + async_count
    
    def on(self, event_name: str) -> Callable:
        """Decorator for subscribing to events"""
        def decorator(func: Callable) -> Callable:
            self.subscribe(event_name, func)
            return func
        return decorator
    
    def clear(self, event_name: Optional[str] = None) -> None:
        """Clear subscribers"""
        with self._lock:
            if event_name:
                self._subscribers[event_name].clear()
                self._async_subscribers[event_name].clear()
            else:
                self._subscribers.clear()
                self._async_subscribers.clear()


class Stream(Generic[T]):
    """
    Async stream with transformation support.
    
    Features:
    - Async iteration
    - Map/filter/reduce operations
    - Batching and windowing
    - Rate limiting
    - Backpressure handling
    """
    
    def __init__(self, source: AsyncGenerator[T, None]):
        self._source = source
        self._transformations: List[Callable] = []
    
    def __aiter__(self) -> AsyncIterator[T]:
        return self._iterate()
    
    async def _iterate(self) -> AsyncIterator[T]:
        """Iterate with transformations"""
        async for item in self._source:
            result = item
            for transform in self._transformations:
                if asyncio.iscoroutinefunction(transform):
                    result = await transform(result)
                else:
                    result = transform(result)
                
                if result is None:  # Filter
                    break
            
            if result is not None:
                yield result
    
    def map(self, func: Callable[[T], Any]) -> 'Stream':
        """Apply transformation to each item"""
        self._transformations.append(func)
        return self
    
    def filter(self, predicate: Callable[[T], bool]) -> 'Stream':
        """Filter items based on predicate"""
        def filter_transform(item: T) -> Optional[T]:
            return item if predicate(item) else None
        self._transformations.append(filter_transform)
        return self
    
    async def batch(self, size: int) -> AsyncGenerator[List[T], None]:
        """Batch items into groups"""
        batch = []
        async for item in self:
            batch.append(item)
            if len(batch) >= size:
                yield batch
                batch = []
        
        if batch:
            yield batch
    
    async def window(self, size: int, step: int = 1) -> AsyncGenerator[List[T], None]:
        """Sliding window over items"""
        window = deque(maxlen=size)
        count = 0
        
        async for item in self:
            window.append(item)
            count += 1
            
            if len(window) == size and count % step == 0:
                yield list(window)
    
    async def rate_limit(self, items_per_second: float) -> AsyncGenerator[T, None]:
        """Rate limit items"""
        interval = 1.0 / items_per_second
        last_time = time.time()
        
        async for item in self:
            current_time = time.time()
            elapsed = current_time - last_time
            
            if elapsed < interval:
                await asyncio.sleep(interval - elapsed)
            
            yield item
            last_time = time.time()
    
    async def collect(self) -> List[T]:
        """Collect all items into list"""
        return [item async for item in self]
    
    async def first(self, default: Optional[T] = None) -> Optional[T]:
        """Get first item"""
        async for item in self:
            return item
        return default
    
    async def count(self) -> int:
        """Count items"""
        count = 0
        async for _ in self:
            count += 1
        return count
    
    async def reduce(self, func: Callable[[T, T], T], initial: Optional[T] = None) -> Optional[T]:
        """Reduce items to single value"""
        result = initial
        async for item in self:
            if result is None:
                result = item
            else:
                result = func(result, item)
        return result


class ProgressTracker:
    """Track and stream progress updates"""
    
    def __init__(self, 
                 total: int,
                 update_interval: float = 0.5,
                 min_update_percent: float = 1.0):
        self.total = total
        self.update_interval = update_interval
        self.min_update_percent = min_update_percent
        
        self.current = 0
        self.start_time = time.time()
        self.last_update_time = 0.0
        self.last_update_percent = 0.0
        
        self._queue: AsyncQueue[ProgressEvent] = AsyncQueue(maxsize=100)
        self._callbacks: List[Callable[[ProgressEvent], None]] = []
    
    def update(self, increment: int = 1, message: str = "") -> Optional[ProgressEvent]:
        """Update progress and optionally emit event"""
        self.current += increment
        percentage = (self.current / self.total * 100) if self.total > 0 else 0
        
        current_time = time.time()
        elapsed = current_time - self.start_time
        
        # Check if we should emit update
        time_since_update = current_time - self.last_update_time
        percent_change = percentage - self.last_update_percent
        
        should_update = (
            time_since_update >= self.update_interval or
            percent_change >= self.min_update_percent or
            self.current >= self.total
        )
        
        if should_update:
            # Calculate rate and ETA
            items_per_second = self.current / elapsed if elapsed > 0 else 0
            remaining = self.total - self.current
            eta_seconds = remaining / items_per_second if items_per_second > 0 else 0
            
            event = ProgressEvent(
                current=self.current,
                total=self.total,
                percentage=percentage,
                message=message,
                items_per_second=items_per_second,
                eta_seconds=eta_seconds
            )
            
            # Update tracking
            self.last_update_time = current_time
            self.last_update_percent = percentage
            
            # Emit to queue
            self._queue.put_nowait(event)
            
            # Notify callbacks
            for callback in self._callbacks:
                try:
                    callback(event)
                except Exception:
                    pass
            
            return event
        
        return None
    
    def on_progress(self, callback: Callable[[ProgressEvent], None]) -> None:
        """Register progress callback"""
        self._callbacks.append(callback)
    
    async def stream(self) -> AsyncGenerator[ProgressEvent, None]:
        """Stream progress events"""
        while self.current < self.total:
            event = await self._queue.get(timeout=1.0)
            if event:
                yield event
        
        # Final event
        yield ProgressEvent(
            current=self.total,
            total=self.total,
            percentage=100.0,
            message="Complete"
        )
    
    def reset(self) -> None:
        """Reset progress tracker"""
        self.current = 0
        self.start_time = time.time()
        self.last_update_time = 0.0
        self.last_update_percent = 0.0


class StreamProcessor(Generic[T]):
    """
    Process items with real-time streaming output.
    
    Features:
    - Async processing with progress streaming
    - Concurrent processing with rate limiting
    - Error handling with retry
    - Result streaming
    """
    
    def __init__(self,
                 processor: Callable[[T], Awaitable[Any]],
                 concurrency: int = 5,
                 rate_limit: Optional[float] = None):
        self.processor = processor
        self.concurrency = concurrency
        self.rate_limit = rate_limit
        self.logger = logging.getLogger(__name__)
        
        self._event_bus = EventBus()
        self._results: AsyncQueue = AsyncQueue(maxsize=10000)
        self._progress: Optional[ProgressTracker] = None
    
    async def process(self, items: List[T]) -> AsyncGenerator[StreamEvent, None]:
        """Process items and stream results"""
        total = len(items)
        self._progress = ProgressTracker(total)
        
        # Emit start event
        yield StreamEvent(
            event_type=EventType.METADATA,
            data={'total': total, 'concurrency': self.concurrency}
        )
        
        # Create semaphore for concurrency control
        semaphore = asyncio.Semaphore(self.concurrency)
        
        # Rate limiter
        rate_interval = 1.0 / self.rate_limit if self.rate_limit else 0
        last_process_time = time.time()
        
        async def process_item(item: T, index: int) -> StreamEvent:
            nonlocal last_process_time
            
            async with semaphore:
                # Rate limiting
                if rate_interval > 0:
                    current_time = time.time()
                    elapsed = current_time - last_process_time
                    if elapsed < rate_interval:
                        await asyncio.sleep(rate_interval - elapsed)
                    last_process_time = time.time()
                
                try:
                    result = await self.processor(item)
                    
                    # Update progress
                    progress_event = self._progress.update(1)
                    
                    return StreamEvent(
                        event_type=EventType.DATA,
                        data={
                            'index': index,
                            'input': item,
                            'result': result,
                            'success': True
                        },
                        metadata={'progress': progress_event.to_dict() if progress_event else None}
                    )
                    
                except Exception as e:
                    self.logger.error(f"Processing error for item {index}: {e}")
                    
                    self._progress.update(1, message=f"Error: {str(e)}")
                    
                    return StreamEvent(
                        event_type=EventType.ERROR,
                        data={
                            'index': index,
                            'input': item,
                            'error': str(e),
                            'success': False
                        }
                    )
        
        # Process all items concurrently
        tasks = [
            asyncio.create_task(process_item(item, i))
            for i, item in enumerate(items)
        ]
        
        # Yield results as they complete
        for coro in asyncio.as_completed(tasks):
            event = await coro
            yield event
            
            # Yield progress updates periodically
            if self._progress and event.metadata.get('progress'):
                yield StreamEvent(
                    event_type=EventType.PROGRESS,
                    data=event.metadata['progress']
                )
        
        # Emit completion event
        yield StreamEvent(
            event_type=EventType.COMPLETE,
            data={
                'total_processed': total,
                'duration': time.time() - self._progress.start_time
            }
        )
    
    async def process_stream(self, 
                            source: AsyncGenerator[T, None]) -> AsyncGenerator[StreamEvent, None]:
        """Process items from async stream"""
        count = 0
        start_time = time.time()
        
        async for item in source:
            try:
                result = await self.processor(item)
                count += 1
                
                yield StreamEvent(
                    event_type=EventType.DATA,
                    data={
                        'index': count,
                        'input': item,
                        'result': result,
                        'success': True
                    }
                )
                
            except Exception as e:
                yield StreamEvent(
                    event_type=EventType.ERROR,
                    data={
                        'input': item,
                        'error': str(e),
                        'success': False
                    }
                )
        
        yield StreamEvent(
            event_type=EventType.COMPLETE,
            data={
                'total_processed': count,
                'duration': time.time() - start_time
            }
        )


# Utility functions

async def stream_from_list(items: List[T]) -> AsyncGenerator[T, None]:
    """Create async generator from list"""
    for item in items:
        yield item


async def stream_from_queue(queue: AsyncQueue[T]) -> AsyncGenerator[T, None]:
    """Create async generator from queue"""
    while True:
        item = await queue.get(timeout=1.0)
        if item is None:
            break
        yield item


def create_stream(source: Union[List[T], AsyncGenerator[T, None]]) -> Stream[T]:
    """Create Stream from list or async generator"""
    if isinstance(source, list):
        async def gen():
            for item in source:
                yield item
        return Stream(gen())
    return Stream(source)


# Global event bus
_global_event_bus: Optional[EventBus] = None


def get_event_bus() -> EventBus:
    """Get or create global event bus"""
    global _global_event_bus
    if _global_event_bus is None:
        _global_event_bus = EventBus()
    return _global_event_bus


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("Real-time Streaming Demo")
    print("=" * 60)
    
    async def main():
        # 1. Event Bus Demo
        print("\n1. Event Bus Demo:")
        bus = EventBus()
        
        events_received = []
        
        @bus.on("test_event")
        def handler(data):
            events_received.append(data)
            print(f"   Received: {data}")
        
        bus.publish("test_event", "Hello")
        bus.publish("test_event", "World")
        print(f"   Total received: {len(events_received)}")
        
        # 2. Stream Demo
        print("\n2. Stream Demo:")
        
        async def generate_numbers():
            for i in range(10):
                await asyncio.sleep(0.01)
                yield i
        
        stream = Stream(generate_numbers())
        
        # Transform stream
        results = await (
            stream
            .map(lambda x: x * 2)
            .filter(lambda x: x > 5)
            .collect()
        )
        print(f"   Transformed: {results}")
        
        # 3. Progress Tracker Demo
        print("\n3. Progress Tracker Demo:")
        
        tracker = ProgressTracker(total=20, update_interval=0.1)
        
        for i in range(20):
            event = tracker.update(1, f"Processing item {i+1}")
            if event:
                print(f"   Progress: {event.percentage:.0f}% "
                      f"({event.items_per_second:.1f}/s, ETA: {event.eta_seconds:.1f}s)")
            await asyncio.sleep(0.05)
        
        # 4. Stream Processor Demo
        print("\n4. Stream Processor Demo:")
        
        async def process_url(url: str) -> Dict[str, Any]:
            await asyncio.sleep(0.1)  # Simulate work
            return {"url": url, "status": "processed"}
        
        processor = StreamProcessor(
            processor=process_url,
            concurrency=3,
            rate_limit=10.0
        )
        
        urls = [f"https://example{i}.com" for i in range(5)]
        
        event_count = 0
        async for event in processor.process(urls):
            event_count += 1
            if event.event_type == EventType.DATA:
                print(f"   Processed: {event.data['input']} -> {event.data['success']}")
            elif event.event_type == EventType.COMPLETE:
                print(f"   Complete: {event.data['total_processed']} items "
                      f"in {event.data['duration']:.2f}s")
        
        # 5. SSE Output Demo
        print("\n5. Server-Sent Events Format:")
        event = StreamEvent(
            event_type=EventType.DATA,
            data={"message": "Hello, SSE!"}
        )
        print(event.to_sse())
        
        # 6. Batch Processing Demo
        print("\n6. Batch Processing Demo:")
        
        async def generate_items():
            for i in range(15):
                yield i
        
        stream = create_stream([i for i in range(15)])
        
        batch_count = 0
        async for batch in stream.batch(5):
            batch_count += 1
            print(f"   Batch {batch_count}: {batch}")
        
        print("\nDemo complete!")
    
    asyncio.run(main())
