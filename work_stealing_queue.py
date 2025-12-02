"""
Work-Stealing Queue - High-performance parallel processing with load balancing

Implements work-stealing queue pattern:
- Priority-based task scheduling
- Work stealing for load balancing
- Adaptive worker management
- Fairness guarantees
- Starvation prevention
"""

import asyncio
import heapq
import logging
import random
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Coroutine, Dict, Generic, List, Optional, Set, TypeVar


T = TypeVar('T')


class TaskPriority(Enum):
    """Task priority levels"""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass(order=True)
class PriorityTask:
    """Task with priority for scheduling"""
    priority: int
    timestamp: float = field(compare=False)
    task_id: str = field(compare=False)
    payload: Any = field(compare=False)
    created_at: float = field(default_factory=time.time, compare=False)
    
    def __post_init__(self):
        # Negate priority for max-heap behavior with heapq (min-heap)
        self.priority = -self.priority


class WorkerDeque(Generic[T]):
    """
    Double-ended queue for work-stealing.
    
    Owner pushes/pops from bottom (LIFO for locality).
    Thieves steal from top (FIFO for fairness).
    """
    
    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self._deque: deque = deque()
        self._lock = threading.Lock()
        self._stats = {
            'pushed': 0,
            'popped': 0,
            'stolen': 0
        }
    
    def push(self, item: T) -> None:
        """Push item to bottom (owner operation)"""
        with self._lock:
            self._deque.append(item)
            self._stats['pushed'] += 1
    
    def pop(self) -> Optional[T]:
        """Pop item from bottom (owner operation)"""
        with self._lock:
            if self._deque:
                self._stats['popped'] += 1
                return self._deque.pop()
            return None
    
    def steal(self) -> Optional[T]:
        """Steal item from top (thief operation)"""
        with self._lock:
            if len(self._deque) > 1:  # Leave at least one for owner
                self._stats['stolen'] += 1
                return self._deque.popleft()
            return None
    
    def size(self) -> int:
        """Get current size"""
        with self._lock:
            return len(self._deque)
    
    def is_empty(self) -> bool:
        """Check if empty"""
        with self._lock:
            return len(self._deque) == 0
    
    def get_stats(self) -> Dict[str, int]:
        """Get deque statistics"""
        with self._lock:
            return {
                'size': len(self._deque),
                **self._stats
            }


class WorkStealingScheduler:
    """
    Work-stealing scheduler for parallel task execution.
    
    Features:
    - Per-worker task queues
    - Work stealing for load balancing
    - Priority scheduling
    - Fairness with anti-starvation
    """
    
    def __init__(self, num_workers: int = 4):
        self.num_workers = num_workers
        self.logger = logging.getLogger(__name__)
        
        # Worker queues
        self._worker_queues: Dict[str, WorkerDeque] = {}
        self._workers: List[str] = []
        
        # Global priority queue for initial distribution
        self._global_queue: List[PriorityTask] = []
        self._global_lock = threading.Lock()
        
        # Round-robin index for fair distribution
        self._next_worker = 0
        
        # Stats
        self._stats = {
            'tasks_submitted': 0,
            'tasks_completed': 0,
            'steal_attempts': 0,
            'successful_steals': 0
        }
        
        # Initialize workers
        for i in range(num_workers):
            worker_id = f"worker_{i}"
            self._workers.append(worker_id)
            self._worker_queues[worker_id] = WorkerDeque(worker_id)
    
    def submit(self, payload: Any, priority: TaskPriority = TaskPriority.NORMAL) -> str:
        """Submit task to scheduler"""
        task = PriorityTask(
            priority=priority.value,
            timestamp=time.time(),
            task_id=str(uuid.uuid4())[:8],
            payload=payload
        )
        
        # Add to global queue first
        with self._global_lock:
            heapq.heappush(self._global_queue, task)
            self._stats['tasks_submitted'] += 1
        
        return task.task_id
    
    def get_task(self, worker_id: str) -> Optional[PriorityTask]:
        """Get task for worker (with work stealing)"""
        # First try own queue
        own_queue = self._worker_queues.get(worker_id)
        if own_queue:
            task = own_queue.pop()
            if task:
                return task
        
        # Try global queue
        with self._global_lock:
            if self._global_queue:
                return heapq.heappop(self._global_queue)
        
        # Try stealing from other workers
        return self._steal_task(worker_id)
    
    def _steal_task(self, thief_id: str) -> Optional[PriorityTask]:
        """Attempt to steal task from another worker"""
        self._stats['steal_attempts'] += 1
        
        # Randomly shuffle workers to avoid contention
        other_workers = [w for w in self._workers if w != thief_id]
        random.shuffle(other_workers)
        
        for victim_id in other_workers:
            victim_queue = self._worker_queues.get(victim_id)
            if victim_queue and victim_queue.size() > 1:
                task = victim_queue.steal()
                if task:
                    self._stats['successful_steals'] += 1
                    self.logger.debug(f"Worker {thief_id} stole from {victim_id}")
                    return task
        
        return None
    
    def distribute_to_worker(self, worker_id: str, task: PriorityTask) -> None:
        """Distribute task directly to worker queue"""
        queue = self._worker_queues.get(worker_id)
        if queue:
            queue.push(task)
    
    def mark_completed(self) -> None:
        """Mark a task as completed"""
        self._stats['tasks_completed'] += 1
    
    def get_least_loaded_worker(self) -> str:
        """Get worker with smallest queue"""
        min_size = float('inf')
        min_worker = self._workers[0]
        
        for worker_id in self._workers:
            size = self._worker_queues[worker_id].size()
            if size < min_size:
                min_size = size
                min_worker = worker_id
        
        return min_worker
    
    def get_stats(self) -> Dict[str, Any]:
        """Get scheduler statistics"""
        worker_stats = {
            wid: q.get_stats() 
            for wid, q in self._worker_queues.items()
        }
        
        with self._global_lock:
            global_size = len(self._global_queue)
        
        steal_rate = 0.0
        if self._stats['steal_attempts'] > 0:
            steal_rate = self._stats['successful_steals'] / self._stats['steal_attempts'] * 100
        
        return {
            'num_workers': self.num_workers,
            'global_queue_size': global_size,
            'steal_success_rate': f"{steal_rate:.1f}%",
            'worker_stats': worker_stats,
            **self._stats
        }


class WorkStealingExecutor:
    """
    Executor using work-stealing for parallel execution.
    
    Features:
    - Async task execution
    - Work-stealing load balancing
    - Graceful shutdown
    - Progress tracking
    """
    
    def __init__(self,
                 num_workers: int = 4,
                 task_handler: Optional[Callable[[Any], Coroutine]] = None):
        self.num_workers = num_workers
        self.task_handler = task_handler or self._default_handler
        self.logger = logging.getLogger(__name__)
        
        self._scheduler = WorkStealingScheduler(num_workers)
        self._running = False
        self._workers: List[asyncio.Task] = []
        self._results: Dict[str, Any] = {}
        self._lock = asyncio.Lock()
    
    async def _default_handler(self, payload: Any) -> Any:
        """Default task handler"""
        if callable(payload):
            if asyncio.iscoroutinefunction(payload):
                return await payload()
            return payload()
        return payload
    
    async def start(self) -> None:
        """Start executor workers"""
        if self._running:
            return
        
        self._running = True
        
        for worker_id in self._scheduler._workers:
            task = asyncio.create_task(self._worker_loop(worker_id))
            self._workers.append(task)
        
        self.logger.info(f"Started {self.num_workers} workers")
    
    async def stop(self) -> None:
        """Stop executor workers"""
        self._running = False
        
        # Wait for workers to finish
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
            self._workers.clear()
        
        self.logger.info("Executor stopped")
    
    async def _worker_loop(self, worker_id: str) -> None:
        """Worker execution loop"""
        while self._running:
            try:
                # Get task
                task = self._scheduler.get_task(worker_id)
                
                if task:
                    try:
                        # Execute task
                        result = await self.task_handler(task.payload)
                        
                        # Store result
                        async with self._lock:
                            self._results[task.task_id] = {
                                'status': 'completed',
                                'result': result,
                                'worker': worker_id
                            }
                        
                        self._scheduler.mark_completed()
                        
                    except Exception as e:
                        async with self._lock:
                            self._results[task.task_id] = {
                                'status': 'failed',
                                'error': str(e),
                                'worker': worker_id
                            }
                        self.logger.error(f"Task {task.task_id} failed: {e}")
                else:
                    # No task available, wait a bit
                    await asyncio.sleep(0.01)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Worker {worker_id} error: {e}")
                await asyncio.sleep(0.1)
    
    def submit(self, payload: Any, priority: TaskPriority = TaskPriority.NORMAL) -> str:
        """Submit task for execution"""
        return self._scheduler.submit(payload, priority)
    
    async def submit_and_wait(self, payload: Any, 
                              priority: TaskPriority = TaskPriority.NORMAL,
                              timeout: float = 60.0) -> Any:
        """Submit task and wait for result"""
        task_id = self.submit(payload, priority)
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            async with self._lock:
                if task_id in self._results:
                    result = self._results.pop(task_id)
                    if result['status'] == 'completed':
                        return result['result']
                    else:
                        raise RuntimeError(result.get('error', 'Task failed'))
            
            await asyncio.sleep(0.01)
        
        raise TimeoutError(f"Task {task_id} timed out")
    
    async def map(self, func: Callable, items: List[Any],
                  priority: TaskPriority = TaskPriority.NORMAL) -> List[Any]:
        """Map function over items in parallel"""
        task_ids = []
        
        for item in items:
            async def wrapper(item=item):
                return func(item)
            task_id = self.submit(wrapper, priority)
            task_ids.append(task_id)
        
        # Wait for all results
        results = []
        for task_id in task_ids:
            while True:
                async with self._lock:
                    if task_id in self._results:
                        result = self._results.pop(task_id)
                        if result['status'] == 'completed':
                            results.append(result['result'])
                        else:
                            results.append(None)
                        break
                await asyncio.sleep(0.01)
        
        return results
    
    def get_result(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get task result (non-blocking)"""
        return self._results.get(task_id)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get executor statistics"""
        return {
            'running': self._running,
            'pending_results': len(self._results),
            'scheduler': self._scheduler.get_stats()
        }


class FairnessController:
    """
    Ensures fairness in task processing.
    
    Prevents starvation of low-priority tasks.
    """
    
    def __init__(self,
                 aging_interval: float = 60.0,
                 max_age_boost: int = 2):
        self.aging_interval = aging_interval
        self.max_age_boost = max_age_boost
        
        self._task_ages: Dict[str, float] = {}
        self._lock = threading.Lock()
    
    def record_task(self, task_id: str) -> None:
        """Record task submission time"""
        with self._lock:
            self._task_ages[task_id] = time.time()
    
    def get_priority_boost(self, task_id: str, base_priority: int) -> int:
        """Get priority boost based on age"""
        with self._lock:
            if task_id not in self._task_ages:
                return base_priority
            
            age = time.time() - self._task_ages[task_id]
            boost = min(int(age / self.aging_interval), self.max_age_boost)
            
            return base_priority + boost
    
    def task_completed(self, task_id: str) -> None:
        """Remove task from tracking"""
        with self._lock:
            self._task_ages.pop(task_id, None)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get fairness statistics"""
        with self._lock:
            if not self._task_ages:
                return {'tracked_tasks': 0, 'avg_age': 0}
            
            ages = [time.time() - t for t in self._task_ages.values()]
            return {
                'tracked_tasks': len(ages),
                'avg_age': sum(ages) / len(ages),
                'max_age': max(ages),
                'min_age': min(ages)
            }


# Factory function
def create_work_stealing_executor(config: Optional[Dict[str, Any]] = None) -> WorkStealingExecutor:
    """Factory function for work-stealing executor"""
    config = config or {}
    return WorkStealingExecutor(
        num_workers=config.get('num_workers', 4),
        task_handler=config.get('task_handler')
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("Work-Stealing Queue Demo")
    print("=" * 60)
    
    async def main():
        # Create executor
        executor = WorkStealingExecutor(num_workers=4)
        
        # Define task handler
        async def process_item(item: Dict[str, Any]) -> Dict[str, Any]:
            await asyncio.sleep(random.uniform(0.05, 0.15))
            return {'input': item, 'processed': True}
        
        executor.task_handler = process_item
        
        # Start executor
        print("\n1. Starting executor...")
        await executor.start()
        
        # Submit tasks with different priorities
        print("\n2. Submitting tasks...")
        
        task_ids = []
        for i in range(20):
            priority = random.choice([
                TaskPriority.LOW,
                TaskPriority.NORMAL,
                TaskPriority.HIGH
            ])
            task_id = executor.submit(
                {'item_id': i, 'data': f'data_{i}'},
                priority=priority
            )
            task_ids.append(task_id)
            print(f"   Submitted task {task_id} with priority {priority.name}")
        
        # Wait for completion
        print("\n3. Waiting for completion...")
        await asyncio.sleep(2)
        
        # Check results
        print("\n4. Results:")
        completed = 0
        for task_id in task_ids:
            result = executor.get_result(task_id)
            if result and result['status'] == 'completed':
                completed += 1
        
        print(f"   Completed: {completed}/{len(task_ids)}")
        
        # Show statistics
        print("\n5. Statistics:")
        stats = executor.get_stats()
        print(f"   Running: {stats['running']}")
        print(f"   Tasks submitted: {stats['scheduler']['tasks_submitted']}")
        print(f"   Tasks completed: {stats['scheduler']['tasks_completed']}")
        print(f"   Steal success rate: {stats['scheduler']['steal_success_rate']}")
        
        print("\n   Worker queues:")
        for worker_id, worker_stats in stats['scheduler']['worker_stats'].items():
            print(f"     {worker_id}: pushed={worker_stats['pushed']}, "
                  f"popped={worker_stats['popped']}, stolen={worker_stats['stolen']}")
        
        # Test map function
        print("\n6. Testing map function...")
        
        def square(x):
            return x * x
        
        items = list(range(10))
        results = await executor.map(square, items)
        print(f"   Input: {items}")
        print(f"   Output: {results}")
        
        # Shutdown
        print("\n7. Shutting down...")
        await executor.stop()
        print("   Executor stopped")
    
    asyncio.run(main())
    print("\nDemo complete!")
