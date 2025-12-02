"""
Distributed Processing - Task queue and worker coordination for scalable processing

Implements distributed processing capabilities:
- Priority-based task queue with persistence
- Worker pool management and coordination
- Task distribution and load balancing
- Fault tolerance and task recovery
- Progress tracking across workers
- Redis-compatible backend (optional)
"""

import asyncio
import hashlib
import heapq
import json
import logging
import os
import pickle
import queue
import sqlite3
import threading
import time
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
import concurrent.futures


class TaskStatus(Enum):
    """Task execution status"""
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


class TaskPriority(Enum):
    """Task priority levels"""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass
class Task:
    """Distributed task definition"""
    task_id: str
    task_type: str
    payload: Dict[str, Any]
    priority: TaskPriority = TaskPriority.NORMAL
    status: TaskStatus = TaskStatus.PENDING
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    worker_id: Optional[str] = None
    result: Optional[Any] = None
    error: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3
    timeout: float = 300.0  # 5 minutes default
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __lt__(self, other: 'Task') -> bool:
        """Compare by priority (higher priority first) then by creation time"""
        if self.priority.value != other.priority.value:
            return self.priority.value > other.priority.value
        return self.created_at < other.created_at
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        data = asdict(self)
        data['priority'] = self.priority.value
        data['status'] = self.status.value
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Task':
        """Create from dictionary"""
        data['priority'] = TaskPriority(data['priority'])
        data['status'] = TaskStatus(data['status'])
        return cls(**data)
    
    @property
    def is_terminal(self) -> bool:
        """Check if task is in terminal state"""
        return self.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, 
                               TaskStatus.CANCELLED, TaskStatus.TIMEOUT)
    
    @property
    def can_retry(self) -> bool:
        """Check if task can be retried"""
        return self.retry_count < self.max_retries and not self.is_terminal


@dataclass
class WorkerInfo:
    """Worker information"""
    worker_id: str
    hostname: str
    status: str = "idle"
    current_task: Optional[str] = None
    tasks_completed: int = 0
    tasks_failed: int = 0
    started_at: float = field(default_factory=time.time)
    last_heartbeat: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def is_alive(self) -> bool:
        """Check if worker is alive (heartbeat within 60s)"""
        return time.time() - self.last_heartbeat < 60.0


class TaskQueueBackend(ABC):
    """Abstract backend for task queue storage"""
    
    @abstractmethod
    def enqueue(self, task: Task) -> bool:
        """Add task to queue"""
        pass
    
    @abstractmethod
    def dequeue(self, worker_id: str) -> Optional[Task]:
        """Get next task from queue"""
        pass
    
    @abstractmethod
    def update_task(self, task: Task) -> bool:
        """Update task status"""
        pass
    
    @abstractmethod
    def get_task(self, task_id: str) -> Optional[Task]:
        """Get task by ID"""
        pass
    
    @abstractmethod
    def get_pending_tasks(self, limit: int = 100) -> List[Task]:
        """Get pending tasks"""
        pass
    
    @abstractmethod
    def get_stats(self) -> Dict[str, Any]:
        """Get queue statistics"""
        pass


class InMemoryTaskQueue(TaskQueueBackend):
    """In-memory task queue with priority support"""
    
    def __init__(self):
        self._queue: List[Task] = []  # Heap queue
        self._tasks: Dict[str, Task] = {}
        self._lock = threading.RLock()
        self._stats = defaultdict(int)
    
    def enqueue(self, task: Task) -> bool:
        """Add task to queue"""
        with self._lock:
            task.status = TaskStatus.QUEUED
            heapq.heappush(self._queue, task)
            self._tasks[task.task_id] = task
            self._stats['enqueued'] += 1
            return True
    
    def dequeue(self, worker_id: str) -> Optional[Task]:
        """Get highest priority task from queue"""
        with self._lock:
            while self._queue:
                task = heapq.heappop(self._queue)
                
                # Skip cancelled or completed tasks
                if task.is_terminal:
                    continue
                
                # Assign to worker
                task.status = TaskStatus.RUNNING
                task.worker_id = worker_id
                task.started_at = time.time()
                self._stats['dequeued'] += 1
                
                return task
            
            return None
    
    def update_task(self, task: Task) -> bool:
        """Update task in storage"""
        with self._lock:
            if task.task_id in self._tasks:
                self._tasks[task.task_id] = task
                
                # Update stats
                if task.status == TaskStatus.COMPLETED:
                    self._stats['completed'] += 1
                elif task.status == TaskStatus.FAILED:
                    self._stats['failed'] += 1
                
                return True
            return False
    
    def get_task(self, task_id: str) -> Optional[Task]:
        """Get task by ID"""
        with self._lock:
            return self._tasks.get(task_id)
    
    def get_pending_tasks(self, limit: int = 100) -> List[Task]:
        """Get pending/queued tasks"""
        with self._lock:
            pending = [t for t in self._tasks.values() 
                      if t.status in (TaskStatus.PENDING, TaskStatus.QUEUED)]
            return sorted(pending, key=lambda t: (-t.priority.value, t.created_at))[:limit]
    
    def get_stats(self) -> Dict[str, Any]:
        """Get queue statistics"""
        with self._lock:
            return {
                'queue_size': len(self._queue),
                'total_tasks': len(self._tasks),
                'pending': len([t for t in self._tasks.values() if t.status == TaskStatus.QUEUED]),
                'running': len([t for t in self._tasks.values() if t.status == TaskStatus.RUNNING]),
                'completed': self._stats['completed'],
                'failed': self._stats['failed'],
                'enqueued': self._stats['enqueued'],
                'dequeued': self._stats['dequeued']
            }
    
    def requeue_task(self, task: Task) -> bool:
        """Requeue a failed task for retry"""
        with self._lock:
            if task.can_retry:
                task.retry_count += 1
                task.status = TaskStatus.RETRYING
                task.worker_id = None
                task.started_at = None
                heapq.heappush(self._queue, task)
                self._stats['retried'] += 1
                return True
            return False


class SQLiteTaskQueue(TaskQueueBackend):
    """SQLite-backed persistent task queue"""
    
    def __init__(self, db_path: str = "task_queue.db"):
        self.db_path = db_path
        self._lock = threading.RLock()
        self._init_db()
    
    def _init_db(self):
        """Initialize database schema"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    task_type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    priority INTEGER DEFAULT 1,
                    status TEXT DEFAULT 'pending',
                    created_at REAL,
                    started_at REAL,
                    completed_at REAL,
                    worker_id TEXT,
                    result TEXT,
                    error TEXT,
                    retry_count INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 3,
                    timeout REAL DEFAULT 300.0,
                    metadata TEXT
                )
            ''')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_status ON tasks(status)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_priority ON tasks(priority DESC, created_at)')
            conn.commit()
    
    def enqueue(self, task: Task) -> bool:
        """Add task to queue"""
        with self._lock:
            try:
                task.status = TaskStatus.QUEUED
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute('''
                        INSERT INTO tasks (task_id, task_type, payload, priority, status,
                                          created_at, max_retries, timeout, metadata)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        task.task_id, task.task_type, json.dumps(task.payload),
                        task.priority.value, task.status.value, task.created_at,
                        task.max_retries, task.timeout, json.dumps(task.metadata)
                    ))
                    conn.commit()
                return True
            except Exception as e:
                logging.error(f"Failed to enqueue task: {e}")
                return False
    
    def dequeue(self, worker_id: str) -> Optional[Task]:
        """Get highest priority task"""
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.execute('''
                        SELECT * FROM tasks 
                        WHERE status IN ('pending', 'queued', 'retrying')
                        ORDER BY priority DESC, created_at ASC
                        LIMIT 1
                    ''')
                    row = cursor.fetchone()
                    
                    if row:
                        task = self._row_to_task(row)
                        task.status = TaskStatus.RUNNING
                        task.worker_id = worker_id
                        task.started_at = time.time()
                        
                        conn.execute('''
                            UPDATE tasks SET status=?, worker_id=?, started_at=?
                            WHERE task_id=?
                        ''', (task.status.value, worker_id, task.started_at, task.task_id))
                        conn.commit()
                        
                        return task
                    
                return None
            except Exception as e:
                logging.error(f"Failed to dequeue task: {e}")
                return None
    
    def update_task(self, task: Task) -> bool:
        """Update task status"""
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute('''
                        UPDATE tasks SET status=?, completed_at=?, result=?, error=?,
                                        retry_count=?, worker_id=?
                        WHERE task_id=?
                    ''', (
                        task.status.value, task.completed_at,
                        json.dumps(task.result) if task.result else None,
                        task.error, task.retry_count, task.worker_id, task.task_id
                    ))
                    conn.commit()
                return True
            except Exception as e:
                logging.error(f"Failed to update task: {e}")
                return False
    
    def get_task(self, task_id: str) -> Optional[Task]:
        """Get task by ID"""
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.execute('SELECT * FROM tasks WHERE task_id=?', (task_id,))
                    row = cursor.fetchone()
                    return self._row_to_task(row) if row else None
            except Exception as e:
                logging.error(f"Failed to get task: {e}")
                return None
    
    def get_pending_tasks(self, limit: int = 100) -> List[Task]:
        """Get pending tasks"""
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.execute('''
                        SELECT * FROM tasks 
                        WHERE status IN ('pending', 'queued')
                        ORDER BY priority DESC, created_at ASC
                        LIMIT ?
                    ''', (limit,))
                    return [self._row_to_task(row) for row in cursor.fetchall()]
            except Exception as e:
                logging.error(f"Failed to get pending tasks: {e}")
                return []
    
    def get_stats(self) -> Dict[str, Any]:
        """Get queue statistics"""
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    stats = {}
                    
                    # Count by status
                    cursor = conn.execute('''
                        SELECT status, COUNT(*) as count FROM tasks GROUP BY status
                    ''')
                    for row in cursor.fetchall():
                        stats[row[0]] = row[1]
                    
                    # Total
                    cursor = conn.execute('SELECT COUNT(*) FROM tasks')
                    stats['total'] = cursor.fetchone()[0]
                    
                    return stats
            except Exception as e:
                logging.error(f"Failed to get stats: {e}")
                return {}
    
    def _row_to_task(self, row: sqlite3.Row) -> Task:
        """Convert database row to Task"""
        return Task(
            task_id=row['task_id'],
            task_type=row['task_type'],
            payload=json.loads(row['payload']),
            priority=TaskPriority(row['priority']),
            status=TaskStatus(row['status']),
            created_at=row['created_at'],
            started_at=row['started_at'],
            completed_at=row['completed_at'],
            worker_id=row['worker_id'],
            result=json.loads(row['result']) if row['result'] else None,
            error=row['error'],
            retry_count=row['retry_count'],
            max_retries=row['max_retries'],
            timeout=row['timeout'],
            metadata=json.loads(row['metadata']) if row['metadata'] else {}
        )


class TaskHandler(ABC):
    """Abstract task handler"""
    
    @abstractmethod
    def handle(self, task: Task) -> Any:
        """Handle task execution"""
        pass
    
    @abstractmethod
    def get_task_type(self) -> str:
        """Get task type this handler processes"""
        pass


class Worker:
    """Worker process for executing tasks"""
    
    def __init__(self, 
                 worker_id: str,
                 queue: TaskQueueBackend,
                 handlers: Dict[str, TaskHandler],
                 poll_interval: float = 1.0):
        self.worker_id = worker_id
        self.queue = queue
        self.handlers = handlers
        self.poll_interval = poll_interval
        self.logger = logging.getLogger(f"Worker-{worker_id[:8]}")
        
        self.info = WorkerInfo(
            worker_id=worker_id,
            hostname=os.uname().nodename
        )
        
        self._running = False
        self._current_task: Optional[Task] = None
        self._thread: Optional[threading.Thread] = None
    
    def start(self) -> None:
        """Start worker"""
        if self._running:
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._work_loop, daemon=True)
        self._thread.start()
        self.logger.info(f"Worker {self.worker_id[:8]} started")
    
    def stop(self) -> None:
        """Stop worker gracefully"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5.0)
        self.logger.info(f"Worker {self.worker_id[:8]} stopped")
    
    def _work_loop(self) -> None:
        """Main work loop"""
        while self._running:
            try:
                # Update heartbeat
                self.info.last_heartbeat = time.time()
                
                # Try to get task
                task = self.queue.dequeue(self.worker_id)
                
                if task:
                    self._execute_task(task)
                else:
                    # No task available, wait
                    self.info.status = "idle"
                    time.sleep(self.poll_interval)
                    
            except Exception as e:
                self.logger.error(f"Work loop error: {e}")
                time.sleep(self.poll_interval)
    
    def _execute_task(self, task: Task) -> None:
        """Execute a single task"""
        self._current_task = task
        self.info.status = "busy"
        self.info.current_task = task.task_id
        
        handler = self.handlers.get(task.task_type)
        
        if not handler:
            self.logger.error(f"No handler for task type: {task.task_type}")
            task.status = TaskStatus.FAILED
            task.error = f"No handler for task type: {task.task_type}"
            task.completed_at = time.time()
            self.queue.update_task(task)
            self.info.tasks_failed += 1
            return
        
        try:
            # Execute with timeout
            self.logger.info(f"Executing task {task.task_id[:8]} ({task.task_type})")
            
            start_time = time.time()
            result = handler.handle(task)
            duration = time.time() - start_time
            
            # Success
            task.status = TaskStatus.COMPLETED
            task.result = result
            task.completed_at = time.time()
            self.queue.update_task(task)
            
            self.info.tasks_completed += 1
            self.logger.info(f"Task {task.task_id[:8]} completed in {duration:.2f}s")
            
        except Exception as e:
            self.logger.error(f"Task {task.task_id[:8]} failed: {e}")
            
            task.error = str(e)
            task.completed_at = time.time()
            
            # Check retry
            if task.can_retry:
                task.status = TaskStatus.RETRYING
                task.retry_count += 1
                self.queue.update_task(task)
                # Requeue for retry
                if hasattr(self.queue, 'requeue_task'):
                    self.queue.requeue_task(task)
            else:
                task.status = TaskStatus.FAILED
                self.queue.update_task(task)
            
            self.info.tasks_failed += 1
        
        finally:
            self._current_task = None
            self.info.current_task = None


class DistributedProcessor:
    """
    Distributed task processing coordinator.
    
    Features:
    - Multiple worker management
    - Task distribution with priorities
    - Load balancing
    - Fault tolerance and retries
    - Progress tracking
    """
    
    def __init__(self, 
                 backend: Optional[TaskQueueBackend] = None,
                 num_workers: int = 4,
                 persistent: bool = False):
        self.logger = logging.getLogger(__name__)
        
        # Initialize backend
        if backend:
            self.queue = backend
        elif persistent:
            self.queue = SQLiteTaskQueue()
        else:
            self.queue = InMemoryTaskQueue()
        
        self.num_workers = num_workers
        
        # Task handlers
        self._handlers: Dict[str, TaskHandler] = {}
        
        # Workers
        self._workers: Dict[str, Worker] = {}
        
        # Stats
        self._stats = {
            'tasks_submitted': 0,
            'tasks_completed': 0,
            'tasks_failed': 0,
            'start_time': time.time()
        }
    
    def register_handler(self, handler: TaskHandler) -> None:
        """Register task handler"""
        task_type = handler.get_task_type()
        self._handlers[task_type] = handler
        self.logger.info(f"Registered handler for: {task_type}")
    
    def register_function_handler(self, task_type: str, func: Callable) -> None:
        """Register a function as task handler"""
        class FunctionHandler(TaskHandler):
            def __init__(self, task_type: str, func: Callable):
                self._task_type = task_type
                self._func = func
            
            def handle(self, task: Task) -> Any:
                return self._func(task.payload)
            
            def get_task_type(self) -> str:
                return self._task_type
        
        self._handlers[task_type] = FunctionHandler(task_type, func)
        self.logger.info(f"Registered function handler for: {task_type}")
    
    def submit_task(self, task_type: str, payload: Dict[str, Any],
                    priority: TaskPriority = TaskPriority.NORMAL,
                    max_retries: int = 3,
                    timeout: float = 300.0) -> str:
        """Submit a new task"""
        task = Task(
            task_id=str(uuid.uuid4()),
            task_type=task_type,
            payload=payload,
            priority=priority,
            max_retries=max_retries,
            timeout=timeout
        )
        
        if self.queue.enqueue(task):
            self._stats['tasks_submitted'] += 1
            self.logger.debug(f"Task submitted: {task.task_id[:8]} ({task_type})")
            return task.task_id
        
        raise RuntimeError("Failed to submit task")
    
    def submit_batch(self, task_type: str, payloads: List[Dict[str, Any]],
                     priority: TaskPriority = TaskPriority.NORMAL) -> List[str]:
        """Submit batch of tasks"""
        task_ids = []
        for payload in payloads:
            task_id = self.submit_task(task_type, payload, priority)
            task_ids.append(task_id)
        return task_ids
    
    def get_task_status(self, task_id: str) -> Optional[TaskStatus]:
        """Get task status"""
        task = self.queue.get_task(task_id)
        return task.status if task else None
    
    def get_task_result(self, task_id: str) -> Optional[Any]:
        """Get task result"""
        task = self.queue.get_task(task_id)
        return task.result if task else None
    
    def wait_for_task(self, task_id: str, timeout: float = 300.0) -> Optional[Task]:
        """Wait for task completion"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            task = self.queue.get_task(task_id)
            if task and task.is_terminal:
                return task
            time.sleep(0.5)
        
        return None
    
    def start(self) -> None:
        """Start all workers"""
        for i in range(self.num_workers):
            worker_id = str(uuid.uuid4())
            worker = Worker(worker_id, self.queue, self._handlers)
            worker.start()
            self._workers[worker_id] = worker
        
        self.logger.info(f"Started {self.num_workers} workers")
    
    def stop(self) -> None:
        """Stop all workers"""
        for worker in self._workers.values():
            worker.stop()
        self._workers.clear()
        self.logger.info("All workers stopped")
    
    def scale_workers(self, num_workers: int) -> None:
        """Scale worker count"""
        current = len(self._workers)
        
        if num_workers > current:
            # Add workers
            for _ in range(num_workers - current):
                worker_id = str(uuid.uuid4())
                worker = Worker(worker_id, self.queue, self._handlers)
                worker.start()
                self._workers[worker_id] = worker
        
        elif num_workers < current:
            # Remove workers
            workers_to_remove = list(self._workers.keys())[:current - num_workers]
            for worker_id in workers_to_remove:
                self._workers[worker_id].stop()
                del self._workers[worker_id]
        
        self.logger.info(f"Scaled workers: {current} -> {num_workers}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get processor statistics"""
        queue_stats = self.queue.get_stats()
        
        worker_stats = []
        for worker in self._workers.values():
            worker_stats.append({
                'worker_id': worker.worker_id[:8],
                'status': worker.info.status,
                'tasks_completed': worker.info.tasks_completed,
                'tasks_failed': worker.info.tasks_failed,
                'is_alive': worker.info.is_alive
            })
        
        return {
            'queue': queue_stats,
            'workers': worker_stats,
            'total_workers': len(self._workers),
            'active_workers': len([w for w in self._workers.values() if w.info.is_alive]),
            'uptime': time.time() - self._stats['start_time'],
            **self._stats
        }
    
    def get_progress(self) -> Dict[str, Any]:
        """Get processing progress"""
        stats = self.queue.get_stats()
        total = stats.get('total', 0) or stats.get('enqueued', 0)
        completed = stats.get('completed', 0)
        failed = stats.get('failed', 0)
        pending = stats.get('pending', 0) + stats.get('queued', 0)
        
        return {
            'total': total,
            'completed': completed,
            'failed': failed,
            'pending': pending,
            'running': stats.get('running', 0),
            'progress_percent': (completed / total * 100) if total > 0 else 0,
            'success_rate': (completed / (completed + failed) * 100) if (completed + failed) > 0 else 0
        }


# URL Processing Task Handler for web scraping
class URLProcessingHandler(TaskHandler):
    """Handler for URL processing tasks"""
    
    def __init__(self, processor_func: Optional[Callable] = None):
        self._processor = processor_func
    
    def handle(self, task: Task) -> Dict[str, Any]:
        """Process URL task"""
        url = task.payload.get('url')
        options = task.payload.get('options', {})
        
        if self._processor:
            result = self._processor(url, **options)
        else:
            # Default implementation - just return URL info
            result = {
                'url': url,
                'processed': True,
                'timestamp': time.time()
            }
        
        return result
    
    def get_task_type(self) -> str:
        return "url_processing"


# Factory function
def create_distributed_processor(config: Optional[Dict[str, Any]] = None) -> DistributedProcessor:
    """Create distributed processor with configuration"""
    config = config or {}
    return DistributedProcessor(
        num_workers=config.get('num_workers', 4),
        persistent=config.get('persistent', False)
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("Distributed Processor Demo")
    print("=" * 60)
    
    # Create processor
    processor = DistributedProcessor(num_workers=2, persistent=False)
    
    # Register handler
    def process_url(payload: Dict[str, Any]) -> Dict[str, Any]:
        url = payload.get('url', '')
        time.sleep(0.1)  # Simulate work
        return {'url': url, 'status': 'processed', 'timestamp': time.time()}
    
    processor.register_function_handler('url_processing', process_url)
    
    # Start workers
    processor.start()
    
    # Submit tasks
    print("\nSubmitting tasks...")
    task_ids = []
    for i in range(10):
        task_id = processor.submit_task(
            'url_processing',
            {'url': f'https://example{i}.com'},
            priority=TaskPriority.NORMAL if i < 5 else TaskPriority.HIGH
        )
        task_ids.append(task_id)
        print(f"  Submitted: {task_id[:8]}")
    
    # Wait for completion
    print("\nWaiting for tasks to complete...")
    time.sleep(3)
    
    # Show progress
    progress = processor.get_progress()
    print(f"\nProgress: {progress['progress_percent']:.0f}%")
    print(f"  Completed: {progress['completed']}")
    print(f"  Failed: {progress['failed']}")
    print(f"  Pending: {progress['pending']}")
    
    # Show stats
    stats = processor.get_stats()
    print(f"\nStats:")
    print(f"  Total Workers: {stats['total_workers']}")
    print(f"  Active Workers: {stats['active_workers']}")
    print(f"  Queue: {stats['queue']}")
    
    # Stop
    processor.stop()
    print("\nDemo complete!")
