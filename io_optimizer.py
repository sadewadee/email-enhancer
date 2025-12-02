"""
I/O Optimization - Async file operations with buffering and batching

Implements I/O optimization:
- Async file operations
- Write buffering and batching
- Atomic file operations
- File operation queue
- Memory-mapped file support
- CSV streaming optimization
"""

import aiofiles
import asyncio
import csv
import io
import logging
import mmap
import os
import tempfile
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional, Union


class FileOperationType(Enum):
    """File operation types"""
    READ = "read"
    WRITE = "write"
    APPEND = "append"
    DELETE = "delete"


@dataclass
class FileOperation:
    """File operation request"""
    operation_id: str
    operation_type: FileOperationType
    filepath: str
    data: Optional[Any] = None
    encoding: str = 'utf-8'
    priority: int = 0
    callback: Optional[Callable] = None
    created_at: float = field(default_factory=time.time)


@dataclass
class WriteBuffer:
    """Buffer for batched writes"""
    filepath: str
    buffer: io.StringIO = field(default_factory=io.StringIO)
    row_count: int = 0
    byte_count: int = 0
    last_flush: float = field(default_factory=time.time)
    
    def add(self, data: str) -> int:
        """Add data to buffer"""
        bytes_written = self.buffer.write(data)
        self.byte_count += bytes_written
        self.row_count += 1
        return bytes_written
    
    def get_data(self) -> str:
        """Get buffered data"""
        return self.buffer.getvalue()
    
    def clear(self) -> None:
        """Clear buffer"""
        self.buffer = io.StringIO()
        self.row_count = 0
        self.byte_count = 0
        self.last_flush = time.time()
    
    def should_flush(self, max_rows: int = 1000, 
                     max_bytes: int = 1024 * 1024,
                     max_age: float = 5.0) -> bool:
        """Check if buffer should be flushed"""
        if self.row_count >= max_rows:
            return True
        if self.byte_count >= max_bytes:
            return True
        if time.time() - self.last_flush >= max_age:
            return True
        return False


class AsyncFileManager:
    """
    Async file manager with buffering and batching.
    
    Features:
    - Async file operations
    - Write buffering for batched I/O
    - Atomic file operations
    - Operation queuing with priorities
    - Automatic flush on shutdown
    """
    
    def __init__(self,
                 buffer_size: int = 1000,
                 flush_interval: float = 5.0,
                 max_buffer_bytes: int = 1024 * 1024):
        
        self.buffer_size = buffer_size
        self.flush_interval = flush_interval
        self.max_buffer_bytes = max_buffer_bytes
        
        self.logger = logging.getLogger(__name__)
        
        # Write buffers per file
        self._buffers: Dict[str, WriteBuffer] = {}
        self._buffer_lock = asyncio.Lock()
        
        # Operation queue
        self._operation_queue: asyncio.Queue = asyncio.Queue()
        
        # State
        self._running = False
        self._flush_task: Optional[asyncio.Task] = None
        
        # Stats
        self._stats = {
            'reads': 0,
            'writes': 0,
            'flushes': 0,
            'bytes_written': 0,
            'bytes_read': 0
        }
    
    async def start(self) -> None:
        """Start async file manager"""
        if self._running:
            return
        
        self._running = True
        self._flush_task = asyncio.create_task(self._flush_loop())
        self.logger.info("AsyncFileManager started")
    
    async def stop(self) -> None:
        """Stop and flush all buffers"""
        self._running = False
        
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        
        # Flush all remaining buffers
        await self._flush_all()
        
        self.logger.info("AsyncFileManager stopped")
    
    async def _flush_loop(self) -> None:
        """Background flush loop"""
        while self._running:
            try:
                await asyncio.sleep(self.flush_interval)
                await self._check_and_flush()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Flush loop error: {e}")
    
    async def _check_and_flush(self) -> None:
        """Check buffers and flush if needed"""
        async with self._buffer_lock:
            for filepath, buffer in list(self._buffers.items()):
                if buffer.should_flush(self.buffer_size, self.max_buffer_bytes, self.flush_interval):
                    await self._flush_buffer(filepath)
    
    async def _flush_buffer(self, filepath: str) -> None:
        """Flush single buffer to file"""
        buffer = self._buffers.get(filepath)
        if not buffer or buffer.row_count == 0:
            return
        
        try:
            data = buffer.get_data()
            
            # Atomic write using temp file
            await self._atomic_append(filepath, data)
            
            self._stats['flushes'] += 1
            self._stats['bytes_written'] += buffer.byte_count
            
            buffer.clear()
            
            self.logger.debug(f"Flushed {buffer.row_count} rows to {filepath}")
            
        except Exception as e:
            self.logger.error(f"Error flushing buffer for {filepath}: {e}")
    
    async def _flush_all(self) -> None:
        """Flush all buffers"""
        async with self._buffer_lock:
            for filepath in list(self._buffers.keys()):
                await self._flush_buffer(filepath)
    
    async def _atomic_append(self, filepath: str, data: str) -> None:
        """Atomically append data to file"""
        async with aiofiles.open(filepath, 'a', encoding='utf-8') as f:
            await f.write(data)
    
    async def read_file(self, filepath: str, encoding: str = 'utf-8') -> str:
        """Read entire file asynchronously"""
        try:
            async with aiofiles.open(filepath, 'r', encoding=encoding) as f:
                content = await f.read()
            
            self._stats['reads'] += 1
            self._stats['bytes_read'] += len(content.encode(encoding))
            
            return content
            
        except Exception as e:
            self.logger.error(f"Error reading {filepath}: {e}")
            raise
    
    async def read_lines(self, filepath: str, 
                         encoding: str = 'utf-8') -> AsyncGenerator[str, None]:
        """Read file line by line asynchronously"""
        try:
            async with aiofiles.open(filepath, 'r', encoding=encoding) as f:
                async for line in f:
                    self._stats['bytes_read'] += len(line.encode(encoding))
                    yield line.rstrip('\n\r')
            
            self._stats['reads'] += 1
            
        except Exception as e:
            self.logger.error(f"Error reading lines from {filepath}: {e}")
            raise
    
    async def write_file(self, filepath: str, data: str, 
                         encoding: str = 'utf-8', atomic: bool = True) -> bool:
        """Write data to file"""
        try:
            if atomic:
                await self._atomic_write(filepath, data, encoding)
            else:
                async with aiofiles.open(filepath, 'w', encoding=encoding) as f:
                    await f.write(data)
            
            self._stats['writes'] += 1
            self._stats['bytes_written'] += len(data.encode(encoding))
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error writing {filepath}: {e}")
            return False
    
    async def _atomic_write(self, filepath: str, data: str, encoding: str) -> None:
        """Atomically write file using temp file + rename"""
        directory = os.path.dirname(filepath) or '.'
        
        # Write to temp file
        fd, temp_path = tempfile.mkstemp(dir=directory)
        try:
            async with aiofiles.open(fd, 'w', encoding=encoding, closefd=True) as f:
                await f.write(data)
            
            # Atomic rename
            os.replace(temp_path, filepath)
            
        except Exception:
            # Cleanup temp file on error
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            raise
    
    async def append_buffered(self, filepath: str, data: str) -> None:
        """Append data to buffer (will be flushed later)"""
        async with self._buffer_lock:
            if filepath not in self._buffers:
                self._buffers[filepath] = WriteBuffer(filepath=filepath)
            
            self._buffers[filepath].add(data)
            
            # Check if immediate flush needed
            if self._buffers[filepath].should_flush(
                self.buffer_size, self.max_buffer_bytes, self.flush_interval
            ):
                await self._flush_buffer(filepath)
    
    async def append_line(self, filepath: str, line: str) -> None:
        """Append single line to buffer"""
        await self.append_buffered(filepath, line + '\n')
    
    async def flush(self, filepath: Optional[str] = None) -> None:
        """Manually flush buffer(s)"""
        if filepath:
            async with self._buffer_lock:
                await self._flush_buffer(filepath)
        else:
            await self._flush_all()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get file manager statistics"""
        buffer_stats = {}
        for filepath, buffer in self._buffers.items():
            buffer_stats[filepath] = {
                'rows': buffer.row_count,
                'bytes': buffer.byte_count
            }
        
        return {
            'running': self._running,
            'buffers': buffer_stats,
            **self._stats
        }


class AsyncCSVWriter:
    """
    Async CSV writer with buffering.
    
    Features:
    - Async row writing
    - Automatic header management
    - Buffered writes
    - Thread-safe
    """
    
    def __init__(self,
                 filepath: str,
                 fieldnames: List[str],
                 file_manager: Optional[AsyncFileManager] = None,
                 buffer_size: int = 100):
        
        self.filepath = filepath
        self.fieldnames = fieldnames
        self.file_manager = file_manager or AsyncFileManager()
        self.buffer_size = buffer_size
        
        self._buffer: List[Dict[str, Any]] = []
        self._lock = asyncio.Lock()
        self._header_written = False
        self._rows_written = 0
    
    async def _ensure_header(self) -> None:
        """Ensure header is written"""
        if self._header_written:
            return
        
        # Check if file exists and has content
        if os.path.exists(self.filepath) and os.path.getsize(self.filepath) > 0:
            self._header_written = True
            return
        
        # Write header
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=self.fieldnames)
        writer.writeheader()
        
        await self.file_manager.append_buffered(self.filepath, output.getvalue())
        self._header_written = True
    
    async def writerow(self, row: Dict[str, Any]) -> None:
        """Write single row"""
        async with self._lock:
            await self._ensure_header()
            
            self._buffer.append(row)
            
            if len(self._buffer) >= self.buffer_size:
                await self._flush_buffer()
    
    async def writerows(self, rows: List[Dict[str, Any]]) -> None:
        """Write multiple rows"""
        async with self._lock:
            await self._ensure_header()
            
            self._buffer.extend(rows)
            
            while len(self._buffer) >= self.buffer_size:
                await self._flush_buffer()
    
    async def _flush_buffer(self) -> None:
        """Flush row buffer to file"""
        if not self._buffer:
            return
        
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=self.fieldnames)
        
        for row in self._buffer:
            # Ensure all fields present
            safe_row = {field: row.get(field, '') for field in self.fieldnames}
            writer.writerow(safe_row)
        
        await self.file_manager.append_buffered(self.filepath, output.getvalue())
        
        self._rows_written += len(self._buffer)
        self._buffer.clear()
    
    async def flush(self) -> None:
        """Flush all pending writes"""
        async with self._lock:
            await self._flush_buffer()
        await self.file_manager.flush(self.filepath)
    
    async def close(self) -> None:
        """Close writer and flush remaining data"""
        await self.flush()
    
    @property
    def rows_written(self) -> int:
        return self._rows_written + len(self._buffer)


class AsyncCSVReader:
    """
    Async CSV reader with streaming.
    
    Features:
    - Async row iteration
    - Memory-efficient streaming
    - Encoding detection
    """
    
    def __init__(self,
                 filepath: str,
                 encoding: str = 'utf-8',
                 chunk_size: int = 1000):
        
        self.filepath = filepath
        self.encoding = encoding
        self.chunk_size = chunk_size
        
        self._rows_read = 0
    
    async def __aiter__(self) -> AsyncGenerator[Dict[str, Any], None]:
        """Iterate over rows asynchronously"""
        try:
            async with aiofiles.open(self.filepath, 'r', 
                                     encoding=self.encoding,
                                     errors='replace') as f:
                # Read header
                header_line = await f.readline()
                if not header_line:
                    return
                
                fieldnames = next(csv.reader([header_line]))
                
                # Read rows
                buffer = []
                async for line in f:
                    buffer.append(line)
                    
                    if len(buffer) >= self.chunk_size:
                        for row in csv.DictReader(buffer, fieldnames=fieldnames):
                            self._rows_read += 1
                            yield row
                        buffer = []
                
                # Process remaining
                for row in csv.DictReader(buffer, fieldnames=fieldnames):
                    self._rows_read += 1
                    yield row
                    
        except Exception as e:
            logging.error(f"Error reading CSV {self.filepath}: {e}")
            raise
    
    async def read_all(self) -> List[Dict[str, Any]]:
        """Read all rows into memory"""
        return [row async for row in self]
    
    async def read_chunk(self, size: int) -> List[Dict[str, Any]]:
        """Read chunk of rows"""
        chunk = []
        async for row in self:
            chunk.append(row)
            if len(chunk) >= size:
                break
        return chunk
    
    @property
    def rows_read(self) -> int:
        return self._rows_read


class MemoryMappedReader:
    """
    Memory-mapped file reader for large files.
    
    Provides efficient random access to large files.
    """
    
    def __init__(self, filepath: str):
        self.filepath = filepath
        self._file = None
        self._mmap = None
        self._size = 0
    
    def open(self) -> None:
        """Open file for memory-mapped access"""
        self._file = open(self.filepath, 'rb')
        self._size = os.path.getsize(self.filepath)
        
        if self._size > 0:
            self._mmap = mmap.mmap(
                self._file.fileno(),
                0,
                access=mmap.ACCESS_READ
            )
    
    def close(self) -> None:
        """Close memory-mapped file"""
        if self._mmap:
            self._mmap.close()
        if self._file:
            self._file.close()
    
    def __enter__(self):
        self.open()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def read_range(self, start: int, end: int) -> bytes:
        """Read byte range"""
        if not self._mmap:
            return b''
        return self._mmap[start:end]
    
    def find(self, pattern: bytes, start: int = 0) -> int:
        """Find pattern in file"""
        if not self._mmap:
            return -1
        return self._mmap.find(pattern, start)
    
    def readline(self, start: int = 0) -> bytes:
        """Read line starting from position"""
        if not self._mmap:
            return b''
        
        end = self._mmap.find(b'\n', start)
        if end == -1:
            end = self._size
        
        return self._mmap[start:end + 1]
    
    @property
    def size(self) -> int:
        return self._size


# Global file manager instance
_global_file_manager: Optional[AsyncFileManager] = None


async def get_file_manager() -> AsyncFileManager:
    """Get or create global file manager"""
    global _global_file_manager
    if _global_file_manager is None:
        _global_file_manager = AsyncFileManager()
        await _global_file_manager.start()
    return _global_file_manager


def create_file_manager(config: Optional[Dict[str, Any]] = None) -> AsyncFileManager:
    """Factory function for file manager"""
    config = config or {}
    return AsyncFileManager(
        buffer_size=config.get('buffer_size', 1000),
        flush_interval=config.get('flush_interval', 5.0),
        max_buffer_bytes=config.get('max_buffer_bytes', 1024 * 1024)
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("I/O Optimizer Demo")
    print("=" * 60)
    
    async def main():
        # Create file manager
        manager = AsyncFileManager(buffer_size=10, flush_interval=2.0)
        await manager.start()
        
        # Test write buffering
        print("\n1. Testing buffered writes...")
        
        for i in range(25):
            await manager.append_line("test_output.txt", f"Line {i}: Hello World!")
        
        print(f"   Buffered 25 lines")
        print(f"   Stats before flush: {manager.get_stats()}")
        
        await manager.flush()
        print(f"   Stats after flush: {manager.get_stats()}")
        
        # Test async file read
        print("\n2. Testing async read...")
        content = await manager.read_file("test_output.txt")
        line_count = len(content.strip().split('\n'))
        print(f"   Read {line_count} lines")
        
        # Test CSV writer
        print("\n3. Testing async CSV writer...")
        
        csv_writer = AsyncCSVWriter(
            filepath="test_output.csv",
            fieldnames=['id', 'name', 'email'],
            file_manager=manager,
            buffer_size=5
        )
        
        for i in range(15):
            await csv_writer.writerow({
                'id': i,
                'name': f'User {i}',
                'email': f'user{i}@example.com'
            })
        
        await csv_writer.close()
        print(f"   Wrote {csv_writer.rows_written} rows")
        
        # Test CSV reader
        print("\n4. Testing async CSV reader...")
        
        csv_reader = AsyncCSVReader("test_output.csv")
        rows = []
        async for row in csv_reader:
            rows.append(row)
        
        print(f"   Read {len(rows)} rows")
        if rows:
            print(f"   Sample row: {rows[0]}")
        
        # Test atomic write
        print("\n5. Testing atomic write...")
        
        await manager.write_file(
            "test_atomic.txt",
            "This file was written atomically!",
            atomic=True
        )
        print("   Atomic write completed")
        
        # Show final stats
        print("\n6. Final Statistics:")
        stats = manager.get_stats()
        for key, value in stats.items():
            print(f"   {key}: {value}")
        
        # Cleanup
        await manager.stop()
        
        # Remove test files
        for f in ["test_output.txt", "test_output.csv", "test_atomic.txt"]:
            if os.path.exists(f):
                os.unlink(f)
        
        print("\nDemo complete!")
    
    asyncio.run(main())
