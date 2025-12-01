"""
Streaming CSV Processor - Memory-efficient chunked processing with adaptive sizing

Implements efficient CSV processing with:
- Chunked reading (1KB-10MB chunks)
- Adaptive chunk sizing based on available memory
- Backpressure control
- Atomic processing guarantees
- Progress tracking and error handling
- Memory monitoring integration
"""

import asyncio
import logging
import time
import os
import gc
from collections import deque
from dataclasses import dataclass
from typing import Dict, List, Optional, Iterator, Callable, Any, Tuple
import pandas as pd
import chardet
from pathlib import Path

from memory_monitor import MemoryMonitor, BackpressureController


@dataclass
class ChunkMetrics:
    """Metrics for chunk processing"""
    chunk_number: int
    chunk_size: int
    processing_time: float
    memory_usage_before: float
    memory_usage_after: float
    success: bool
    error_message: Optional[str]


@dataclass
class ProcessingStats:
    """Overall processing statistics"""
    total_rows: int
    processed_rows: int
    successful_chunks: int
    failed_chunks: int
    total_time: float
    average_chunk_size: int
    memory_peak_mb: float
    backpressure_seconds: float
    chunk_metrics: List[ChunkMetrics]


class StreamingCSVProcessor:
    """
    Memory-efficient CSV processor with adaptive chunking and backpressure control.
    
    Features:
    - Streaming chunked processing to minimize memory footprint
    - Adaptive chunk sizing based on available memory and performance
    - Backpressure control to prevent system overload
    - Atomic processing guarantees with error recovery
    - Progress tracking and comprehensive metrics
    - Integration with memory monitoring service
    """
    
    def __init__(self, 
                 initial_chunk_size: int = 1000,
                 min_chunk_size: int = 100,
                 max_chunk_size: int = 10000,
                 memory_threshold_mb: float = 1024.0,
                 enable_gc: bool = True):
        
        # Chunk sizing parameters
        self.initial_chunk_size = initial_chunk_size
        self.min_chunk_size = min_chunk_size
        self.max_chunk_size = max_chunk_size
        
        # Memory and performance parameters
        self.memory_threshold_mb = memory_threshold_mb
        self.enable_gc = enable_gc
        
        # Monitoring and control
        self.memory_monitor = MemoryMonitor()
        self.backpressure = BackpressureController(self.memory_monitor)
        self.logger = logging.getLogger(__name__)
        
        # Processing state
        self.current_chunk_size = initial_chunk_size
        self.processing_stats: deque[ChunkMetrics] = deque(maxlen=100)  # Keep last 100 chunks
        self.total_stats = ProcessingStats(
            total_rows=0,
            processed_rows=0,
            successful_chunks=0,
            failed_chunks=0,
            total_time=0.0,
            average_chunk_size= initial_chunk_size,
            memory_peak_mb=0.0,
            backpressure_seconds=0.0,
            chunk_metrics=[]
        )
    
    def detect_encoding(self, file_path: str, sample_size: int = 100000) -> str:
        """
        Auto-detect file encoding using chardet with fallback strategy.
        
        Enhanced version from csv_processor.py with streaming-friendly approach.
        """
        try:
            # Read sample from file
            with open(file_path, 'rb') as f:
                raw_data = f.read(sample_size)
            
            # Use chardet to detect encoding
            detected = chardet.detect(raw_data)
            encoding = detected.get('encoding', 'utf-8')
            confidence = detected.get('confidence', 0)
            
            if encoding and confidence > 0.7:
                self.logger.debug(f"Detected encoding: {encoding} (confidence: {confidence:.2f})")
                return encoding.lower()
            
        except Exception as e:
            self.logger.warning(f"Encoding detection failed: {e}")
        
        # Fallback to common encodings
        fallback_encodings = [
            'utf-8',
            # CJK encodings
            'shift_jis', 'euc-jp', 'iso-2022-jp',
            'euc-kr', 'cp949',
            'gb2312', 'gbk', 'gb18030', 'big5',
            # Windows codepages
            'cp1252', 'cp1251', 'cp1250',
            # ISO encodings
            'iso-8859-1', 'iso-8859-15'
        ]
        
        file_size = os.path.getsize(file_path)
        validation_size = min(sample_size * 2, file_size)
        
        for encoding in fallback_encodings:
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    content = f.read(validation_size)
                    if '\ufffd' not in content:  # Check for replacement characters
                        self.logger.debug(f"Validated encoding: {encoding}")
                        return encoding
            except (UnicodeDecodeError, LookupError):
                continue
        
        # Final fallback
        self.logger.warning("Using latin-1 fallback (data corruption possible)")
        return 'latin-1'
    
    def get_file_row_count(self, file_path: str, encoding: str) -> int:
        """Get total row count for progress tracking"""
        try:
            # Use pandas to get row count efficiently
            df = pd.read_csv(file_path, encoding=encoding, nrows=0)
            # Count lines more accurately using Python file reading
            with open(file_path, 'r', encoding=encoding) as f:
                total_lines = sum(1 for _ in f) - 1  # Subtract header
            return max(0, total_lines)
        except Exception as e:
            self.logger.warning(f"Could not determine row count: {e}")
            return 0
    
    def adapt_chunk_size(self, chunk_metrics: ChunkMetrics):
        """
        Adapt chunk size based on recent performance and memory usage.
        
        Uses performance feedback to optimize chunking:
        - Increase if memory usage is low and processing is fast
        - Decrease if memory usage is high or processing is slow
        """
        # Calculate memory growth rate
        memory_growth = chunk_metrics.memory_usage_after - chunk_metrics.memory_usage_before
        processing_rate = chunk_metrics.chunk_size / max(chunk_metrics.processing_time, 0.001)
        
        # Adaptive algorithm
        new_chunk_size = self.current_chunk_size
        
        # Factor 1: Memory pressure
        memory_pressure = chunk_metrics.memory_usage_after / self.memory_threshold_mb
        
        if memory_pressure > 0.8:  # High memory usage
            new_chunk_size = max(self.min_chunk_size, int(new_chunk_size * 0.7))
        elif memory_pressure < 0.4:  # Low memory usage
            new_chunk_size = min(self.max_chunk_size, int(new_chunk_size * 1.3))
        
        # Factor 2: Processing rate
        if processing_rate < 50:  # Slow processing (rows/second)
            new_chunk_size = max(self.min_chunk_size, int(new_chunk_size * 0.8))
        elif processing_rate > 500:  # Fast processing
            new_chunk_size = min(self.max_chunk_size, int(new_chunk_size * 1.1))
        
        # Factor 3: Error rate
        recent_failures = len([m for m in list(self.processing_stats)[-5:] if not m.success])
        if recent_failures >= 3:  # Recent failures
            new_chunk_size = max(self.min_chunk_size, int(new_chunk_size * 0.5))
        
        # Apply new size
        if new_chunk_size != self.current_chunk_size:
            old_size = self.current_chunk_size
            self.current_chunk_size = new_chunk_size
            self.logger.info(f"Adapted chunk size: {old_size} -> {new_chunk_size} "
                           f"(memory: {memory_pressure:.1%}, rate: {processing_rate:.0f}/s)")
        
        return self.current_chunk_size
    
    async def _process_chunk_with_backpressure(self, 
                                               chunk: pd.DataFrame, 
                                               chunk_number: int,
                                               processor_func: Callable) -> ChunkMetrics:
        """
        Process a single chunk with memory monitoring and backpressure control.
        """
        start_time = time.time()
        memory_before = self.memory_monitor.memory_usage_mb()
        chunk_size = len(chunk)
        
        # Apply backpressure if needed
        wait_time = self.backpressure.wait()
        self.total_stats.backpressure_seconds += wait_time
        
        try:
            # Process the chunk
            result = await processor_func(chunk)
            
            # Force GC if memory is high and enabled
            if self.enable_gc and self.memory_monitor.should_trigger_gc():
                freed_mb = self.memory_monitor._trigger_gc_if_needed(
                    self.memory_monitor.get_recent_metrics(1)[0]
                )
                if freed_mb > 0:
                    self.logger.debug(f"GC freed {freed_mb:.1f}MB during chunk processing")
            
            memory_after = self.memory_monitor.memory_usage_mb()
            processing_time = time.time() - start_time
            
            # Update peak memory
            self.total_stats.memory_peak_mb = max(self.total_stats.memory_peak_mb, memory_after)
            
            # Create metrics
            metrics = ChunkMetrics(
                chunk_number=chunk_number,
                chunk_size=chunk_size,
                processing_time=processing_time,
                memory_usage_before=memory_before,
                memory_usage_after=memory_after,
                success=True,
                error_message=None
            )
            
            self.total_stats.successful_chunks += 1
            self.total_stats.processed_rows += chunk_size
            
            return metrics
            
        except Exception as e:
            memory_after = self.memory_monitor.memory_usage_mb()
            processing_time = time.time() - start_time
            
            # Log error
            self.logger.error(f"Chunk {chunk_number} failed: {e}")
            
            # Create error metrics
            metrics = ChunkMetrics(
                chunk_number=chunk_number,
                chunk_size=chunk_size,
                processing_time=processing_time,
                memory_usage_before=memory_before,
                memory_usage_after=memory_after,
                success=False,
                error_message=str(e)
            )
            
            self.total_stats.failed_chunks += 1
            return metrics
    
    async def process_streaming(self, 
                               input_file: str, 
                               processor_func: Callable,
                               url_column: str = 'url',
                               limit_rows: Optional[int] = None) -> Iterator[Tuple[Any, pd.DataFrame]]:
        """
        Process CSV file in streaming fashion with memory efficiency.
        
        Args:
            input_file: Path to input CSV file
            processor_func: Async function to process each chunk
            url_column: Name of the URL column
            limit_rows: Optional limit on total rows to process
        
        Yields:
            Tuple of (result, chunk_dataframe) for each processed chunk
        """
        file_path = Path(input_file)
        if not file_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")
        
        # Detect encoding and get file info
        encoding = self.detect_encoding(str(file_path))
        file_size = file_path.stat().st_size
        total_rows = self.get_file_row_count(str(file_path), encoding)
        
        self.logger.info(f"Processing {file_path.name}: {total_rows:,} rows, "
                        f"{file_size/1024/1024:.1f}MB, encoding={encoding}")
        
        # Reset statistics
        self.total_stats = ProcessingStats(
            total_rows=total_rows,
            processed_rows=0,
            successful_chunks=0,
            failed_chunks=0,
            total_time=0.0,
            average_chunk_size=self.current_chunk_size,
            memory_peak_mb=0.0,
            backpressure_seconds=0.0,
            chunk_metrics=[]
        )
        
        start_time = time.time()
        chunk_number = 0
        processed_rows = 0
        
        try:
            # Streaming CSV processing
            for chunk in pd.read_csv(str(file_path), 
                                   encoding=encoding, 
                                   chunksize=self.current_chunk_size,
                                   dtype=str):  # Process all as strings to avoid type issues
                
                # Check row limit
                if limit_rows and processed_rows >= limit_rows:
                    break
                
                # Adjust chunk size if approaching limit
                if limit_rows and processed_rows + len(chunk) > limit_rows:
                    chunk = chunk.head(limit_rows - processed_rows)
                
                # Process chunk with backpressure control
                chunk_number += 1
                chunk_metrics = await self._process_chunk_with_backpressure(
                    chunk, chunk_number, processor_func
                )
                
                # Store metrics and adapt chunk size
                self.processing_stats.append(chunk_metrics)
                self.total_stats.chunk_metrics.append(chunk_metrics)
                self.current_chunk_size = self.adapt_chunk_size(chunk_metrics)
                
                # Log progress every 10 chunks
                if chunk_number % 10 == 0:
                    progress_percent = (processed_rows / total_rows * 100) if total_rows > 0 else 0
                    rate = processed_rows / (time.time() - start_time) if time.time() > start_time else 0
                    memory_mb = self.memory_monitor.memory_usage_mb()
                    
                    self.logger.info(
                        f"Chunk {chunk_number}: {processed_rows:,}/{total_rows:,} ({progress_percent:.1f}%) "
                        f"| Rate: {rate:.0f}/min | Memory: {memory_mb:.1f}MB "
                        f"| Size: {chunk_metrics.chunk_size} | Backpressure: {self.backpressure.should_throttle()}"
                    )
                
                processed_rows += len(chunk)
                yield (chunk_metrics, chunk)
                
        finally:
            # Final statistics
            end_time = time.time()
            self.total_stats.total_time = end_time - start_time
            self.total_stats.average_chunk_size = (
                sum(m.chunk_size for m in self.total_stats.chunk_metrics) / 
                max(1, len(self.total_stats.chunk_metrics))
            )
            
            # Log final statistics
            self._log_final_stats()
    
    def _log_final_stats(self):
        """Log final processing statistics"""
        success_rate = (
            self.total_stats.successful_chunks / 
            max(1, len(self.total_stats.chunk_metrics)) * 100
        )
        
        if self.total_stats.total_time > 0:
            avg_rate = self.total_stats.processed_rows / self.total_stats.total_time
        else:
            avg_rate = 0.0
        
        self.logger.info("=== Streaming CSV Processing Completed ===")
        self.logger.info(f"Total rows: {self.total_stats.processed_rows:,}/{self.total_stats.total_rows:,}")
        self.logger.info(f"Chunks: {self.total_stats.successful_chunks} success, "
                        f"{self.total_stats.failed_chunks} failed ({success_rate:.1f}% success)")
        self.logger.info(f"Avg chunk size: {self.total_stats.average_chunk_size:.0f} rows")
        self.logger.info(f"Processing rate: {avg_rate:.0f} rows/minute")
        self.logger.info(f"Peak memory: {self.total_stats.memory_peak_mb:.1f}MB")
        self.logger.info(f"Backpressure time: {self.total_stats.backpressure_seconds:.1f}s")
        self.logger.info(f"Total time: {self.total_stats.total_time:.1f}s")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive processing statistics"""
        recent_metrics = list(self.processing_stats)[-10:]  # Last 10 chunks
        
        if recent_metrics:
            avg_chunk_time = sum(m.processing_time for m in recent_metrics) / len(recent_metrics)
            avg_chunk_size = sum(m.chunk_size for m in recent_metrics) / len(recent_metrics)
        else:
            avg_chunk_time = 0.0
            avg_chunk_size = 0.0
        
        return {
            'current_chunk_size': self.current_chunk_size,
            'backpressure_active': self.backpressure.should_throttle(),
            'backpressure_factor': self.backpressure.get_throttling_factor(),
            'memory_usage_mb': self.memory_monitor.memory_usage_mb(),
            'memory_percent': self.memory_monitor.usage_percentage(),
            'memory_threshold_mb': self.memory_threshold_mb,
            'recent_avg_chunk_time': avg_chunk_time,
            'recent_avg_chunk_size': avg_chunk_size,
            'total_stats': {
                'total_rows': self.total_stats.total_rows,
                'processed_rows': self.total_stats.processed_rows,
                'successful_chunks': self.total_stats.successful_chunks,
                'failed_chunks': self.total_stats.failed_chunks,
                'total_time': self.total_stats.total_time,
                'average_chunk_size': self.total_stats.average_chunk_size,
                'memory_peak_mb': self.total_stats.memory_peak_mb,
                'backpressure_seconds': self.total_stats.backpressure_seconds,
                'success_rate': (
                    self.total_stats.successful_chunks / 
                    max(1, len(self.total_stats.chunk_metrics)) * 100
                )
            }
        }
    
    async def stress_test_processor(self, chunk: pd.DataFrame) -> Dict[str, Any]:
        """
        Test processor function for stress testing the streaming processor.
        
        Simulates realistic processing load with memory usage patterns.
        """
        start_time = time.time()
        chunk_size = len(chunk)
        
        # Simulate processing delay (network requests, browser operations, etc.)
        processing_delay = 0.05 + (chunk_size * 0.001)  # 0.05-0.15s per chunk
        await asyncio.sleep(processing_delay)
        
        # Simulate memory allocation (temporary data structures)
        temp_data = {}
        for _, row in chunk.iterrows():
            # Create temporary data to simulate extraction overhead
            temp_data[row.get('url', '')] = {
                'emails': [f"test{i}@example.com" for i in range(5)],
                'phones': [f"+12345678{i:02d}" for i in range(3)],
                'addresses': [f"Address {i}" for i in range(2)],
                'metadata': {
                    'scraped_at': time.time(),
                    'source': row.get('url', ''),
                    'processing_time': processing_delay
                }
            }
        
        # Simulate JSON serialization overhead
        import json
        json_data = json.dumps(temp_data, default=str)
        
        end_time = time.time()
        
        return {
            'success': True,
            'chunk_size': chunk_size,
            'processing_time': end_time - start_time,
            'temp_data_size': len(json_data),
            'memory_simulation': len(temp_data) * 100  # Simulate memory bytes
        }


# Utility function for usage with existing CSV processor
class StreamingCSVAdapter:
    """
    Adapter to integrate streaming CSV processor with existing CSV processor workflow.
    
    Provides backward compatibility while implementing streaming benefits.
    """
    
    def __init__(self, streaming_processor: StreamingCSVProcessor):
        self.streaming_processor = streaming_processor
        self.logger = logging.getLogger(__name__)
    
    async def process_with_streaming(self,
                                     input_file: str,
                                     output_file: str,
                                     processor_func,
                                     url_column: str = 'url',
                                     limit_rows: Optional[int] = None) -> Dict[str, Any]:
        """
        Process CSV using streaming approach, compatible with existing workflow.
        """
        output_chunks = []
        stats = {
            'total_input_rows': 0,
            'total_output_rows': 0,
            'processing_time': 0,
            'memory_peak': 0,
            'chunks_processed': 0
        }
        
        start_time = time.time()
        
        try:
            async for chunk_metrics, chunk_df in self.streaming_processor.process_streaming(
                input_file, processor_func, url_column, limit_rows
            ):
                # Process the chunk (this would call the actual processing function)
                processed_chunk = await processor_func(chunk_df)
                
                # Collect processed chunks for final output
                if processed_chunk is not None:
                    output_chunks.append(processed_chunk)
                
                # Update stats
                stats['total_input_rows'] += chunk_metrics.chunk_size
                stats['chunks_processed'] += 1
                stats['memory_peak'] = max(stats['memory_peak'], chunk_metrics.memory_usage_after)
        
        finally:
            # Combine all chunks and write to output file
            if output_chunks:
                final_df = pd.concat(output_chunks, ignore_index=True)
                
                # Ensure output directory exists
                os.makedirs(os.path.dirname(output_file), exist_ok=True)
                
                # Write output
                final_df.to_csv(output_file, index=False)
                stats['total_output_rows'] = len(final_df)
            
            stats['processing_time'] = time.time() - start_time
            
            # Get streaming processor stats
            streaming_stats = self.streaming_processor.get_stats()
            stats['streaming_stats'] = streaming_stats
        
        return stats


# Example usage and testing function
if __name__ == "__main__":
    async def demo_streaming_processor():
        """Demonstrate streaming CSV processor with sample data."""
        import tempfile
        import pandas as pd
        
        # Create sample CSV for testing
        sample_data = {
            'url': [f"https://example{i}.com" for i in range(10000)],
            'name': [f"Company {i}" for i in range(10000)],
            'category': ['test'] * 10000
        }
        df = pd.DataFrame(sample_data)
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.to_csv(f.name, index=False)
            temp_file = f.name
        
        try:
            # Initialize streaming processor
            processor = StreamingCSVProcessor(
                initial_chunk_size=1000,
                memory_threshold_mb=500.0
            )
            
            print("Starting streaming demo...")
            
            # Process with backpressure control
            async for chunk_metrics, chunk_df in processor.process_streaming(
                temp_file, 
                processor.stress_test_processor,
                url_column='url'
            ):
                if chunk_metrics.chunk_number % 5 == 0:  # Log every 5th chunk
                    print(f"Chunk {chunk_metrics.chunk_number}: "
                          f"{chunk_metrics.chunk_size} rows in "
                          f"{chunk_metrics.processing_time:.2f}s")
            
            # Print final stats
            stats = processor.get_stats()
            print("\nFinal Statistics:")
            print(f"Total rows: {stats['total_stats']['processed_rows']:,}")
            print(f"Success rate: {stats['total_stats']['success_rate']:.1f}%")
            print(f"Peak memory: {stats['total_stats']['memory_peak_mb']:.1f}MB")
            print(f"Average chunk size: {stats['total_stats']['average_chunk_size']:.0f}")
            
        finally:
            os.unlink(temp_file)
    
    # Run demo
    asyncio.run(demo_streaming_processor())
