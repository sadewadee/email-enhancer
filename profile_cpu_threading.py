#!/usr/bin/env python3
"""
CPU & Threading Profiler
Analyzes CPU usage patterns and threading behavior of the scraper.

Usage:
    python profile_cpu_threading.py
"""

import psutil
import threading
import time
import sys
from collections import deque
import statistics

class CPUThreadingProfiler:
    """Profile CPU usage and threading patterns."""
    
    def __init__(self, sample_interval=0.5):
        """
        Initialize profiler.
        
        Args:
            sample_interval: Seconds between samples
        """
        self.sample_interval = sample_interval
        self.cpu_samples = deque(maxlen=120)  # Last 60 seconds
        self.thread_samples = deque(maxlen=120)
        self.process_samples = deque(maxlen=120)
        self.monitoring = False
        self.monitor_thread = None
        self.start_time = None
        
    def _monitor_loop(self):
        """Background monitoring loop."""
        process = psutil.Process()
        
        while self.monitoring:
            try:
                # Get CPU usage
                cpu_percent = psutil.cpu_percent(interval=self.sample_interval)
                process_cpu = process.cpu_percent(interval=self.sample_interval)
                
                # Get thread count
                thread_count = threading.active_count()
                
                # Get child process count
                try:
                    children = process.children(recursive=True)
                    child_count = len(children)
                    child_cpu = sum(p.cpu_percent(interval=0) for p in children if p.is_running())
                except:
                    child_count = 0
                    child_cpu = 0
                
                # Store samples
                timestamp = time.time() - self.start_time
                self.cpu_samples.append({
                    'time': timestamp,
                    'system_cpu': cpu_percent,
                    'process_cpu': process_cpu,
                    'child_cpu': child_cpu
                })
                
                self.thread_samples.append({
                    'time': timestamp,
                    'count': thread_count
                })
                
                self.process_samples.append({
                    'time': timestamp,
                    'count': child_count
                })
                
            except Exception as e:
                print(f"Monitor error: {e}")
                
    def start(self):
        """Start monitoring."""
        self.monitoring = True
        self.start_time = time.time()
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        print("🔍 CPU & Threading profiler started")
        
    def stop(self):
        """Stop monitoring."""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=2)
        print("\n✅ Profiler stopped")
        
    def analyze(self):
        """Analyze collected data."""
        if not self.cpu_samples:
            print("No data collected")
            return
            
        print("\n" + "="*80)
        print("CPU & THREADING ANALYSIS")
        print("="*80)
        
        # === CPU Analysis ===
        print("\n📊 CPU USAGE ANALYSIS")
        print("-" * 80)
        
        system_cpu = [s['system_cpu'] for s in self.cpu_samples]
        process_cpu = [s['process_cpu'] for s in self.cpu_samples]
        child_cpu = [s['child_cpu'] for s in self.cpu_samples]
        
        print(f"\nSystem CPU (Overall):")
        print(f"  Min:     {min(system_cpu):6.1f}%")
        print(f"  Max:     {max(system_cpu):6.1f}%")
        print(f"  Average: {statistics.mean(system_cpu):6.1f}%")
        print(f"  Median:  {statistics.median(system_cpu):6.1f}%")
        print(f"  StdDev:  {statistics.stdev(system_cpu):6.1f}%")
        
        print(f"\nMain Process CPU:")
        print(f"  Min:     {min(process_cpu):6.1f}%")
        print(f"  Max:     {max(process_cpu):6.1f}%")
        print(f"  Average: {statistics.mean(process_cpu):6.1f}%")
        print(f"  Median:  {statistics.median(process_cpu):6.1f}%")
        
        print(f"\nChild Processes CPU (Browser workers):")
        print(f"  Min:     {min(child_cpu):6.1f}%")
        print(f"  Max:     {max(child_cpu):6.1f}%")
        print(f"  Average: {statistics.mean(child_cpu):6.1f}%")
        print(f"  Median:  {statistics.median(child_cpu):6.1f}%")
        
        # Detect spikes
        spike_threshold = 90.0
        spikes = [cpu for cpu in system_cpu if cpu >= spike_threshold]
        spike_rate = len(spikes) / len(system_cpu) * 100
        
        print(f"\n⚠️  CPU SPIKE ANALYSIS:")
        print(f"  Threshold:     {spike_threshold}%")
        print(f"  Spike samples: {len(spikes)}/{len(system_cpu)}")
        print(f"  Spike rate:    {spike_rate:.1f}%")
        
        if spike_rate > 30:
            print(f"  Status:        🔴 CRITICAL - Frequent spikes (>{spike_threshold}%)")
        elif spike_rate > 10:
            print(f"  Status:        🟡 WARNING - Occasional spikes")
        else:
            print(f"  Status:        🟢 HEALTHY - Rare spikes")
        
        # === Threading Analysis ===
        print("\n" + "-" * 80)
        print("🧵 THREADING ANALYSIS")
        print("-" * 80)
        
        thread_counts = [s['count'] for s in self.thread_samples]
        
        print(f"\nThread Count:")
        print(f"  Min:     {min(thread_counts):3d}")
        print(f"  Max:     {max(thread_counts):3d}")
        print(f"  Average: {statistics.mean(thread_counts):6.1f}")
        print(f"  Median:  {statistics.median(thread_counts):3d}")
        
        # Detect thread creation pattern
        if len(thread_counts) > 10:
            first_10 = thread_counts[:10]
            increase = max(first_10) - min(first_10)
            time_span = self.thread_samples[9]['time'] - self.thread_samples[0]['time']
            
            print(f"\nThread Ramp-Up (first {time_span:.1f}s):")
            print(f"  Initial: {min(first_10):3d} threads")
            print(f"  Peak:    {max(first_10):3d} threads")
            print(f"  Increase: {increase:3d} threads")
            
            if increase > 5 and time_span < 2:
                print(f"  Pattern:  🔴 INSTANT SPIKE ({increase} threads in {time_span:.1f}s)")
                print(f"            This causes CPU burst!")
            elif increase > 5 and time_span < 5:
                print(f"  Pattern:  🟡 FAST RAMP ({increase} threads in {time_span:.1f}s)")
            else:
                print(f"  Pattern:  🟢 GRADUAL ({increase} threads in {time_span:.1f}s)")
        
        # === Process Analysis ===
        print("\n" + "-" * 80)
        print("🔧 CHILD PROCESS ANALYSIS (Browser Workers)")
        print("-" * 80)
        
        process_counts = [s['count'] for s in self.process_samples]
        
        if process_counts:
            print(f"\nBrowser Process Count:")
            print(f"  Min:     {min(process_counts):3d}")
            print(f"  Max:     {max(process_counts):3d}")
            print(f"  Average: {statistics.mean(process_counts):6.1f}")
            print(f"  Median:  {statistics.median(process_counts):3d}")
            
            # Each browser process uses ~60-85% CPU
            estimated_cpu_per_process = 70  # Conservative estimate
            max_processes = max(process_counts)
            estimated_peak_cpu = max_processes * estimated_cpu_per_process
            
            print(f"\nEstimated CPU Impact:")
            print(f"  Max processes:      {max_processes}")
            print(f"  CPU per process:    ~{estimated_cpu_per_process}%")
            print(f"  Estimated peak CPU: ~{estimated_peak_cpu}%")
            
            # Get actual CPU cores
            cpu_cores = psutil.cpu_count(logical=False)
            cpu_threads = psutil.cpu_count(logical=True)
            
            print(f"\nSystem Capacity:")
            print(f"  Physical cores: {cpu_cores}")
            print(f"  Logical cores:  {cpu_threads}")
            print(f"  Total capacity: {cpu_threads * 100}%")
            
            if estimated_peak_cpu > cpu_threads * 100:
                print(f"\n  ⚠️  WARNING: Peak demand ({estimated_peak_cpu}%) exceeds capacity ({cpu_threads * 100}%)")
                print(f"      Recommend: Reduce workers to {cpu_threads // 2} or less")
        
        # === Visualization ===
        print("\n" + "-" * 80)
        print("📈 CPU PATTERN VISUALIZATION")
        print("-" * 80)
        
        # Show first 20 samples (ramp-up phase)
        print("\nFirst 20 samples (ramp-up phase):")
        print("Time(s) | System CPU | Process CPU | Child CPU | Threads")
        print("-" * 70)
        
        for i in range(min(20, len(self.cpu_samples))):
            cpu_s = self.cpu_samples[i]
            thread_s = self.thread_samples[i] if i < len(self.thread_samples) else {'count': 0}
            
            print(f"{cpu_s['time']:7.1f} | "
                  f"{cpu_s['system_cpu']:10.1f} | "
                  f"{cpu_s['process_cpu']:11.1f} | "
                  f"{cpu_s['child_cpu']:9.1f} | "
                  f"{thread_s['count']:7d}")
        
        # ASCII chart of CPU usage
        print("\nCPU Usage Pattern (System CPU %):")
        self._print_ascii_chart(system_cpu, max_val=100, height=10)
        
        print("\n" + "="*80)
        
    def _print_ascii_chart(self, data, max_val=100, height=10):
        """Print ASCII chart of data."""
        if not data:
            return
            
        # Normalize data to chart height
        normalized = [int(val / max_val * height) for val in data]
        
        # Print chart from top to bottom
        for row in range(height, -1, -1):
            val_label = f"{row * max_val / height:5.0f}% |"
            line = val_label
            for val in normalized:
                if val >= row:
                    line += "█"
                else:
                    line += " "
            print(line)
        
        # Print time axis
        print("      +" + "-" * len(data))
        print("       " + "^" + " " * (len(data)//2 - 2) + "time" + " " * (len(data)//2))


def test_profiler():
    """Test the profiler with simulated workload."""
    print("Testing CPU & Threading Profiler")
    print("This will simulate a workload for 30 seconds")
    print()
    
    profiler = CPUThreadingProfiler(sample_interval=0.5)
    profiler.start()
    
    # Simulate workload
    print("Simulating workload...")
    import numpy as np
    
    def cpu_intensive_task():
        """Simulate CPU-intensive work."""
        for _ in range(5):
            # Matrix multiplication (CPU intensive)
            a = np.random.rand(500, 500)
            b = np.random.rand(500, 500)
            c = np.dot(a, b)
            time.sleep(0.5)
    
    # Start worker threads gradually
    workers = []
    for i in range(6):
        print(f"  Starting worker {i+1}/6...")
        t = threading.Thread(target=cpu_intensive_task)
        t.start()
        workers.append(t)
        time.sleep(1)  # 1 second between spawns
    
    # Wait for completion
    print("  Waiting for workers to complete...")
    for t in workers:
        t.join()
    
    # Extra monitoring time
    time.sleep(5)
    
    # Stop and analyze
    profiler.stop()
    profiler.analyze()


if __name__ == "__main__":
    try:
        test_profiler()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
