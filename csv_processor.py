"""
CSV Processor Module for Email Scraper & Validator
Handles parallel processing of CSV files with URL scraping and contact extraction.
"""

import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple, Any
import time
from tqdm import tqdm
import os
from urllib.parse import urlparse
import csv
from collections import deque
import threading
import queue
import json
from datetime import datetime
from pathlib import Path
import chardet
import psutil

from web_scraper import WebScraper
from contact_extractor import ContactExtractor
from email_validation import EmailValidator
from whatsapp_validator import WhatsAppValidator
from url_cleaner import URLCleaner

# Phase 2 Integration (optional)
try:
    from phase2_integration import (
        get_phase2_manager, is_phase2_enabled, 
        check_memory, should_throttle, record_metric
    )
    PHASE2_AVAILABLE = True
except ImportError:
    PHASE2_AVAILABLE = False

# ============================================================================
# ENCODING DETECTION UTILITY
# ============================================================================

def detect_file_encoding(file_path: str, sample_size: int = 100000) -> str:
    """
    Auto-detect file encoding using chardet with fallback strategy.

    Args:
        file_path: Path to the file
        sample_size: Number of bytes to analyze (default 100KB)

    Returns:
        Detected encoding (e.g., 'utf-8', 'latin-1', 'cp1252')

    FIX: Improved encoding detection:
    - Only read sample for validation (not entire file)
    - Better error handling
    - Warn when falling back to latin-1
    """
    logger = logging.getLogger(__name__)

    try:
        # Read sample from file
        with open(file_path, 'rb') as f:
            raw_data = f.read(sample_size)

        # Use chardet to detect encoding
        detected = chardet.detect(raw_data)
        encoding = detected.get('encoding', 'utf-8')
        confidence = detected.get('confidence', 0)

        if encoding and confidence > 0.7:
            logger.debug(f"Detected encoding: {encoding} (confidence: {confidence:.2f})")
            return encoding.lower()
        else:
            logger.debug(f"Low confidence encoding detection: {encoding} ({confidence:.2f})")
    except Exception as e:
        logger.warning(f"Encoding detection failed: {e}")

    # Fallback: Try common encodings in order
    # FIX: Added CJK encodings for Japanese/Korean/Chinese content
    fallback_encodings = [
        'utf-8',
        # CJK encodings (Japanese, Korean, Chinese)
        'shift_jis', 'euc-jp', 'iso-2022-jp',  # Japanese
        'euc-kr', 'cp949',  # Korean
        'gb2312', 'gbk', 'gb18030', 'big5',  # Chinese
        # Windows codepages
        'cp1252', 'cp1251', 'cp1250',
        # ISO encodings
        'iso-8859-1', 'iso-8859-15',
        # Unicode variants
        'utf-16', 'utf-16-le', 'utf-16-be',
    ]
    file_size = os.path.getsize(file_path)
    validation_size = min(sample_size * 2, file_size)  # Read max 200KB for validation

    for encoding in fallback_encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                content = f.read(validation_size)
                # Check for replacement characters (indicates wrong encoding)
                if '\ufffd' not in content:
                    logger.debug(f"Fallback encoding validated: {encoding}")
                    return encoding
        except (UnicodeDecodeError, LookupError):
            continue

    # Final fallback: latin-1 is most permissive (accepts all bytes 0x00-0xFF)
    # FIX: Log warning when using latin-1 as it may cause data corruption
    logger.warning(
        f"⚠️ Using latin-1 fallback encoding for {os.path.basename(file_path)}. "
        "This may cause character corruption for CJK/non-ASCII text."
    )
    return 'latin-1'

# ============================================================================
# INTELLIGENT CHUNK SIZE CONFIGURATION FOR LARGE FILES
# ============================================================================
# Accounts for concurrent browser instances which consume significant RAM
# Each browser instance: ~300MB
# Formula: Available_RAM = Total_RAM - System_Overhead - Browser_RAM
#
# Examples:
# - System with 8GB RAM, 5 workers (1.5GB browsers): Can handle ~6.5GB chunks
# - System with 4GB RAM, 3 workers (0.9GB browsers): Can handle ~3GB chunks
# - System with 16GB RAM, 20 workers (6GB browsers): Can handle ~9GB chunks
# ============================================================================

def calculate_optimal_chunksize(max_workers: int, file_size_mb: int,
                               system_ram_gb: float = None) -> int:
    """
    Calculate optimal CSV chunk size based on system resources and worker count.

    Args:
        max_workers: Number of concurrent browser instances
        file_size_mb: Size of input CSV file in MB
        system_ram_gb: Available system RAM (auto-detect if None)

    Returns:
        Optimal chunk size in rows (or 0 for all-at-once if file is small)
    """
    # Auto-detect available system RAM if not provided
    if system_ram_gb is None:
        try:
            available_ram_mb = psutil.virtual_memory().available / (1024 * 1024)
            system_ram_gb = available_ram_mb / 1024
        except:
            system_ram_gb = 4  # Conservative fallback

    # Constants
    BROWSER_RAM_MB = 300  # Average per browser instance
    SYSTEM_OVERHEAD_MB = 500  # OS and misc processes
    SAFETY_MARGIN = 0.7  # Use only 70% of calculated available RAM

    # Calculate RAM reserved for browsers
    browser_ram_mb = max_workers * BROWSER_RAM_MB

    # Calculate available RAM for CSV processing
    system_ram_mb = system_ram_gb * 1024
    available_for_csv_mb = (system_ram_mb - SYSTEM_OVERHEAD_MB - browser_ram_mb) * SAFETY_MARGIN

    # If file is small enough to fit entirely in available RAM, no chunking needed
    if file_size_mb <= available_for_csv_mb:
        logging.debug(f"💚 File ({file_size_mb}MB) fits in available RAM ({available_for_csv_mb:.0f}MB), no chunking needed")
        return 0  # Signal: load all-at-once

    # For large files, calculate optimal chunk size
    # Assume average row size ~5KB for typical CSV with contact data
    ESTIMATED_ROW_SIZE_KB = 5

    # Create chunks of ~50MB each (or smaller if RAM constrained)
    TARGET_CHUNK_SIZE_MB = min(50, available_for_csv_mb / 4)
    chunk_rows = int((TARGET_CHUNK_SIZE_MB * 1024) / ESTIMATED_ROW_SIZE_KB)

    # Minimum chunk size: 1000 rows (for processing efficiency)
    # Maximum chunk size: 100000 rows (to prevent any single chunk overflow)
    optimal_chunk = max(1000, min(100000, chunk_rows))

    logging.info(
        f"📊 Chunk calculation: file={file_size_mb}MB, workers={max_workers}, "
        f"browser_ram={browser_ram_mb}MB, available_for_csv={available_for_csv_mb:.0f}MB → "
        f"chunk_size={optimal_chunk} rows (~{(optimal_chunk * ESTIMATED_ROW_SIZE_KB) / 1024:.0f}MB)"
    )

    return optimal_chunk


# Completion marker utilities
def write_completion_marker(csv_path: str, stats: Dict[str, Any], status: str = "complete", error_message: Optional[str] = None) -> None:
    """
    Write a completion marker file alongside the processed CSV.

    Args:
        csv_path: Path to the CSV file
        stats: Processing statistics dictionary
        status: Completion status ('complete' or 'partial')
        error_message: Optional error message if status is 'partial'

    The marker file contains JSON metadata about the processing results.
    This prevents race conditions where monitor.py reads incomplete CSV files.
    """
    try:
        marker_path = Path(csv_path).with_suffix('.complete')

        # Build marker metadata
        marker_data = {
            "status": status,
            "csv_file": os.path.basename(csv_path),
            "total_rows": stats.get('processed', 0),
            "successful_rows": stats.get('successful', 0),
            "failed_rows": stats.get('failed', 0),
            "success_rate": stats.get('success_rate', 0),
            "total_emails": stats.get('total_emails', 0),
            "total_validated_emails": stats.get('total_validated_emails', 0),
            "total_phones": stats.get('total_phones', 0),
            "total_whatsapp": stats.get('total_whatsapp', 0),
            "processing_rate_per_min": stats.get('processing_per_menit', 0),
            "created_at": datetime.now().isoformat(),
            "error_message": error_message
        }

        # Write atomically: write to temp file first, then rename
        temp_path = marker_path.with_suffix('.tmp')
        with open(temp_path, 'w', encoding='utf-8') as f:
            json.dump(marker_data, f, indent=2)

        # Atomic rename (prevents partial reads)
        temp_path.rename(marker_path)

        logging.getLogger(__name__).info(
            f"✓ Completion marker written: {marker_path.name} "
            f"(status={status}, rows={stats.get('processed', 0)})"
        )
    except Exception as e:
        # Marker write failure should NOT crash the main process
        logging.getLogger(__name__).warning(f"Failed to write completion marker for {csv_path}: {e}")


def read_completion_marker(csv_path: str) -> Optional[Dict[str, Any]]:
    """
    Read completion marker metadata.

    Args:
        csv_path: Path to the CSV file

    Returns:
        Marker metadata dict or None if marker doesn't exist
    """
    try:
        marker_path = Path(csv_path).with_suffix('.complete')
        if not marker_path.exists():
            return None

        with open(marker_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logging.getLogger(__name__).warning(f"Failed to read completion marker for {csv_path}: {e}")
        return None


# Enhanced processing rate calculator - imports already exist above

class ProcessingRateCalculator:
    """
    Enhanced processing rate calculator with multiple metrics.

    FIX: Added thread safety and memory leak prevention:
    - All shared state protected by threading.Lock
    - completion_times deque has maxlen to prevent unbounded growth
    """

    def __init__(self, window_minutes=3, smoothing_factor=0.2):
        # FIX: Add maxlen to prevent memory leak for long-running processes
        self.completion_times = deque(maxlen=1000)  # Cap at 1000 entries
        self.window_secs = window_minutes * 60  # 3 minutes vs old 10
        self.smoothing_factor = smoothing_factor
        self.smoothed_rate = 0.0
        self.start_time = time.time()
        self.total_processed = 0
        self.recent_rates = deque(maxlen=10)
        self.last_rate_time = time.time()
        # FIX: Add lock for thread safety
        self._lock = threading.Lock()

    def add_completion(self):
        # FIX: Thread-safe access to shared state
        with self._lock:
            now = time.time()
            self.completion_times.append(now)
            self.total_processed += 1

            # Clean old entries (now also bounded by maxlen)
            while self.completion_times and (now - self.completion_times[0]) > self.window_secs:
                self.completion_times.popleft()

            # Calculate rate based on total time elapsed since start
            # This gives more stable and realistic numbers
            elapsed_time = now - self.start_time

            # Use minimum elapsed time to prevent unrealistic rates
            # At least 1 second for calculation to avoid division by very small numbers
            elapsed_minutes = max(elapsed_time / 60.0, 1.0 / 60.0)

            # Calculate overall rate: total processed / elapsed time
            current_rate = self.total_processed / elapsed_minutes

            # Cap maximum rate at reasonable value (1000 per minute = ~16 per second)
            # Most web scraping won't exceed this due to network/processing constraints
            max_rate = 1000.0
            current_rate = min(current_rate, max_rate)

            # Update smoothed rate with exponential moving average
            if self.smoothed_rate == 0.0:
                self.smoothed_rate = current_rate
            else:
                self.smoothed_rate = (
                    self.smoothing_factor * current_rate +
                    (1 - self.smoothing_factor) * self.smoothed_rate
                )

            self.last_rate_time = now

    def get_current_rate(self) -> float:
        # FIX: Thread-safe read
        with self._lock:
            return self.smoothed_rate

    def get_instantaneous_rate(self, last_seconds=30) -> float:
        """Calculate instantaneous rate based on recent completions."""
        # FIX: Thread-safe access
        with self._lock:
            now = time.time()
            recent = [t for t in self.completion_times if now - t <= last_seconds]

            if len(recent) >= 2:
                # Calculate rate based on recent completions
                time_span = now - recent[0]
                # Use minimum time span to avoid unrealistic rates
                time_span_minutes = max(time_span / 60.0, 1.0 / 60.0)
                rate = len(recent) / time_span_minutes
                # Cap at 1000/min
                return min(rate, 1000.0)
            elif len(recent) == 1:
                # Fall back to overall rate for single recent completion
                return self.smoothed_rate

            return 0.0

    def get_eta_minutes(self, total_urls: int) -> Optional[float]:
        # FIX: Thread-safe read
        with self._lock:
            if total_urls <= self.total_processed:
                return 0.0
            remaining = total_urls - self.total_processed
            current_rate = self.smoothed_rate
            return remaining / current_rate if current_rate > 0 else None

    def get_eta_formatted(self, total_urls: int) -> str:
        eta = self.get_eta_minutes(total_urls)
        if eta is None:
            return "N/A"
        elif eta < 1:
            return "<1min"
        elif eta < 60:
            return f"{eta:.0f}min"
        else:
            hours = int(eta // 60)
            minutes = int(eta % 60)
            return f"{hours}h{minutes:02d}m"


class CSVProcessor:
    """
    Handles parallel processing of CSV files containing URLs for contact extraction.
    """

    def __init__(self, max_workers: int = 5, timeout: int = 30, headless: bool = True, block_images: bool = False, disable_resources: bool = False, network_idle: bool = True, cf_wait_timeout: int = 60, skip_on_challenge: bool = False, proxy_file: str = "proxy.txt", max_concurrent_browsers: int = None, normal_budget: int = 60, challenge_budget: int = 120, dead_site_budget: int = 20, min_retry_threshold: int = 5, fast: bool = False, db_writer=None, use_pool: bool = False):
        """
        Initialize CSV processor.

        Args:
            max_workers: Maximum number of concurrent threads (reduced to 5 for stability)
            timeout: Timeout for web scraping requests
            block_images: Block image loading to reduce bandwidth
            disable_resources: Disable fonts/media/other non-essential resources
            network_idle: Wait for network idle state during dynamic fetch
            cf_wait_timeout: Per-URL maximum wait for Cloudflare challenge (seconds)
            skip_on_challenge: Skip immediately when Cloudflare challenge detected
            proxy_file: Path to proxy file for automatic proxy detection
            max_concurrent_browsers: Maximum concurrent browser instances (defaults to max_workers)
            normal_budget: Budget for normal sites in seconds (default: 60)
            challenge_budget: Budget for Cloudflare/challenge sites in seconds (default: 120)
            dead_site_budget: Budget for dead sites in seconds (default: 20)
            min_retry_threshold: Minimum remaining budget to attempt retry in seconds (default: 5)
            fast: Fast mode - limit extraction (1 WA, 1 social profile, 1 phone, 4 emails max)
            db_writer: Optional DatabaseWriter instance for PostgreSQL export
            use_pool: Use browser worker pool for 3-5x faster scraping
        """
        self.max_workers = max_workers
        self.timeout = timeout
        self.headless = headless
        self.fast_mode = fast
        self.db_writer = db_writer  # Store db_writer for consumer loop

        # Auto-set max_concurrent_browsers to match max_workers if not specified
        if max_concurrent_browsers is None:
            max_concurrent_browsers = max_workers

        self.scraper = WebScraper(
            headless=headless,
            timeout=timeout,
            block_images=block_images,
            disable_resources=disable_resources,
            network_idle=network_idle,
            cf_wait_timeout=cf_wait_timeout,
            skip_on_challenge=skip_on_challenge,
            proxy_file=proxy_file,
            static_first=False,
            max_concurrent_browsers=max_concurrent_browsers,
            normal_budget=normal_budget,
            challenge_budget=challenge_budget,
            dead_site_budget=dead_site_budget,
            min_retry_threshold=min_retry_threshold,
            fast_mode=fast,
            use_pool=use_pool
        )
        self.extractor = ContactExtractor()
        self.validator = EmailValidator(
            use_third_party=True,
            third_party_provider="rapid",
            sender_email="verify@example.com"
        )
        self.whatsapp_validator = WhatsAppValidator()
        # Logging levels are now controlled by main.py setup
        # email_validation, web_scraper, and csv_processor will log DEBUG to file
        # only INFO+ will appear in console

        # Setup module logger - rely on root configuration
        self.logger = logging.getLogger(__name__)

    def process_single_url(self, url_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single URL and extract contact information.

        Args:
            url_data: Dictionary containing URL and metadata

        Returns:
            Dictionary with extracted contact information
        """
        # Handle NaN values from pandas
        raw_url = url_data.get('url', '')
        if pd.isna(raw_url):
            url = ''
        else:
            url = str(raw_url).strip()
        row_index = url_data.get('index', 0)

        result = {
            'index': row_index,
            'url': url,
            'status': 'failed',
            'emails': [],
            'phones': [],
            'whatsapp': [],
            'validated_emails': {},
            'error': None,
            'processing_time': 0
        }
        # Preserve original row data for streaming CSV writing
        result['original_data'] = url_data.get('original_data', {})

        start_time = time.time()

        try:
            # ========================================================================
            # STEP 1: Clean URL before validation
            # ========================================================================
            # Handles:
            # - Google redirect URLs (/url?q=...)
            # - Tracking parameters (utm_*, opi, ved, etc)
            # - URL encoding issues
            # - Protocol normalization
            if url:
                cleaned_url = URLCleaner.clean_url(url, aggressive=False)
                if cleaned_url:
                    if cleaned_url != url:
                        self.logger.debug(f"URL cleaned: '{url}' → '{cleaned_url}'")
                    url = cleaned_url
                else:
                    result['error'] = 'Invalid URL format after cleanup'
                    self.logger.debug(f"Skipping invalid URL (failed cleanup): {url}")
                    return result

            # ========================================================================
            # STEP 2: Validate cleaned URL
            # ========================================================================
            if not url or not self._is_valid_url(url):
                result['error'] = 'Invalid URL format'
                self.logger.debug(f"Skipping invalid URL: {url}")
                return result

            # Update result with cleaned URL
            result['url'] = url

            # Skip social media URLs (Facebook, Instagram, LinkedIn, etc.)
            if self._is_social_url(url):
                result['status'] = 'skipped_social'
                result['error'] = 'Social media URL skipped'
                self.logger.debug(f"Skipping social media URL: {url}")
                return result

            self.logger.debug(f"Starting scrape for: {url}")
            # Use the new gather_contact_info method for comprehensive scraping
            scrape_result = self.scraper.gather_contact_info(url)
            self.logger.debug(f"Scrape completed for: {url} | status: {scrape_result.get('status')}")

            # Check for errors
            if scrape_result.get('error'):
                result['error'] = scrape_result['error']
                return result

            # Check if we got any content
            if scrape_result.get('status') == 'no_contacts_found':
                result['status'] = 'no_contacts_found'
                result['error'] = 'No contact information found on the website'
                return result

            # Extract contact information directly from scrape_result
            result['emails'] = scrape_result.get('emails', [])
            result['phones'] = scrape_result.get('phones', [])
            result['whatsapp'] = scrape_result.get('whatsapp', [])
            result['pages_scraped'] = len(scrape_result.get('pages_scraped', []))
            # Propagate social media profiles to result for CSV writer
            result['facebook'] = scrape_result.get('facebook', '')
            result['instagram'] = scrape_result.get('instagram', '')
            result['tiktok'] = scrape_result.get('tiktok', '')
            result['youtube'] = scrape_result.get('youtube', '')

            # FAST MODE: Limit extraction to speed up processing
            if self.fast_mode:
                result['emails'] = result['emails'][:4]  # Max 4 emails
                result['phones'] = result['phones'][:1]  # Max 1 phone
                result['whatsapp'] = result['whatsapp'][:1]  # Max 1 WhatsApp
                # Social media profiles are limited in web_scraper extraction

            self.logger.debug(f"Extracted from {url}: emails={len(result['emails'])}, phones={len(result['phones'])}, whatsapp={len(result['whatsapp'])}, pages={result['pages_scraped']}")

            # Validation moved to dedicated SMTP pool (consumer stage)

            # Set success status
            if result['emails'] or result['phones'] or result['whatsapp']:
                result['status'] = 'success'
                # Log extracted contacts (only to file, not CLI)
                try:
                    domain = urlparse(url).netloc
                    parts = []
                    if result['emails']:
                        parts.append(f"email: {', '.join(result['emails'][:5])}")
                    if result['whatsapp']:
                        parts.append(f"wa: {', '.join(result['whatsapp'][:3])}")
                    if result['phones']:
                        parts.append(f"telp: {', '.join(result['phones'][:3])}")
                    if parts:
                        self.logger.debug(f"✓ {domain} → {' | '.join(parts)}")
                except Exception:
                    pass  # Don't fail on logging errors
            else:
                result['status'] = 'no_contacts_found'

        except Exception as e:
            self.logger.error(f"Error processing URL {url}: {str(e)}")
            result['error'] = str(e)

        finally:
            result['processing_time'] = time.time() - start_time

        return result

    def process_csv_file(self,
                        input_file: str,
                        output_file: str,
                        batch_size: int = 100,
                        input_chunksize: int = 0,
                        limit_rows: Optional[int] = None) -> Dict[str, Any]:
        """
        Process a CSV file with URLs in parallel.
        Auto-detects URL column and selects only useful columns.

        Args:
            input_file: Path to input CSV file
            output_file: Path to output CSV file
            batch_size: Number of URLs to process in each batch
            input_chunksize: Number of rows to read per chunk (0 = auto-calculate for optimal performance)
            limit_rows: Optional limit on number of rows to process (for testing)

        Returns:
            Dictionary with processing statistics
        """
        # Auto-detect url_column, will be set in the try block below
        url_column = 'url'  # default fallback
        try:
            # ========================================================================
            # AUTO-DETECT FILE ENCODING
            # ========================================================================
            detected_encoding = detect_file_encoding(input_file)
            self.logger.debug(f"🔍 Detected file encoding: {detected_encoding}")

            # Read header only to validate columns without loading full data
            # IMPORTANT: Use dtype=str to preserve phone_number format (with '+' prefix)
            # and prevent pandas from auto-converting to INT
            try:
                columns_df = pd.read_csv(input_file, nrows=0, dtype=str, encoding=detected_encoding)
            except UnicodeDecodeError:
                # Fallback: detected encoding failed, retry with latin-1
                self.logger.warning(f"⚠️ Detected encoding '{detected_encoding}' failed during header read. Falling back to latin-1")
                detected_encoding = 'latin-1'
                columns_df = pd.read_csv(input_file, nrows=0, dtype=str, encoding=detected_encoding)

            # ============================================================================
            # AUTO-CALCULATE OPTIMAL CHUNKSIZE (if not explicitly provided)
            # ============================================================================
            # This considers:
            # 1. File size (larger files need smaller chunks)
            # 2. Number of workers/browser instances (more workers = less RAM per chunk)
            # 3. Available system RAM
            # ============================================================================
            file_size_mb = os.path.getsize(input_file) / (1024 * 1024)  # Calculate once, use throughout

            if input_chunksize == 0:
                calculated_chunksize = calculate_optimal_chunksize(
                    max_workers=self.max_workers,
                    file_size_mb=file_size_mb
                )
                if calculated_chunksize > 0:
                    input_chunksize = calculated_chunksize
                    self.logger.info(f"🔧 Auto-enabled chunking: chunk_size={input_chunksize} rows (~{(input_chunksize * 5) / 1024:.0f}MB per chunk)")
                else:
                    self.logger.info(f"✅ File size ({file_size_mb:.0f}MB) small enough for all-at-once loading")
            else:
                self.logger.debug(f"🔧 Using explicit chunk_size: {input_chunksize} rows")

            # AUTO-DETECT URL column: look for 'url', 'website', 'link', etc.
            detected_url_column = None
            for possible_name in ['url', 'website', 'link', 'site', 'domain']:
                if possible_name in columns_df.columns:
                    detected_url_column = possible_name
                    break

            if detected_url_column:
                url_column = detected_url_column
                self.logger.debug(f"🔍 Auto-detected URL column: '{url_column}'")
            elif url_column not in columns_df.columns:
                raise ValueError(f"No URL column found. Expected one of: url, website, link")

            # AUTO-SELECT only useful columns (ignore all the bloat)
            # Supports both Model 1 (standard) and Model 2 (Google Maps export) formats
            useful_columns = ['title', 'name', 'category', 'address', 'phone', 'emails', 'email',
                            'facebook', 'instagram', 'linkedin', 'twitter', 'whatsapp', 'website',
                            'complete_address']

            # Select columns that exist in the CSV
            # Includes both Model 1 (direct columns) and Model 2 (Google Maps) column names
            mandatory_cols_for_select = [
                'name', 'street', 'city', 'country_code', 'url',
                'phone_number', 'google_business_categories',
                'facebook', 'instagram', 'email',
                # Model 2 (Google Maps export) columns
                'title', 'website', 'phone', 'category', 'complete_address', 'emails'
            ]
            select_set = set([url_column])
            for col in columns_df.columns:
                col_lower = col.lower()
                if (col in mandatory_cols_for_select) or any(useful in col_lower for useful in useful_columns):
                    select_set.add(col)
            select_columns = list(select_set)

            self.logger.debug(f"📋 Auto-selected {len(select_columns)} columns from {len(columns_df.columns)} total: {select_columns}")

            # Determine total rows with non-empty URL for progress bar
            total_urls = 0
            try:
                with open(input_file, 'r', encoding=detected_encoding, newline='') as f_in:
                    reader = csv.DictReader(f_in)
                    for r in reader:
                        u = (r.get(url_column) or '').strip()
                        if u:
                            total_urls += 1
                        # Apply limit if specified
                        if limit_rows and total_urls >= limit_rows:
                            break
            except UnicodeDecodeError:
                # Fallback: detected encoding failed, retry with latin-1
                self.logger.warning(f"⚠️ Detected encoding '{detected_encoding}' failed at URL count. Falling back to latin-1")
                detected_encoding = 'latin-1'
                with open(input_file, 'r', encoding=detected_encoding, newline='') as f_in:
                    reader = csv.DictReader(f_in)
                    for r in reader:
                        u = (r.get(url_column) or '').strip()
                        if u:
                            total_urls += 1
                        # Apply limit if specified
                        if limit_rows and total_urls >= limit_rows:
                            break

            if limit_rows:
                self.logger.debug(f"🔬 Testing mode: Processing limited to {total_urls} URLs (--limit {limit_rows})")
            self.logger.debug(f"📊 Processing {total_urls} URLs with {self.max_workers} workers")

            # Prepare streaming CSV writer
            os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)

            # Build header with mandatory columns first, then additional scraper columns
            # Mandatory columns that must always be present (even if empty)
            mandatory_cols = [
                'No', 'name', 'street', 'city', 'country_code', 'url',
                'phone_number', 'google_business_categories',
                'facebook', 'instagram', 'tiktok', 'youtube'
            ]

            # Redirect tracking columns
            # Track if URL was redirected to different domain during scraping
            redirect_cols = ['final_url', 'was_redirected']

            # Contact and validation columns added by scraper
            # Note: 'email' from input CSV is placed after 'whatsapp'
            contact_cols = ['emails', 'phones', 'whatsapp', 'email', 'validated_emails', 'validated_whatsapp']

            # Metrics columns
            metrics_cols = [
                'scraping_status', 'scraping_error', 'processing_time', 'pages_scraped',
                'emails_found', 'phones_found', 'whatsapp_found', 'validated_emails_count', 'validated_whatsapp_count'
            ]

            # Final header: mandatory + redirect tracking + contacts + metrics
            header = mandatory_cols + redirect_cols + contact_cols + metrics_cols

            # FIX: Track csv_file for guaranteed cleanup in finally block
            csv_file = None

            # Start timing the processing
            start_time = time.time()

            # Stats counters
            processed_count = 0  # URLs fully processed (validated + written)
            scraped_count = 0    # URLs scraped (producer done)
            success_count = 0
            total_emails = 0
            total_validated_emails = 0
            total_phones = 0
            total_whatsapp = 0
            total_processing_time = 0.0

            # Initialize optimized rate calculator - remove old redundant code
            rate_calculator = ProcessingRateCalculator(window_minutes=3, smoothing_factor=0.2)
            last_log_time = time.time()
            per_menit = 0.0

            # FIX: Open CSV file with try/finally to guarantee close on exception
            csv_file = open(output_file, 'w', newline='', encoding='utf-8')
            writer = csv.DictWriter(csv_file, fieldnames=header)
            writer.writeheader()

            # Create progress bar with clean format showing rate and ETA
            with tqdm(total=total_urls, desc="Processing URLs", unit="URL",
                     bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} {postfix}',
                     ncols=100) as pbar:
                # Locks for thread-safe writer and progress updates
                writer_lock = threading.Lock()
                progress_lock = threading.Lock()

                # Validation workers: 150% of scraping workers for faster queue processing
                smtp_workers = max(3, int(self.max_workers * 1.5))

                # Queue size: 2x total workers for adequate buffering
                queue_size = (self.max_workers + smtp_workers) * 2

                # Shared queue between producer (scraping) and consumer (API validation)
                scraped_queue: "queue.Queue[Dict[str, Any] | None]" = queue.Queue(maxsize=queue_size)

                # FIX: Shutdown event to signal consumers to exit gracefully
                # This prevents blocked get() calls from hanging forever
                shutdown_event = threading.Event()

                def consumer_loop():
                    nonlocal processed_count, scraped_count, success_count, total_emails, total_validated_emails, total_phones, total_whatsapp, total_processing_time, per_menit, last_log_time
                    while not shutdown_event.is_set():
                        # FIX: Use timeout-based get() to prevent indefinite blocking
                        # This allows consumer to periodically check shutdown_event
                        try:
                            item = scraped_queue.get(timeout=2)
                        except queue.Empty:
                            # No item available, loop back and check shutdown_event
                            continue

                        if item is None:
                            scraped_queue.task_done()
                            break

                        try:
                            result = item
                            url = result.get('url', 'unknown')
                            self.logger.debug(f"Consumer processing: {url} | status: {result.get('status')} | emails: {len(result.get('emails', []))}")

                            validated_emails = {}
                            emails_list = result.get('emails', []) or []
                            if emails_list:
                                self.logger.debug(f"Validating {len(emails_list)} emails from {url}: {emails_list}")
                                try:
                                    validated_emails = self.validator.validate_batch(emails_list)
                                    for email, validation in validated_emails.items():
                                        self.logger.debug(f"Email validation: {email} | valid: {validation.get('valid')} | reason: {validation.get('reason')} | conf: {validation.get('confidence')} | big: {validation.get('is_big_provider')}")
                                except Exception as e:
                                    self.logger.debug(f"Email validation failed for {url}: {str(e)}")
                                    validated_emails = {}
                            result['validated_emails'] = validated_emails

                            validated_whatsapp = {}
                            whatsapp_list = result.get('whatsapp', []) or []
                            if whatsapp_list:
                                self.logger.debug(f"Validating {len(whatsapp_list)} WhatsApp numbers from {url}: {whatsapp_list}")
                                try:
                                    validated_whatsapp = self.whatsapp_validator.validate_batch(whatsapp_list)
                                    for number, validation in validated_whatsapp.items():
                                        self.logger.debug(f"WhatsApp validation: {number} | valid: {validation.get('valid')} | reason: {validation.get('reason')} | type: {validation.get('type')} | country: {validation.get('country')}")
                                except Exception as e:
                                    self.logger.debug(f"WhatsApp validation failed for {url}: {str(e)}")
                                    validated_whatsapp = {}
                            result['validated_whatsapp'] = validated_whatsapp

                            # ============================================================================
                            # DATABASE EXPORT (if enabled)
                            # ============================================================================
                            # Write to PostgreSQL database if --export-db flag is set
                            # This happens BEFORE CSV export to ensure database is enriched first
                            if self.db_writer is not None:
                                try:
                                    import time
                                    db_start = time.time()
                                    db_success = self.db_writer.upsert_contact(result)
                                    db_elapsed = time.time() - db_start

                                    if db_success:
                                        self.logger.debug(f"DB: UPSERT successful for {url} ({db_elapsed:.3f}s)")
                                    else:
                                        self.logger.warning(f"DB: UPSERT failed for {url} after retries")

                                except Exception as db_err:
                                    self.logger.error(f"DB: Unexpected error for {url}: {db_err}", exc_info=True)
                                    # Continue with CSV export regardless of DB error

                            emails_count = len(emails_list)
                            phones_count = len(result.get('phones', []) or [])
                            whatsapp_count = len(whatsapp_list)
                            validated_emails_count = len(validated_emails)
                            validated_whatsapp_count = len(validated_whatsapp)

                            total_emails += emails_count
                            total_phones += phones_count
                            total_whatsapp += whatsapp_count
                            total_validated_emails += validated_emails_count
                            total_processing_time += float(result.get('processing_time', 0) or 0)

                            original_row = result.get('original_data', {}) or {}

                            def get_value(key):
                                """
                                Get value from original row with alias support.
                                Supports both Model 1 (standard) and Model 2 (Google Maps export) CSV formats.
                                """
                                # Define aliases for each key to support both CSV models
                                # Model 1: Direct column names
                                # Model 2: Google Maps export format with different naming
                                aliases = {
                                    'name': ['name', 'title', 'business_name', 'company_name'],
                                    'phone_number': ['phone_number', 'phone', 'phone_mobile'],
                                    'google_business_categories': ['google_business_categories', 'category', 'categories', 'type'],
                                    'email': ['email', 'emails', 'email_address'],
                                }

                                # Try primary key first
                                val = original_row.get(key, '')
                                if val and not pd.isna(val):
                                    return str(val).strip()

                                # Try aliases if primary key not found
                                if key in aliases:
                                    for alias in aliases[key]:
                                        val = original_row.get(alias, '')
                                        if val and not pd.isna(val):
                                            return str(val).strip()

                                return ''

                            # ============================================================================
                            # WAHA WHATSAPP VALIDATION
                            # ============================================================================
                            # Validate WhatsApp numbers using WAHA API
                            # Flow: 1) Check existing whatsapp first, 2) If invalid, check phone_number
                            #       3) If both invalid, whatsapp column will be empty
                            waha_validated_whatsapp = None
                            try:
                                scraped_whatsapp = whatsapp_list[0] if whatsapp_list else None
                                csv_phone_number = get_value('phone_number')
                                csv_country_code = get_value('country_code')
                                
                                waha_validated_whatsapp = self.whatsapp_validator.validate_for_whatsapp(
                                    whatsapp=scraped_whatsapp,
                                    phone_number=csv_phone_number,
                                    country_code=csv_country_code
                                )
                                
                                if waha_validated_whatsapp:
                                    self.logger.debug(f"WAHA validated WhatsApp for {url}: {waha_validated_whatsapp}")
                            except Exception as e:
                                self.logger.debug(f"WAHA validation error for {url}: {e}")
                                waha_validated_whatsapp = whatsapp_list[0] if whatsapp_list else None

                            # ============================================================================
                            # PREPARE OUTPUT ROW WITH REDIRECT TRACKING
                            # ============================================================================
                            # Track whether URL was redirected during scraping
                            original_url = result.get('url', '')
                            final_url = result.get('final_url', original_url)
                            was_redirected = 'true' if final_url and original_url and final_url != original_url else 'false'

                            output_row = {
                                'No': str(processed_count + 1),
                                'name': get_value('name'),
                                'street': get_value('street'),
                                'city': get_value('city'),
                                'country_code': get_value('country_code'),
                                'url': original_url,
                                'phone_number': get_value('phone_number'),
                                'google_business_categories': get_value('google_business_categories'),
                                'facebook': result.get('facebook', '') or get_value('facebook'),
                                'instagram': result.get('instagram', '') or get_value('instagram'),
                                'tiktok': result.get('tiktok', '') or '',
                                'youtube': result.get('youtube', '') or '',
                                'final_url': final_url,
                                'was_redirected': was_redirected,
                                'emails': '; '.join(emails_list),
                                'phones': '; '.join(result.get('phones', []) or []),
                                'whatsapp': waha_validated_whatsapp or '',
                                'email': get_value('email'),
                                'validated_emails': '; '.join([
                                    f"{email} (reason:{validation_result.get('reason', 'unknown')}, conf:{validation_result.get('confidence', 'unknown')}, big:{validation_result.get('is_big_provider', False)})"
                                    for email, validation_result in validated_emails.items()
                                ]),
                                'validated_whatsapp': f"{waha_validated_whatsapp} (waha_valid)" if waha_validated_whatsapp else '',
                                'scraping_status': result['status'],
                                'scraping_error': result.get('error', ''),
                                'processing_time': result.get('processing_time', 0),
                                'pages_scraped': result.get('pages_scraped', 0),
                                'emails_found': emails_count,
                                'phones_found': phones_count,
                                'whatsapp_found': whatsapp_count,
                                'validated_emails_count': validated_emails_count,
                                'validated_whatsapp_count': 1 if waha_validated_whatsapp else 0,
                            }

                            # ============================================================================
                            # EXTRACT DATA FROM COMPLETE_ADDRESS JSON (Model 2: Google Maps export)
                            # ============================================================================
                            # For Google Maps export format, complete_address is JSON containing:
                            # {"street": "...", "city": "...", "state": "...", "country": "...", "postal_code": "..."}
                            # Use this to fill empty fields if they exist
                            if 'complete_address' in original_row and original_row['complete_address']:
                                try:
                                    addr_str = str(original_row['complete_address']).strip()
                                    if addr_str:
                                        addr_data = json.loads(addr_str)

                                        # Fill street if empty
                                        if not output_row['street']:
                                            output_row['street'] = addr_data.get('street', '')

                                        # Fill city - use 'city' field, fallback to 'state'
                                        if not output_row['city']:
                                            output_row['city'] = addr_data.get('city') or addr_data.get('state', '')

                                        # Fill country_code from 'country' field
                                        if not output_row['country_code']:
                                            output_row['country_code'] = addr_data.get('country', '')
                                except (json.JSONDecodeError, ValueError, TypeError) as e:
                                    # Log JSON parse error but continue processing
                                    self.logger.debug(f"Failed to parse complete_address JSON for {url}: {str(e)}")

                            # FIX: Move processed_count increment inside writer_lock to prevent race
                            # Intelligent flush frequency: flush more often for large files
                            # - Every 10 rows for files < 10MB (default)
                            # - Every 50 rows for files 10-100MB (less overhead)
                            # - Every 100 rows for files > 100MB (optimal balance)
                            flush_interval = 10 if file_size_mb < 10 else (50 if file_size_mb < 100 else 100)

                            with writer_lock:
                                writer.writerow(output_row)
                                # FIX: Increment processed_count inside lock for consistent flush logic
                                processed_count += 1
                                if processed_count % flush_interval == 0:
                                    csv_file.flush()

                            with progress_lock:
                                if result['status'] == 'success':
                                    success_count += 1
                                per_menit = rate_calculator.get_current_rate()

                        except Exception as e:
                            try:
                                url = (item or {}).get('url', 'unknown')
                            except Exception:
                                url = 'unknown'
                            self.logger.error(f"Consumer error: {url} | {str(e)}")
                        finally:
                            try:
                                scraped_queue.task_done()
                            except Exception:
                                pass

                    # FIX: Drain remaining items when shutdown is set
                    # This ensures task_done() is called for all items, allowing join() to complete
                    while shutdown_event.is_set():
                        try:
                            remaining_item = scraped_queue.get_nowait()
                            scraped_queue.task_done()
                            if remaining_item is None:
                                break
                        except queue.Empty:
                            break

                # Start consumer pool
                consumer_executor = ThreadPoolExecutor(max_workers=smtp_workers)
                for _ in range(smtp_workers):
                    consumer_executor.submit(consumer_loop)

                # Start producer pool
                producer_executor = ThreadPoolExecutor(max_workers=self.max_workers)

                sentinel_sent = False
                try:
                    # Build chunk iterator (streaming if input_chunksize > 0)
                    # Apply select_columns filter to reduce memory usage
                    # Apply limit_rows if specified
                    # IMPORTANT: Use dtype=str to preserve phone_number format (with '+' prefix)
                    # and prevent pandas from auto-converting numeric strings to INT
                    try:
                        if input_chunksize and input_chunksize > 0:
                            chunk_iter = pd.read_csv(input_file, chunksize=input_chunksize, usecols=select_columns, nrows=limit_rows, dtype=str, encoding=detected_encoding)
                        else:
                            chunk_iter = [pd.read_csv(input_file, usecols=select_columns, nrows=limit_rows, dtype=str, encoding=detected_encoding)]
                    except UnicodeDecodeError:
                        # Fallback: detected encoding failed, retry with latin-1
                        self.logger.warning(f"⚠️ Detected encoding '{detected_encoding}' failed during CSV read. Falling back to latin-1")
                        detected_encoding = 'latin-1'
                        if input_chunksize and input_chunksize > 0:
                            chunk_iter = pd.read_csv(input_file, chunksize=input_chunksize, usecols=select_columns, nrows=limit_rows, dtype=str, encoding=detected_encoding)
                        else:
                            chunk_iter = [pd.read_csv(input_file, usecols=select_columns, nrows=limit_rows, dtype=str, encoding=detected_encoding)]

                    # Track total processed for limit enforcement
                    total_processed_so_far = 0

                    # Process each chunk
                    for df_chunk in chunk_iter:
                        # Efficient URL data preparation - avoid slow iterrows()
                        url_data_list = []
                        for index in df_chunk.index:
                            # Check if we've reached the limit
                            if limit_rows and total_processed_so_far >= limit_rows:
                                break

                            url = df_chunk.loc[index, url_column] if url_column in df_chunk.columns else None
                            if url and str(url).strip():
                                # Filter out Unnamed columns from original data and convert NaN to empty string
                                row_dict = df_chunk.loc[index].to_dict()
                                filtered_data = {}
                                for k, v in row_dict.items():
                                    if not k.startswith('Unnamed:'):
                                        # Convert NaN/None to empty string, otherwise keep the value
                                        if pd.isna(v):
                                            filtered_data[k] = ''
                                        else:
                                            filtered_data[k] = v
                                url_data = {
                                    'index': index,
                                    'url': str(url).strip(),
                                    'original_data': filtered_data
                                }
                                url_data_list.append(url_data)
                                total_processed_so_far += 1

                        # Submit tasks in batches to control memory
                        for i in range(0, len(url_data_list), batch_size):
                            # Phase 2: Memory backpressure check
                            if PHASE2_AVAILABLE and is_phase2_enabled():
                                while should_throttle():
                                    self.logger.warning("Memory pressure detected, throttling...")
                                    time.sleep(2.0)
                            
                            batch = url_data_list[i:i + batch_size]

                            future_to_url = {
                                producer_executor.submit(self.process_single_url, url_data): url_data
                                for url_data in batch
                            }

                            # As producers finish, push results to consumer queue
                            for future in as_completed(future_to_url):
                                try:
                                    result = future.result()
                                    url = result.get('url', 'unknown')
                                    self.logger.debug(f"Producer completed: {url} | status: {result.get('status')} | emails: {len(result.get('emails', []))} | phones: {len(result.get('phones', []))} | time: {result.get('processing_time', 0):.2f}s")
                                except Exception as e:
                                    # Create minimal error result to keep pipeline flowing
                                    bad = future_to_url.get(future, {})
                                    self.logger.warning(f"Producer failed for {bad.get('url', 'unknown')}: {str(e)}")
                                    result = {
                                        'index': bad.get('index', 0),
                                        'url': str(bad.get('url', '')),
                                        'status': 'error',
                                        'emails': [],
                                        'phones': [],
                                        'whatsapp': [],
                                        'validated_emails': {},
                                        'error': str(e),
                                        'processing_time': 0,
                                        'pages_scraped': 0,
                                        'original_data': bad.get('original_data', {})
                                    }
                                    self.logger.error(f"Error in producer result: {str(e)}")

                                # Update progress bar immediately after scraping (PRODUCER UPDATE)
                                rate_calculator.add_completion()
                                current_rate = rate_calculator.get_current_rate()
                                inst_rate = rate_calculator.get_instantaneous_rate(30)
                                eta_str = rate_calculator.get_eta_formatted(total_urls)
                                queue_size = scraped_queue.qsize()

                                with progress_lock:
                                    scraped_count += 1
                                    pbar.update(1)
                                    pbar.set_postfix({
                                        "rate": f"{current_rate:.1f}/min",
                                        "inst": f"{inst_rate:.1f}",
                                        "queue": queue_size,
                                        "ETA": eta_str
                                    })
                                
                                # Phase 2: Record metrics
                                if PHASE2_AVAILABLE and is_phase2_enabled():
                                    record_metric('urls_processed', 1)
                                    if result.get('status') == 'success':
                                        record_metric('urls_success', 1)
                                    record_metric('processing_time', result.get('processing_time', 0), 'timing')

                                # Push to queue for consumer validation and writing
                                scraped_queue.put(result)

                    # FIX: Set shutdown event FIRST to signal consumers to exit
                    # Consumers check this flag on each iteration (with 2s timeout on get())
                    shutdown_event.set()

                    # FIX: Send sentinels using non-blocking put
                    # Even if some fail, consumers will exit due to shutdown_event
                    for _ in range(smtp_workers):
                        try:
                            scraped_queue.put_nowait(None)
                        except queue.Full:
                            # Queue is full, but consumers will exit via shutdown_event anyway
                            pass
                        except Exception:
                            pass
                    sentinel_sent = True

                    # FIX: Use queue.join() with timeout thread
                    # Reduced timeout since consumers will exit within ~2s due to shutdown_event
                    join_complete = threading.Event()

                    def queue_join_with_timeout():
                        try:
                            scraped_queue.join()
                            join_complete.set()
                        except Exception:
                            pass

                    join_thread = threading.Thread(target=queue_join_with_timeout, daemon=True)
                    join_thread.start()

                    # Wait up to 15 seconds for queue to drain (reduced from 60s)
                    # Consumers will self-exit within ~2s due to shutdown_event + timeout get()
                    if not join_complete.wait(timeout=15):
                        self.logger.warning("Queue join timeout (15s); consumers should have exited")
                        # Drain any remaining items to release join()
                        try:
                            while not scraped_queue.empty():
                                try:
                                    scraped_queue.get_nowait()
                                    scraped_queue.task_done()
                                except Exception:
                                    break
                        except Exception as e:
                            self.logger.debug(f"Queue drain error (ignorable): {e}")

                    # Shutdown producer executor
                    try:
                        producer_executor.shutdown(wait=True, cancel_futures=True)
                    except TypeError:
                        # Python < 3.9 doesn't support cancel_futures
                        producer_executor.shutdown(wait=False)
                    except Exception as e:
                        self.logger.debug(f"Producer shutdown: {e}")

                    # Shutdown consumer executor
                    try:
                        consumer_executor.shutdown(wait=True, cancel_futures=True)
                    except TypeError:
                        # Python < 3.9 doesn't support cancel_futures
                        consumer_executor.shutdown(wait=False)
                    except Exception as e:
                        self.logger.debug(f"Consumer shutdown: {e}")

                finally:
                    # FIX: Always set shutdown_event in finally block
                    # This ensures consumers exit even on exception
                    try:
                        shutdown_event.set()
                    except Exception:
                        pass
                    try:
                        if not sentinel_sent:
                            for _ in range(smtp_workers):
                                try:
                                    scraped_queue.put_nowait(None)
                                except Exception:
                                    break
                    except Exception:
                        pass
                    try:
                        producer_executor.shutdown(wait=False)
                    except Exception:
                        pass
                    try:
                        consumer_executor.shutdown(wait=False)
                    except Exception:
                        pass

            # NOTE: csv_file.close() moved to finally block for guaranteed cleanup

            # End timing
            end_time = time.time()
            total_duration_seconds = end_time - start_time

            # Calculate statistics
            stats = {
                'total_urls': total_urls,
                'processed': processed_count,
                'successful': success_count,
                'failed': processed_count - success_count,
                'success_rate': (success_count / processed_count * 100) if processed_count > 0 else 0,
                'total_emails': total_emails,
                'total_validated_emails': total_validated_emails,
                'total_phones': total_phones,
                'total_whatsapp': total_whatsapp,
                'average_processing_time': (total_processing_time / processed_count) if processed_count > 0 else 0,
                'processing_per_menit': per_menit,
                'start_time': start_time,
                'end_time': end_time,
                'total_duration_seconds': total_duration_seconds
            }

            self.logger.info(f"✅ Completed! Success rate: {stats['success_rate']:.1f}% ({success_count}/{processed_count})")

            # Write completion marker (success case)
            write_completion_marker(output_file, stats, status="complete")

            return stats

        except Exception as e:
            self.logger.error(f"Error processing CSV file: {str(e)}")

            # Write partial completion marker (error case)
            # This allows monitor to upload partial data if acceptable
            partial_stats = {
                'processed': processed_count if 'processed_count' in locals() else 0,
                'successful': success_count if 'success_count' in locals() else 0,
                'failed': 0,
                'success_rate': 0,
                'total_emails': total_emails if 'total_emails' in locals() else 0,
                'total_validated_emails': total_validated_emails if 'total_validated_emails' in locals() else 0,
                'total_phones': total_phones if 'total_phones' in locals() else 0,
                'total_whatsapp': total_whatsapp if 'total_whatsapp' in locals() else 0,
                'processing_per_menit': per_menit if 'per_menit' in locals() else 0
            }
            write_completion_marker(output_file, partial_stats, status="partial", error_message=str(e))

            raise

        finally:
            # FIX: Guarantee CSV file is closed even on exception
            # This prevents file descriptor leak
            if csv_file is not None:
                try:
                    csv_file.close()
                    self.logger.debug(f"CSV file closed: {output_file}")
                except Exception as close_err:
                    self.logger.warning(f"Error closing CSV file: {close_err}")

    def _save_results_to_csv(self, results: List[Dict], original_df: pd.DataFrame, output_file: str):
        """
        Save processing results to CSV file.

        Args:
            results: List of processing results
            original_df: Original DataFrame
            output_file: Path to output CSV file
        """
        # Create output DataFrame
        output_data = []

        for result in results:
            row_index = result['index']
            original_row = original_df.iloc[row_index].to_dict()

            # Create base row with original data
            output_row = original_row.copy()

            # Add processing results
            output_row.update({
                'scraping_status': result['status'],
                'scraping_error': result.get('error', ''),
                'processing_time': result.get('processing_time', 0),
                'pages_scraped': result.get('pages_scraped', 0),
                'emails_found': len(result.get('emails', [])),
                'phones_found': len(result.get('phones', [])),
                'whatsapp_found': len(result.get('whatsapp', [])),
                'validated_emails_count': len(result.get('validated_emails', {})),
                'emails': '; '.join(result.get('emails', [])),
                'phones': '; '.join(result.get('phones', [])),
                'whatsapp': '; '.join(result.get('whatsapp', [])),
                'validated_emails': '; '.join([
                    f"{email} ({validation_result.get('reason', 'unknown')})"
                    for email, validation_result in result.get('validated_emails', {}).items()
                ])
            })

            output_data.append(output_row)

        # Create DataFrame and save
        output_df = pd.DataFrame(output_data)

        # Remove duplicate/sumber kolom 'email' dari input jika ada,
        # agar hanya satu kolom email yang digunakan ('emails')
        if 'email' in output_df.columns and 'emails' in output_df.columns:
            try:
                output_df = output_df.drop(columns=['email'])
            except Exception:
                pass

        # Reorder columns: taruh metrik di paling belakang setelah 'validated_emails'
        try:
            original_cols = list(original_df.columns)
            # pastikan mengabaikan kolom 'email' jika sudah dihapus
            original_cols = [c for c in original_cols if c != 'email' and c in output_df.columns]

            contact_cols = [c for c in ['emails', 'phones', 'whatsapp', 'validated_emails'] if c in output_df.columns]
            metrics_cols = [c for c in [
                'scraping_status', 'scraping_error', 'processing_time', 'pages_scraped',
                'emails_found', 'phones_found', 'whatsapp_found', 'validated_emails_count'
            ] if c in output_df.columns]

            # Susun urutan: original -> contacts -> metrics (metrics paling belakang)
            desired_order = original_cols + contact_cols + metrics_cols
            # Tambahkan kolom lain yang belum tercakup di posisi sebelum metrics
            remaining = [c for c in output_df.columns if c not in desired_order]
            # Tempatkan remaining sebelum metrics jika ada
            # temukan indeks mulai metrics dalam desired_order
            if metrics_cols:
                # sisipkan remaining tepat sebelum blok metrics
                insert_pos = len(original_cols + contact_cols)
                desired_order = desired_order[:insert_pos] + remaining + desired_order[insert_pos:]
            else:
                desired_order = desired_order + remaining

            output_df = output_df[desired_order]
        except Exception:
            # Jika ada masalah, tetap lanjut tanpa mengubah urutan
            pass

        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        # Save to CSV
        output_df.to_csv(output_file, index=False)
        self.logger.info(f"Results saved to {output_file}")

    def _is_valid_url(self, url: str) -> bool:
        """
        Check if URL is valid.

        Args:
            url: URL to validate

        Returns:
            True if URL is valid, False otherwise
        """
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except Exception:
            return False

    def _is_social_url(self, url: str) -> bool:
        """Detect if a URL belongs to a social media domain and should be skipped."""
        try:
            parsed = urlparse(url)
            host = (parsed.netloc or '').lower()
            social_domains = [
                'facebook.com', 'instagram.com', 'twitter.com', 'x.com', 'linkedin.com',
                'youtube.com', 'tiktok.com', 'pinterest.com', 'vk.com', 'snapchat.com',
                'wa.me', 'web.whatsapp.com', 'discord.com'
            ]
            return any(host.endswith(d) or d in host for d in social_domains)
        except Exception:
            return False

    def process_multiple_csv_files(self,
                                  input_files: List[str],
                                  output_dir: str,
                                  url_column: str = 'url') -> Dict[str, Any]:
        """
        Process multiple CSV files.

        Args:
            input_files: List of input CSV file paths
            output_dir: Output directory for processed files
            url_column: Name of the column containing URLs

        Returns:
            Dictionary with overall processing statistics
        """
        os.makedirs(output_dir, exist_ok=True)

        overall_stats = {
            'files_processed': 0,
            'total_urls': 0,
            'total_successful': 0,
            'total_failed': 0,
            'file_results': []
        }

        for input_file in input_files:
            try:
                # Generate output filename
                base_name = os.path.splitext(os.path.basename(input_file))[0]
                output_file = os.path.join(output_dir, f"{base_name}_processed.csv")

                # Process file
                stats = self.process_csv_file(input_file, output_file)

                # Update overall statistics
                overall_stats['files_processed'] += 1
                overall_stats['total_urls'] += stats['total_urls']
                overall_stats['total_successful'] += stats['successful']
                overall_stats['total_failed'] += stats['failed']
                overall_stats['file_results'].append({
                    'input_file': input_file,
                    'output_file': output_file,
                    'stats': stats
                })

            except Exception as e:
                self.logger.error(f"Error processing file {input_file}: {str(e)}")
                overall_stats['file_results'].append({
                    'input_file': input_file,
                    'error': str(e)
                })

        # Calculate overall success rate
        if overall_stats['total_urls'] > 0:
            overall_stats['overall_success_rate'] = (
                overall_stats['total_successful'] / overall_stats['total_urls'] * 100
            )
        else:
            overall_stats['overall_success_rate'] = 0

        return overall_stats


if __name__ == "__main__":
    # Example usage
    processor = CSVProcessor(max_workers=5, timeout=30)

    # Process single CSV file
    stats = processor.process_csv_file(
        input_file="input_urls.csv",
        output_file="output_results.csv"
    )

    print(f"Processing completed with {stats['success_rate']:.1f}% success rate")
    print(f"Total emails found: {stats['total_emails']}")
    print(f"Total validated emails: {stats['total_validated_emails']}")
