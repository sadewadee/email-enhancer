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

from web_scraper import WebScraper
from contact_extractor import ContactExtractor
from email_validation import EmailValidator
from whatsapp_validator import WhatsAppValidator



# Enhanced processing rate calculator - imports already exist above

class ProcessingRateCalculator:
    """Enhanced processing rate calculator with multiple metrics"""
    
    def __init__(self, window_minutes=3, smoothing_factor=0.2):
        self.completion_times = deque()
        self.window_secs = window_minutes * 60  # 3 minutes vs old 10
        self.smoothing_factor = smoothing_factor
        self.smoothed_rate = 0.0
        self.start_time = time.time()
        self.total_processed = 0
        self.recent_rates = deque(maxlen=10)
        self.last_rate_time = time.time()
        
    def add_completion(self):
        now = time.time()
        self.completion_times.append(now)
        self.total_processed += 1

        # Clean old entries
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
        return self.smoothed_rate
    
    def get_instantaneous_rate(self, last_seconds=30) -> float:
        """Calculate instantaneous rate based on recent completions."""
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
            return self.get_current_rate()

        return 0.0
    
    def get_eta_minutes(self, total_urls: int) -> Optional[float]:
        if total_urls <= self.total_processed:
            return 0.0
        remaining = total_urls - self.total_processed
        current_rate = self.get_current_rate()
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

    def __init__(self, max_workers: int = 10, timeout: int = 30, block_images: bool = False, disable_resources: bool = False, network_idle: bool = True, cf_wait_timeout: int = 60, skip_on_challenge: bool = False, proxy_file: str = "proxy.txt"):
        """
        Initialize CSV processor.

        Args:
            max_workers: Maximum number of concurrent threads
            timeout: Timeout for web scraping requests
            block_images: Block image loading to reduce bandwidth
            disable_resources: Disable fonts/media/other non-essential resources
            network_idle: Wait for network idle state during dynamic fetch
            cf_wait_timeout: Per-URL maximum wait for Cloudflare challenge (seconds)
            skip_on_challenge: Skip immediately when Cloudflare challenge detected
            proxy_file: Path to proxy file for automatic proxy detection
        """
        self.max_workers = max_workers
        self.timeout = timeout
        self.scraper = WebScraper(timeout=timeout, block_images=block_images, disable_resources=disable_resources, network_idle=network_idle, cf_wait_timeout=cf_wait_timeout, skip_on_challenge=skip_on_challenge, proxy_file=proxy_file, static_first=False)
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
            if not url or not self._is_valid_url(url):
                result['error'] = 'Invalid URL format'
                self.logger.debug(f"Skipping invalid URL: {url}")
                return result

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

            self.logger.debug(f"Extracted from {url}: emails={len(result['emails'])}, phones={len(result['phones'])}, whatsapp={len(result['whatsapp'])}, pages={result['pages_scraped']}")

            # Validation moved to dedicated SMTP pool (consumer stage)

            # Set success status
            if result['emails'] or result['phones'] or result['whatsapp']:
                result['status'] = 'success'
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
            input_chunksize: Number of rows to read per chunk (0 = read all at once)
            limit_rows: Optional limit on number of rows to process (for testing)

        Returns:
            Dictionary with processing statistics
        """
        # Auto-detect url_column, will be set in the try block below
        url_column = 'url'  # default fallback
        try:
            # Read header only to validate columns without loading full data
            columns_df = pd.read_csv(input_file, nrows=0)

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
            useful_columns = ['title', 'name', 'category', 'address', 'phone', 'emails', 'email',
                            'facebook', 'instagram', 'linkedin', 'twitter', 'whatsapp']

            # Select columns that exist in the CSV
            mandatory_cols_for_select = [
                'name', 'street', 'city', 'country_code', 'url',
                'phone_number', 'google_business_categories',
                'facebook', 'instagram', 'email'
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
            with open(input_file, 'r', encoding='utf-8', newline='') as f_in:
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
                'facebook', 'instagram'
            ]

            # Contact and validation columns added by scraper
            # Note: 'email' from input CSV is placed after 'whatsapp'
            contact_cols = ['emails', 'phones', 'whatsapp', 'email', 'validated_emails', 'validated_whatsapp']

            # Metrics columns
            metrics_cols = [
                'scraping_status', 'scraping_error', 'processing_time', 'pages_scraped',
                'emails_found', 'phones_found', 'whatsapp_found', 'validated_emails_count', 'validated_whatsapp_count'
            ]

            # Final header: mandatory + contacts + metrics
            header = mandatory_cols + contact_cols + metrics_cols

            # Open output file and write header
            csv_file = open(output_file, 'w', newline='', encoding='utf-8')
            writer = csv.DictWriter(csv_file, fieldnames=header)
            writer.writeheader()

            # Stats counters
            processed_count = 0
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

            # Create progress bar with clean format showing rate and ETA
            with tqdm(total=total_urls, desc="Processing URLs", unit="URL",
                     bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} {postfix}',
                     ncols=100) as pbar:
                # Locks for thread-safe writer and progress updates
                writer_lock = threading.Lock()
                progress_lock = threading.Lock()

                # Optimize SMTP worker count for better performance
                smtp_workers = min(3, max(1, self.max_workers // 3))

                # Shared queue between producer (scraping) and consumer (SMTP validation)
                scraped_queue: "queue.Queue[Dict[str, Any] | None]" = queue.Queue(maxsize=batch_size * 2)

                def consumer_loop():
                    nonlocal processed_count, success_count, total_emails, total_validated_emails, total_phones, total_whatsapp, total_processing_time, per_menit, last_log_time
                    while True:
                        item = scraped_queue.get()
                        if item is None:
                            scraped_queue.task_done()
                            break

                        result = item
                        url = result.get('url', 'unknown')
                        self.logger.debug(f"Consumer processing: {url} | status: {result.get('status')} | emails: {len(result.get('emails', []))}")

                        # Efficient email validation with reduced logging
                        validated_emails = {}
                        emails_list = result.get('emails', []) or []
                        if emails_list:
                            self.logger.debug(f"Validating {len(emails_list)} emails from {url}: {emails_list}")
                            try:
                                validated_emails = self.validator.validate_batch(emails_list)
                                # Log validation results for each email
                                for email, validation in validated_emails.items():
                                    self.logger.debug(f"Email validation: {email} | valid: {validation.get('valid')} | reason: {validation.get('reason')} | conf: {validation.get('confidence')} | big: {validation.get('is_big_provider')}")
                            except Exception as e:
                                self.logger.debug(f"Email validation failed for {url}: {str(e)}")
                                validated_emails = {}
                        result['validated_emails'] = validated_emails

                        # WhatsApp validation
                        validated_whatsapp = {}
                        whatsapp_list = result.get('whatsapp', []) or []
                        if whatsapp_list:
                            self.logger.debug(f"Validating {len(whatsapp_list)} WhatsApp numbers from {url}: {whatsapp_list}")
                            try:
                                validated_whatsapp = self.whatsapp_validator.validate_batch(whatsapp_list)
                                # Log validation results for each WhatsApp number
                                for number, validation in validated_whatsapp.items():
                                    self.logger.debug(f"WhatsApp validation: {number} | valid: {validation.get('valid')} | reason: {validation.get('reason')} | type: {validation.get('type')} | country: {validation.get('country')}")
                            except Exception as e:
                                self.logger.debug(f"WhatsApp validation failed for {url}: {str(e)}")
                                validated_whatsapp = {}
                        result['validated_whatsapp'] = validated_whatsapp

                        # Update stats
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

                        # Prepare CSV row - map input data to mandatory columns
                        original_row = result.get('original_data', {}) or {}

                        # Helper function to get value from original_row with fallback to empty string
                        def get_value(key):
                            val = original_row.get(key, '')
                            return '' if pd.isna(val) else str(val)

                        # Build output row with all mandatory columns mapped from input
                        output_row = {
                            # Mandatory columns
                            'No': str(processed_count + 1),
                            'name': get_value('name'),
                            'street': get_value('street'),
                            'city': get_value('city'),
                            'country_code': get_value('country_code'),
                            'url': result.get('url', ''),
                            'phone_number': get_value('phone_number'),
                            'google_business_categories': get_value('google_business_categories'),
                            'facebook': get_value('facebook'),
                            'instagram': get_value('instagram'),
                            # Scraped contact columns
                            'emails': '; '.join(emails_list),
                            'phones': '; '.join(result.get('phones', []) or []),
                            'whatsapp': '; '.join(whatsapp_list),
                            'email': get_value('email'),  # Original email column from input (moved to after whatsapp)
                            'validated_emails': '; '.join([
                                f"{email} (reason:{validation_result.get('reason', 'unknown')}, conf:{validation_result.get('confidence', 'unknown')}, big:{validation_result.get('is_big_provider', False)})"
                                for email, validation_result in validated_emails.items()
                            ]),
                            'validated_whatsapp': '; '.join([
                                f"{number} (valid:{validation_result.get('valid', False)}, type:{validation_result.get('type', 'unknown')}, country:{validation_result.get('country', 'unknown')}, reason:{validation_result.get('reason', 'unknown')})"
                                for number, validation_result in validated_whatsapp.items()
                            ]),
                            # Metrics columns
                            'scraping_status': result['status'],
                            'scraping_error': result.get('error', ''),
                            'processing_time': result.get('processing_time', 0),
                            'pages_scraped': result.get('pages_scraped', 0),
                            'emails_found': emails_count,
                            'phones_found': phones_count,
                            'whatsapp_found': whatsapp_count,
                            'validated_emails_count': validated_emails_count,
                            'validated_whatsapp_count': validated_whatsapp_count,
                        }

                        # Batch CSV writing for better I/O performance
                        with writer_lock:
                            writer.writerow(output_row)
                            # Flush periodically for data safety (file handle in context)
                            if processed_count % 10 == 0:
                                csv_file.flush()

                        # Fast progress update - calculations moved outside lock
                        rate_calculator.add_completion()
                        
                        # Calculate stats outside critical section
                        current_rate = rate_calculator.get_current_rate()
                        inst_rate = rate_calculator.get_instantaneous_rate(30)
                        eta_str = rate_calculator.get_eta_formatted(total_urls)
                        
                        # Minimal lock duration for thread safety
                        with progress_lock:
                            if result['status'] == 'success':
                                success_count += 1
                            processed_count += 1
                            pbar.update(1)

                            # Update display every completion for better responsiveness
                            pbar.set_postfix({
                                "rate": f"{current_rate:.1f}/min",
                                "inst": f"{inst_rate:.1f}",
                                "ETA": eta_str
                            })

                        per_menit = current_rate

                        scraped_queue.task_done()

                # Start consumer pool
                consumer_executor = ThreadPoolExecutor(max_workers=smtp_workers)
                for _ in range(smtp_workers):
                    consumer_executor.submit(consumer_loop)

                # Start producer pool
                producer_executor = ThreadPoolExecutor(max_workers=self.max_workers)

                try:
                    # Build chunk iterator (streaming if input_chunksize > 0)
                    # Apply select_columns filter to reduce memory usage
                    # Apply limit_rows if specified
                    if input_chunksize and input_chunksize > 0:
                        chunk_iter = pd.read_csv(input_file, chunksize=input_chunksize, usecols=select_columns, nrows=limit_rows)
                    else:
                        chunk_iter = [pd.read_csv(input_file, usecols=select_columns, nrows=limit_rows)]

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

                                # Push to queue for consumer validation and writing
                                scraped_queue.put(result)

                    # All producers submitted and completed; shut down producer executor
                    producer_executor.shutdown(wait=True)

                    # Signal consumers to exit by sending sentinel values
                    for _ in range(smtp_workers):
                        scraped_queue.put(None)

                    # Wait until all queued items are processed
                    scraped_queue.join()

                    # Shutdown consumer pool
                    consumer_executor.shutdown(wait=True)

                finally:
                    try:
                        producer_executor.shutdown(wait=False)
                    except Exception:
                        pass
                    try:
                        consumer_executor.shutdown(wait=False)
                    except Exception:
                        pass

            # Close CSV file after streaming writes
            try:
                csv_file.close()
            except Exception:
                pass

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
                'processing_per_menit': per_menit
            }

            self.logger.info(f"✅ Completed! Success rate: {stats['success_rate']:.1f}% ({success_count}/{processed_count})")
            return stats

        except Exception as e:
            self.logger.error(f"Error processing CSV file: {str(e)}")
            raise

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
