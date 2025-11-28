#!/usr/bin/env python3
"""
Email Scraper & Validator - Main Orchestration Script
Comprehensive tool for extracting and validating contact information from websites.
"""

import argparse
import os
import sys
import logging
import json
import time
import signal
from datetime import datetime
from typing import Dict, List, Optional, Any
import traceback

# Import our modules
from csv_processor import CSVProcessor
from post_processor import PostProcessor
from web_scraper import WebScraper
from contact_extractor import ContactExtractor
from email_validation import EmailValidator


class EmailScraperValidator:
    """
    Main orchestrator class for the Email Scraper & Validator system.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the Email Scraper & Validator.

        Args:
            config: Configuration dictionary
        """
        self.config = config or self._get_default_config()

        # Setup logging
        self._setup_logging()

        # Initialize components
        self.csv_processor = CSVProcessor(
            max_workers=self.config['max_workers'],
            timeout=self.config['timeout'],
            block_images=self.config.get('block_images', True),
            disable_resources=self.config.get('disable_resources', False),
            network_idle=self.config.get('network_idle', True),
            cf_wait_timeout=self.config.get('cf_wait_timeout', 30),
            skip_on_challenge=self.config.get('skip_on_challenge', False),
            proxy_file=self.config.get('proxy_file', 'proxy.txt'),
            max_concurrent_browsers=self.config['max_workers'],
            normal_budget=self.config.get('normal_budget', 60),
            challenge_budget=self.config.get('challenge_budget', 120),
            dead_site_budget=self.config.get('dead_site_budget', 20),
            min_retry_threshold=self.config.get('min_retry_threshold', 5)
        )
        self.post_processor = PostProcessor()

        # Use root logger and mark to prevent duplicate logging
        self.logger = logging.getLogger()

        # Simple hack to prevent duplicate: add marker to see if already logged
        _logged_markers = getattr(self.logger, '_logged_markers', set())
        if not hasattr(self.logger, '_logged_markers'):
            self.logger._logged_markers = _logged_markers

        # Log only if not already logged (prevent duplicates)
        msg1 = "🚀 Email Scraper & Validator started"
        if msg1 not in _logged_markers:
            self.logger.info(msg1)
            _logged_markers.add(msg1)

        msg2 = f"⚙️  Config: {self.config['max_workers']} workers, timeout={self.config['timeout']}s, light_load={'ON' if self.config.get('block_images') else 'OFF'}"
        if msg2 not in _logged_markers:
            self.logger.info(msg2)
            _logged_markers.add(msg2)

    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration."""
        return {
            'max_workers': 3,
            'timeout': 120,
            'batch_size': 100,
            'chunk_size': 0,
            'max_contacts_per_type': 10,
            'output_format': 'long',
            'generate_report': False,
            'deduplicate': True,
            'log_level': 'INFO',
            # Default light-load behavior
            'block_images': True,
            'disable_resources': False,
            'network_idle': True,
            # Proxy configuration
            'proxy_file': 'proxy.txt'
        }

    def _setup_logging(self):
        """Setup logging configuration."""
        log_level = getattr(logging, self.config.get('log_level', 'INFO').upper())

        # Create logs directory
        os.makedirs('logs', exist_ok=True)

        # Custom formatter without milliseconds
        class NoMillisecondsFormatter(logging.Formatter):
            def formatTime(self, record, datefmt=None):
                ct = self.converter(record.created)
                if datefmt:
                    s = time.strftime(datefmt, ct)
                else:
                    s = time.strftime("%Y-%m-%d %H:%M:%S", ct)
                return s

        # Duplicate suppression filter for console only
        class DuplicateFilter(logging.Filter):
            def __init__(self):
                super().__init__()
                self.seen_messages = {}
                self.timeout = 1.0  # Suppress duplicates within 1 second

            def filter(self, record):
                current_time = time.time()
                msg_key = f"{record.levelname}:{record.getMessage()}"

                # Clean old entries
                self.seen_messages = {k: v for k, v in self.seen_messages.items()
                                      if current_time - v < self.timeout}

                # Check if seen recently
                if msg_key in self.seen_messages:
                    return False  # Suppress duplicate

                # Mark as seen
                self.seen_messages[msg_key] = current_time
                return True

        # Console level filter - only show INFO and above on console
        class ConsoleFilter(logging.Filter):
            def filter(self, record):
                # Allow INFO, WARNING, ERROR on console
                # Block DEBUG on console (but keep in file)
                return record.levelno >= logging.INFO

        # Configure root logger once and force reset existing handlers to avoid duplicates
        # Remove all existing handlers first
        root_logger = logging.getLogger()

        # Remove existing handlers to allow reconfiguration
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        # Create formatters
        console_formatter = NoMillisecondsFormatter('%(asctime)s %(levelname)s - %(message)s')
        file_formatter = NoMillisecondsFormatter('%(asctime)s %(levelname)s [%(name)s] - %(message)s')

        # Create duplicate filter
        dup_filter = DuplicateFilter()
        console_filter = ConsoleFilter()

        # File handler - VERBOSE (DEBUG level, no filters)
        file_handler = logging.FileHandler(f'logs/email_scraper_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.DEBUG)  # Log everything to file

        # Console handler - MINIMAL (INFO level, with filters)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(logging.INFO)  # Only INFO and above on console
        console_handler.addFilter(dup_filter)
        console_handler.addFilter(console_filter)

        # Set handlers
        root_logger.setLevel(logging.DEBUG)  # Root accepts all levels
        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)

        # Suppress noisy third-party loggers
        logging.getLogger('filelock').setLevel(logging.WARNING)
        logging.getLogger('scrapling').setLevel(logging.WARNING)
        logging.getLogger('playwright').setLevel(logging.WARNING)
        logging.getLogger('urllib3').setLevel(logging.WARNING)

    def process_single_csv(self,
                           input_file: str,
                           output_dir: Optional[str] = None,
                           limit_rows: Optional[int] = None) -> Dict[str, Any]:
        """
        Process a single CSV file.
        Auto-detects URL column and useful columns.

        Args:
            input_file: Path to input CSV file
            output_dir: Output directory (default: same as input)
            limit_rows: Optional limit on number of rows to process (for testing)

        Returns:
            Dictionary with processing results
        """
        # Setup signal handlers for graceful shutdown
        shutdown_requested = False
        handler_running = False  # Prevent re-entrance

        def signal_handler(signum, frame):
            nonlocal shutdown_requested, handler_running

            # Prevent re-entrance (double trigger from SIGINT + SIGTERM)
            if handler_running:
                return
            handler_running = True

            if shutdown_requested:
                # Second signal - force quit (use os._exit for immediate termination)
                # Exit with code 0 to prevent monitor from respawning (it's intentional exit)
                try:
                    self.logger.warning("❌ Force quit!")
                except:
                    pass
                os._exit(0)  # Changed from exit(1): graceful force quit, not a crash

            shutdown_requested = True
            try:
                self.logger.warning("⚠️  Shutdown requested, finishing current tasks...")
                self.logger.info("💡 Tip: Press Ctrl+C or send signal again to force quit")
            except:
                pass  # Logging may fail in signal handler

            handler_running = False

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        try:
            # Setup output directory
            if output_dir is None:
                output_dir = os.path.dirname(input_file)

            os.makedirs(output_dir, exist_ok=True)

            # Generate output filenames
            base_name = os.path.splitext(os.path.basename(input_file))[0]
            processed_file = os.path.join(output_dir, f"{base_name}_processed.csv")

            # Log with limit info if specified
            limit_info = f" (limit: {limit_rows})" if limit_rows else ""
            self.logger.info(f"📂 Processing file: {os.path.basename(input_file)}{limit_info}")

            # Step 1: Process CSV with contact extraction
            processing_stats = self.csv_processor.process_csv_file(
                input_file=input_file,
                output_file=processed_file,
                batch_size=self.config['batch_size'],
                input_chunksize=self.config.get('chunk_size', 0),
                limit_rows=limit_rows
            )

            # Step 2: Post-processing
            final_results = self._post_process_results(
                processed_file, output_dir, base_name
            )

            # Combine results
            results = {
                'input_file': input_file,
                'output_directory': output_dir,
                'processing_stats': processing_stats,
                'post_processing': final_results,
                'status': 'completed'
            }

            self.logger.info(f"🎉 Processing completed for {os.path.basename(input_file)}")
            return results

        except Exception as e:
            self.logger.error(f"Error processing {input_file}: {str(e)}")
            self.logger.error(traceback.format_exc())
            return {
                'input_file': input_file,
                'status': 'failed',
                'error': str(e)
            }
        finally:
            # Explicit cleanup to ensure resources are released
            # This prevents process hangs in shutdown phase
            try:
                # Force garbage collection to cleanup any remaining resources
                import gc
                gc.collect()
            except:
                pass

    def process_multiple_csv(self,
                           input_files: List[str],
                           output_dir: str,
                           merge_results: bool = True,
                           limit_rows: Optional[int] = None) -> Dict[str, Any]:
        """
        Process multiple CSV files.

        Args:
            input_files: List of input CSV file paths
            output_dir: Output directory
            url_column: Name of column containing URLs
            merge_results: Whether to merge all results into single file

        Returns:
            Dictionary with processing results
        """
        try:
            os.makedirs(output_dir, exist_ok=True)

            self.logger.info(f"Starting batch processing of {len(input_files)} files")

            # Process each file
            individual_results = []
            processed_files = []

            for input_file in input_files:
                result = self.process_single_csv(input_file, output_dir, limit_rows)
                individual_results.append(result)

                if result['status'] == 'completed':
                    # Find the processed file
                    base_name = os.path.splitext(os.path.basename(input_file))[0]
                    processed_file = os.path.join(output_dir, f"{base_name}_processed.csv")
                    if os.path.exists(processed_file):
                        processed_files.append(processed_file)

            # Merge results if requested
            merge_stats = None
            if merge_results and processed_files:
                merge_stats = self._merge_all_results(processed_files, output_dir)

            # Generate overall statistics
            overall_stats = self._calculate_overall_stats(individual_results)

            results = {
                'total_files': len(input_files),
                'successful_files': len([r for r in individual_results if r['status'] == 'completed']),
                'failed_files': len([r for r in individual_results if r['status'] == 'failed']),
                'individual_results': individual_results,
                'overall_stats': overall_stats,
                'merge_stats': merge_stats,
                'output_directory': output_dir
            }

            self.logger.info(f"Batch processing completed: {results['successful_files']}/{results['total_files']} files successful")
            return results

        except Exception as e:
            self.logger.error(f"Error in batch processing: {str(e)}")
            self.logger.error(traceback.format_exc())
            raise

    def process_single_url(self,
                           url: str,
                           output_file: Optional[str] = None) -> Dict[str, Any]:
        """
        Process a single URL for contact extraction.

        Args:
            url: URL to process
            output_file: Optional output file path

        Returns:
            Dictionary with extraction results
        """
        try:
            self.logger.info(f"🎯 Processing: {url}")

            # Create temporary CSV with single URL
            import tempfile
            import pandas as pd

            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
                temp_df = pd.DataFrame({'url': [url]})
                temp_df.to_csv(temp_file.name, index=False)
                temp_csv_path = temp_file.name

            try:
                # Process the temporary CSV
                if output_file:
                    output_dir = os.path.dirname(output_file)
                else:
                    output_dir = tempfile.mkdtemp()

                result = self.process_single_csv(temp_csv_path, output_dir)

                # If specific output file requested, copy result
                if output_file and result['status'] == 'completed':
                    processed_file = os.path.join(output_dir,
                                                os.path.splitext(os.path.basename(temp_csv_path))[0] + '_processed.csv')
                    if os.path.exists(processed_file):
                        import shutil
                        shutil.copy2(processed_file, output_file)
                        result['output_file'] = output_file

                return result

            finally:
                # Clean up temporary file
                if os.path.exists(temp_csv_path):
                    os.unlink(temp_csv_path)

        except Exception as e:
            self.logger.error(f"Error processing single URL {url}: {str(e)}")
            return {
                'url': url,
                'status': 'failed',
                'error': str(e)
            }

    def _post_process_results(self,
                            processed_file: str,
                            output_dir: str,
                            base_name: str) -> Dict[str, Any]:
        """
        Perform post-processing on results.

        Args:
            processed_file: Path to processed CSV file
            output_dir: Output directory
            base_name: Base name for output files

        Returns:
            Dictionary with post-processing results
        """
        results = {}

        try:
            # Deduplicate if enabled (in-place: no extra CSV created)
            if self.config.get('deduplicate', True):
                dedup_stats = self.post_processor.deduplicate_contacts(
                    processed_file, processed_file,
                    dedup_columns=self.config.get('dedup_columns', None)
                )
                results['deduplication'] = dedup_stats

            # Create wide-form output if requested
            if self.config.get('output_format') == 'wide':
                wide_file = os.path.join(output_dir, f"{base_name}_wide_form.csv")
                wide_stats = self.post_processor.create_wide_form_output(
                    processed_file,
                    wide_file,
                    max_contacts_per_type=self.config.get('max_contacts_per_type', 10)
                )
                results['wide_form'] = wide_stats
                results['final_output_file'] = wide_file
            else:
                results['final_output_file'] = processed_file

            # Generate summary report if enabled
            if self.config.get('generate_report', True):
                report_file = os.path.join(output_dir, f"{base_name}_report.txt")
                report_stats = self.post_processor.generate_summary_report(
                    processed_file, report_file
                )
                results['report'] = report_stats
                results['report_file'] = report_file

            return results

        except Exception as e:
            self.logger.error(f"Error in post-processing: {str(e)}")
            results['error'] = str(e)
            return results

    def _merge_all_results(self,
                          processed_files: List[str],
                          output_dir: str) -> Dict[str, Any]:
        """
        Merge all processed files into a single output.

        Args:
            processed_files: List of processed CSV files
            output_dir: Output directory

        Returns:
            Dictionary with merge statistics
        """
        try:
            merged_file = os.path.join(output_dir, "merged_results.csv")
            merge_stats = self.post_processor.merge_csv_files(
                processed_files, merged_file
            )

            # Create wide-form of merged results
            if self.config.get('output_format') == 'wide':
                wide_merged_file = os.path.join(output_dir, "merged_results_wide_form.csv")
                wide_stats = self.post_processor.create_wide_form_output(
                    merged_file, wide_merged_file,
                    max_contacts_per_type=self.config.get('max_contacts_per_type', 10)
                )
                merge_stats['wide_form'] = wide_stats
                merge_stats['final_merged_file'] = wide_merged_file
            else:
                merge_stats['final_merged_file'] = merged_file

            # Generate merged report
            if self.config.get('generate_report', True):
                merged_report_file = os.path.join(output_dir, "merged_results_report.txt")
                report_stats = self.post_processor.generate_summary_report(
                    merged_file, merged_report_file
                )
                merge_stats['report'] = report_stats

            return merge_stats

        except Exception as e:
            self.logger.error(f"Error merging results: {str(e)}")
            return {'error': str(e)}

    def _calculate_overall_stats(self, individual_results: List[Dict]) -> Dict[str, Any]:
        """
        Calculate overall statistics from individual results.

        Args:
            individual_results: List of individual processing results

        Returns:
            Dictionary with overall statistics
        """
        successful_results = [r for r in individual_results if r['status'] == 'completed']

        if not successful_results:
            return {'error': 'No successful processing results'}

        total_urls = sum(r['processing_stats']['total_urls'] for r in successful_results)
        total_successful = sum(r['processing_stats']['successful'] for r in successful_results)
        total_emails = sum(r['processing_stats']['total_emails'] for r in successful_results)
        total_validated_emails = sum(r['processing_stats']['total_validated_emails'] for r in successful_results)
        total_phones = sum(r['processing_stats']['total_phones'] for r in successful_results)
        total_whatsapp = sum(r['processing_stats']['total_whatsapp'] for r in successful_results)
        overall_processing_per_menit = (
            sum(r['processing_stats'].get('processing_per_menit', 0) for r in successful_results) / len(successful_results)
        ) if successful_results else 0

        return {
            'total_files_processed': len(successful_results),
            'total_urls_processed': total_urls,
            'total_successful_scrapes': total_successful,
            'overall_success_rate': (total_successful / total_urls * 100) if total_urls > 0 else 0,
            'total_emails_found': total_emails,
            'total_validated_emails_found': total_validated_emails,
            'total_phones_found': total_phones,
            'total_whatsapp_found': total_whatsapp,
            'email_validation_rate': (total_validated_emails / total_emails * 100) if total_emails > 0 else 0,
            'overall_processing_per_menit': overall_processing_per_menit
        }


def create_config_from_args(args) -> Dict[str, Any]:
    """Create configuration dictionary from command line arguments."""
    # Support positive flags to enable features, with minimal defaults
    generate_report = getattr(args, 'report', False) or (not args.no_report)
    deduplicate = getattr(args, 'dedup', False) or (not args.no_dedup)

    # Guarantee batch_size > max_workers regardless of user input/default
    max_workers = args.workers
    requested_batch = args.batch_size
    # Ensure strictly greater than workers; fallback to requested_batch if already larger
    auto_batch_size = max(int(requested_batch), int(max_workers) + 1)

    config = {
        'max_workers': max_workers,
        'timeout': args.timeout,
        'cf_wait_timeout': getattr(args, 'cf_wait_timeout', 60),
        'skip_on_challenge': getattr(args, 'skip_on_challenge', False),
        'batch_size': auto_batch_size,
        'chunk_size': args.chunk_size,
        'max_contacts_per_type': args.max_contacts,
        'output_format': args.output_format,
        'generate_report': generate_report,
        'deduplicate': deduplicate,
        'dedup_columns': getattr(args, 'dedup_by', None),  # Custom dedup columns
        'log_level': args.log_level,
        # Safe light-load default: block_images ON, disable_resources OFF; --no-light-load disables both
        'block_images': False if getattr(args, 'no_light_load', False) else True,
        'disable_resources': True if getattr(args, 'no_light_load', False) else False,
        # Network idle control for Cloudflare wait page/long-polling sites
        'network_idle': False if getattr(args, 'no_network_idle', False) else True,
        # Proxy configuration
        'proxy_file': getattr(args, 'proxy_file', 'proxy.txt'),
        # Budget configuration for time-based retry management
        'normal_budget': getattr(args, 'normal_budget', 60),
        'challenge_budget': getattr(args, 'challenge_budget', 120),
        'dead_site_budget': getattr(args, 'dead_site_budget', 20),
        'min_retry_threshold': getattr(args, 'min_retry_threshold', 5)
    }
    return config


def main():
    """Main entry point for the Email Scraper & Validator."""
    parser = argparse.ArgumentParser(
        description='Email Scraper & Validator - Extract and validate contact information from websites',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process single CSV file
  python main.py single input.csv --output-dir results/

  # Process multiple CSV files
  python main.py batch file1.csv file2.csv --output-dir results/ --merge

  # Process single URL
  python main.py url https://example.com --output results/example.csv

  # Custom configuration
  python main.py single input.csv --workers 20 --timeout 60 --max-contacts 15
        """
    )

    # Subcommands
    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Single CSV processing
    single_parser = subparsers.add_parser('single', help='Process single CSV file')
    single_parser.add_argument('input_file', help='Input CSV file path')
    single_parser.add_argument('--output-dir', help='Output directory (default: same as input)')
    single_parser.add_argument('--limit', type=int, metavar='N', help='Limit processing to first N rows (for testing). Example: --limit 10')

    # Batch CSV processing
    batch_parser = subparsers.add_parser('batch', help='Process multiple CSV files')
    batch_parser.add_argument('input_files', nargs='+', help='Input CSV file paths')
    batch_parser.add_argument('--output-dir', required=True, help='Output directory')
    batch_parser.add_argument('--limit', type=int, metavar='N', help='Limit processing to first N rows per file (for testing). Example: --limit 10')
    batch_parser.add_argument('--merge', action='store_true', help='Merge all results into single file')

    # Single URL processing
    url_parser = subparsers.add_parser('url', help='Process single URL')
    url_parser.add_argument('url', help='URL to process')
    url_parser.add_argument('--output', help='Output file path')

    # Common arguments
    for p in [single_parser, batch_parser, url_parser]:
        p.add_argument('--workers', type=int, default=3, help='Number of worker threads (default: 3)')
        p.add_argument('--timeout', type=int, default=150, help='Request timeout in seconds (default: 150)')
        p.add_argument('--batch-size', type=int, default=100, help='Batch size for processing (default: 100; auto-adjusted to be > workers)')
        p.add_argument('--chunk-size', type=int, default=0, help='Chunked CSV read size (rows per chunk). 0 disables chunking (default).')
        p.add_argument('--max-contacts', type=int, default=10, help='Max contacts per type in wide format (default: 10)')
        p.add_argument('--output-format', choices=['wide', 'long'], default='long', help='Output format (default: long)')
        # Light-load is default; add override to disable
        p.add_argument('--light-load', action='store_true', help='[Default ON] Enable light-load: block images and apply allowlist routing (keeps Cloudflare-critical JS/CSS)')
        p.add_argument('--no-light-load', action='store_true', help='Disable light-load (load all resources; no allowlist routing)')
        p.add_argument('--disable-resources', action='store_true', help='Disable non-essential resources (fonts, video, media) to save bandwidth')
        # Cloudflare control
        p.add_argument('--cf-wait-timeout', type=int, default=90, help='Per-URL Cloudflare wait timeout in seconds (default: 90)')
        p.add_argument('--skip-on-challenge', action='store_true', help='Skip immediately when Cloudflare challenge is detected (no retries)')
        # Minimal defaults: skip report and dedup unless explicitly enabled
        p.add_argument('--report', action='store_true', help='Enable summary report output')
        p.add_argument('--dedup', action='store_true', help='Enable deduplication output (redundant when default ON)')
        p.add_argument('--no-report', action='store_true', default=True, help='Skip generating summary report (default: skip)')
        p.add_argument('--no-dedup', action='store_true', default=False, help='Skip deduplication (default: dedupe ON)')
        p.add_argument('--dedup-by', nargs='+', metavar='COLUMN', help='Columns to use for deduplication (e.g., --dedup-by name address). Default: smart detection (name+address if available, otherwise url)')
        p.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default='INFO', help='Log level (default: INFO)')
        # Proxy configuration
        p.add_argument('--proxy-file', default='proxy.txt', help='Path to proxy file for automatic proxy detection (default: proxy.txt)')

        # Budget configuration for time-based retry management
        p.add_argument('--normal-budget', type=int, default=90, help='Budget for normal sites in seconds (default: 90)')
        p.add_argument('--challenge-budget', type=int, default=180, help='Budget for Cloudflare/challenge sites in seconds (default: 180)')
        p.add_argument('--dead-site-budget', type=int, default=20, help='Budget for dead sites in seconds (default: 20)')
        p.add_argument('--min-retry-threshold', type=int, default=5, help='Minimum remaining budget to attempt retry in seconds (default: 5)')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    try:
        # Create configuration
        config = create_config_from_args(args)

        # Initialize scraper
        scraper = EmailScraperValidator(config)

        # Execute command
        if args.command == 'single':
            result = scraper.process_single_csv(
                input_file=args.input_file,
                output_dir=args.output_dir,
                limit_rows=getattr(args, 'limit', None)
            )

            if result['status'] == 'completed':
                stats = result['processing_stats']
                print(f"Processing completed successfully!")
                print(f"Rate : {stats['success_rate']:.1f}% | {stats.get('processing_per_menit', 0):.2f} URL/min")
                print(f"Email : {stats['total_emails']} | Valid Email : {stats['total_validated_emails']} | Phone : {stats['total_phones']} | WA : {stats['total_whatsapp']}")
                if 'final_output_file' in result['post_processing']:
                    print(f"Result file: {result['post_processing']['final_output_file']}")

                # Format and display completion time
                if 'total_duration_seconds' in stats:
                    duration = int(stats['total_duration_seconds'])
                    hours = duration // 3600
                    minutes = (duration % 3600) // 60
                    seconds = duration % 60

                    if hours > 0:
                        time_str = f"{hours}h {minutes}m"
                    elif minutes > 0:
                        time_str = f"{minutes}m {seconds}s"
                    else:
                        time_str = f"{seconds}s"

                    print(f"Completetion time : {time_str}")
            else:
                print(f"Processing failed: {result.get('error', 'Unknown error')}")
                sys.exit(1)

        elif args.command == 'batch':
            result = scraper.process_multiple_csv(
                input_files=args.input_files,
                output_dir=args.output_dir,
                merge_results=args.merge,
                limit_rows=getattr(args, 'limit', None)
            )

            print(f"Batch processing completed!")
            print(f"Files processed: {result['successful_files']}/{result['total_files']}")
            if 'overall_stats' in result:
                stats = result['overall_stats']
                print(f"Overall success rate: {stats['overall_success_rate']:.1f}%")
                print(f"Processing per menit: {stats.get('overall_processing_per_menit', 0):.2f} URL/min")
                print(f"Total emails found: {stats['total_emails_found']}")
                print(f"Validated emails: {stats['total_validated_emails_found']}")
                print(f"Phone numbers: {stats['total_phones_found']}")
                print(f"WhatsApp contacts: {stats['total_whatsapp_found']}")
            print(f"Output directory: {result['output_directory']}")
        elif args.command == 'url':
            result = scraper.process_single_url(
                url=args.url,
                output_file=args.output
            )

            if result['status'] == 'completed':
                print(f"URL processing completed successfully!")
                print(f"URL: {args.url}")
                if 'output_file' in result:
                    print(f"Output file: {result['output_file']}")
            else:
                print(f"URL processing failed: {result.get('error', 'Unknown error')}")
                sys.exit(1)

    except KeyboardInterrupt:
        print("\n❌ Processing forcibly interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        logging.error(f"Fatal error: {str(e)}")
        logging.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()