#!/usr/bin/env python3
"""
Debug script untuk menganalisis Cloudflare bypass issue dengan light-load mode.
Tracks parameter flow, network requests, dan timing issues.
"""

import logging
import sys
import time
from web_scraper import WebScraper
from typing import Dict, Any

# Setup verbose logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler('debug_cloudflare.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


class CloudflareDebugger:
    """Debug helper untuk Cloudflare bypass issues."""

    def __init__(self):
        self.test_urls = [
            'https://example.com',
            'https://cloudflare.com',
        ]

    def test_scraper_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Test scraper dengan specific configuration.

        Args:
            config: Configuration dictionary

        Returns:
            Dictionary with test results
        """
        results = {
            'config': config,
            'scrapes': [],
            'analysis': {}
        }

        logger.info(f"Testing with config: {config}")
        logger.info("=" * 80)

        # Create scraper dengan config
        scraper = WebScraper(
            timeout=config.get('timeout', 30),
            network_idle=config.get('network_idle', True),
            block_images=config.get('block_images', False),
            disable_resources=config.get('disable_resources', False),
            solve_cloudflare=config.get('solve_cloudflare', True)
        )

        logger.debug(f"WebScraper initialized:")
        logger.debug(f"  - timeout: {scraper.timeout}")
        logger.debug(f"  - network_idle: {scraper.network_idle}")
        logger.debug(f"  - block_images: {scraper.block_images}")
        logger.debug(f"  - disable_resources: {scraper.disable_resources}")
        logger.debug(f"  - solve_cloudflare: {scraper.solve_cloudflare}")

        # Test dengan sample URL
        test_url = 'https://example.com'
        logger.info(f"\nTesting URL: {test_url}")
        logger.info("-" * 80)

        start_time = time.time()
        try:
            result = scraper.scrape_url(test_url)
            elapsed = time.time() - start_time

            logger.info(f"Scrape completed in {elapsed:.2f}s")
            logger.info(f"Status: {result.get('status')}")
            logger.info(f"Error: {result.get('error')}")
            logger.info(f"HTML length: {len(result.get('html', ''))}")
            logger.info(f"Final URL: {result.get('final_url')}")

            results['scrapes'].append({
                'url': test_url,
                'elapsed': elapsed,
                'status': result.get('status'),
                'error': result.get('error'),
                'html_length': len(result.get('html', '')),
                'success': result.get('status') == 200
            })

        except Exception as e:
            logger.error(f"Error during scrape: {str(e)}", exc_info=True)
            results['scrapes'].append({
                'url': test_url,
                'error': str(e),
                'success': False
            })

        return results

    def compare_configs(self) -> Dict[str, Any]:
        """
        Compare berbagai konfigurasi untuk identify masalah.

        Returns:
            Comparison results
        """
        configs = [
            {
                'name': 'Current (Light-load: ON, network_idle: ON)',
                'block_images': True,
                'disable_resources': False,
                'network_idle': True,
                'timeout': 120
            },
            {
                'name': 'Light-load OFF, network_idle: ON',
                'block_images': False,
                'disable_resources': False,
                'network_idle': True,
                'timeout': 120
            },
            {
                'name': 'Light-load: ON, network_idle: OFF',
                'block_images': True,
                'disable_resources': False,
                'network_idle': False,
                'timeout': 120
            },
            {
                'name': 'Light-load: ON, disable_resources: ON, network_idle: OFF',
                'block_images': True,
                'disable_resources': True,
                'network_idle': False,
                'timeout': 120
            },
        ]

        comparison_results = {}

        for config in configs:
            logger.info(f"\n\n{'#' * 80}")
            logger.info(f"# Config: {config['name']}")
            logger.info(f"{'#' * 80}\n")

            name = config.pop('name')
            result = self.test_scraper_config(config)
            comparison_results[name] = result

        return comparison_results

    def analyze_parameter_flow(self):
        """Analyze parameter flow dari main.py ke web_scraper.py."""
        logger.info("\n\nPARAMETER FLOW ANALYSIS")
        logger.info("=" * 80)

        logger.info("\n1. main.py default config:")
        logger.info("   block_images: True (default)")
        logger.info("   disable_resources: False (default)")
        logger.info("   network_idle: True (default)")

        logger.info("\n2. main.py -> csv_processor.py:")
        logger.info("   CSVProcessor.__init__(")
        logger.info("     block_images=config.get('block_images', False),")
        logger.info("     disable_resources=config.get('disable_resources', False),")
        logger.info("     network_idle=config.get('network_idle', True)")
        logger.info("   )")

        logger.info("\n3. csv_processor.py -> web_scraper.py:")
        logger.info("   WebScraper(")
        logger.info("     block_images=block_images,")
        logger.info("     disable_resources=disable_resources,")
        logger.info("     network_idle=network_idle")
        logger.info("   )")

        logger.info("\n4. web_scraper.py -> StealthyFetcher.fetch():")
        logger.info("   ⚠️  ISSUE AT LINE 307:")
        logger.info("   disable_resources=False,  # HARDCODED!")
        logger.info("   Expected: disable_resources=self.disable_resources")

        logger.info("\n5. Router allowlist fallback (Line 135):")
        logger.info("   ⚠️  ISSUE:")
        logger.info("   return route.continue_()  # Allows ALL resources!")
        logger.info("   Should: Block by default in light-load mode")

        logger.info("\n6. network_idle behavior:")
        logger.info("   ⚠️  ISSUE:")
        logger.info("   network_idle=True causes StealthyFetcher to wait indefinitely")
        logger.info("   for Cloudflare long-polling to finish (never fully idles)")

    def generate_recommendations(self):
        """Generate fix recommendations based on analysis."""
        logger.info("\n\nRECOMMENDATIONS FOR FIXING CLOUDFLARE ISSUE")
        logger.info("=" * 80)

        logger.info("\nFIX #1: Remove hardcoded disable_resources=False (Line 307)")
        logger.info("-" * 80)
        logger.info("BEFORE:")
        logger.info("  disable_resources=False,")
        logger.info("\nAFTER:")
        logger.info("  disable_resources=self.disable_resources,")
        logger.info("\nIMPACT: Respects light-load configuration, prevents unnecessary resource loading")

        logger.info("\n\nFIX #2: Fix router fallback logic (Line 135)")
        logger.info("-" * 80)
        logger.info("BEFORE:")
        logger.info("  return route.continue_()")
        logger.info("\nAFTER:")
        logger.info("  if self.block_images or self.disable_resources:")
        logger.info("      return route.abort()")
        logger.info("  return route.continue_()")
        logger.info("\nIMPACT: Blocks non-essential resources in light-load mode")

        logger.info("\n\nFIX #3: Disable network_idle for Cloudflare sites (Line 295)")
        logger.info("-" * 80)
        logger.info("BEFORE:")
        logger.info("  network_idle=self.network_idle,")
        logger.info("\nAFTER:")
        logger.info("  network_idle=False if self.solve_cloudflare else self.network_idle,")
        logger.info("\nIMPACT: Prevents indefinite waiting for Cloudflare long-polling")


def main():
    """Main debug execution."""
    debugger = CloudflareDebugger()

    # Analyze parameter flow
    debugger.analyze_parameter_flow()

    # Run tests
    logger.info("\n\n" + "=" * 80)
    logger.info("RUNNING CONFIGURATION TESTS")
    logger.info("=" * 80)

    comparison_results = debugger.compare_configs()

    # Generate recommendations
    debugger.generate_recommendations()

    # Summary
    logger.info("\n\nDEBUG SUMMARY")
    logger.info("=" * 80)
    for config_name, results in comparison_results.items():
        logger.info(f"\nConfig: {config_name}")
        for scrape in results['scrapes']:
            logger.info(f"  URL: {scrape.get('url')}")
            logger.info(f"  Success: {scrape.get('success')}")
            logger.info(f"  Elapsed: {scrape.get('elapsed', 'N/A')}s")
            if scrape.get('error'):
                logger.info(f"  Error: {scrape.get('error')}")

    logger.info("\n\nDetailed log saved to: debug_cloudflare.log")


if __name__ == '__main__':
    main()
