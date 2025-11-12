#!/usr/bin/env python3
"""Debug script to find where the code is stuck"""

import sys
import logging

# Setup logging first
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)

logger.info("Step 1: Starting debug...")

try:
    logger.info("Step 2: Importing csv_processor...")
    from csv_processor import CSVProcessor
    logger.info("✅ csv_processor imported successfully")

    logger.info("Step 3: Creating CSVProcessor instance...")
    processor = CSVProcessor(
        max_workers=2,
        timeout=30,
        block_images=True,
        disable_resources=False,
        network_idle=True,
        cf_wait_timeout=60,
        skip_on_challenge=False,
        proxy_file='proxy.txt'
    )
    logger.info("✅ CSVProcessor created successfully")

    logger.info("Step 4: All initialization complete!")

except Exception as e:
    logger.error(f"❌ Error during initialization: {e}")
    import traceback
    traceback.print_exc()
