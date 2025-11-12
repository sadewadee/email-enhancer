#!/usr/bin/env python3
"""Debug logger handlers"""

import sys
import logging

# Setup like main.py
import time
from datetime import datetime

class NoMillisecondsFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = time.strftime(datefmt, ct)
        else:
            s = time.strftime("%Y-%m-%d %H:%M:%S", ct)
        return s

# Remove all existing handlers first
root_logger = logging.getLogger()
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

# Create formatter
formatter = NoMillisecondsFormatter('%(asctime)s %(levelname)s - %(message)s')

# File handler
file_handler = logging.FileHandler(f'logs/test_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
file_handler.setFormatter(formatter)

# Console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)

# Set handlers
root_logger.setLevel(logging.INFO)
root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

print(f"\n=== Root Logger Handlers ({len(root_logger.handlers)}): ===")
for i, h in enumerate(root_logger.handlers):
    print(f"  Handler {i}: {type(h).__name__}")

# Now import modules
print("\n=== Importing csv_processor... ===")
from csv_processor import CSVProcessor

print(f"\n=== After Import - Root Logger Handlers ({len(root_logger.handlers)}): ===")
for i, h in enumerate(root_logger.handlers):
    print(f"  Handler {i}: {type(h).__name__}")

# Test logging
logger = logging.getLogger()
print("\n=== Test Logging: ===")
logger.info("Test message 1")
logger.info("Test message 2")
