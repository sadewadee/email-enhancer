#!/bin/bash
# Script untuk menjalankan Email Scraper & Validator

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "❌ Virtual environment tidak ditemukan. Jalankan installer.sh terlebih dahulu."
    exit 1
fi

# Activate virtual environment
source venv/bin/activate

# Run the main script with all arguments
python main.py "$@"
