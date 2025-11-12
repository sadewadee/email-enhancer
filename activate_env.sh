#!/bin/bash
# Script untuk mengaktifkan virtual environment

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ -d "venv" ]; then
    source venv/bin/activate
    echo "✅ Virtual environment diaktifkan"
    echo "📁 Working directory: $(pwd)"
    echo "🐍 Python: $(which python)"
    echo ""
    echo "Untuk menjalankan program:"
    echo "  python main.py single input.csv"
    echo "  python main.py batch file1.csv file2.csv --output-dir results/"
    echo "  python main.py url https://example.com"
    echo ""
    echo "Untuk keluar dari virtual environment, ketik: deactivate"
else
    echo "❌ Virtual environment tidak ditemukan. Jalankan installer.sh terlebih dahulu."
fi
