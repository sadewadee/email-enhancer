#!/bin/bash

# Email Scraper & Validator - Installer Script
# Mengatasi error "externally managed environment" dengan virtual environment

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Main installation function
main() {
    echo "=================================================="
    echo "  Email Scraper & Validator - Installer"
    echo "=================================================="
    echo ""

    # Check if Python 3 is installed
    if ! command_exists python3; then
        print_error "Python 3 tidak ditemukan. Silakan install Python 3 terlebih dahulu."
        echo "Untuk macOS: brew install python3"
        echo "Untuk Ubuntu/Debian: sudo apt install python3 python3-pip python3-venv"
        exit 1
    fi

    PYTHON_VERSION=$(python3 --version 2>&1 | cut -d' ' -f2 | cut -d'.' -f1,2)
    print_status "Python version: $PYTHON_VERSION"

    # Check minimum Python version (3.8+)
    if ! python3 -c "import sys; exit(0 if sys.version_info >= (3, 8) else 1)" 2>/dev/null; then
        print_error "Python 3.8+ diperlukan. Version saat ini: $PYTHON_VERSION"
        exit 1
    fi

    # Check if pip is available
    if ! command_exists pip3 && ! python3 -m pip --version >/dev/null 2>&1; then
        print_error "pip tidak ditemukan. Silakan install pip terlebih dahulu."
        exit 1
    fi

    # Create virtual environment
    VENV_DIR="venv"
    
    if [ -d "$VENV_DIR" ]; then
        print_warning "Virtual environment sudah ada. Menghapus yang lama..."
        rm -rf "$VENV_DIR"
    fi

    print_status "Membuat virtual environment..."
    python3 -m venv "$VENV_DIR"

    if [ ! -d "$VENV_DIR" ]; then
        print_error "Gagal membuat virtual environment"
        exit 1
    fi

    print_success "Virtual environment berhasil dibuat"

    # Activate virtual environment
    print_status "Mengaktifkan virtual environment..."
    source "$VENV_DIR/bin/activate"

    # Upgrade pip in virtual environment
    print_status "Mengupgrade pip..."
    python -m pip install --upgrade pip

    # Install requirements
    if [ -f "requirements.txt" ]; then
        print_status "Installing dependencies dari requirements.txt..."
        pip install -r requirements.txt
        
        if [ $? -eq 0 ]; then
            print_success "Semua dependencies berhasil diinstall"
        else
            print_error "Gagal menginstall beberapa dependencies"
            exit 1
        fi
    else
        print_error "File requirements.txt tidak ditemukan"
        exit 1
    fi

    # Create activation script
    print_status "Membuat script aktivasi..."
    cat > activate_env.sh << 'EOF'
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
EOF

    chmod +x activate_env.sh

    # Create run script
    print_status "Membuat script runner..."
    cat > run.sh << 'EOF'
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
EOF

    chmod +x run.sh

    # Create sample input file if it doesn't exist
    if [ ! -f "sample-input.csv" ]; then
        print_status "Membuat sample input file..."
        cat > sample-input.csv << 'EOF'
url,company_name,industry
https://example.com,Example Corp,Technology
https://google.com,Google,Technology
https://github.com,GitHub,Technology
EOF
        print_success "Sample input file dibuat: sample-input.csv"
    fi

    # Test installation
    print_status "Testing instalasi..."
    python -c "
import scrapling
import email_validator
import validate_email
from email_validation import EmailValidator
import pandas
import tqdm
import bs4
import phonenumbers
print('✅ Semua dependencies berhasil diimport')
"

    if [ $? -eq 0 ]; then
        print_success "Testing berhasil - semua dependencies berfungsi dengan baik"
    else
        print_error "Testing gagal - ada masalah dengan dependencies"
        exit 1
    fi

    # Deactivate virtual environment
    deactivate

    echo ""
    echo "=================================================="
    print_success "INSTALASI BERHASIL!"
    echo "=================================================="
    echo ""
    echo "📋 Cara penggunaan:"
    echo ""
    echo "1️⃣  Aktifkan virtual environment:"
    echo "   ./activate_env.sh"
    echo ""
    echo "2️⃣  Atau langsung jalankan program:"
    echo "   ./run.sh single sample-input.csv"
    echo "   ./run.sh batch file1.csv file2.csv --output-dir results/"
    echo "   ./run.sh url https://example.com"
    echo ""
    echo "📁 File yang dibuat:"
    echo "   - venv/                 (virtual environment)"
    echo "   - activate_env.sh       (script aktivasi)"
    echo "   - run.sh               (script runner)"
    echo "   - sample-input.csv     (contoh input)"
    echo ""
    echo "🔧 Untuk uninstall:"
    echo "   rm -rf venv activate_env.sh run.sh"
    echo ""
    print_success "Selamat menggunakan Email Scraper & Validator!"
}

# Run main function
main "$@"