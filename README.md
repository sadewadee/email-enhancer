# Email Scraper & Validator

A comprehensive tool for extracting and validating contact information (emails, phone numbers, WhatsApp) from websites with multi-source approach and bot protection bypass (Cloudflare).

## 🚀 Key Features

- **Multi-Source Contact Extraction**: Header, footer, contact pages, structured data (JSON-LD, microdata/RDFa), and general content
- **Advanced Email Detection**:
  - Mailto links and HTML attributes
  - Cloudflare-protected emails (`data-cfemail`, CDN-CGI decoding)
  - Deobfuscated emails (`[at]`, `[dot]` patterns)
  - Multi-layer priority: header → footer → structured data → contact pages → general content
- **SMTP-Level Email Validation**: Without sending emails, includes validation reason in `validated_emails` column
- **Bot Protection Bypass**: Cloudflare bypass via stealth browser engine with headless JavaScript rendering
- **Social Media Auto-Skip**: Detects and skips social media URLs (`scraping_status = skipped_social`)
- **Smart Resource Optimization**: Light-load mode by default (blocks images, keeps critical JS/CSS)
- **Proxy Support**: Automatic proxy rotation from file with failure handling
- **Parallel Processing**: Configurable workers with batch processing and structured logging
- **Minimal Output by Default**: Single `*_processed.csv` file (long-form)
- **Optional Outputs**: Wide-form format and summary reports via flags

## 📋 System Requirements

- Python 3.11 (recommended) or 3.8+
- Internet connection
- 4GB RAM (8GB+ recommended for large datasets)
- For Playwright browser: `python -m playwright install`

## ⚡ Quick Setup

### Option A — Automated
```bash
./installer.sh
./run.sh single sample-input.csv --output-dir results/
```

### Option B — Manual
```bash
python3 -m venv venv
source venv/bin/activate  # macOS/Linux
# On Windows: venv\Scripts\activate
pip install -r requirements.txt
python -m playwright install  # if needed
python main.py single sample-input.csv --output-dir results/
```

## 📁 Input File Structure

Requires at minimum a `url` column. Other columns will be preserved in output.

```csv
url,name,street,city,country_code
https://example.com,Example Corp,Main St,New York,US
https://another-site.com,Another Co,Central Rd,London,GB
```

## 📖 Usage

### Single CSV File
```bash
python main.py single sample-input.csv --output-dir datas --workers 10
```

**Common Options:**
- `--output-dir`: Output directory (default: same as input directory)
- `--url-column`: URL column name (default: `url`)
- `--workers`: Number of parallel workers (default: 10)
- `--timeout`: Request timeout in seconds (default: 30)
- `--log-level`: Logging level - DEBUG, INFO, WARNING, ERROR (default: INFO)

### Batch Processing (Multiple CSV Files)
```bash
python main.py batch file1.csv file2.csv --output-dir datas --merge
```

**Batch Options:**
- `--merge`: Combine all results into `merged_results.csv`

### Single URL
```bash
python main.py url https://example.com --output datas/example_processed.csv
```

## ⚙️ Configuration Options

### Processing Options
- `--workers`: Number of parallel workers (default: 10)
- `--timeout`: Request timeout in seconds (default: 30)
- `--batch-size`: Batch processing size (default: 100; auto-adjusted to be > workers)
- `--chunk-size`: Chunked CSV read size in rows (default: 0 = disabled)

### Output Options
- `--output-format`: `long` (default) or `wide`
  - **long**: One email per row (recommended for large datasets)
  - **wide**: Multiple emails in columns (up to `--max-contacts`)
- `--max-contacts`: Maximum contacts per type in wide format (default: 10)
- `--report` / `--no-report`: Generate summary report (default: OFF)
- `--dedup` / `--no-dedup`: In-place deduplication (default: ON)

### Performance & Resource Options
- `--light-load`: **[Default ON]** Enable light-load mode
  - Blocks images
  - Keeps Cloudflare-critical JS/CSS via allowlist routing
- `--no-light-load`: Disable light-load (loads all resources; no allowlist)
- `--disable-resources`: Disable non-essential resources (fonts, video, media)
- `--no-network-idle`: Don't wait for network idle (useful for wait pages/long-polling)

### Cloudflare Options
- `--cf-wait-timeout`: Per-URL Cloudflare wait timeout in seconds (default: 60)
- `--skip-on-challenge`: Skip immediately when Cloudflare challenge detected (no retries)

### Proxy Options
- `--proxy-file`: Path to proxy file for automatic rotation (default: `proxy.txt`)

**Light-Load Mode Details:**
- **Default "safe light-load"**: `block_images=ON`, `disable_resources=OFF`, `network_idle=ON`
- **With `--no-light-load`**: `block_images=OFF`, `disable_resources=ON` (blocks non-essential resources globally)

**Cloudflare Bypass References:**
- See documentation: `CLOUDFLARE_BYPASS_SOLUTION.md` and `FINAL_CLOUDFLARE_SOLUTION.md`

## 📦 Output Files

**Default Output:**
- `*_processed.csv` (long-form with all columns)

**Optional Outputs:**
- Wide format: Add `--output-format wide` → generates `*_wide_form.csv`
- Report: Add `--report` → generates `*_report.txt`

**Batch with Merge:**
- `merged_results.csv`
- If `--output-format wide`: `merged_results_wide_form.csv`
- If `--report`: `merged_results_report.txt`

**Deduplication:**
- Performed in-place on `*_processed.csv` (no separate `*_deduplicated.csv` file)

## 🧾 Output Columns (Processed CSV)

**Preserved Columns:**
- All original columns from input CSV

**New Contact Columns:**
- `emails`: List of emails found (semicolon-separated)
- `phones`: List of phone numbers (semicolon-separated)
- `whatsapp`: List of WhatsApp contacts (semicolon-separated)
- `validated_emails`: Emails with validation reasons, e.g., `name@domain.com (mx_ok)`, `info@x.y (smtp_timeout)`

**Metadata Columns:**
- `scraping_status`: `success` | `no_contacts_found` | `skipped_social` | `error`
- `scraping_error`: Error message (if any)
- `processing_time`: Processing duration per URL (seconds)
- `pages_scraped`: Number of pages processed
- `emails_found`: Count of emails found
- `phones_found`: Count of phone numbers found
- `whatsapp_found`: Count of WhatsApp contacts found
- `validated_emails_count`: Count of validated emails

## 🔍 How It Works

### Email Extraction Process

The scraper uses a **multi-layer approach** to extract emails:

#### 1. **Priority-Based Extraction**
- **Header** (highest priority) → `<header>`, `.header`, `<nav>`, `.navbar`
- **Footer** → `<footer>`, `.footer`
- **Structured Data** → JSON-LD, Schema.org microdata
- **Contact Pages** → Auto-detect and scrape `/contact`, `/kontak`, etc.
- **General Content** (fallback)

#### 2. **Detection Methods**
- **Mailto Links**: `<a href="mailto:...">` tags
- **Cloudflare-Protected**:
  - `data-cfemail` attribute decoding
  - `/cdn-cgi/l/email-protection#` links with XOR cipher
- **HTML Attributes**: `data-email`, `data-mail`, `onclick="mailto:..."`
- **Deobfuscation**:
  - `name [at] domain [dot] com` → `name@domain.com`
  - `name (at) domain (dot) com` → `name@domain.com`
- **Text Pattern Matching**: Standard email regex with boundary detection

#### 3. **Validation & Filtering**
- Filters placeholder domains (`example.com`, `mysite.com`)
- Removes implausible local-parts (containing 'email', 'mailto')
- Deduplicates suspicious variants
- TLD validation using Public Suffix List
- SMTP-level validation with detailed reasons

### Contact Page Discovery
- Automatically finds and scrapes contact pages (`/contact`, `/contact-us`, `/kontakt`, etc.)
- Combines results from main page and all discovered contact pages

### Social Media Detection
- Popular social media URLs are automatically skipped
- Status marked as `skipped_social`

## 🧪 Example Commands

```bash
# Minimal run (single processed file output)
python main.py single sample-input.csv --output-dir datas

# With wide-form and report
python main.py single sample-input.csv --output-dir datas --output-format wide --report

# Batch merge with dedup OFF and report ON
python main.py batch file1.csv file2.csv --output-dir datas --merge --no-dedup --report

# Single URL to specific file
python main.py url https://example.com --output datas/example_processed.csv

# High-performance run with 20 workers and DEBUG logging
python main.py single large-input.csv --workers 20 --timeout 60 --log-level DEBUG

# Skip Cloudflare challenges immediately (fast mode)
python main.py single input.csv --skip-on-challenge --cf-wait-timeout 30

# Full resources mode (no light-load)
python main.py single input.csv --no-light-load
```

## 🌐 Proxy Support

Use `--proxy-file` to provide a list of proxies (default: `proxy.txt`).

**Proxy File Format:**
```
http://user:pass@host:port
socks5://host:port
https://host:port
```

**Features:**
- Automatic proxy rotation
- Failure detection and retry with different proxy
- Playwright-compatible format conversion

See guide: `PROXY_SETUP.md`

## 📚 Related Documentation

- `Technical Documentation_ Email Scraper & Validator.md` - Detailed technical documentation
- `CLOUDFLARE_BYPASS_SOLUTION.md` - Cloudflare bypass implementation
- `FINAL_CLOUDFLARE_SOLUTION.md` - Latest Cloudflare solution
- `QUICK_START_FIX.md` - Quick troubleshooting guide
- `DEVELOPMENT.md` - Development guide
- `DEPLOYMENT_CHECKLIST.md` - Production deployment checklist
- `PROXY_SETUP.md` - Proxy configuration guide

## 🛠️ Logging & Troubleshooting

### Logging
- Logs saved in `logs/` directory with timestamp in filename
- Console output: INFO level (minimal)
- File output: DEBUG level (verbose with URL, validation details)
- Format: `logs/email_scraper_YYYYMMDD_HHMMSS.log`

### Common Issues

**Module Import Errors:**
```bash
pip install -r requirements.txt
```

**Browser/Playwright Errors:**
```bash
python -m playwright install
```

**Timeout/Slow Connection:**
- Increase `--timeout` (e.g., `--timeout 60`)
- Decrease `--workers` (e.g., `--workers 5`)
- Use `--no-network-idle` if needed
- Use `--skip-on-challenge` to skip slow Cloudflare pages

**High Memory Usage:**
- Decrease `--workers`
- Enable `--chunk-size` for large CSV files (e.g., `--chunk-size 1000`)
- Use `--light-load` (default) to reduce resource consumption

**Cloudflare Challenges:**
- Default timeout: 60s per URL
- Adjust with `--cf-wait-timeout`
- Use `--skip-on-challenge` to skip immediately
- Check `logs/` for detailed error messages

## 🔧 Advanced Configuration

### Performance Tuning
```bash
# Fast mode (may miss some content)
python main.py single input.csv --workers 20 --timeout 20 --skip-on-challenge

# Thorough mode (slower but comprehensive)
python main.py single input.csv --workers 5 --timeout 60 --no-light-load

# Balanced mode (recommended)
python main.py single input.csv --workers 10 --timeout 30
```

### Memory-Constrained Environments
```bash
# Process large CSV files in chunks
python main.py single large-file.csv --chunk-size 500 --workers 5
```

### Debugging
```bash
# Enable DEBUG logging to see detailed extraction process
python main.py single input.csv --log-level DEBUG

# Check log file for detailed information
tail -f logs/email_scraper_*.log
```

## 📝 Best Practices & Ethics

- **Respect `robots.txt`** and website terms of service
- **Apply rate limiting** as needed (adjust `--workers` and `--timeout`)
- **No external data transmission**: All processing runs locally
- **Use proxies responsibly**: Don't overload proxy servers
- **Check legal requirements**: Ensure compliance with GDPR, CCPA, and local regulations
- **Test on small datasets first**: Verify configuration before large-scale scraping

## 🔒 Privacy & Security

- All data processing occurs locally on your machine
- No telemetry or external API calls (except for target websites)
- Email validation uses local SMTP checks only
- Proxy credentials are handled securely in memory only

## 📈 Performance Metrics

**Typical Performance:**
- **Small sites** (1-5 pages): ~5-15 seconds per URL
- **Medium sites** (5-20 pages): ~15-45 seconds per URL
- **Large sites** (20+ pages): ~45-120 seconds per URL
- **With Cloudflare**: Add 10-60 seconds for challenge solving

**Throughput:**
- 10 workers: ~20-40 URLs per minute (depends on site complexity)
- 20 workers: ~40-80 URLs per minute (requires 8GB+ RAM)

## 🤝 Contributing

This is a production-ready tool. For bug reports or feature requests, please check the following:
1. Review `DEVELOPMENT.md` for architecture overview
2. Check existing logs in `logs/` directory
3. Test with `--log-level DEBUG` for detailed output

## 📄 License

This tool is provided as-is for legitimate business use cases including lead generation, contact discovery, and business intelligence.

---

**Email Scraper & Validator** — Accurate, flexible, and easy-to-use contact extraction and validation.
