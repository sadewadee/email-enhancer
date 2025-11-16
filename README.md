# Email Scraper & Validator

A comprehensive tool to extract and validate contact information (emails, phone numbers, WhatsApp) from websites at scale. Designed with anti-bot bypass, parallel processing, proxy rotation, and robust post-processing.

- Language: Python 3.9+
- Scraping: Scrapling [all] (Playwright + stealth/Camoufox)
- Parsing: BeautifulSoup4
- Data: pandas, tqdm
- Validation: email-validator, py3-validate-email, validate_email
- Extras: TypeScript/Puppeteer helper for public proxy collection

---

## Contents
- Installation
- Quick Start
- CLI Usage and Options
- Proxies
- Outputs
- Google Sheets Integration
- Debugging & Monitoring
- Performance Tips
- Development Conventions
- Mandatory Progress Tracking (WAJIB)
- Troubleshooting

---

## Installation
Use the provided installer to bootstrap a local virtual environment and dependencies.

```bash
# Make scripts executable (first time only)
chmod +x installer.sh run.sh

# One-time setup
./installer.sh
```

The installer will:
- Create a `venv/` virtual environment
- Install packages from `requirements.txt`
- Validate Python/pip and baseline tooling

If you prefer manual setup:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

## Quick Start

```bash
# Run via wrapper (activates venv and forwards args)
./run.sh single input.csv --output-dir results/

# Or run the entrypoint directly (ensure venv is active)
source venv/bin/activate
python main.py url https://example.com --output results/example.csv
```

Input CSV should contain a URL column. The processor auto-detects common names like `url`, `website`, etc.

---

## CLI Usage and Options
The CLI supports three subcommands: `single`, `batch`, and `url`.

```bash
python main.py single <input.csv> [options]
python main.py batch <file1.csv> <file2.csv> ... --output-dir <dir> [--merge] [options]
python main.py url <https://site> [--output <file>] [options]
```

Common options (apply to all subcommands):
- --workers INT                Number of worker threads (default: 10)
- --timeout INT                Request timeout in seconds (default: 30)
- --batch-size INT             Batch size for processing (default: 100; auto-adjusted to be > workers)
- --chunk-size INT             Chunked CSV read size (rows per chunk). 0 disables chunking (default)
- --max-contacts INT           Max contacts per type in wide format (default: 10)
- --output-format {wide,long}  Output format (default: long)

Light-load and resource controls:
- --light-load                 [Default ON] Enable light-load: block images and apply allowlist routing (keeps CF-critical JS/CSS)
- --no-light-load              Disable light-load (load all resources; no allowlist routing)
- --no-network-idle            Do not wait for network idle; useful for Cloudflare wait pages or long-polling sites
- --disable-resources          Disable non-essential resources (fonts, video, media) to save bandwidth

Cloudflare controls:
- --cf-wait-timeout INT        Per-URL Cloudflare wait timeout in seconds (default: 60)
- --skip-on-challenge          Skip immediately when Cloudflare challenge is detected (no retries)

Output/reporting & logging:
- --report                     Enable summary report output
- --dedup                      Enable deduplication output (redundant when default ON)
- --no-report                  Skip generating summary report (default: skip)
- --no-dedup                   Skip deduplication (default: dedupe ON)
- --dedup-by COLUMN [...]      Columns to use for deduplication (smart defaults if omitted)
- --log-level {DEBUG,INFO,WARNING,ERROR}  Log level (default: INFO)

Proxy config:
- --proxy-file PATH            Path to proxy file for auto detection (default: proxy.txt)

Subcommand-specific options:
- single: --limit N            Process first N rows (for testing)
- batch:  --limit N            Per-file limit; --merge combines results into a single file
- url:    --output PATH        Write single-URL result to a CSV file

Examples:
```bash
# Process a single CSV with custom workers/timeouts
python main.py single input.csv --output-dir results/ --workers 20 --timeout 60

# Batch process multiple CSVs and merge outputs
python main.py batch data/*.csv --output-dir results/ --merge

# Scrape a single URL into a file
python main.py url https://example.com --output results/example.csv
```

---

## Proxies
- Provide proxies in `proxy.txt` (ignored by git) with one per line:
  - username:password@host:port
  - host:port
- Proxies are rotated automatically; failures are tracked and skipped.
- A helper script `scrapeCroxyProxy.ts` (Node/Puppeteer) is included to scrape public proxy endpoints.

---

## Outputs
- Long-form CSV (default) written as `*_processed.csv`
- Wide-form CSV when `--output-format wide` is selected
- Optional summary report (`--report`) and deduplication (default ON; can be disabled with `--no-dedup`)

The post-processing step also computes useful stats such as overall success rates and contact counts.

---

## Google Sheets Integration (optional)
Integration utilities live in `gsheets_sync.py`. To enable syncing results to Google Sheets:
- Set up Google credentials (see gspread/google-auth docs)
- Load and use the provided helpers in your pipeline to push data to a specific sheet

---

## Debugging & Monitoring
- debug_cloudflare.py   Analyze Cloudflare wait/challenge flows and timings
- debug_stuck.py        Diagnose hanging processes
- debug_logger.py       Verify logging configurations
- monitor.py            Live monitoring helpers

Useful tips:
- Increase verbosity with `--log-level DEBUG`
- Save logs to files by configuring logging handlers as needed

---

## Performance Tips

### ⚙️ Worker Configuration

**IMPORTANT:** The `--workers` parameter controls multiple resource allocations:
- **Browser instances**: Equals `--workers` (each worker needs 1 browser for scraping)
- **Producer threads**: `--workers` threads for scraping URLs
- **Consumer threads**: `workers × 1.5` threads for email validation

**Resource Usage Example:**
```bash
--workers 10  # Allocates:
              # - 10 browser instances (memory intensive!)
              # - 10 producer threads (scraping)
              # - 15 consumer threads (email validation)
```

### 📊 Recommended Values by System Specs

| System RAM | CPU Cores | Recommended --workers | Notes |
|------------|-----------|----------------------|-------|
| 8 GB       | 4-6       | `5-8`                | Conservative, stable |
| 16 GB      | 8-12      | `10-15`              | Balanced performance |
| 32 GB+     | 16+       | `20-30`              | High throughput |

**⚠️ WARNING:** High `--workers` values consume significant system resources:
- Each browser instance: ~200-500 MB RAM
- 50 workers = ~10-25 GB RAM for browsers alone!
- Monitor system resources when increasing workers

### 🚀 Performance Tuning Tips

- **Start conservative**: Begin with `--workers 10` and increase gradually
- **Monitor system**: Watch CPU/RAM usage; reduce workers if system becomes unstable
- **Light-load mode**: Keep enabled (default) to reduce bandwidth/memory
- **Cloudflare handling**:
  - Tune `--cf-wait-timeout` for Cloudflare-heavy sites
  - Use `--no-network-idle` for CF wait pages
- **Memory-constrained systems**: Reduce `--chunk-size` or `--workers`
- **Network bottleneck**: Increase `--workers` doesn't help if network is the limit

---

## Development Conventions
- Read AGENTS.md for architecture overview, patterns, and best practices
- Use module-level loggers (logging.getLogger(__name__)) and structured error handling
- Ensure thread-safety with locks for shared state
- Respect `.gitignore` (results, logs, data, local notes are excluded)

---

## Mandatory Progress Tracking (WAJIB)
- Maintain `todos.md` to track progress, blockers, and completion
- Maintain `claude.md` to log AI-assisted sessions and code changes
- Both files are excluded from git; update them at the start and end of each session

Templates are provided in the repository root.

---

## Troubleshooting
Cloudflare blocks frequently
- Try `--no-network-idle`, increase `--cf-wait-timeout`, or `--skip-on-challenge`
- Ensure proxies are healthy; rotate or replace low-quality endpoints

Hangs / timeouts
- Lower `--workers`, increase `--timeout`, verify network/proxy uptime

High memory usage
- Reduce `--workers`, use smaller `--chunk-size`, avoid very large batches

Missing dependencies
- Re-run `./installer.sh` or `pip install -r requirements.txt`

---

## License
This repository does not include an explicit OSS license. Use internally unless a license is added.
