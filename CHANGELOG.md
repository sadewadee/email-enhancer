##### [0.1.5] - 2025-11-26

        - Added
            - **JavaScript-rendered Social Media Extraction**: Extract social media dari link aggregator sites (Taplink, Linktree, Beacons)
                - Tambah JSON extraction dari `<script>` tags di `extract_social_media()` method (lines 529-601)
                - Parse `window.data`, `window.__data`, `__NEXT_DATA__` dan JSON objects lainnya
                - Recursive JSON traversal dengan `_extract_social_from_json()` helper method (lines 644-699)
                - Direct URL pattern matching di script content untuk fallback
                - Support sites tanpa `<a>` tags yang store data di JavaScript/JSON
                - Extraction methods (in order): `<a>` tags → `<script>` JSON → text content

            - **WhatsApp Number Normalization Enhancement**: Parse international numbers tanpa + prefix
                - Enhanced `_normalize_phone()` dengan fallback: try add + prefix jika parse gagal (lines 984-993)
                - Fixes WhatsApp extraction dari Taplink/Linktree URLs (e.g., "393518013001" → "+393518013001")
                - Sekarang dapat parse numbers dengan country code tanpa perlu country_code parameter
                - Backward compatible: existing logic tetap works untuk numbers dengan + prefix

        - Fixed
            - **CSV Export Silent Failure**: Added 'tiktok' dan 'youtube' to mandatory_cols fieldnames (csv_processor.py:688)
                - Issue: csv.DictWriter throws silent error "dict contains fields not in fieldnames"
                - Impact: japan_processed.csv had 0 rows despite 485 emails extracted
                - Fix: All 4 social media columns now correctly exported to CSV

            - **Malformed Email Extraction**: Enhanced email local-part validation (contact_extractor.py:759-774)
                - Issue: Art of Living website extracted 478 malformed emails like "ca949-509-1050losangeles@domain.com"
                - Root cause: HTML parsing concatenated state code + phone + city into email local-part
                - Fix: Added 3 regex checks to reject emails with embedded phone patterns and state codes
                - Result: Reduced from 478 to 2 legitimate emails (info@artofliving.org, secretariat@artofliving.org)

        - Technical Details
            - Root cause (Taplink): Link aggregators store social media URLs in `<script>window.data={...}</script>` JSON
            - 0 `<a>` tags in static HTML, data JavaScript-rendered → BeautifulSoup cannot extract
            - Solution: Multi-method extraction (HTML tags → JSON → text) dalam 1 HTTP request (NO browser rendering needed)
            - Impact: Dapat extract Instagram, WhatsApp, dan social media lain dari Taplink, Linktree, Beacons, dll
            - Files modified: contact_extractor.py (+160 lines social media JSON extraction, +13 lines phone normalization)
            - Performance: <5ms overhead per URL (JSON parsing faster than JS rendering)
            - Backward compatible: Standard websites dengan `<a>` tags tetap works seperti biasa

##### [0.1.4] - 2025-11-25

        - Added
            - **Social Media Extraction**: Ekstrak Facebook, Instagram, TikTok, YouTube dari website HTML
                - Baru method `extract_social_media()` di ContactExtractor untuk mengambil social media profiles
                - Hanya mengambil first occurrence per platform (deduplication otomatis)
                - Support multiple format: facebook.com/, instagram.com/, tiktok.com/@, youtube.com/channel/c/@/user/
                - Ekstrak dari `<a>` tags (prioritas) dan text content (fallback)
                - Integrasi ke `gather_contact_info()` di web_scraper dengan scanning order: header → footer → general page → contact pages
                - CSV output sekarang mencakup facebook, instagram, tiktok, youtube fields (prefer scraped data over CSV input)

            - **Improved URL Handling**: Normalisasi dan reconstruct URLs dengan proper format
                - TikTok: auto-add @ prefix jika missing
                - YouTube: handle channel/, c/, @, user/ formats
                - Extract username/handle dari URL untuk metadata

        - Technical Details
            - Root cause identified: Social media tidak diekstrak dari HTML, hanya pass-through dari CSV
            - Solution: Multi-layer extraction (link scan → text fallback) dengan first-match-only tracking
            - Impact: Sekarang dapat mengisi facebook/instagram/tiktok/youtube field dari scraping hasil, tidak hanya dari CSV input
            - Files modified: contact_extractor.py (+125 lines), web_scraper.py (+46 lines), csv_processor.py (+6 lines)
            - Backward compatible: semua field optional, tidak break existing workflow

##### [0.1.3] - 2025-11-22

        - Fixed (CRITICAL: Unkillable Process & Infinite Spawn Loop)
            - **Daemon subprocess prevention**: Changed `daemon=True` to `daemon=False` di web_scraper.py untuk graceful cleanup
            - **Queue deadlock**: Gunakan `cancel_join_thread()` instead of `join_thread()` untuk prevent blocking
            - **Monitor orphan processes**: Tambah SIGTERM/SIGINT handler di monitor.py untuk cleanup saat killed
            - **Signal handler respawn loop**: Exit code 0 untuk force quit (bukan exit code 1) agar monitor tidak respawn
            - **Signal handler re-entrance**: Tambah flag `handler_running` untuk prevent double trigger
            - **ThreadPoolExecutor hang**: Tambah 30s timeout untuk executor.shutdown() di csv_processor.py
            - **Queue join timeout**: Force cleanup dengan `_cond.notify_all()` dan `cancel_join_thread()` saat timeout
            - **Process exit hang**: Tambah explicit `gc.collect()` di main.py finally block

        - Technical Details
            - Root cause: Cascade failure dari daemon subprocess → queue deadlock → semaphore leak → executor hang → force exit → orphaned processes → respawn loop
            - Impact: main.py bisa exit dalam 5 detik pada SIGTERM, tidak ada zombie/orphan processes
            - Files modified: web_scraper.py, main.py, csv_processor.py, monitor.py

##### [0.1.2] - 2025-11-22

        - Fixed
            - Force quit (Ctrl+C kedua kali) di Linux tidak benar-benar exit
            - Ganti `sys.exit(1)` dengan `os._exit(1)` di signal handler untuk immediate kernel-level termination
            - Sekarang force quit bekerja reliably di semua platform (macOS, Linux, Windows)

##### [0.1.1] - 2025-11-04

        - Added
            - Opsi CLI baru `--cf-wait-timeout` dan `--skip-on-challenge`
            - Pemetaan `cf_wait_timeout` dan `skip_on_challenge` ke config dan propagasi ke `CSVProcessor` dan `WebScraper`
        - Changed
            - `WebScraper` kini mendukung `skip_on_challenge` untuk skip dini saat halaman Cloudflare terdeteksi pada fetch statis
        - Fixed
            - Tidak ada

##### [0.1.0] - 2025-11-02

        - Added
            - Skrip monitoring `monitor.py` untuk:
                - Memantau folder `country` dan `data` setiap 30 detik
                - Menjalankan `main.py` dengan subcommand `single` untuk negara baru
                - Membatasi maksimal 3 instance `main.py` berjalan sekaligus
                - Logging ke `logs/monitor.log` dan penanganan file sedang ditulis/korup
                - Operasi thread-safe menggunakan lock

        - Changed
            - Tidak ada

        - Fixed
            - Tidak ada