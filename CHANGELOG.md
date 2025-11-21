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