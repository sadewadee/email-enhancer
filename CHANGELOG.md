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