#!/usr/bin/env python3
import os
import shutil
import sys

# Ensure PYTHON_BIN is defined early to prevent NameError during runtime
PYTHON_BIN = sys.executable
import time
import logging
import subprocess
import threading
import csv
import re
from pathlib import Path
from typing import List, Set, Dict

# Direktori yang dipantau
COUNTRY_DIR = Path("country")
DATA_DIR = Path("new_data")

# Interval pengecekan (detik)
CHECK_INTERVAL_SEC = 30

# Batas maksimal instance main.py yang berjalan
MAX_MAIN_INSTANCES = 3

_lock = threading.Lock()
_procs: Dict[str, subprocess.Popen] = {}


def prune_finished() -> None:
    """Hapus entri proses internal yang sudah selesai."""
    with _lock:
        finished = [c for c, p in _procs.items() if p.poll() is not None]
        for c in finished:
            _procs.pop(c, None)


def get_internal_running_count() -> int:
    prune_finished()
    return len(_procs)


def get_internal_running_countries() -> Set[str]:
    prune_finished()
    return set(_procs.keys())


def setup_logging() -> None:
    os.makedirs("logs", exist_ok=True)
    logger = logging.getLogger("monitor")
    logger.setLevel(logging.INFO)

    fh = logging.FileHandler("logs/monitor.log")
    ch = logging.StreamHandler()
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)

    # Hindari duplikasi handler kalau fungsi dipanggil ulang
    if not logger.handlers:
        logger.addHandler(fh)
        logger.addHandler(ch)


def ensure_dirs() -> None:
    COUNTRY_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def is_file_stable(path: Path, stability_window: int = 5) -> bool:
    """
    Deteksi file sedang ditulis dengan memeriksa ukuran dan mtime yang stabil
    selama stability_window detik.
    """
    try:
        if not path.exists():
            return False
        s1 = path.stat()
        time.sleep(stability_window)
        s2 = path.stat()
        return s1.st_size > 0 and s1.st_size == s2.st_size and s1.st_mtime == s2.st_mtime
    except Exception:
        return False


def validate_csv_basic(path: Path) -> bool:
    """
    Validasi CSV dasar: bisa dibuka dan memiliki setidaknya satu baris.
    """
    try:
        with path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            first = next(reader, None)
            return first is not None
    except UnicodeDecodeError:
        # Coba encoding lain jika UTF-8 gagal
        try:
            with path.open("r", newline="", encoding="latin-1") as f:
                reader = csv.reader(f)
                first = next(reader, None)
                return first is not None
        except Exception:
            return False
    except Exception:
        return False


def list_unprocessed_countries() -> List[str]:
    """
    Negara yang memiliki file [negara].csv di country tetapi belum ada
    [negara]_processed.csv di data.
    """
    countries = []
    for csv_file in COUNTRY_DIR.glob("*.csv"):
        country = csv_file.stem
        processed = DATA_DIR / f"{country}_processed.csv"
        if not processed.exists():
            countries.append(country)
    return sorted(countries)


def get_running_main_instances() -> int:
    """
    Hitung jumlah proses main.py yang sedang berjalan menggunakan ps aux.
    """
    try:
        res = subprocess.run(["ps", "aux"], capture_output=True, text=True, check=False)
        lines = res.stdout.splitlines()
        count = 0
        for line in lines:
            if "main.py" in line and ("python " in line or "python3 " in line):
                count += 1
        return count
    except Exception:
        return 0


def get_running_countries() -> Set[str]:
    """
    Deteksi negara yang sedang diproses dari argumen command di ps aux.
    Pola yang dicari: main.py ... single ... country/<negara>.csv
    """
    running: Set[str] = set()
    try:
        res = subprocess.run(["ps", "aux"], capture_output=True, text=True, check=False)
        # Tangkap country/<nama>.csv, ambil nama negaranya (group 2)
        pattern = re.compile(r"main\.py.*\bsingle\b.*?(country/([^/\s]+)\.csv)")
        for line in res.stdout.splitlines():
            m = pattern.search(line)
            if m:
                name = m.group(2)
                if name:
                    running.add(Path(name).stem)
    except Exception:
        pass
    return running


def start_main_for_country(country: str, logger: logging.Logger) -> bool:
    """
    Jalankan main.py untuk negara tertentu sebagai proses baru.
    """
    input_csv = COUNTRY_DIR / f"{country}.csv"
    if not input_csv.exists():
        logger.warning(f"File input hilang: {input_csv}")
        return False

    # Edge case: file harus stabil dan valid
    if not is_file_stable(input_csv):
        logger.info(f"Menunda {input_csv} (file belum stabil/masih ditulis)")
        return False
    if not validate_csv_basic(input_csv):
        logger.error(f"File corrupt/tidak valid CSV: {input_csv}")
        return False

    cmd = [
        PYTHON_BIN,
        "main.py",
        "single",
        str(input_csv),
        "--output-dir",
        str(DATA_DIR),
        "--workers",
        "25",
    ]
    log_file = Path("logs") / f"main_{country}.log"
    logger.info(f"Menjalankan: {' '.join(cmd)} | py: {PYTHON_BIN} | log: {log_file}")
    try:
        with _lock:
            # Tulis stdout & stderr main.py ke file log per-negara agar mudah diagnosis
            with open(log_file, "ab") as fout:
                p = subprocess.Popen(
                    cmd,
                    stdout=fout,
                    stderr=fout,
                    start_new_session=True,  # buat proses independen dari parent
                )
                _procs[country] = p
        return True
    except Exception as e:
        logger.error(f"Gagal menjalankan main.py untuk {country}: {e}")
        return False


def monitor_loop():
    setup_logging()
    logger = logging.getLogger("monitor")
    ensure_dirs()
    logger.info("Memulai monitoring folder 'country' dan 'data'")

    while True:
        try:
            unprocessed = list_unprocessed_countries()
            running_ps = get_running_countries()
            running_internal = get_internal_running_countries()
            running = running_ps | running_internal

            # Gunakan angka terbesar agar tidak undercount
            total_running = max(get_running_main_instances(), get_internal_running_count())

            # Hindari meluncurkan negara yang sudah sedang diproses
            candidates = [c for c in unprocessed if c not in running]

            logger.info(
                f"Status: belum diproses={len(unprocessed)}, kandidat_start={len(candidates)}, "
                f"sedang_berjalan={total_running}"
            )

            # Hitung slot proses yang boleh diluncurkan kali ini
            slots = max(0, MAX_MAIN_INSTANCES - total_running)
            if slots == 0:
                logger.info(f"Maksimal {MAX_MAIN_INSTANCES} main.py berjalan. Menunggu...")
            else:
                # Luncurkan maksimal 'slots' negara saja pada iterasi ini
                for country in candidates[:slots]:
                    started = start_main_for_country(country, logger)
                    if started:
                        logger.info(f"Diluncurkan proses untuk negara: {country}")
                    else:
                        logger.info(f"Melewati negara: {country} (file belum siap/invalid)")

            # Hint untuk status selesai semua input saat ini
            if not unprocessed and total_running == 0:
                logger.info("Tidak ada file baru di 'country' dan tidak ada proses berjalan.")

        except Exception as e:
            logging.getLogger("monitor").error(f"Kesalahan loop monitoring: {e}")

        time.sleep(CHECK_INTERVAL_SEC)


if __name__ == "__main__":
    monitor_loop()
def resolve_python_bin() -> str:
    """
    Tentukan interpreter Python yang digunakan.
    Prioritas: venv/bin/python -> .venv/bin/python -> env/bin/python -> which('python3') -> 'python3'.
    """
    candidates = [
        Path("venv/bin/python"),
        Path(".venv/bin/python"),
        Path("env/bin/python"),
    ]
    for p in candidates:
        if p.exists():
            return str(p)
    return shutil.which("python3") or "python3"


PYTHON_BIN = resolve_python_bin()