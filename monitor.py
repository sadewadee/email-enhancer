#!/usr/bin/env python3
import os
import shutil
import sys

# Ensure PYTHON_BIN is defined early to prevent NameError during runtime
PYTHON_BIN = sys.executable
import time
import logging
import subprocess
import gsheets_sync
import threading
import csv
import re
import signal
import atexit
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
_logger = None  # Global logger ref for signal handler


def sigchld_handler(signum, frame):
    """Reap zombie child processes when SIGCHLD signal received."""
    try:
        while True:
            pid, status = os.waitpid(-1, os.WNOHANG)
            if pid == 0:
                break
            if _logger:
                _logger.debug(f"Child process {pid} reaped with status {status >> 8}")
    except ChildProcessError:
        # No more children to reap
        pass
    except Exception as e:
        if _logger:
            _logger.warning(f"Error in SIGCHLD handler: {e}")


def cleanup_processes():
    """Terminate all spawned processes gracefully before exit."""
    if not _logger:
        return

    _logger.info("Cleaning up spawned processes...")
    with _lock:
        for country, proc in list(_procs.items()):
            try:
                if proc.poll() is None:  # Still running
                    _logger.debug(f"Terminating process for {country} (PID: {proc.pid})")
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                        _logger.debug(f"Process for {country} terminated gracefully")
                    except subprocess.TimeoutExpired:
                        _logger.warning(f"Timeout terminating {country}, sending SIGKILL")
                        proc.kill()
                        proc.wait()
            except Exception as e:
                _logger.error(f"Error cleaning up process for {country}: {e}")

        _procs.clear()


def prune_finished() -> None:
    """Hapus entri proses internal yang sudah selesai dan reap zombies."""
    with _lock:
        finished = []
        for country, proc in list(_procs.items()):
            poll_result = proc.poll()
            if poll_result is not None:
                # Process has exited - explicitly wait to reap the zombie
                try:
                    exit_code = proc.wait(timeout=1)
                    if _logger:
                        _logger.info(f"Process for {country} (PID {proc.pid}) reaped with exit code {exit_code}")
                    finished.append(country)
                except subprocess.TimeoutExpired:
                    if _logger:
                        _logger.warning(f"Timeout waiting for {country} (PID {proc.pid}), removing from tracking")
                    finished.append(country)
                except Exception as e:
                    if _logger:
                        _logger.warning(f"Error reaping {country} (PID {proc.pid}): {e}")
                    finished.append(country)

        for country in finished:
            _procs.pop(country, None)


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
    Hitung jumlah proses main.py yang BENAR-BENAR SEDANG BERJALAN.
    HANYA count proses dengan status "S" (sleeping) atau "R" (running).
    JANGAN count zombie "Z" atau defunct "D".

    Format ps aux:
    USER PID %CPU %MEM VSZ RSS TT STAT STARTED TIME COMMAND
    """
    try:
        res = subprocess.run(["ps", "aux"], capture_output=True, text=True, check=False)
        lines = res.stdout.splitlines()
        count = 0

        for line in lines:
            # Skip jika tidak ada main.py
            if "main.py" not in line or ("python " not in line and "python3 " not in line):
                continue

            # Parse status field (field ke-7, index 7)
            parts = line.split()
            if len(parts) < 8:
                continue

            status = parts[7]  # STAT field

            # HANYA count proses yang AKTIF
            # S = interruptible sleep
            # R = running
            # T = stopped
            # Jangan count:
            # Z = zombie
            # D = uninterruptible sleep (disk I/O wait)
            # X = dead
            if status in ['S', 'R', 'Ss', 'Rs', 'S+', 'R+']:
                count += 1
                _logger.debug(f"Active main.py process detected: {line[:120]}")

        return count
    except Exception as e:
        if _logger:
            _logger.warning(f"Error counting main.py instances: {e}")
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
        "--timeout",
        "120",  # Request timeout in seconds
        "--cf-wait-timeout",
        "90",  # Cloudflare challenge timeout (increased from default 60)
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
    global _logger
    setup_logging()
    logger = logging.getLogger("monitor")
    _logger = logger  # Set global reference for signal handler
    ensure_dirs()
    logger.info("Memulai monitoring folder 'country' dan 'data'")

    # Register signal handler to reap zombie processes
    signal.signal(signal.SIGCHLD, sigchld_handler)

    # Register cleanup function to run on normal exit
    atexit.register(cleanup_processes)

    try:
        while True:
            try:
                # AGGRESSIVE ZOMBIE REAPING: Reap any orphaned processes
                # This is a safety net against missed signals
                try:
                    while True:
                        pid, status = os.waitpid(-1, os.WNOHANG)
                        if pid == 0:
                            break
                        if _logger:
                            _logger.info(f"🧟 Reaped orphaned process (PID {pid}, status {status >> 8}) in monitor loop")
                except ChildProcessError:
                    pass  # No more children to reap

                unprocessed = list_unprocessed_countries()
                running_ps = get_running_countries()
                running_internal = get_internal_running_countries()
                running = running_ps | running_internal

                # HANYA gunakan internal count (ps aux bisa salah hitung zombie)
                total_running = get_internal_running_count()

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

                try:
                    spreadsheet_id = os.environ.get("SPREADSHEET_ID", "1aL_7HyyGpTKogW0nniiOq0n2w9O7K77fTBAEVADnItY")
                    if spreadsheet_id:
                        did_import = False
                        last_sid = None
                        for csv_file in DATA_DIR.glob("*.csv"):
                            # Check for completion marker - skip if not present
                            marker_file = csv_file.with_suffix('.complete')
                            if not marker_file.exists():
                                logger.debug(f"⏭️  Skip {csv_file.name} - no completion marker (still processing or failed)")
                                continue

                            # Read marker metadata for logging
                            try:
                                import json
                                with open(marker_file, 'r') as f:
                                    marker_data = json.load(f)
                                status = marker_data.get('status', 'unknown')
                                rows = marker_data.get('total_rows', 0)
                                logger.info(f"📋 Found completed file: {csv_file.name} (status={status}, rows={rows})")
                            except Exception:
                                # Marker exists but couldn't read - proceed anyway
                                logger.warning(f"⚠️  Marker exists but unreadable: {marker_file.name}")

                            title = csv_file.stem
                            if title.endswith('_processed'):
                                title = title[:-10]
                            try:
                                ss = gsheets_sync._get_client().open_by_key(spreadsheet_id)
                                try:
                                    ss.worksheet(title)
                                    logger.debug(f"⏭️  Sheet '{title}' already exists, skipping")
                                    continue
                                except Exception:
                                    pass
                                last_sid = gsheets_sync.sync_csv_to_sheet(str(csv_file), spreadsheet_id, title, replace=True)
                                did_import = True
                                logger.info(f"✅ Sinkronisasi ke Google Sheets selesai: {csv_file} -> tab '{title}'")

                                # Optional: Remove marker after successful upload to allow re-processing
                                # Uncomment the line below if you want automatic marker cleanup
                                # marker_file.unlink()

                            except Exception as e:
                                logger.error(f"❌ Sinkronisasi ke Google Sheets gagal untuk {csv_file}: {e}")
                        if did_import and last_sid:
                            gsheets_sync.build_global_summary(last_sid, "Summary")
                    else:
                        logger.debug("SPREADSHEET_ID tidak diset; melewati sinkronisasi Google Sheets")
                except Exception as e:
                    logger.error(f"Kesalahan sinkronisasi Google Sheets: {e}")

            except Exception as e:
                logging.getLogger("monitor").error(f"Kesalahan loop monitoring: {e}")

            time.sleep(CHECK_INTERVAL_SEC)
    except KeyboardInterrupt:
        logger.info("Monitor interrupted by user (Ctrl+C)")
        cleanup_processes()
        sys.exit(0)


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
