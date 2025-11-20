import os
import re
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.request import Request, build_opener, ProxyHandler, urlopen
from urllib.error import URLError, HTTPError

SOURCES = [
    "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt",
    "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/https/data.txt",
    "https://github.com/zloi-user/hideip.me/raw/refs/heads/master/http.txt",
    "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS.txt",
    "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
    "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
    "https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt",
    "https://raw.githubusercontent.com/mmpx12/proxy-list/master/https.txt",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/ProxyScraper/ProxyScraper/main/http.txt",
]

OUTPUT_FILE = "proxy.txt"
LOG_FILE = "logs/proxies.log"
TIMEOUT = 10
INTERVAL = 3600
MAX_WORKERS = 32
HEALTH_URL = "https://www.google.com/generate_204"
MAX_CANDIDATES_PER_CYCLE = 3000

logger = logging.getLogger("proxies")

def configure_logger():
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    try:
        fh = logging.FileHandler(LOG_FILE)
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    except Exception:
        pass

def read_existing(path):
    s = set()
    if not os.path.exists(path):
        return s
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    s.add(line)
    except Exception:
        pass
    return s

def normalize_ip_port(line):
    line = line.strip()
    if not line:
        return None
    if line.startswith("http://") or line.startswith("https://"):
        line = re.sub(r"^https?://", "", line, flags=re.IGNORECASE)
        line = line.strip()
    m_ip = re.match(r"^(\d{1,3}(?:\.\d{1,3}){3}):(\d{1,5})$", line)
    if m_ip:
        ip = m_ip.group(1)
        port = int(m_ip.group(2))
        parts = ip.split(".")
        for p in parts:
            v = int(p)
            if v < 0 or v > 255:
                return None
        if port < 1 or port > 65535:
            return None
        return f"{ip}:{port}"
    m_host = re.match(r"^([A-Za-z0-9.-]+):(\d{1,5})$", line)
    if m_host:
        host = m_host.group(1)
        port = int(m_host.group(2))
        if port < 1 or port > 65535:
            return None
        if host.startswith("-") or host.startswith("."):
            return None
        if not re.match(r"^[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?)*$", host):
            return None
        return f"{host}:{port}"
    return None

def fetch_sources(urls):
    lines = []
    for url in urls:
        try:
            req = Request(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"})
            with urlopen(req, timeout=TIMEOUT) as resp:
                body = resp.read().decode("utf-8", errors="ignore")
                lines.extend(body.splitlines())
        except Exception as e:
            logger.info(f"source_failed {url} {str(e)[:80]}")
    return lines

def health_check(ip_port):
    proxy = f"http://{ip_port}"
    handler = ProxyHandler({"http": proxy, "https": proxy})
    opener = build_opener(handler)
    ua = {"User-Agent": "Mozilla/5.0"}
    t0 = time.time()
    try:
        http_url = re.sub(r"^https://", "http://", HEALTH_URL)
        req_http = Request(http_url, headers=ua)
        with opener.open(req_http, timeout=TIMEOUT) as resp:
            code = resp.getcode()
            if 200 <= code < 400:
                return (ip_port, True, time.time() - t0, "")
    except Exception:
        pass
    try:
        https_url = re.sub(r"^http://", "https://", HEALTH_URL)
        req_https = Request(https_url, headers=ua)
        with opener.open(req_https, timeout=TIMEOUT) as resp:
            code = resp.getcode()
            return (ip_port, 200 <= code < 400, time.time() - t0, "")
    except (URLError, HTTPError) as e:
        return (ip_port, False, time.time() - t0, str(e))
    except Exception as e:
        return (ip_port, False, time.time() - t0, str(e))

def check_many(candidates):
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = [ex.submit(health_check, c) for c in candidates]
        for fut in as_completed(futs):
            try:
                results.append(fut.result())
            except Exception as e:
                logger.info(f"check_error {str(e)[:80]}")
    return results

def append_new(path, items):
    if not items:
        return 0
    try:
        with open(path, "a", encoding="utf-8") as f:
            for it in items:
                f.write(it + "\n")
        return len(items)
    except Exception as e:
        logger.info(f"append_failed {str(e)[:80]}")
        return 0

def rewrite_file_atomic(path, items):
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            for it in items:
                f.write(it + "\n")
        os.replace(tmp, path)
        return len(items)
    except Exception as e:
        logger.info(f"rewrite_failed {str(e)[:80]}")
        return 0

def run_once():
    existing = read_existing(OUTPUT_FILE)
    if existing:
        logger.info(f"existing_count {len(existing)}")
        ex_results = check_many(list(existing))
        ok = sum(1 for _, passed, _, _ in ex_results if passed)
        bad = len(ex_results) - ok
        existing_healthy = [ip for ip, passed, _, _ in ex_results if passed]
        avg = 0.0
        if ex_results:
            avg = sum(d for _, _, d, _ in ex_results) / len(ex_results)
        logger.info(f"existing_health ok={ok} bad={bad} avg={avg:.2f}s")
    raw = fetch_sources(SOURCES)
    normalized = []
    seen = set()
    for line in raw:
        v = normalize_ip_port(line)
        if v and v not in seen:
            seen.add(v)
            normalized.append(v)
    if not normalized:
        logger.info("no_candidates")
        return
    candidates = [c for c in normalized if c not in existing]
    if not candidates:
        logger.info("no_new_candidates")
        # Tetap lanjut ke penulisan ulang untuk membersihkan bad
        candidates = []
    if len(candidates) > MAX_CANDIDATES_PER_CYCLE:
        logger.info(f"truncate_candidates {len(candidates)}->{MAX_CANDIDATES_PER_CYCLE}")
        candidates = candidates[:MAX_CANDIDATES_PER_CYCLE]
    logger.info(f"checking_new {len(candidates)}")
    results = check_many(candidates) if candidates else []
    new_healthy = [ip for ip, passed, _, _ in results if passed]
    logger.info(f"new_healthy {len(new_healthy)}")
    final_set = set(existing_healthy)
    for ip in new_healthy:
        final_set.add(ip)
    final_list = sorted(final_set)
    removed = len(existing) - len(existing_healthy)
    written = rewrite_file_atomic(OUTPUT_FILE, final_list)
    logger.info(f"cleanup_removed {removed} appended_new {len(new_healthy)} final_count {written}")

def main():
    configure_logger()
    once = False
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--once":
        once = True
    run_once()
    if once:
        return
    while True:
        time.sleep(INTERVAL)
        run_once()

if __name__ == "__main__":
    main()