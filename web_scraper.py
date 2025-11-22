"""
Web Scraper Module
Handles web scraping with anti-bot bypass and JavaScript rendering using Scrapling.
"""

import time
import logging
import re
import sys
import multiprocessing
import queue
from types import SimpleNamespace
from typing import Dict, Optional, List
import os
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs
import json
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
import threading
import chardet  # FIX: For proper encoding detection of non-Latin content


# ============================================================================
# ENCODING UTILITIES FOR NON-LATIN CHARACTER SUPPORT (CJK, Arabic, etc.)
# ============================================================================

def _extract_charset_from_content_type(content_type: str) -> Optional[str]:
    """
    Extract charset from Content-Type header.
    Example: 'text/html; charset=shift_jis' -> 'shift_jis'
    """
    if not content_type:
        return None
    match = re.search(r'charset=([^\s;]+)', content_type, re.IGNORECASE)
    if match:
        charset = match.group(1).strip('"\'')
        return charset.lower()
    return None


def _detect_encoding_from_html(html_bytes: bytes) -> Optional[str]:
    """
    Detect encoding from HTML meta tags.
    Supports: <meta charset="..."> and <meta http-equiv="Content-Type" content="...;charset=...">
    """
    # Only check first 4KB for meta tags
    sample = html_bytes[:4096]
    try:
        # Try to decode as ASCII to search for meta tags
        sample_str = sample.decode('ascii', errors='ignore')

        # Pattern 1: <meta charset="utf-8">
        match = re.search(r'<meta[^>]+charset=["\']?([^"\'\s>]+)', sample_str, re.IGNORECASE)
        if match:
            return match.group(1).lower()

        # Pattern 2: <meta http-equiv="Content-Type" content="text/html; charset=...">
        match = re.search(r'<meta[^>]+content=["\'][^"\']*charset=([^"\'\s;>]+)', sample_str, re.IGNORECASE)
        if match:
            return match.group(1).lower()
    except Exception:
        pass
    return None


def decode_html_content(raw_bytes: bytes, content_type: str = None) -> str:
    """
    Decode HTML content with proper encoding detection for non-Latin characters.

    Priority:
    1. charset from Content-Type header
    2. charset from HTML meta tags
    3. chardet auto-detection
    4. utf-8 with surrogateescape (preserves bytes)

    Args:
        raw_bytes: Raw HTML bytes
        content_type: Content-Type header value (optional)

    Returns:
        Decoded HTML string with non-Latin characters preserved
    """
    logger = logging.getLogger(__name__)

    # 1. Try charset from Content-Type header
    charset = _extract_charset_from_content_type(content_type)
    if charset:
        try:
            return raw_bytes.decode(charset)
        except (UnicodeDecodeError, LookupError) as e:
            logger.debug(f"Header charset '{charset}' failed: {e}")

    # 2. Try charset from HTML meta tags
    meta_charset = _detect_encoding_from_html(raw_bytes)
    if meta_charset:
        try:
            return raw_bytes.decode(meta_charset)
        except (UnicodeDecodeError, LookupError) as e:
            logger.debug(f"Meta charset '{meta_charset}' failed: {e}")

    # 3. Try chardet auto-detection (good for CJK)
    try:
        detected = chardet.detect(raw_bytes[:10000])  # Sample first 10KB
        if detected and detected.get('encoding'):
            enc = detected['encoding']
            confidence = detected.get('confidence', 0)
            if confidence > 0.7:
                try:
                    return raw_bytes.decode(enc)
                except (UnicodeDecodeError, LookupError) as e:
                    logger.debug(f"Chardet encoding '{enc}' (conf={confidence:.2f}) failed: {e}")
    except Exception as e:
        logger.debug(f"Chardet detection failed: {e}")

    # 4. UTF-8 with surrogateescape (preserves bytes that can't be decoded)
    try:
        return raw_bytes.decode('utf-8', errors='surrogateescape')
    except Exception:
        pass

    # 5. Final fallback: latin-1 (never fails, but may corrupt non-ASCII)
    logger.warning("Using latin-1 fallback - non-Latin characters may be corrupted")
    return raw_bytes.decode('latin-1', errors='replace')

# Import proxy manager
from proxy_manager import ProxyManager

# Import URL cleaner
from url_cleaner import URLCleaner

# Import SSL error handler
from ssl_error_handler import SSLErrorHandler



import random
import json
from typing import Dict, Any, Optional

class BrowserFingerprinter:
    """Advanced browser fingerprinting to avoid detection"""

    def __init__(self, profile_type: str = "random"):
        self.profile_type = profile_type
        self.profiles = self._get_fingerprint_profiles()
        self.current_profile = self._select_profile()

    def _get_fingerprint_profiles(self) -> Dict[str, Dict]:
        """Comprehensive fingerprint profiles with realistic variations"""
        return {
            "windows_chrome_120": {
                "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "viewport": {"width": 1920, "height": 1080},
                "screen": {"width": 1920, "height": 1080, "colorDepth": 24},
                "timezone": "America/New_York",
                "language": "en-US,en;q=0.9",
                "platform": "Win32",
                "hardware_concurrency": 8,
                "device_memory": 8,
                "webgl_vendor": "Google Inc. (NVIDIA)",
                "webgl_renderer": "ANGLE (NVIDIA GeForce RTX 3080 Direct3D11 vs_5_0 ps_5_0)",
                "plugins": ["Chrome PDF Plugin", "Chrome PDF Viewer"],
                "accept_language": "en-US,en;q=0.9",
                "accept_encoding": "gzip, deflate, br",
                "os": "Windows"
            },
            "windows_chrome_119": {
                "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                "viewport": {"width": 1366, "height": 768},
                "screen": {"width": 1366, "height": 768, "colorDepth": 24},
                "timezone": "America/Chicago",
                "language": "en-US,en;q=0.9",
                "platform": "Win32",
                "hardware_concurrency": 4,
                "device_memory": 4,
                "webgl_vendor": "Google Inc. (Intel)",
                "webgl_renderer": "ANGLE (Intel(R) UHD Graphics Direct3D11 vs_5_0 ps_5_0)",
                "plugins": ["Chrome PDF Plugin", "Chrome PDF Viewer"],
                "accept_language": "en-US,en;q=0.9",
                "accept_encoding": "gzip, deflate, br",
                "os": "Windows"
            },
            "mac_safari_17": {
                "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
                "viewport": {"width": 1440, "height": 900},
                "screen": {"width": 1440, "height": 900, "colorDepth": 24},
                "timezone": "America/Los_Angeles",
                "language": "en-US,en;q=0.9",
                "platform": "MacIntel",
                "hardware_concurrency": 10,
                "device_memory": 16,
                "webgl_vendor": "Apple Inc.",
                "webgl_renderer": "Apple M1 Pro",
                "plugins": [],
                "accept_language": "en-US,en;q=0.9",
                "accept_encoding": "gzip, deflate, br",
                "os": "Mac"
            },
            "mac_chrome_120": {
                "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "viewport": {"width": 1440, "height": 900},
                "screen": {"width": 1440, "height": 900, "colorDepth": 24},
                "timezone": "America/Los_Angeles",
                "language": "en-US,en;q=0.9",
                "platform": "MacIntel",
                "hardware_concurrency": 8,
                "device_memory": 16,
                "webgl_vendor": "Google Inc. (Apple)",
                "webgl_renderer": "ANGLE (Apple, Apple M1 Pro, OpenGL 4.1)",
                "plugins": ["Chrome PDF Plugin", "Chrome PDF Viewer"],
                "accept_language": "en-US,en;q=0.9",
                "accept_encoding": "gzip, deflate, br",
                "os": "Mac"
            },
            "linux_firefox_120": {
                "user_agent": "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0",
                "viewport": {"width": 1280, "height": 1024},
                "screen": {"width": 1280, "height": 1024, "colorDepth": 24},
                "timezone": "Europe/London",
                "language": "en-GB,en;q=0.8",
                "platform": "Linux x86_64",
                "hardware_concurrency": 4,
                "device_memory": 8,
                "webgl_vendor": "Mesa",
                "webgl_renderer": "Mesa DRI Intel(R) HD Graphics",
                "plugins": [],
                "accept_language": "en-GB,en;q=0.8",
                "accept_encoding": "gzip, deflate, br",
                "os": "Linux"
            }
        }

    def _select_profile(self) -> Dict[str, Any]:
        """Select fingerprint profile"""
        if self.profile_type == "random":
            profile_name = random.choice(list(self.profiles.keys()))
        else:
            profile_name = self.profile_type if self.profile_type in self.profiles else list(self.profiles.keys())[0]

        return self.profiles[profile_name].copy()

    def get_enhanced_page_action(self):
        """Create enhanced page action with comprehensive fingerprint spoofing"""
        profile = self.current_profile

        def _enhanced_page_action(page):
            try:
                # Set viewport
                page.set_viewport_size(
                    width=profile["viewport"]["width"],
                    height=profile["viewport"]["height"]
                )

                # Inject comprehensive fingerprint spoofing script
                page.add_init_script(f"""
                    // ======================
                    // COMPREHENSIVE BROWSER FINGERPRINT SPOOFING
                    // ======================

                    // Override navigator properties
                    Object.defineProperty(navigator, 'userAgent', {{
                        get: () => '{profile["user_agent"]}'
                    }});

                    Object.defineProperty(navigator, 'platform', {{
                        get: () => '{profile["platform"]}'
                    }});

                    Object.defineProperty(navigator, 'hardwareConcurrency', {{
                        get: () => {profile["hardware_concurrency"]}
                    }});

                    Object.defineProperty(navigator, 'deviceMemory', {{
                        get: () => {profile["device_memory"]}
                    }});

                    Object.defineProperty(navigator, 'language', {{
                        get: () => '{profile["language"].split(",")[0]}'
                    }});

                    Object.defineProperty(navigator, 'languages', {{
                        get: () => {json.dumps([lang.strip() for lang in profile["language"].split(",")])}
                    }});

                    Object.defineProperty(navigator, 'appVersion', {{
                        get: () => '{profile["user_agent"].split("Mozilla/")[1] if "Mozilla/" in profile["user_agent"] else "5.0"}'
                    }});

                    Object.defineProperty(navigator, 'vendor', {{
                        get: () => '{"Apple Computer, Inc." if "Safari" in profile["user_agent"] else "Google Inc."}'
                    }});

                    Object.defineProperty(navigator, 'doNotTrack', {{
                        get: () => '1'
                    }});

                    // Override screen properties with realistic variations
                    const screenVariation = Math.floor(Math.random() * 3) - 1; // -1, 0, or 1

                    Object.defineProperty(screen, 'width', {{
                        get: () => {profile["screen"]["width"]} + screenVariation
                    }});

                    Object.defineProperty(screen, 'height', {{
                        get: () => {profile["screen"]["height"]} + screenVariation
                    }});

                    Object.defineProperty(screen, 'availWidth', {{
                        get: () => {profile["screen"]["width"]} + screenVariation
                    }});

                    Object.defineProperty(screen, 'availHeight', {{
                        get: () => {profile["screen"]["height"]} - 40 + screenVariation
                    }});

                    Object.defineProperty(screen, 'colorDepth', {{
                        get: () => {profile["screen"]["colorDepth"]}
                    }});

                    Object.defineProperty(screen, 'pixelDepth', {{
                        get: () => {profile["screen"]["colorDepth"]}
                    }});

                    // Override WebGL fingerprint
                    const getParameter = WebGLRenderingContext.prototype.getParameter;
                    WebGLRenderingContext.prototype.getParameter = function(parameter) {{
                        if (parameter === 37445) {{ // UNMASKED_VENDOR_WEBGL
                            return '{profile["webgl_vendor"]}';
                        }}
                        if (parameter === 37446) {{ // UNMASKED_RENDERER_WEBGL
                            return '{profile["webgl_renderer"]}';
                        }}
                        return getParameter.call(this, parameter);
                    }};

                    // Also override WebGL2 if available
                    if (window.WebGL2RenderingContext) {{
                        const getParameter2 = WebGL2RenderingContext.prototype.getParameter;
                        WebGL2RenderingContext.prototype.getParameter = function(parameter) {{
                            if (parameter === 37445) {{
                                return '{profile["webgl_vendor"]}';
                            }}
                            if (parameter === 37446) {{
                                return '{profile["webgl_renderer"]}';
                            }}
                            return getParameter2.call(this, parameter);
                        }};
                    }}

                    // Override timezone with realistic handling
                    const originalDateTimeFormat = Intl.DateTimeFormat;
                    Intl.DateTimeFormat = function(...args) {{
                        if (args.length === 0 || args[0] === undefined) {{
                            args[0] = 'en-US';
                        }}
                        if (!args[1] || !args[1].timeZone) {{
                            args[1] = {{ ...args[1], timeZone: '{profile["timezone"]}' }};
                        }}
                        return new originalDateTimeFormat(...args);
                    }};

                    // Override Date.getTimezoneOffset
                    const originalGetTimezoneOffset = Date.prototype.getTimezoneOffset;
                    Date.prototype.getTimezoneOffset = function() {{
                        // Return timezone offset based on profile
                        const timezoneOffsets = {{
                            'America/New_York': 300,
                            'America/Chicago': 360,
                            'America/Los_Angeles': 480,
                            'Europe/London': 0,
                            'UTC': 0
                        }};
                        return timezoneOffsets['{profile["timezone"]}'] || 0;
                    }};

                    // Canvas fingerprint protection with subtle noise
                    const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
                    HTMLCanvasElement.prototype.toDataURL = function() {{
                        // Add context-dependent noise
                        const context = this.getContext('2d');
                        if (context) {{
                            const imageData = context.getImageData(0, 0, this.width, this.height);
                            const data = imageData.data;
                            for (let i = 0; i < data.length; i += 4) {{
                                // Add very subtle noise (< 1%)
                                const noise = (Math.random() - 0.5) * 2;
                                data[i] = Math.min(255, Math.max(0, data[i] + noise));
                            }}
                            context.putImageData(imageData, 0, 0);
                        }}
                        return originalToDataURL.apply(this, arguments);
                    }};

                    // Audio fingerprint protection
                    if (window.AudioBuffer && AudioBuffer.prototype.getChannelData) {{
                        const originalGetChannelData = AudioBuffer.prototype.getChannelData;
                        AudioBuffer.prototype.getChannelData = function() {{
                            const originalData = originalGetChannelData.apply(this, arguments);
                            // Add very subtle audio noise
                            for (let i = 0; i < originalData.length; i++) {{
                                originalData[i] += (Math.random() - 0.5) * 0.0001;
                            }}
                            return originalData;
                        }};
                    }}

                    // Block WebRTC IP leak
                    if (window.RTCPeerConnection) {{
                        const originalRTCPeerConnection = window.RTCPeerConnection;
                        window.RTCPeerConnection = function() {{
                            const pc = new originalRTCPeerConnection(...arguments);
                            const originalCreateDataChannel = pc.createDataChannel;
                            pc.createDataChannel = function() {{
                                return originalCreateDataChannel.apply(this, arguments);
                            }};
                            return pc;
                        }};
                    }}

                    // Font fingerprint protection
                    const originalOffsetWidth = Object.getOwnPropertyDescriptor(HTMLElement.prototype, 'offsetWidth');
                    const originalOffsetHeight = Object.getOwnPropertyDescriptor(HTMLElement.prototype, 'offsetHeight');

                    if (originalOffsetWidth) {{
                        Object.defineProperty(HTMLElement.prototype, 'offsetWidth', {{
                            get: function() {{
                                const originalValue = originalOffsetWidth.get.call(this);
                                // Add slight variation for font fingerprint protection
                                return originalValue + Math.round(Math.random() * 2 - 1);
                            }}
                        }});
                    }}

                    if (originalOffsetHeight) {{
                        Object.defineProperty(HTMLElement.prototype, 'offsetHeight', {{
                            get: function() {{
                                const originalValue = originalOffsetHeight.get.call(this);
                                return originalValue + Math.round(Math.random() * 2 - 1);
                            }}
                        }});
                    }}

                    // Battery API blocking (privacy protection)
                    if ('getBattery' in navigator) {{
                        Object.defineProperty(navigator, 'getBattery', {{
                            get: () => undefined
                        }});
                    }}

                    // GamePad API spoofing
                    if ('getGamepads' in navigator) {{
                        Object.defineProperty(navigator, 'getGamepads', {{
                            get: () => () => []
                        }});
                    }}

                    // MediaDevices fingerprint protection
                    if (navigator.mediaDevices && navigator.mediaDevices.enumerateDevices) {{
                        const originalEnumerateDevices = navigator.mediaDevices.enumerateDevices;
                        navigator.mediaDevices.enumerateDevices = function() {{
                            return Promise.resolve([]);
                        }};
                    }}

                    // Plugins spoofing
                    Object.defineProperty(navigator, 'plugins', {{
                        get: () => {{
                            const plugins = {json.dumps(profile["plugins"])};
                            return plugins;
                        }}
                    }});

                    // Connection information spoofing
                    if (navigator.connection) {{
                        Object.defineProperty(navigator.connection, 'effectiveType', {{
                            get: () => '4g'
                        }});
                        Object.defineProperty(navigator.connection, 'downlink', {{
                            get: () => 10
                        }});
                        Object.defineProperty(navigator.connection, 'rtt', {{
                            get: () => 50
                        }});
                    }}

                    // Memory information spoofing
                    if ('memory' in performance) {{
                        Object.defineProperty(performance, 'memory', {{
                            get: () => ({{
                                usedJSHeapSize: Math.floor(Math.random() * 50000000) + 10000000,
                                totalJSHeapSize: Math.floor(Math.random() * 100000000) + 50000000,
                                jsHeapSizeLimit: {profile["device_memory"] * 1024 * 1024 * 1024}
                            }})
                        }});
                    }}

                    // Permissions API spoofing
                    if (navigator.permissions && navigator.permissions.query) {{
                        const originalQuery = navigator.permissions.query;
                        navigator.permissions.query = function(permissionDesc) {{
                            return originalQuery.call(this, permissionDesc).then(result => {{
                                // Spoof some common permission states
                                const spoofed = ['geolocation', 'notifications', 'microphone', 'camera'];
                                if (spoofed.includes(permissionDesc.name)) {{
                                    return {{ state: 'prompt', onchange: null }};
                                }}
                                return result;
                            }});
                        }};
                    }}

                    // Console debug protection
                    const originalLog = console.log;
                    console.log = function() {{
                        // Filter out potential fingerprinting debug messages
                        const message = Array.from(arguments).join(' ').toLowerCase();
                        if (message.includes('fingerprint') || message.includes('detection') || message.includes('bot')) {{
                            return;
                        }}
                        return originalLog.apply(this, arguments);
                    }};

                    // ======================
                    // STEALTH MODE ENHANCEMENTS
                    // ======================

                    // Remove automation indicators
                    delete navigator.__proto__.webdriver;
                    delete navigator.webdriver;

                    // Override chrome runtime
                    if (window.chrome) {{
                        Object.defineProperty(window.chrome, 'runtime', {{
                            get: () => undefined
                        }});
                    }}

                    // Remove automation properties
                    Object.defineProperty(navigator, 'webdriver', {{
                        get: () => undefined
                    }});

                    // Spoof notification permissions
                    Object.defineProperty(window.Notification, 'permission', {{
                        get: () => 'default'
                    }});

                    console.log('🎭 Advanced fingerprinting applied - Profile: {profile.get("os", "Unknown")}');
                """)

                # Resource filtering with enhanced headers
                def handler(route):
                    req = route.request
                    url_lower = (getattr(req, 'url', '') or '').lower()
                    rtype = (getattr(req, 'resource_type', '') or '').lower()

                    # Always allow Cloudflare/challenge resources
                    cf_domains = (
                        'cloudflare', 'cf-challenge', 'challenge', 'turnstile', 'captcha',
                        'cdnjs.cloudflare.com', 'cfassets', 'cfcdn', 'hcaptcha', 'recaptcha',
                        'cdn-cgi', 'cf-ray', 'cflare', 'data-cf'
                    )
                    if any(domain in url_lower for domain in cf_domains):
                        return route.continue_()

                    # Enhanced headers for all requests
                    headers = {{
                        'User-Agent': profile["user_agent"],
                        'Accept-Language': profile["accept_language"],
                        'Accept-Encoding': profile["accept_encoding"],
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                        'Cache-Control': 'no-cache',
                        'Pragma': 'no-cache',
                        'Sec-Fetch-Dest': 'document',
                        'Sec-Fetch-Mode': 'navigate',
                        'Sec-Fetch-Site': 'none',
                        'Sec-Fetch-User': '?1',
                        'Upgrade-Insecure-Requests': '1',
                        'DNT': '1'
                    }}

                    # Always allow essential types with enhanced headers
                    essential_types = ('document', 'script', 'stylesheet', 'xhr', 'fetch', 'websocket')
                    if rtype in essential_types:
                        return route.continue_(headers=headers)

                    # Apply smart blocking for non-essential resources
                    if rtype in ('image', 'imageset'):
                        return route.abort()

                    if rtype in ('font', 'media', 'video', 'audio'):
                        return route.abort()

                    # Default: continue with headers
                    return route.continue_(headers=headers)

                page.route("**/*", handler)

            except Exception as e:
                # Fallback: continue without advanced fingerprinting
                print(f"Warning: Advanced fingerprinting failed: {e}")
                pass

        return _enhanced_page_action

    def get_user_agent(self) -> str:
        """Get user agent for current profile"""
        return self.current_profile["user_agent"]

    def get_viewport(self) -> Dict[str, int]:
        """Get viewport for current profile"""
        return self.current_profile["viewport"]

    def get_profile_info(self) -> Dict[str, Any]:
        """Get current profile information"""
        return self.current_profile.copy()

    def rotate_profile(self):
        """Rotate to a new random profile"""
        self.current_profile = self._select_profile()
        return self.current_profile


# Thread-local context to carry current domain for Scrapling logs
_SCRAPLING_CONTEXT = threading.local()

# Subprocess fetch helper to hard-isolate StealthyFetcher logging and enforce wall-clock timeout
def _subprocess_fetch(q, url, headless, solve_cloudflare, network_idle, google_search, timeout, block_images, disable_resources, proxy_config=None):
    """
    Child process target: perform StealthyFetcher.fetch and return minimal page fields via Queue.
    Logging is globally disabled and stdout/stderr are wrapped to suppress Cloudflare progress spam.
    """
    # FIX: Handle SIGTERM gracefully to prevent Node.js EPIPE errors
    # When parent kills this process, we need to exit cleanly before Playwright driver can complain
    import signal
    import os as _os

    def _graceful_exit(signum, frame):
        """Exit immediately on SIGTERM without letting Playwright complain."""
        try:
            # Send partial result if queue is still open
            q.put_nowait({'ok': False, 'error': 'Process terminated'})
        except Exception:
            pass
        # Use os._exit() to skip Python cleanup which would let Playwright print EPIPE
        _os._exit(0)

    try:
        signal.signal(signal.SIGTERM, _graceful_exit)
    except Exception:
        pass  # Ignore if signal handling fails

    try:

        # Wrap stdout/stderr to suppress known Cloudflare progress lines
        try:
            subs = (
                'waiting for cloudflare wait page to disappear',
                'turnstile version discovered',
                'no cloudflare challenge found',
            )
            class _SuppressingStream:
                def __init__(self, wrapped, substrings):
                    self._wrapped = wrapped
                    self._subs = tuple(s.lower() for s in substrings)

                def write(self, s):
                    try:
                        low = str(s).lower()
                        if any(sub in low for sub in self._subs):
                            return len(s)
                        if re.search(r'waiting\s+for\s+cloudflare\s+wait\s+page\s+to\s+disappear', low):
                            return len(s)
                    except Exception:
                        pass
                    try:
                        return self._wrapped.write(s)
                    except Exception:
                        return 0

                def flush(self):
                    try:
                        return self._wrapped.flush()
                    except Exception:
                        return None

            sys.stdout = _SuppressingStream(sys.stdout, subs)
            sys.stderr = _SuppressingStream(sys.stderr, subs)
        except Exception:
            pass

        # Lightweight page_action to allow CF-critical resources while trimming heavy assets
        def _page_action(page):
            try:
                def handler(route):
                    req = route.request
                    u = (getattr(req, 'url', '') or '').lower()
                    rtype = (getattr(req, 'resource_type', '') or '').lower()

                    allow_domains = (
                        'cloudflare', 'cf-challenge', 'challenge', 'turnstile', 'captcha',
                        'cdnjs.cloudflare.com', 'cfassets', 'cfcdn', 'hcaptcha', 'recaptcha',
                        'cdn-cgi', 'cf-ray', 'cflare', 'data-cf'
                    )
                    if any(k in u for k in allow_domains):
                        return route.continue_()

                    essential_types = ('document', 'script', 'stylesheet', 'xhr', 'fetch')
                    if rtype in essential_types:
                        return route.continue_()

                    if block_images and rtype in ('image', 'imageset'):
                        return route.abort()

                    if disable_resources and rtype in ('font', 'media', 'video', 'audio'):
                        return route.abort()

                    # Fix: Block non-essential resources in light-load mode instead of allowing all
                    if block_images or disable_resources:
                        return route.abort()
                    return route.continue_()
                page.route("**/*", handler)
            except Exception:
                pass

            # Auto-scroll to trigger lazy-loaded content
            # This runs AFTER network_idle, ensuring all JS observers are registered
            try:
                # Smooth scroll to bottom to trigger lazy load observers
                page.evaluate("""
                    async () => {
                        // Scroll to bottom in steps to trigger all lazy load observers
                        const scrollStep = window.innerHeight;
                        const scrollDelay = 300; // ms between scrolls

                        const totalHeight = document.body.scrollHeight;
                        let currentScroll = 0;

                        while (currentScroll < totalHeight) {
                            window.scrollBy(0, scrollStep);
                            currentScroll += scrollStep;
                            await new Promise(resolve => setTimeout(resolve, scrollDelay));
                        }

                        // Ensure we're at the very bottom
                        window.scrollTo(0, document.body.scrollHeight);

                        // Wait for lazy-loaded content to render (2 seconds)
                        await new Promise(resolve => setTimeout(resolve, 2000));
                    }
                """)

            except Exception:
                # If scroll fails, continue anyway (better to have partial content than fail)
                pass

        # Import Scrapling inside child to avoid initializing its loggers in the parent process
        from scrapling.fetchers import StealthyFetcher  # noqa: E402

        # Enhanced Cloudflare bypass configuration
        import os
        os.environ['PLAYWRIGHT_CF_AGGRESSIVE'] = '1'  # Enable aggressive mode

        # Reduce Playwright/Node driver logging noise and avoid debug output
        try:
            os.environ.setdefault('PLAYWRIGHT_DISABLE_LOG', '1')
            os.environ.setdefault('PWDEBUG', '0')
            # Disable Node debug logs commonly used by Playwright
            if 'DEBUG' in os.environ:
                # Keep user's DEBUG if set but mask playwright verbose categories
                if 'playwright' in str(os.environ.get('DEBUG', '')).lower():
                    os.environ['DEBUG'] = ''
        except Exception:
            pass

        # Prepare StealthyFetcher arguments
        fetch_kwargs = {
            'url': url,
            'headless': headless,
            'solve_cloudflare': solve_cloudflare,
            'network_idle': network_idle,
            'google_search': google_search,
            'timeout': timeout,
            'block_images': block_images,
            'disable_resources': disable_resources,
            'page_action': _page_action,  # Always use page_action for auto-scroll
            'geoip': True  # Fix proxy warning - recommended when using proxies
        }

        # Add proxy configuration if provided
        if proxy_config:
            fetch_kwargs['proxy'] = proxy_config

        page = StealthyFetcher.fetch(**fetch_kwargs)

        q.put({
            'ok': True,
            'status': getattr(page, 'status', 200),
            'html_content': getattr(page, 'html_content', ''),
            'final_url': getattr(page, 'url', url),
        })
    except BrokenPipeError:
        # EPIPE error - browser process terminated, try to return partial result
        try:
            q.put({'ok': False, 'error': 'Browser process terminated (EPIPE)'})
        except Exception:
            pass  # Queue might also be broken
    except Exception as e:
        try:
            # Try to detect and classify SSL errors for better logging/recovery
            error_str = str(e)
            error_type = "unknown"

            # Check if it's an SSL/certificate error
            if 'SEC_ERROR' in error_str or 'SSL_ERROR' in error_str:
                error_type = "ssl_certificate"
                # Extract error code if present
                import re
                match = re.search(r'(SEC_ERROR_\w+|SSL_ERROR_\w+)', error_str)
                if match:
                    error_code = match.group(1)
                    error_type = f"ssl_certificate ({error_code})"

            result = {'ok': False, 'error': error_str, 'error_type': error_type}
            q.put(result)
        except Exception:
            pass  # Silently fail if queue is broken


class WebScraper:
    """Web scraper with Cloudflare bypass and stealth capabilities."""

    def __init__(self,
                 headless: bool = True,
                 solve_cloudflare: bool = True,
                 timeout: int = 30,
                 network_idle: bool = True,
                 block_images: bool = False,
                 disable_resources: bool = False,
                 static_first: bool = True,
                 cf_wait_timeout: int = 60,
                 skip_on_challenge: bool = False,
                 proxy_file: str = "proxy.txt",
                 max_concurrent_browsers: int = 3,
                 normal_budget: int = 60,
                 challenge_budget: int = 120,
                 dead_site_budget: int = 20,
                 min_retry_threshold: int = 5):
        """
        Initialize the web scraper.

        Args:
            headless (bool): Run browser in background
            solve_cloudflare (bool): Enable Cloudflare bypass
            timeout (int): Timeout in seconds
            network_idle (bool): Wait for network idle state
            block_images (bool): Prevent image loading to save bandwidth
            disable_resources (bool): Drop non-essential resources (fonts, media, etc.)
            static_first (bool): Try static HTTP request first before browser automation
            cf_wait_timeout (int): Cloudflare challenge timeout
            skip_on_challenge (bool): Skip URLs with detected challenges
            proxy_file (str): Path to proxy file (default: proxy.txt)
            max_concurrent_browsers (int): Maximum concurrent browser instances to prevent crashes
            normal_budget (int): Budget for normal sites in seconds (default: 60)
            challenge_budget (int): Budget for Cloudflare/challenge sites in seconds (default: 120)
            dead_site_budget (int): Budget for dead sites in seconds (default: 20)
            min_retry_threshold (int): Minimum remaining budget to attempt retry in seconds (default: 5)
        """
        self.headless = headless
        self.solve_cloudflare = solve_cloudflare
        self.timeout = timeout
        self.network_idle = network_idle
        self.block_images = block_images
        self.disable_resources = disable_resources
        self.static_first = static_first
        # Per-URL maximum wait for Cloudflare wait page before skipping
        self.cf_wait_timeout = cf_wait_timeout
        # If True, skip early when a Cloudflare challenge is detected
        self.skip_on_challenge = skip_on_challenge

        # Budget configuration for adaptive time management
        self.normal_budget = normal_budget
        self.challenge_budget = challenge_budget
        self.dead_site_budget = dead_site_budget
        self.min_retry_threshold = min_retry_threshold

        # Initialize proxy manager
        self.proxy_manager = ProxyManager(proxy_file)

        # Initialize advanced browser fingerprinting
        self.fingerprinter = BrowserFingerprinter(profile_type="random")

        # Browser subprocess management - Limit concurrent instances to prevent crashes
        self.max_concurrent_browsers = max_concurrent_browsers
        self.browser_semaphore = threading.Semaphore(max_concurrent_browsers)

        # Proxy usage statistics tracking
        self.proxy_stats = {
            'total_requests': 0,
            'proxy_avoided': 0,
            'proxy_used': 0,
            'proxy_justified': 0,
            'proxy_wasted': 0,
            'proxy_retries': 0,
            'time_saved_seconds': 0.0,
            'direct_success': 0,
            'direct_failed': 0,
            'proxy_success': 0,
            'proxy_failed': 0
        }

        # Log proxy status
        if self.proxy_manager.has_proxies():
            self.logger = logging.getLogger(__name__)
            self.logger.info(f"🔄 Proxy support enabled: {self.proxy_manager.get_proxy_count()} proxies loaded")
            self.logger.info(f"🎯 Smart proxy mode active | Max concurrent browsers: {max_concurrent_browsers}")
        else:
            self.logger = logging.getLogger(__name__)
            self.logger.info("📡 No proxy file detected - running in direct mode")

        # Suppress harmless Cloudflare messages from Scrapling
        class _CloudflareLogFilter(logging.Filter):
            def filter(self, record: logging.LogRecord) -> bool:
                # Be tolerant to logging message formats
                try:
                    msg = record.getMessage()
                except Exception:
                    msg = str(getattr(record, 'msg', ''))
                low = str(msg).lower()

                # Case-insensitive substring suppression for common noisy lines
                suppressed_substrings = (
                    'no cloudflare challenge found',
                    'waiting for cloudflare wait page to disappear',  # handle punctuation via regex too
                    'the turnstile version discovered is',
                    'turnstile version discovered is',
                    'cloudflare wait page detected',
                    'solving cloudflare challenge',
                )
                if any(s in low for s in suppressed_substrings):
                    return False

                # Regex suppression to capture punctuation/format variations
                patterns = (
                    r'waiting\s+for\s+cloudflare\s+wait\s+page\s+to\s+disappear\.?',  # optional trailing period
                    r'turnstile\s+version\s+discovered\s+is\b',
                    r'cloudflare\s+(challenge|wait\s+page)\s+(detected|found)\b',
                )
                for p in patterns:
                    try:
                        if re.search(p, low):
                            return False
                    except Exception:
                        # Never block logging due to regex errors
                        pass

                return True

        # Prefix Scrapling messages with current domain when available
        class _DomainPrefixFilter(logging.Filter):
            def filter(self, record: logging.LogRecord) -> bool:
                try:
                    domain = getattr(_SCRAPLING_CONTEXT, 'domain', None)
                    if domain:
                        msg = record.getMessage()
                        # Avoid double-prefixing
                        if not str(msg).startswith(str(domain)):
                            record.msg = f"{domain} - {msg}"
                            record.args = None
                except Exception:
                    # Be tolerant: never block logging due to prefix issues
                    pass
                return True

        # Attach filters to likely Scrapling logger namespaces to ensure coverage
        # Aggressively suppress Scrapling CF progress logs:
        # - Disable scrapling loggers and remove their handlers
        # - Keep our domain prefix filter available for any remaining messages
        for name in ('scrapling', 'scrapling.fetchers', 'scrapling.playwright', 'scrapling.cloudflare', 'scrapling.turnstile'):
            lg = logging.getLogger(name)
            try:
                # Prevent INFO spam by gating at CRITICAL and disabling propagation
                lg.setLevel(logging.CRITICAL)
                lg.propagate = False

                # Remove existing handlers that might bypass logger-level gates
                for h in list(getattr(lg, 'handlers', [])):
                    try:
                        lg.removeHandler(h)
                    except Exception:
                        pass

                # Add a NullHandler to swallow any stray emits
                lg.addHandler(logging.NullHandler())

                # Hard disable the logger to ensure no records are processed
                lg.disabled = True
            except Exception:
                # Always be tolerant to logging setup errors
                pass

            # Still attach filters; in case the library flips disabled/handlers internally later
            lg.addFilter(_CloudflareLogFilter())
            lg.addFilter(_DomainPrefixFilter())

        # Enumerate ALL registered loggers and silence any 'scrapling*' namespaces,
        # including deep modules like 'scrapling.engines._browsers._camoufox'
        try:
            for lname, lobj in logging.Logger.manager.loggerDict.items():
                if not isinstance(lobj, logging.Logger):
                    continue
                if str(lname).startswith('scrapling'):
                    try:
                        lobj.setLevel(logging.CRITICAL)
                        lobj.propagate = False
                        # Remove and replace handlers to prevent pre-bound streams from bypassing filters
                        for h in list(getattr(lobj, 'handlers', [])):
                            try:
                                lobj.removeHandler(h)
                            except Exception:
                                pass
                        lobj.addHandler(logging.NullHandler())
                        lobj.disabled = True
                        # Attach filters defensively
                        lobj.addFilter(_CloudflareLogFilter())
                        lobj.addFilter(_DomainPrefixFilter())
                    except Exception:
                        # Suppression must never crash initialization
                        pass
        except Exception:
            # Be tolerant to logger manager issues
            pass

        # Additionally, install a global filter on the root logger and its handlers
        # to catch any stray records emitted outside our targeted logger namespaces.
        # SKIP adding to root logger and FileHandler to preserve DEBUG logging
        try:
            root = logging.getLogger()
            # Only add filter to StreamHandler (console), not FileHandler
            for h in getattr(root, 'handlers', []):
                try:
                    # Only add CloudflareLogFilter to StreamHandler, not FileHandler
                    if type(h).__name__ == 'StreamHandler':
                        h.addFilter(_CloudflareLogFilter())
                except Exception:
                    pass
        except Exception:
            # Be tolerant: logging setup should never crash the scraper
            pass

        # Final guardrail: wrap stdout/stderr to suppress print-based progress lines
        # from third-party libraries that bypass logging. This only filters known CF spam lines.
        class _SuppressingStream:
            def __init__(self, wrapped, substrings):
                self._wrapped = wrapped
                self._subs = tuple(s.lower() for s in substrings)
                self._lock = threading.Lock()

            def write(self, s):
                try:
                    low = str(s).lower()
                    # Drop lines containing known CF progress messages
                    if any(sub in low for sub in self._subs):
                        return len(s)
                    # Regex catch-all for phrasing variations
                    if re.search(r'waiting\s+for\s+cloudflare\s+wait\s+page\s+to\s+disappear', low):
                        return len(s)
                except Exception:
                    # Never break output on filter errors
                    pass
                with self._lock:
                    try:
                        return self._wrapped.write(s)
                    except Exception:
                        return 0

            def flush(self):
                try:
                    return self._wrapped.flush()
                except Exception:
                    return None

            def isatty(self):
                try:
                    return self._wrapped.isatty()
                except Exception:
                    return False

        try:
            _subs = (
                'waiting for cloudflare wait page to disappear',
                'turnstile version discovered',
                'no cloudflare challenge found',
            )
            # Wrap streams
            new_stdout = _SuppressingStream(sys.stdout, _subs)
            new_stderr = _SuppressingStream(sys.stderr, _subs)
            sys.stdout = new_stdout
            sys.stderr = new_stderr

            # Retarget existing StreamHandlers to the newly wrapped streams and add suppression filters
            try:
                # All named loggers
                for lname, lobj in logging.Logger.manager.loggerDict.items():
                    if not isinstance(lobj, logging.Logger):
                        continue
                    for h in list(getattr(lobj, 'handlers', [])):
                        try:
                            # Add suppression filter to handlers (except FileHandler to preserve DEBUG logging)
                            if type(h).__name__ != 'FileHandler':
                                h.addFilter(_CloudflareLogFilter())
                        except Exception:
                            pass
                        try:
                            # Force handler to use our wrapped stderr ONLY for StreamHandler, not FileHandler
                            if type(h).__name__ == 'StreamHandler' and getattr(h, 'stream', None) is not None:
                                h.stream = sys.stderr
                        except Exception:
                            pass

                # Root logger handlers
                root = logging.getLogger()
                for h in list(getattr(root, 'handlers', [])):
                    try:
                        # Only add CloudflareLogFilter to StreamHandler, not FileHandler
                        if type(h).__name__ != 'FileHandler':
                            h.addFilter(_CloudflareLogFilter())
                    except Exception:
                        pass
                    try:
                        # Only update stream for StreamHandler, not FileHandler
                        if type(h).__name__ == 'StreamHandler' and getattr(h, 'stream', None) is not None:
                            h.stream = sys.stderr
                    except Exception:
                        pass
            except Exception:
                # Defensive: handler retargeting should never break initialization
                pass
        except Exception:
            # Be tolerant: stdout/stderr wrapping should not crash the scraper
            pass

    def _build_allowlist_page_action(self):
        """Build a page_action that keeps Cloudflare/Turnstile-critical resources while blocking heavy assets.

        - Allows essential types: document, script, stylesheet, xhr/fetch
        - Always allows URLs containing Cloudflare/Turnstile/captcha indicators
        - Blocks images when block_images=True
        - Blocks fonts/media/video/audio when disable_resources=True
        """

        def _page_action(page):
            try:
                def handler(route):
                    req = route.request
                    url = (getattr(req, 'url', '') or '').lower()
                    rtype = (getattr(req, 'resource_type', '') or '').lower()

                    # Always allow Cloudflare/Turnstile/captcha-related resources
                    allow_domains = (
                        'cloudflare', 'cf-challenge', 'challenge', 'turnstile', 'captcha',
                        'cdnjs.cloudflare.com', 'cfassets', 'cfcdn', 'hcaptcha', 'recaptcha',
                        'cdn-cgi', 'cf-ray', 'cflare', 'data-cf'
                    )
                    if any(k in url for k in allow_domains):
                        return route.continue_()

                    # Allow essential resource types for page operation
                    essential_types = ('document', 'script', 'stylesheet', 'xhr', 'fetch')
                    if rtype in essential_types:
                        return route.continue_()

                    # Block heavy assets when requested
                    if self.block_images and rtype in ('image', 'imageset'):
                        return route.abort()

                    if self.disable_resources and rtype in ('font', 'media', 'video', 'audio'):
                        return route.abort()

                    # Default: allow
                    return route.continue_()

                # Install global route handler
                page.route("**/*", handler)
            except Exception:
                # Be tolerant: if routing fails, proceed without page_action
                pass

        return _page_action

    def _looks_like_challenge(self, html: str, url: str) -> bool:
        """Enhanced heuristic to detect ACTUAL Cloudflare challenge pages, not just CF presence."""
        if not html:
            return True

        # If HTML is substantial (>10KB), likely real content even with CF traces
        if len(html) > 10000:
            return False

        low = html.lower()

        # Strong challenge indicators (active challenge state)
        strong_indicators = (
            'just a moment', 'verify you are human', 'checking your browser',
            'please wait while', 'attention required', 'cf-please-wait',
            'redirecting…', 'redirecting...', 'ddos protection by',
            'browser verification', 'security check', 'loading...', 'challenge-platform'
        )

        # Weak indicators (CF presence but could be normal site)
        weak_indicators = (
            'cf-challenge', 'data-cf', 'turnstile', 'captcha', 'recaptcha',
            'hcaptcha', 'ray id', 'cf-ray', 'cdn-cgi', 'data-cf-beacon'
        )

        # Check for strong indicators first
        strong_matches = sum(1 for k in strong_indicators if k in low)
        if strong_matches >= 1:
            return True

        # For weak indicators, need multiple matches AND small HTML
        weak_matches = sum(1 for k in weak_indicators if k in low)
        if weak_matches >= 3 and len(html) < 5000:
            return True

        # Special case: "cloudflare" mention with very small HTML
        if 'cloudflare' in low and len(html) < 2000:
            return True

        return False

    def _is_cloudflare_wait_page(self, url: str, timeout: int = 5) -> bool:
        """
        Lightweight preflight to detect Cloudflare wait/challenge without invoking the dynamic renderer.
        Heuristics:
        - Response headers suggest Cloudflare (Server: cloudflare or any 'cf-' header), especially with 403/429/503
        - HTML snippet includes common CF challenge phrases or cdn-cgi assets
        """
        try:
            ua = (
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/120.0 Safari/537.36'
            )
            req = Request(
                url,
                headers={
                    'User-Agent': ua,
                    'Accept': 'text/html,application/xhtml+xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9,id;q=0.8',
                    'Cache-Control': 'no-cache'
                }
            )
            resp = urlopen(req, timeout=max(1, int(timeout)))
            status = getattr(resp, 'status', None)
            headers = resp.headers or {}
            server = (headers.get('Server') or headers.get('server') or '').lower()
            # Any header key starting with cf- is a strong signal
            has_cf_header = False
            try:
                for k in headers.keys():
                    if str(k).lower().startswith('cf-'):
                        has_cf_header = True
                        break
            except Exception:
                pass
            content_type = (headers.get('Content-Type') or '').lower()

            # Header-based detection
            if server.find('cloudflare') != -1 or has_cf_header:
                if status in (403, 429, 503):
                    return True

            if ('text/html' not in content_type) and ('application/xhtml+xml' not in content_type):
                return False

            raw = resp.read(4096)
            try:
                snippet = raw.decode('utf-8', errors='ignore')
            except Exception:
                snippet = raw.decode('latin-1', errors='ignore')

            final_url = getattr(resp, 'geturl', lambda: url)().lower()
            if self._looks_like_challenge(snippet, final_url):
                return True

            # Weak-signal fallback: Cloudflare server and tiny HTML without a normal body
            if (server.find('cloudflare') != -1 or has_cf_header):
                low = snippet.lower()
                if (len(snippet) < 2048) and ('<body' not in low or 'content="0;url=' in low):
                    return True

            return False
        except Exception:
            return False

    def _try_static_fetch(self, url: str) -> Optional[Dict]:
        """
        Perform a fast static HTTP GET and return a scrape-like result if suitable.

        Returns None if content is not HTML, looks like a challenge, or errors occur.
        """
        start_time = time.time()
        try:
            ua = (
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/120.0 Safari/537.36'
            )
            req = Request(
                url,
                headers={
                    'User-Agent': ua,
                    'Accept': 'text/html,application/xhtml+xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9,id;q=0.8'
                }
            )
            resp = urlopen(req, timeout=min(self.timeout, 8))  # Reduced from 12s → 8s
            content_type = (resp.headers.get('Content-Type') or '')
            content_type_lower = content_type.lower()
            if ('text/html' not in content_type_lower) and ('application/xhtml+xml' not in content_type_lower):
                return None
            raw = resp.read()
            # FIX: Use proper encoding detection for non-Latin characters (CJK, Arabic, etc.)
            html = decode_html_content(raw, content_type)
            final_url = getattr(resp, 'geturl', lambda: url)()

            # Heuristic: skip if content looks like a challenge or is too short
            if self._looks_like_challenge(html, final_url) or len(html) < 400:
                if self.skip_on_challenge:
                    return {
                        'status': 503,
                        'html': '',
                        'url': url,
                        'final_url': final_url,
                        'error': 'cloudflare_challenge_detected_skip_static',
                        'load_time': time.time() - start_time,
                        'page_title': '',
                        'meta_description': '',
                        'is_contact_page': False
                    }
                return None

            result = {
                'status': getattr(resp, 'status', 200) or 200,
                'html': html,
                'url': url,
                'final_url': final_url,
                'error': None,
                'load_time': time.time() - start_time,
                'page_title': '',
                'meta_description': '',
                'is_contact_page': False
            }

            if html:
                soup = BeautifulSoup(html, 'html.parser')
                title_tag = soup.find('title')
                if title_tag:
                    result['page_title'] = title_tag.get_text().strip()
                meta_desc = soup.find('meta', attrs={'name': 'description'})
                if meta_desc:
                    result['meta_description'] = meta_desc.get('content', '').strip()
                result['is_contact_page'] = self.is_contact_page(html, final_url)

            return result
        except (HTTPError, URLError) as e:
            # ====================================================================
            # SSL-SPECIFIC ERROR HANDLING
            # ====================================================================
            if SSLErrorHandler.is_ssl_error(e):
                SSLErrorHandler.log_ssl_error(e, url, context="static_fetch")
                strategy = SSLErrorHandler.get_recovery_strategy(e, url)
                self.logger.debug(f"SSL error recovery strategy: {strategy}")

                if strategy['should_skip']:
                    # Return explicit error result for skipped URLs
                    return {
                        'status': 0,
                        'html': '',
                        'url': url,
                        'final_url': url,
                        'error': f"SSL Error ({strategy['error_type']}): {strategy['reason']}",
                        'load_time': time.time() - start_time,
                        'page_title': '',
                        'meta_description': '',
                        'is_contact_page': False
                    }
                # For retryable SSL errors, return None to fallback to dynamic fetch
                # Dynamic fetch (Playwright) might handle SSL better
                return None
            # Network or HTTP error on static path -> defer to dynamic
            return None
        except Exception:
            return None

    def _detect_if_needs_proxy(self, url: str, attempt_result: Dict) -> Dict:
        """
        Detect if proxy is needed based on first attempt result.
        Smart detection to avoid wasting proxy bandwidth on sites that don't need it.

        Args:
            url: Target URL
            attempt_result: Result from first attempt (direct, no proxy)

        Returns:
            {
                'use_proxy': bool,
                'timeout': int,
                'reason': str,
                'skip': bool
            }
        """
        # CATEGORY 1: Fast-fail scenarios based on error messages
        if attempt_result.get('error'):
            error_msg = str(attempt_result['error']).lower()

            # Cloudflare wait page detected and persisted
            if 'cloudflare wait page' in error_msg and 'persisted' in error_msg:
                return {
                    'use_proxy': True,
                    'timeout': 75,  # Increased from 60s → 75s
                    'reason': 'CF_wait_exceeded',
                    'skip': False
                }

            # Cloudflare challenge detected
            if 'cloudflare challenge' in error_msg:
                return {
                    'use_proxy': True,
                    'timeout': 75,  # Increased from 60s → 75s
                    'reason': 'CF_challenge',
                    'skip': False
                }

            # Generic timeout without CF signature - try proxy with shorter timeout
            if 'timeout' in error_msg and 'cloudflare' not in error_msg:
                elapsed = attempt_result.get('load_time', 0)
                if elapsed >= 50:  # Long timeout suggests server slowness
                    return {
                        'use_proxy': True,
                        'timeout': 60,  # No change - 60s is good for slow sites
                        'reason': 'long_timeout',
                        'skip': False
                    }
                else:
                    return {
                        'use_proxy': True,
                        'timeout': 30,  # Keep at 30s for quick timeouts (already good)
                        'reason': 'quick_timeout',
                        'skip': False
                    }

            # Dynamic fetch timeout
            if 'dynamic_fetch_timeout' in error_msg:
                return {
                    'use_proxy': True,
                    'timeout': 45,
                    'reason': 'dynamic_timeout',
                    'skip': False
                }

        # CATEGORY 2: Status code analysis
        status = attempt_result.get('status', 0)

        if status == 403:
            return {
                'use_proxy': True,
                'timeout': 30,
                'reason': 'forbidden_403',
                'skip': False
            }

        if status == 429:
            return {
                'use_proxy': True,
                'timeout': 30,
                'reason': 'rate_limited_429',
                'skip': False
            }

        if status == 503:
            return {
                'use_proxy': True,
                'timeout': 45,
                'reason': 'service_unavailable_503',
                'skip': False
            }

        if status == 408:  # Request timeout
            elapsed = attempt_result.get('load_time', 0)
            if elapsed >= 60:
                return {
                    'use_proxy': True,
                    'timeout': 60,
                    'reason': 'timeout_408_long',
                    'skip': False
                }
            else:
                return {
                    'use_proxy': True,
                    'timeout': 30,
                    'reason': 'timeout_408_quick',
                    'skip': False
                }

        # CATEGORY 3: Content analysis (if HTML available)
        html = attempt_result.get('html', '')
        if html:
            html_lower = html.lower()

            # Cloudflare signatures
            cf_patterns = [
                'cloudflare',
                'cf-ray',
                'cf_chl_',
                'challenge-platform',
                'turnstile',
                'checking your browser',
                '__cf_bm',
                'cf-browser-verification',
                'cdn-cgi/challenge'
            ]

            if any(pattern in html_lower for pattern in cf_patterns):
                return {
                    'use_proxy': True,
                    'timeout': 75,  # Increased from 60s → 75s
                    'reason': 'CF_detected_content',
                    'skip': False
                }

            # Other anti-bot services
            antibot_patterns = [
                'recaptcha',
                'hcaptcha',
                'distil_r_captcha',
                'perimeterx',
                'px-captcha',
                'you have been blocked',
                'access denied',
                'rate limit exceeded',
                'datadome'
            ]

            if any(pattern in html_lower for pattern in antibot_patterns):
                return {
                    'use_proxy': True,
                    'timeout': 60,  # Increased from 45s → 60s
                    'reason': 'antibot_detected',
                    'skip': False
                }

            # Suspiciously small HTML (likely challenge page)
            if len(html) < 200:
                return {
                    'use_proxy': True,
                    'timeout': 30,
                    'reason': 'small_html',
                    'skip': False
                }

        # CATEGORY 4: Check if marked as requiring proxy
        if attempt_result.get('requires_proxy'):
            return {
                'use_proxy': True,
                'timeout': 75,  # Increased from 60s → 75s
                'reason': 'explicit_proxy_flag',
                'skip': False
            }

        # Default: No clear signal that proxy is needed
        return {
            'use_proxy': False,
            'timeout': 0,
            'reason': 'no_proxy_needed',
            'skip': False
        }

    def get_proxy_efficiency_report(self) -> Dict:
        """
        Get proxy usage efficiency metrics.

        Returns:
            Dictionary with efficiency statistics
        """
        total = self.proxy_stats['total_requests']
        if total == 0:
            return {
                'total_requests': 0,
                'proxy_avoidance_rate': 0.0,
                'proxy_success_rate': 0.0,
                'bandwidth_efficiency': 0.0,
                'time_saved_seconds': 0.0
            }

        proxy_used = self.proxy_stats['proxy_used']
        proxy_justified = self.proxy_stats['proxy_justified']

        return {
            'total_requests': total,
            'proxy_avoided': self.proxy_stats['proxy_avoided'],
            'proxy_used': proxy_used,
            'proxy_avoidance_rate': (self.proxy_stats['proxy_avoided'] / total) * 100,
            'proxy_success_rate': (proxy_justified / proxy_used * 100) if proxy_used > 0 else 0.0,
            'bandwidth_efficiency': (self.proxy_stats['proxy_avoided'] / total) * 100,
            'direct_success': self.proxy_stats['direct_success'],
            'direct_failed': self.proxy_stats['direct_failed'],
            'proxy_success': self.proxy_stats['proxy_success'],
            'proxy_failed': self.proxy_stats['proxy_failed'],
            'proxy_retries': self.proxy_stats['proxy_retries'],
            'time_saved_seconds': self.proxy_stats['time_saved_seconds']
        }

    def _fetch_with_timeout(self, url: str, timeout: int, google_search: bool, network_idle: bool, use_proxy: bool = True):
        """
        Perform StealthyFetcher.fetch in an isolated subprocess with a hard wall-clock timeout.
        Returns a SimpleNamespace page-like object on success, or None on timeout/error.
        This prevents indefinite blocking on Cloudflare wait pages and silences child logs.

        Args:
            url: Target URL
            timeout: Timeout in seconds
            google_search: Whether to use Google search mode
            network_idle: Wait for network idle
            use_proxy: Whether to use proxy (smart proxy mode)
        """
        # Get proxy configuration ONLY if use_proxy is True
        proxy_config = None
        if use_proxy and self.proxy_manager.has_proxies():
            proxy_info = self.proxy_manager.get_next_proxy()
            if proxy_info:
                proxy_config = self.proxy_manager.convert_to_playwright_format(proxy_info)

        # Acquire browser semaphore to limit concurrent browser instances
        # This prevents Camoufox crashes from too many concurrent processes
        # Timeout = scraping timeout + 30s buffer (dynamic based on page complexity)
        semaphore_timeout = self.timeout + 30
        acquired = False
        try:
            acquired = self.browser_semaphore.acquire(timeout=semaphore_timeout)
            if not acquired:
                self.logger.warning(f"Browser semaphore timeout for {url} after {semaphore_timeout}s - increase --workers or reduce concurrent load")
                return None

            result_q = multiprocessing.Queue()
            proc = multiprocessing.Process(
                target=_subprocess_fetch,
                args=(
                    result_q,
                    url,
                    self.headless,
                    self.solve_cloudflare,
                    network_idle,
                    google_search,
                    timeout,
                    self.block_images,
                    self.disable_resources,
                    proxy_config
                ),
                daemon=False  # Changed from True: allows graceful cleanup on exit
            )
            try:
                proc.start()
            except Exception:
                # Fallback to immediate failure if process cannot start
                return None

            msg = None
            try:
                msg = result_q.get(timeout=max(1, int(timeout)))
            except Exception:
                msg = None

            if msg is None:
                try:
                    proc.terminate()
                except Exception:
                    pass
                try:
                    proc.join(timeout=3)
                except Exception:
                    pass
                try:
                    self.logger.error(f"Dynamic fetch timeout: {url}")
                except Exception:
                    pass
                return None

            # Ensure child exits cleanly
            try:
                proc.join(timeout=3)
                if proc.is_alive():
                    proc.terminate()
                    proc.join(timeout=2)
                    if proc.is_alive():
                        proc.kill()
                        proc.join()
            except (BrokenPipeError, OSError) as e:
                # EPIPE during shutdown is expected when user presses Ctrl+C
                import errno
                if hasattr(e, 'errno') and e.errno == errno.EPIPE:
                    self.logger.debug(f"⚠️  EPIPE during process cleanup (shutdown): {e}")
                else:
                    self.logger.error(f"Process cleanup error: {e}")
                try:
                    if proc.is_alive():
                        proc.kill()
                        proc.join()
                except Exception:
                    pass
            except Exception as e:
                try:
                    self.logger.error(f"Process cleanup error: {e}")
                except Exception:
                    pass
                try:
                    if proc.is_alive():
                        proc.kill()
                        proc.join()
                except Exception:
                    pass
            finally:
                # Clean up queue resources
                try:
                    # Use cancel_join_thread() to prevent deadlock if subprocess crashed
                    # before writing to queue. join_thread() can block indefinitely.
                    result_q.cancel_join_thread()
                    result_q.close()
                except Exception:
                    pass

            if msg.get('ok'):
                return SimpleNamespace(
                    status=msg.get('status', 200),
                    html_content=msg.get('html_content', ''),
                    url=msg.get('final_url', url),
                    proxy_used=proxy_config.get('server') if proxy_config else None
                )
            else:
                # ====================================================================
                # SSL ERROR DETECTION & LOGGING (Playwright/Firefox errors)
                # ====================================================================
                error_msg = msg.get('error', '')
                error_type = msg.get('error_type', 'unknown')

                # Check for SSL errors
                if error_type.startswith('ssl_certificate') or 'SEC_ERROR' in error_msg or 'SSL_ERROR' in error_msg:
                    try:
                        # Create exception-like object for SSLErrorHandler
                        class PlaywrightSSLError(Exception):
                            pass
                        ssl_exception = PlaywrightSSLError(error_msg)

                        # Use SSLErrorHandler to classify and log
                        if SSLErrorHandler.is_ssl_error(ssl_exception):
                            SSLErrorHandler.log_ssl_error(ssl_exception, url, context="dynamic_fetch")
                            strategy = SSLErrorHandler.get_recovery_strategy(ssl_exception, url)
                            self.logger.debug(f"SSL recovery strategy: {strategy}")
                    except Exception:
                        pass

                # Mark proxy as failed if proxy-related error
                if proxy_config and 'server' in proxy_config:
                    if any(keyword in error_msg.lower() for keyword in ['proxy', 'connection', 'timeout', 'refused']):
                        self.proxy_manager.mark_proxy_failed(proxy_config['server'], reason=error_msg)

                try:
                    self.logger.error(f"Dynamic fetch error: {url} | Type: {error_type} | {error_msg}")
                except Exception:
                    pass
            return None
        except Exception as e:
            try:
                self.logger.error(f"Dynamic fetch fatal error: {e}")
            except Exception:
                pass
            return None
        finally:
            if acquired:
                try:
                    self.browser_semaphore.release()
                except Exception:
                    pass  # Suppress errors during shutdown

    def scrape_url(self, url: str) -> Dict:
        """
        Fetch webpage content with smart proxy usage and anti-bot bypass.

        SMART PROXY MODE:
        - Attempt 1: Direct (no proxy) with fast timeout
        - Detection: Analyze if proxy is needed
        - Attempt 2-4: With proxy (up to 3 retries with different proxies)

        This dramatically reduces proxy bandwidth usage while maintaining success rate.

        Args:
            url (str): Target URL to scrape

        Returns:
            Dict: Scraping result with status, HTML content, and metadata
        """
        result = {
            'status': 0,
            'html': '',
            'url': url,
            'final_url': url,
            'error': None,
            'load_time': 0,
            'page_title': '',
            'meta_description': '',
            'is_contact_page': False,
            'proxy_used': False
        }

        start_time = time.time()
        self.proxy_stats['total_requests'] += 1
        total_budget = 60  # Default budget (will be overridden after detection)

        # Set domain context for Scrapling logs
        _SCRAPLING_CONTEXT.domain = (urlparse(url).netloc or '').lower()

        try:
            # ===== ATTEMPT 1: Direct (No Proxy) - Fast attempt =====
            attempt1_budget = 15  # seconds

            # Step 1.1: Quick Cloudflare wait page detection
            if self.solve_cloudflare:
                try:
                    if self._is_cloudflare_wait_page(url, timeout=3):
                        # Cloudflare detected - skip direct attempt, go straight to proxy
                        result['error'] = 'cloudflare_wait_detected'
                        result['status'] = 503
                        result['requires_proxy'] = True
                        self.logger.info(f"🔴 CF wait page detected, skipping direct attempt: {url}")
                        # Jump to proxy mode immediately
                        pass
                    else:
                        # No CF wait page, try static first
                        if self.static_first:
                            static_result = self._try_static_fetch(url)
                            if static_result and static_result.get('status') == 200:
                                # ✅ SUCCESS without proxy!
                                static_result['proxy_used'] = False
                                static_result['load_time'] = time.time() - start_time
                                self.proxy_stats['proxy_avoided'] += 1
                                self.proxy_stats['direct_success'] += 1
                                self.proxy_stats['time_saved_seconds'] += (total_budget - static_result['load_time'])
                                return static_result
                except Exception:
                    pass

            # Step 1.2: Try quick dynamic fetch without proxy
            elapsed = time.time() - start_time
            remaining = total_budget - elapsed

            if remaining > 10 and not result.get('requires_proxy'):
                # Try dynamic WITHOUT proxy, short timeout
                page = self._fetch_with_timeout(
                    url,
                    timeout=int(min(10, remaining)),
                    google_search=False,
                    network_idle=False,  # Fast mode
                    use_proxy=False  # KEY: No proxy on first attempt
                )

                if page and page.status == 200:
                    # ✅ SUCCESS without proxy!
                    result['html'] = page.html_content
                    result['status'] = 200
                    result['final_url'] = page.url
                    result['load_time'] = time.time() - start_time
                    result['proxy_used'] = False
                    self.proxy_stats['proxy_avoided'] += 1
                    self.proxy_stats['direct_success'] += 1
                    self.proxy_stats['time_saved_seconds'] += (total_budget - result['load_time'])

                    # Extract metadata
                    if result['html']:
                        soup = BeautifulSoup(result['html'], 'html.parser')
                        title_tag = soup.find('title')
                        if title_tag:
                            result['page_title'] = title_tag.get_text().strip()
                        meta_desc = soup.find('meta', attrs={'name': 'description'})
                        if meta_desc:
                            result['meta_description'] = meta_desc.get('content', '').strip()
                        result['is_contact_page'] = self.is_contact_page(result['html'], result['final_url'])

                    return result

                # Store attempt 1 result for detection
                attempt1_result = {
                    'status': page.status if page else 0,
                    'html': page.html_content if page else '',
                    'error': result.get('error') or ('dynamic_fetch_timeout' if not page else ''),
                    'load_time': time.time() - start_time
                }
                self.proxy_stats['direct_failed'] += 1
            else:
                # Budget exhausted or CF detected
                attempt1_result = {
                    'status': result.get('status', 0),
                    'html': result.get('html', ''),
                    'error': result.get('error', ''),
                    'load_time': time.time() - start_time,
                    'requires_proxy': result.get('requires_proxy', False)
                }

            # ===== DETECTION PHASE =====
            detection = self._detect_if_needs_proxy(url, attempt1_result)

            # ===== ADAPTIVE BUDGET CALCULATION =====
            # Set total_budget based on error type for optimal time allocation
            if detection['reason'] in ['CF_wait_exceeded', 'CF_challenge', 'CF_detected_content', 'explicit_proxy_flag']:
                total_budget = self.challenge_budget  # Challenge websites need more time
            elif detection['reason'] in ['connection_refused', 'dns_error', 'invalid_url']:
                total_budget = self.dead_site_budget   # Dead sites should fail fast
            else:
                total_budget = self.normal_budget   # Normal websites (timeouts, 403, 429, etc)

            self.logger.debug(f"⏱️  Adaptive budget for {detection['reason']}: {total_budget}s")

            if detection['skip']:
                # Fast-fail, don't waste proxy bandwidth
                result['error'] = f"Skipped: {detection['reason']}"
                result['load_time'] = time.time() - start_time
                return result

            if not detection['use_proxy']:
                # Detection says no proxy needed, but attempt 1 failed
                result['error'] = 'failed_without_proxy_indication'
                result['load_time'] = time.time() - start_time
                return result

            # ===== ATTEMPT 2-4: With Proxy (Retry up to 3 times) =====
            # Adaptive max_retries based on detection reason
            if detection['reason'] in ['CF_challenge', 'CF_wait_exceeded', 'CF_detected_content', 'antibot_detected']:
                max_retries = 2  # Challenge sites: fewer retries, longer timeout per attempt
            elif detection['reason'] in ['connection_refused', 'dns_error', 'invalid_url']:
                max_retries = 1  # Dead sites: fail fast (only 1 retry)
            else:
                max_retries = 3  # Normal errors: standard retries

            self.logger.debug(f"⏱️  Adaptive max_retries for {detection['reason']}: {max_retries}")

            tried_proxies = []

            for retry_num in range(max_retries):
                elapsed = time.time() - start_time
                remaining = total_budget - elapsed

                if remaining < self.min_retry_threshold:
                    result['error'] = 'insufficient_budget_for_proxy'
                    result['load_time'] = time.time() - start_time
                    break

                # Get different proxy for each retry
                if retry_num == 0:
                    proxy_timeout = int(min(detection['timeout'], remaining))
                    retry_msg = "initial"
                else:
                    # Adaptive retry timeout based on detection reason
                    if detection['reason'] in ['CF_challenge', 'CF_wait_exceeded', 'CF_detected_content']:
                        # Challenge sites need more time on retry
                        base_retry_timeout = 60
                    elif detection['reason'] in ['antibot_detected']:
                        base_retry_timeout = 50
                    else:
                        # Other errors can use shorter retry timeout
                        base_retry_timeout = 25

                    proxy_timeout = int(min(base_retry_timeout, remaining))
                    retry_msg = f"retry {retry_num}"
                    self.proxy_stats['proxy_retries'] += 1

                self.logger.info(
                    f"🔄 Proxy attempt ({retry_msg}) | Reason: {detection['reason']} | "
                    f"Timeout: {proxy_timeout}s | URL: {url}"
                )

                page = self._fetch_with_timeout(
                    url,
                    timeout=proxy_timeout,
                    google_search=False,
                    network_idle=True,  # Full mode for challenges
                    use_proxy=True  # Use proxy now
                )

                if page and page.status == 200 and page.html_content:
                    # Check if it's actually successful (not a challenge page)
                    if not self._looks_like_challenge(page.html_content, page.url):
                        # ✅ SUCCESS with proxy
                        result['html'] = page.html_content
                        result['status'] = 200
                        result['final_url'] = page.url
                        result['load_time'] = time.time() - start_time
                        result['proxy_used'] = True
                        self.proxy_stats['proxy_used'] += 1
                        self.proxy_stats['proxy_justified'] += 1
                        self.proxy_stats['proxy_success'] += 1

                        # Extract metadata
                        soup = BeautifulSoup(result['html'], 'html.parser')
                        title_tag = soup.find('title')
                        if title_tag:
                            result['page_title'] = title_tag.get_text().strip()
                        meta_desc = soup.find('meta', attrs={'name': 'description'})
                        if meta_desc:
                            result['meta_description'] = meta_desc.get('content', '').strip()
                        result['is_contact_page'] = self.is_contact_page(result['html'], result['final_url'])

                        return result

                # Failed with this proxy, try another
                if page and hasattr(page, 'proxy_used') and page.proxy_used:
                    tried_proxies.append(page.proxy_used)

                # Last retry?
                if retry_num == max_retries - 1:
                    # All retries exhausted
                    result['error'] = f"Failed after {max_retries} proxy attempts: {detection['reason']}"
                    result['status'] = page.status if page else 0
                    result['load_time'] = time.time() - start_time
                    result['proxy_used'] = True
                    self.proxy_stats['proxy_used'] += max_retries
                    self.proxy_stats['proxy_wasted'] += max_retries
                    self.proxy_stats['proxy_failed'] += 1
                    break

            return result

        except Exception as e:
            elapsed = time.time() - start_time
            result['error'] = str(e)
            if result['status'] == 0:
                result['status'] = 500
            result['load_time'] = elapsed
            return result

        finally:
            # Clear domain context
            try:
                _SCRAPLING_CONTEXT.domain = None
            except Exception:
                pass

    def gather_contact_info(self, url: str) -> Dict:
        """
        Comprehensive contact information gathering inspired by sampler.txt.
        Scrapes main page and contact pages for email and phone information.

        Args:
            url (str): Target URL to scrape

        Returns:
            Dict: Comprehensive contact information
        """
        # ====================================================================
        # STEP 1: Clean URL before scraping
        # ====================================================================
        # Handles Google redirects, tracking params, encoding issues, etc.
        original_url = url
        cleaned_url = URLCleaner.clean_url(url, aggressive=False)

        if not cleaned_url:
            return {
                'website': original_url,
                'emails': [],
                'phones': [],
                'whatsapp': [],
                'pages_scraped': [],
                'status': 'failed',
                'error': f'Invalid URL format (failed cleanup): {original_url}'
            }

        if cleaned_url != original_url:
            self.logger.debug(f"URL cleaned before scraping: '{original_url}' → '{cleaned_url}'")

        url = cleaned_url

        result = {
            'website': url,
            'emails': [],
            'phones': [],
            'whatsapp': [],
            'pages_scraped': [],
            'status': 'failed',
            'error': None
        }

        try:
            # First, scrape the main page
            self.logger.debug(f"Gathering contact info from: {url}")
            main_result = self.scrape_url(url)

            if main_result.get('error'):
                result['error'] = main_result['error']
                self.logger.debug(f"Failed to scrape main page: {url} | error: {main_result['error']}")
                return result

            if not main_result.get('html'):
                result['error'] = 'No HTML content retrieved'
                self.logger.debug(f"No HTML content from: {url}")
                return result

            self.logger.debug(f"Main page scraped successfully: {url}")

            # Extract contacts prioritizing header > footer > structured data > general page
            from contact_extractor import ContactExtractor
            extractor = ContactExtractor()

            soup = BeautifulSoup(main_result['html'], 'html.parser')

            # 1) Header-first scan
            header_selectors = 'header, #header, .header, .site-header, .main-header, nav, #nav, .navbar, .navigation'
            for section in soup.select(header_selectors):
                contacts = extractor.extract_all_contacts(str(section), main_result['final_url'])
                for contact in contacts:
                    field = contact.get('field')
                    value = contact.get('value_normalized') or contact.get('value_raw')
                    if not value:
                        continue
                    if field == 'email':
                        result['emails'].append(value)
                    elif field == 'phone':
                        result['phones'].append(value)
                    elif field == 'whatsapp':
                        result['whatsapp'].append(value)

            # 2) Footer scan
            footer_selectors = 'footer, #footer, .footer, .site-footer, .main-footer'
            for section in soup.select(footer_selectors):
                contacts = extractor.extract_all_contacts(str(section), main_result['final_url'])
                for contact in contacts:
                    field = contact.get('field')
                    value = contact.get('value_normalized') or contact.get('value_raw')
                    if not value:
                        continue
                    if field == 'email':
                        result['emails'].append(value)
                    elif field == 'phone':
                        result['phones'].append(value)
                    elif field == 'whatsapp':
                        result['whatsapp'].append(value)

            # 3) Structured data (JSON-LD) scan
            structured = self.extract_structured_data(main_result['html'])
            for em in structured.get('emails', []):
                result['emails'].append(em)
            for ph in structured.get('phones', []):
                result['phones'].append(ph)

            # 4) General page scan as fallback
            main_contacts = extractor.extract_all_contacts(main_result['html'])
            for contact in main_contacts:
                field = contact.get('field')
                value = contact.get('value_normalized') or contact.get('value_raw')
                if not value:
                    continue
                if field == 'email':
                    result['emails'].append(value)
                elif field == 'phone':
                    result['phones'].append(value)
                elif field == 'whatsapp':
                    result['whatsapp'].append(value)
            result['pages_scraped'].append({
                'url': main_result['final_url'],
                'title': main_result.get('page_title', ''),
                'is_contact_page': main_result.get('is_contact_page', False)
            })

            # Look for contact page links
            soup = BeautifulSoup(main_result['html'], 'html.parser')
            contact_links = self._find_contact_links(soup, main_result['final_url'], self._get_priority_paths())

            if contact_links:
                self.logger.debug(f"Found {len(contact_links)} contact page links for {url}")

            # Scrape contact pages for additional information
            for contact_url in contact_links[:3]:  # Limit to 3 contact pages
                self.logger.debug(f"Scraping contact page: {contact_url}")
                contact_result = self.scrape_url(contact_url)

                if contact_result.get('html') and not contact_result.get('error'):
                    # Structured data first on contact page
                    structured_cp = self.extract_structured_data(contact_result['html'])
                    for em in structured_cp.get('emails', []):
                        result['emails'].append(em)
                    for ph in structured_cp.get('phones', []):
                        result['phones'].append(ph)

                    # Then header/footer sections on contact page
                    cp_soup = BeautifulSoup(contact_result['html'], 'html.parser')
                    for section in cp_soup.select(header_selectors):
                        contacts = extractor.extract_all_contacts(str(section), contact_result['final_url'])
                        for contact in contacts:
                            field = contact.get('field')
                            value = contact.get('value_normalized') or contact.get('value_raw')
                            if not value:
                                continue
                            if field == 'email':
                                result['emails'].append(value)
                            elif field == 'phone':
                                result['phones'].append(value)
                            elif field == 'whatsapp':
                                result['whatsapp'].append(value)

                    for section in cp_soup.select(footer_selectors):
                        contacts = extractor.extract_all_contacts(str(section), contact_result['final_url'])
                        for contact in contacts:
                            field = contact.get('field')
                            value = contact.get('value_normalized') or contact.get('value_raw')
                            if not value:
                                continue
                            if field == 'email':
                                result['emails'].append(value)
                            elif field == 'phone':
                                result['phones'].append(value)
                            elif field == 'whatsapp':
                                result['whatsapp'].append(value)

                    # Finally, general page scan
                    contact_contacts = extractor.extract_all_contacts(contact_result['html'])
                    for contact in contact_contacts:
                        field = contact.get('field')
                        value = contact.get('value_normalized') or contact.get('value_raw')
                        if not value:
                            continue
                        if field == 'email':
                            result['emails'].append(value)
                        elif field == 'phone':
                            result['phones'].append(value)
                        elif field == 'whatsapp':
                            result['whatsapp'].append(value)
                    result['pages_scraped'].append({
                        'url': contact_result['final_url'],
                        'title': contact_result.get('page_title', ''),
                        'is_contact_page': contact_result.get('is_contact_page', True)
                    })

            # Remove duplicates
            # Final cleanup: normalize emails to strip accidental tokens then deduplicate
            try:
                cleaned_emails = []
                for em in result['emails']:
                    norm = extractor._normalize_email(em)
                    if norm:
                        cleaned_emails.append(norm)
                # Preserve order while deduplicating
                result['emails'] = list(dict.fromkeys(cleaned_emails))
            except Exception:
                result['emails'] = list(set(result['emails']))
            result['phones'] = list(set(result['phones']))
            result['whatsapp'] = list(set(result['whatsapp']))

            # Set status based on results
            if result['emails'] or result['phones'] or result['whatsapp']:
                result['status'] = 'success'
                self.logger.debug(f"Contact extraction complete: {url} | emails={len(result['emails'])} phones={len(result['phones'])} whatsapp={len(result['whatsapp'])} pages={len(result['pages_scraped'])}")
            else:
                result['status'] = 'no_contacts_found'
                self.logger.debug(f"No contacts found on: {url}")

        except Exception as e:
            result['error'] = str(e)
            result['status'] = 'error'
            self.logger.debug(f"Error gathering contacts from {url}: {str(e)}")

        return result

    def _get_priority_paths(self) -> List[str]:
        """Get priority paths for contact page detection"""
        return [
            # English variants
            '/contact', '/contact-us', '/contacts', '/contactus', '/contact_us',
            '/about', '/about-us', '/aboutus', '/about_us', '/company',
            '/support', '/customer-service', '/customer_support', '/help',

            # Indonesian variants
            '/kontak', '/hubungi', '/hubungi-kami', '/tentang', '/tentang-kami',
            '/layanan', '/bantuan', '/dukungan', '/informasi',

            # Common patterns
            '/info', '/information', '/reach-us', '/get-in-touch'
        ]
        """
        Scrape multiple pages from a website, focusing on contact-related pages.

        Args:
            url (str): Base URL to start scraping
            max_pages (int): Maximum number of pages to scrape

        Returns:
            List[Dict]: List of scraping results for each page
        """
        results = []
        visited_urls = set()

        # Priority pages to look for
        # Enhanced priority pages with multi-language support
        priority_paths = [
            # English variants
            '/contact', '/contact-us', '/contacts', '/contactus', '/contact_us',
            '/about', '/about-us', '/aboutus', '/about_us', '/company',
            '/support', '/customer-service', '/customer_support', '/help', '/faq',
            '/sales', '/info', '/information', '/reach-us', '/get-in-touch',

            # Indonesian variants
            '/kontak', '/hubungi', '/hubungi-kami', '/tentang', '/tentang-kami',
            '/layanan', '/bantuan', '/dukungan', '/informasi', '/perusahaan',

            # Spanish variants
            '/contacto', '/contactanos', '/acerca', '/acerca-de', '/sobre-nosotros',
            '/soporte', '/ayuda', '/informacion', '/empresa', '/servicio-cliente',

            # French variants
            '/contact', '/contactez-nous', '/a-propos', '/apropos', '/sur-nous',
            '/support', '/aide', '/assistance', '/service-client', '/entreprise',

            # German variants
            '/kontakt', '/kontaktieren', '/uber-uns', '/ueber-uns', '/unternehmen',
            '/support', '/hilfe', '/kundenservice', '/kundendienst', '/info',

            # Portuguese variants
            '/contato', '/contatos', '/fale-conosco', '/sobre', '/sobre-nos',
            '/suporte', '/ajuda', '/atendimento', '/empresa', '/informacoes',

            # Italian variants
            '/contatto', '/contatti', '/contattaci', '/chi-siamo', '/azienda',
            '/supporto', '/aiuto', '/assistenza', '/servizio-clienti', '/info',

            # Dutch variants
            '/contact', '/contacteer', '/over-ons', '/bedrijf', '/ondersteuning',
            '/hulp', '/klantenservice', '/informatie', '/bereik-ons',

            # Russian variants (transliterated)
            '/kontakt', '/kontakty', '/o-nas', '/o-kompanii', '/podderzhka',
            '/pomosh', '/informatsiya', '/svyazatsya', '/obsluzhivanie',

            # Chinese (Pinyin)
            '/lianxi', '/guanyu', '/bangzhu', '/kefu', '/fuwu', '/xinxi',

            # Japanese (Romaji)
            '/otoiawase', '/kaisha', '/kaisya', '/support', '/help', '/info',

            # Arabic (transliterated)
            '/ittasal', '/hawl', '/daem', '/musaada', '/malumat', '/sharikat',

            # Common patterns across languages
            '/team', '/staff', '/office', '/location', '/address', '/phone',
            '/email', '/form', '/inquiry', '/enquiry', '/feedback', '/message',
            '/reach', '/connect', '/communicate', '/talk', '/chat', '/call',

            # Business-specific paths
            '/sales-team', '/business', '/partnership', '/investor', '/media',
            '/press', '/careers', '/jobs', '/hr', '/recruitment', '/legal',
            '/privacy', '/terms', '/policy', '/compliance', '/security',

            # Regional/Country specific
            '/us', '/usa', '/uk', '/eu', '/asia', '/global', '/international',
            '/local', '/regional', '/branch', '/subsidiary', '/affiliate'
        ]

        # Start with the main page
        main_result = self.scrape_url(url)
        results.append(main_result)
        visited_urls.add(url)

        if main_result['status'] != 200 or not main_result['html']:
            return results

        # Parse the main page to find contact-related links
        soup = BeautifulSoup(main_result['html'], 'html.parser')
        base_url = self._get_base_url(main_result['final_url'])

        # Find potential contact pages
        contact_links = self._find_contact_links(soup, base_url, priority_paths)

        # Scrape additional pages up to max_pages limit
        scraped_count = 1
        for link_url in contact_links:
            if scraped_count >= max_pages:
                break

            if link_url not in visited_urls:
                page_result = self.scrape_url(link_url)
                results.append(page_result)
                visited_urls.add(link_url)
                scraped_count += 1

                # Small delay between requests to be respectful
                time.sleep(1)

        return results

    def _get_base_url(self, url: str) -> str:
        """Get base URL from a full URL."""
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"

    def _find_contact_links(self, soup: BeautifulSoup, base_url: str, priority_paths: List[str]) -> List[str]:
        """
        Find contact-related links on a page.

        Args:
            soup (BeautifulSoup): Parsed HTML
            base_url (str): Base URL for resolving relative links
            priority_paths (List[str]): Priority paths to look for

        Returns:
            List[str]: List of contact-related URLs
        """
        contact_links = []
        found_paths = set()

        # Find all links
        all_links = soup.find_all('a', href=True)

        # Enhanced contact-related keywords with multi-language support
        contact_keywords = [
            # English keywords
            'contact', 'about', 'support', 'customer', 'sales', 'info', 'help',
            'cs', 'service', 'team', 'staff', 'office', 'reach', 'connect',
            'get in touch', 'talk to us', 'call us', 'email us', 'message',
            'inquiry', 'enquiry', 'feedback', 'company', 'business', 'corporate',

            # Indonesian keywords
            'kontak', 'hubungi', 'tentang', 'layanan', 'bantuan', 'dukungan',
            'informasi', 'perusahaan', 'tim', 'kantor', 'alamat', 'telepon',
            'email', 'formulir', 'pertanyaan', 'masukan', 'saran',

            # Spanish keywords
            'contacto', 'acerca', 'sobre', 'soporte', 'ayuda', 'servicio',
            'empresa', 'equipo', 'oficina', 'direccion', 'telefono',
            'correo', 'formulario', 'consulta', 'informacion',

            # French keywords
            'contact', 'propos', 'support', 'aide', 'service', 'entreprise',
            'equipe', 'bureau', 'adresse', 'telephone', 'courriel',
            'formulaire', 'demande', 'information', 'assistance',

            # German keywords
            'kontakt', 'uber', 'support', 'hilfe', 'service', 'unternehmen',
            'team', 'buro', 'adresse', 'telefon', 'email', 'formular',
            'anfrage', 'information', 'kundendienst',

            # Portuguese keywords
            'contato', 'sobre', 'suporte', 'ajuda', 'atendimento', 'empresa',
            'equipe', 'escritorio', 'endereco', 'telefone', 'email',
            'formulario', 'consulta', 'informacao', 'servico',

            # Italian keywords
            'contatto', 'riguardo', 'supporto', 'aiuto', 'servizio', 'azienda',
            'team', 'ufficio', 'indirizzo', 'telefono', 'email', 'modulo',
            'richiesta', 'informazione', 'assistenza',

            # Dutch keywords
            'contact', 'over', 'ondersteuning', 'hulp', 'service', 'bedrijf',
            'team', 'kantoor', 'adres', 'telefoon', 'email', 'formulier',
            'vraag', 'informatie', 'klantenservice',

            # Universal/Common keywords
            'phone', 'tel', 'fax', 'address', 'location', 'map', 'directions',
            'hours', 'schedule', 'appointment', 'booking', 'reservation',
            'quote', 'estimate', 'demo', 'trial', 'consultation', 'meeting'
        ]

        for link in all_links:
            href = link.get('href', '').strip()
            link_text = link.get_text().strip().lower()

            if not href:
                continue

            # Convert relative URLs to absolute
            full_url = urljoin(base_url, href)

            # Skip external links, javascript, mailto, tel links
            if not full_url.startswith(base_url) or \
               href.startswith(('javascript:', 'mailto:', 'tel:')):
                continue

            # Check if it's a priority path
            parsed_url = urlparse(full_url)
            path = parsed_url.path.lower()

            # Check priority paths first
            for priority_path in priority_paths:
                if priority_path in path and path not in found_paths:
                    contact_links.append(full_url)
                    found_paths.add(path)
                    break
            else:
                # Check link text for contact keywords
                for keyword in contact_keywords:
                    if keyword in link_text and path not in found_paths:
                        contact_links.append(full_url)
                        found_paths.add(path)
                        break

        return contact_links[:5]  # Limit to top 5 contact links

    def extract_structured_data(self, html: str) -> Dict:
        """
        Extract structured data (JSON-LD, microdata) from HTML.

        Args:
            html (str): HTML content

        Returns:
            Dict: Extracted structured data
        """
        structured_data = {
            'json_ld': [],
            'organization': {},
            'contact_points': [],
            'emails': [],
            'phones': []
        }

        if not html:
            return structured_data

        soup = BeautifulSoup(html, 'html.parser')
        # Use ContactExtractor for robust email matching in targeted fields
        try:
            from contact_extractor import ContactExtractor
            extractor = ContactExtractor()
        except Exception:
            extractor = None

        # Helper to normalize email strings robustly
        def _clean_email_str(s: str) -> Optional[str]:
            if not s:
                return None
            try:
                if extractor:
                    return extractor._normalize_email(s)
            except Exception:
                pass
            # Fallback strict pattern
            m = re.search(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}', str(s).lower())
            return m.group(0) if m else None

        def collect_from_json_ld(obj):
            """Collect emails and phones from common JSON-LD structures recursively."""
            if isinstance(obj, dict):
                atype = obj.get('@type') or obj.get('type')
                # Organization, Person, LocalBusiness
                if atype in ('Organization', 'Person', 'LocalBusiness'):
                    email = obj.get('email')
                    telephone = obj.get('telephone')
                    if isinstance(email, str):
                        cleaned = _clean_email_str(email)
                        if cleaned:
                            structured_data['emails'].append(cleaned)
                    elif isinstance(email, list):
                        for e in email:
                            if isinstance(e, str):
                                cleaned = _clean_email_str(e)
                                if cleaned:
                                    structured_data['emails'].append(cleaned)
                    if isinstance(telephone, str):
                        structured_data['phones'].append(telephone)
                    elif isinstance(telephone, list):
                        structured_data['phones'].extend([t for t in telephone if isinstance(t, str)])

                    # ContactPoint(s)
                    contact_point = obj.get('contactPoint') or obj.get('contactPoints')
                    if isinstance(contact_point, dict):
                        cp_email = contact_point.get('email')
                        cp_tel = contact_point.get('telephone')
                        if isinstance(cp_email, str):
                            cleaned = _clean_email_str(cp_email)
                            if cleaned:
                                structured_data['emails'].append(cleaned)
                        elif isinstance(cp_email, list):
                            for e in cp_email:
                                if isinstance(e, str):
                                    cleaned = _clean_email_str(e)
                                    if cleaned:
                                        structured_data['emails'].append(cleaned)
                        if isinstance(cp_tel, str):
                            structured_data['phones'].append(cp_tel)
                        elif isinstance(cp_tel, list):
                            structured_data['phones'].extend([t for t in cp_tel if isinstance(t, str)])
                    elif isinstance(contact_point, list):
                        for cp in contact_point:
                            if isinstance(cp, dict):
                                cp_email = cp.get('email')
                                cp_tel = cp.get('telephone')
                                if isinstance(cp_email, str):
                                    cleaned = _clean_email_str(cp_email)
                                    if cleaned:
                                        structured_data['emails'].append(cleaned)
                                elif isinstance(cp_email, list):
                                    for e in cp_email:
                                        if isinstance(e, str):
                                            cleaned = _clean_email_str(e)
                                            if cleaned:
                                                structured_data['emails'].append(cleaned)
                                if isinstance(cp_tel, str):
                                    structured_data['phones'].append(cp_tel)
                                elif isinstance(cp_tel, list):
                                    structured_data['phones'].extend([t for t in cp_tel if isinstance(t, str)])

                # Generic keys fallback
                for k, v in obj.items():
                    if k.lower() == 'email':
                        if isinstance(v, str):
                            cleaned = _clean_email_str(v)
                            if cleaned:
                                structured_data['emails'].append(cleaned)
                        elif isinstance(v, list):
                            for e in v:
                                if isinstance(e, str):
                                    cleaned = _clean_email_str(e)
                                    if cleaned:
                                        structured_data['emails'].append(cleaned)
                    if k.lower() in ('telephone', 'phone'):
                        if isinstance(v, str):
                            structured_data['phones'].append(v)
                        elif isinstance(v, list):
                            structured_data['phones'].extend([t for t in v if isinstance(t, str)])
                    if isinstance(v, (dict, list)):
                        collect_from_json_ld(v)
            elif isinstance(obj, list):
                for item in obj:
                    collect_from_json_ld(item)

        # Extract JSON-LD data
        json_ld_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_ld_scripts:
            try:
                data = json.loads(script.string)
                structured_data['json_ld'].append(data)

                # Extract organization data
                if isinstance(data, dict):
                    if data.get('@type') == 'Organization':
                        structured_data['organization'] = data
                    elif data.get('@type') == 'ContactPage':
                        structured_data['contact_points'].append(data)
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            if item.get('@type') == 'Organization':
                                structured_data['organization'] = item
                            elif item.get('@type') == 'ContactPage':
                                structured_data['contact_points'].append(item)
                # Collect emails/phones from data
                collect_from_json_ld(data)
            except (json.JSONDecodeError, AttributeError):
                continue

        # Microdata: scan itemprop attributes for email/telephone
        email_props = {
            'email', 'emailaddress', 'contactemail', 'e-mail', 'mail', 'email_address'
        }
        phone_props = {
            'telephone', 'phone', 'phonenumber', 'contactphone', 'contactnumber', 'tel', 'phone_number'
        }

        for tag in soup.find_all(attrs={'itemprop': True}):
            prop = (tag.get('itemprop') or '').strip().lower()
            if not prop:
                continue
            candidates = []
            content_val = tag.get('content')
            href_val = tag.get('href')
            text_val = tag.get_text(' ', strip=True)
            if content_val:
                candidates.append(content_val)
            if href_val:
                candidates.append(href_val)
            if text_val:
                candidates.append(text_val)
            if prop in email_props:
                for c in candidates:
                    if not c:
                        continue
                    if 'mailto:' in c:
                        email = c.split('mailto:', 1)[1].split('?')[0].strip()
                        cleaned = _clean_email_str(email)
                        if cleaned:
                            structured_data['emails'].append(cleaned)
                    else:
                        if extractor:
                            try:
                                for e in extractor.extract_emails(c):
                                    val = e.get('value_normalized') or e.get('value_raw')
                                    cleaned = _clean_email_str(val)
                                    if cleaned:
                                        structured_data['emails'].append(cleaned)
                            except Exception:
                                pass
            if prop in phone_props:
                for c in candidates:
                    if not c:
                        continue
                    if 'tel:' in c:
                        phone = c.split('tel:', 1)[1].split('?')[0].strip()
                        if phone:
                            structured_data['phones'].append(phone)
                    else:
                        # Naive phone capture from targeted microdata value
                        digits = ''.join(ch for ch in c if ch.isdigit() or ch in '+() -')
                        if sum(ch.isdigit() for ch in digits) >= 6:
                            structured_data['phones'].append(digits.strip())

        # RDFa: scan property/typeof for common email/telephone predicates
        for tag in soup.find_all(attrs={'property': True}):
            prop = (tag.get('property') or '').strip().lower()
            candidates = []
            for attr in ('content', 'href', 'resource'):
                val = tag.get(attr)
                if val:
                    candidates.append(val)
            text_val = tag.get_text(' ', strip=True)
            if text_val:
                candidates.append(text_val)

            if prop in ('email', 'schema:email', 'vcard:email', 'foaf:mbox', 'contactemail'):
                for c in candidates:
                    if not c:
                        continue
                    if 'mailto:' in c:
                        email = c.split('mailto:', 1)[1].split('?')[0].strip()
                        cleaned = _clean_email_str(email)
                        if cleaned:
                            structured_data['emails'].append(cleaned)
                    else:
                        if extractor:
                            try:
                                for e in extractor.extract_emails(c):
                                    val = e.get('value_normalized') or e.get('value_raw')
                                    cleaned = _clean_email_str(val)
                                    if cleaned:
                                        structured_data['emails'].append(cleaned)
                            except Exception:
                                pass

            if prop in ('telephone', 'phone', 'schema:telephone', 'vcard:tel', 'contactphone'):
                for c in candidates:
                    if not c:
                        continue
                    if 'tel:' in c:
                        phone = c.split('tel:', 1)[1].split('?')[0].strip()
                        if phone:
                            structured_data['phones'].append(phone)
                    else:
                        digits = ''.join(ch for ch in c if ch.isdigit() or ch in '+() -')
                        if sum(ch.isdigit() for ch in digits) >= 6:
                            structured_data['phones'].append(digits.strip())

        # Link rel: extract mailto or email-like hrefs
        for link in soup.find_all('link', href=True):
            rels = link.get('rel') or []
            rels_list = rels if isinstance(rels, list) else [rels]
            rels_norm = [str(r).lower() for r in rels_list]
            href = link.get('href', '').strip()
            if not href:
                continue
            if 'mailto:' in href or any(r in ('me', 'author', 'contact', 'reply-to', 'email') for r in rels_norm):
                if 'mailto:' in href:
                    email = href.split('mailto:', 1)[1].split('?')[0].strip()
                    cleaned = _clean_email_str(email)
                    if cleaned:
                        structured_data['emails'].append(cleaned)
                else:
                    if extractor:
                        try:
                            for e in extractor.extract_emails(href):
                                val = e.get('value_normalized') or e.get('value_raw')
                                cleaned = _clean_email_str(val)
                                if cleaned:
                                    structured_data['emails'].append(cleaned)
                        except Exception:
                            pass

        # Meta tags: scan content for email hints
        for meta in soup.find_all('meta'):
            content = meta.get('content')
            name = (meta.get('name') or meta.get('property') or '').lower()
            if not content:
                continue
            if any(k in name for k in ('email', 'contact', 'reply-to', 'author', 'publisher', 'creator')):
                if extractor:
                    try:
                        for e in extractor.extract_emails(content):
                            val = e.get('value_normalized') or e.get('value_raw')
                            cleaned = _clean_email_str(val)
                            if cleaned:
                                structured_data['emails'].append(cleaned)
                    except Exception:
                        pass
            else:
                if 'mailto:' in content:
                    email = content.split('mailto:', 1)[1].split('?')[0].strip()
                    cleaned = _clean_email_str(email)
                    if cleaned:
                        structured_data['emails'].append(cleaned)
                else:
                    if extractor:
                        try:
                            for e in extractor.extract_emails(content):
                                val = e.get('value_normalized') or e.get('value_raw')
                                cleaned = _clean_email_str(val)
                                if cleaned:
                                    structured_data['emails'].append(cleaned)
                        except Exception:
                            pass

        # Deduplicate collected structured emails/phones
        structured_data['emails'] = list({e.strip().lower() for e in structured_data['emails'] if isinstance(e, str) and e.strip()})
        structured_data['phones'] = list({p.strip() for p in structured_data['phones'] if isinstance(p, str) and p.strip()})

        return structured_data

    def is_contact_page(self, html: str, url: str) -> bool:
        """
        Determine if a page is likely a contact page.

        Args:
            html (str): HTML content
            url (str): Page URL

        Returns:
            bool: True if likely a contact page
        """
        if not html:
            return False

        # Check URL path
        url_lower = url.lower()
        # Enhanced multi-language contact URL keywords
        contact_url_keywords = [
            # English
            'contact', 'about', 'support', 'help', 'info', 'team', 'company',
            'reach', 'connect', 'touch', 'call', 'phone', 'email', 'message',

            # Indonesian
            'kontak', 'hubungi', 'tentang', 'layanan', 'bantuan', 'dukungan',
            'informasi', 'perusahaan', 'tim', 'kantor', 'alamat', 'telepon',

            # Spanish
            'contacto', 'acerca', 'sobre', 'soporte', 'ayuda', 'servicio',
            'empresa', 'equipo', 'oficina', 'direccion', 'telefono', 'correo',

            # French
            'contact', 'propos', 'support', 'aide', 'service', 'entreprise',
            'equipe', 'bureau', 'adresse', 'telephone', 'courriel',

            # German
            'kontakt', 'uber', 'hilfe', 'service', 'unternehmen', 'team',
            'buro', 'adresse', 'telefon', 'email', 'kundendienst',

            # Portuguese
            'contato', 'sobre', 'suporte', 'ajuda', 'atendimento', 'empresa',
            'equipe', 'escritorio', 'endereco', 'telefone',

            # Italian
            'contatto', 'riguardo', 'supporto', 'aiuto', 'servizio', 'azienda',
            'team', 'ufficio', 'indirizzo', 'telefono', 'assistenza',

            # Dutch
            'contact', 'over', 'ondersteuning', 'hulp', 'service', 'bedrijf',
            'team', 'kantoor', 'adres', 'telefoon', 'klantenservice'
        ]

        for keyword in contact_url_keywords:
            if keyword in url_lower:
                return True

        # Check page content
        soup = BeautifulSoup(html, 'html.parser')

        # Check title
        title = soup.find('title')
        if title:
            title_text = title.get_text().lower()
            for keyword in contact_url_keywords:
                if keyword in title_text:
                    return True

        # Check headings
        headings = soup.find_all(['h1', 'h2', 'h3'])
        for heading in headings:
            heading_text = heading.get_text().lower()
            for keyword in contact_url_keywords:
                if keyword in heading_text:
                    return True

        # Enhanced form field detection with multi-language support
        forms = soup.find_all('form')
        form_keywords = [
            # English
            'email', 'message', 'name', 'phone', 'subject', 'inquiry', 'comment',
            'feedback', 'question', 'request', 'contact', 'address', 'company',

            # Indonesian
            'email', 'pesan', 'nama', 'telepon', 'subjek', 'pertanyaan', 'komentar',
            'masukan', 'saran', 'permintaan', 'kontak', 'alamat', 'perusahaan',

            # Spanish
            'correo', 'mensaje', 'nombre', 'telefono', 'asunto', 'consulta', 'comentario',
            'pregunta', 'solicitud', 'contacto', 'direccion', 'empresa',

            # French
            'courriel', 'message', 'nom', 'telephone', 'sujet', 'demande', 'commentaire',
            'question', 'requete', 'contact', 'adresse', 'entreprise',

            # German
            'email', 'nachricht', 'name', 'telefon', 'betreff', 'anfrage', 'kommentar',
            'frage', 'antrag', 'kontakt', 'adresse', 'unternehmen',

            # Portuguese
            'email', 'mensagem', 'nome', 'telefone', 'assunto', 'consulta', 'comentario',
            'pergunta', 'solicitacao', 'contato', 'endereco', 'empresa',

            # Universal patterns
            'submit', 'send', 'enviar', 'kirim', 'envoyer', 'senden', 'inviare'
        ]

        for form in forms:
            form_text = form.get_text().lower()
            # Also check input field names and placeholders
            inputs = form.find_all(['input', 'textarea', 'select'])
            for input_field in inputs:
                field_name = (input_field.get('name', '') + ' ' +
                            input_field.get('placeholder', '') + ' ' +
                            input_field.get('id', '')).lower()
                form_text += ' ' + field_name

            if any(keyword in form_text for keyword in form_keywords):
                return True

        return False