"""
Anti-Detection Techniques - Browser fingerprint randomization and behavior simulation

Implements advanced anti-detection capabilities:
- Browser fingerprint randomization
- Human-like behavior simulation
- Request header management
- Timing randomization
- Mouse/scroll simulation patterns
- Cookie and session management
- User agent rotation
"""

import asyncio
import hashlib
import json
import logging
import math
import random
import string
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from collections import deque


class BrowserType(Enum):
    """Browser type for fingerprinting"""
    CHROME = "chrome"
    FIREFOX = "firefox"
    SAFARI = "safari"
    EDGE = "edge"


class DeviceType(Enum):
    """Device type for fingerprinting"""
    DESKTOP = "desktop"
    MOBILE = "mobile"
    TABLET = "tablet"


class OSType(Enum):
    """Operating system type"""
    WINDOWS = "windows"
    MACOS = "macos"
    LINUX = "linux"
    IOS = "ios"
    ANDROID = "android"


@dataclass
class ScreenResolution:
    """Screen resolution configuration"""
    width: int
    height: int
    pixel_ratio: float = 1.0
    color_depth: int = 24
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'width': self.width,
            'height': self.height,
            'availWidth': self.width,
            'availHeight': self.height - random.randint(30, 100),  # Taskbar
            'pixelDepth': self.color_depth,
            'colorDepth': self.color_depth,
            'devicePixelRatio': self.pixel_ratio
        }


@dataclass 
class BrowserFingerprint:
    """Complete browser fingerprint configuration"""
    user_agent: str
    browser_type: BrowserType
    browser_version: str
    os_type: OSType
    os_version: str
    device_type: DeviceType
    screen: ScreenResolution
    language: str = "en-US"
    languages: List[str] = field(default_factory=lambda: ["en-US", "en"])
    timezone: str = "America/New_York"
    platform: str = "Win32"
    
    # Hardware
    hardware_concurrency: int = 8
    device_memory: int = 8
    
    # WebGL
    webgl_vendor: str = "Google Inc. (Intel)"
    webgl_renderer: str = "ANGLE (Intel, Intel(R) UHD Graphics 630, OpenGL 4.6)"
    
    # Plugins
    plugins: List[str] = field(default_factory=list)
    
    # Canvas fingerprint noise
    canvas_noise: float = 0.0001
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'userAgent': self.user_agent,
            'browser': {
                'type': self.browser_type.value,
                'version': self.browser_version
            },
            'os': {
                'type': self.os_type.value,
                'version': self.os_version
            },
            'device': self.device_type.value,
            'screen': self.screen.to_dict(),
            'language': self.language,
            'languages': self.languages,
            'timezone': self.timezone,
            'platform': self.platform,
            'hardwareConcurrency': self.hardware_concurrency,
            'deviceMemory': self.device_memory,
            'webgl': {
                'vendor': self.webgl_vendor,
                'renderer': self.webgl_renderer
            },
            'plugins': self.plugins
        }


class UserAgentGenerator:
    """Generate realistic user agents"""
    
    # Chrome versions (recent)
    CHROME_VERSIONS = [
        "119.0.0.0", "118.0.0.0", "117.0.0.0", "116.0.0.0", "115.0.0.0"
    ]
    
    # Firefox versions
    FIREFOX_VERSIONS = [
        "120.0", "119.0", "118.0", "117.0", "116.0"
    ]
    
    # Safari versions
    SAFARI_VERSIONS = [
        "17.1", "17.0", "16.6", "16.5", "16.4"
    ]
    
    # Windows versions
    WINDOWS_VERSIONS = ["10.0", "11.0"]
    
    # macOS versions
    MACOS_VERSIONS = ["14_1", "14_0", "13_6", "13_5", "12_7"]
    
    @classmethod
    def generate(cls, 
                 browser: Optional[BrowserType] = None,
                 os: Optional[OSType] = None,
                 device: DeviceType = DeviceType.DESKTOP) -> Tuple[str, BrowserType, OSType]:
        """Generate random user agent"""
        # Random selections if not specified
        browser = browser or random.choice(list(BrowserType))
        os = os or random.choice([OSType.WINDOWS, OSType.MACOS, OSType.LINUX])
        
        if browser == BrowserType.CHROME:
            return cls._chrome_ua(os, device), browser, os
        elif browser == BrowserType.FIREFOX:
            return cls._firefox_ua(os, device), browser, os
        elif browser == BrowserType.SAFARI:
            return cls._safari_ua(), BrowserType.SAFARI, OSType.MACOS
        elif browser == BrowserType.EDGE:
            return cls._edge_ua(os), browser, os
        
        return cls._chrome_ua(os, device), BrowserType.CHROME, os
    
    @classmethod
    def _chrome_ua(cls, os: OSType, device: DeviceType) -> str:
        """Generate Chrome user agent"""
        version = random.choice(cls.CHROME_VERSIONS)
        
        if os == OSType.WINDOWS:
            win_version = random.choice(cls.WINDOWS_VERSIONS)
            return f"Mozilla/5.0 (Windows NT {win_version}; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version} Safari/537.36"
        elif os == OSType.MACOS:
            mac_version = random.choice(cls.MACOS_VERSIONS)
            return f"Mozilla/5.0 (Macintosh; Intel Mac OS X {mac_version}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version} Safari/537.36"
        elif os == OSType.LINUX:
            return f"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version} Safari/537.36"
        
        return f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version} Safari/537.36"
    
    @classmethod
    def _firefox_ua(cls, os: OSType, device: DeviceType) -> str:
        """Generate Firefox user agent"""
        version = random.choice(cls.FIREFOX_VERSIONS)
        
        if os == OSType.WINDOWS:
            win_version = random.choice(cls.WINDOWS_VERSIONS)
            return f"Mozilla/5.0 (Windows NT {win_version}; Win64; x64; rv:{version}) Gecko/20100101 Firefox/{version}"
        elif os == OSType.MACOS:
            mac_version = random.choice(cls.MACOS_VERSIONS)
            return f"Mozilla/5.0 (Macintosh; Intel Mac OS X {mac_version}; rv:{version}) Gecko/20100101 Firefox/{version}"
        elif os == OSType.LINUX:
            return f"Mozilla/5.0 (X11; Linux x86_64; rv:{version}) Gecko/20100101 Firefox/{version}"
        
        return f"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:{version}) Gecko/20100101 Firefox/{version}"
    
    @classmethod
    def _safari_ua(cls) -> str:
        """Generate Safari user agent (macOS only)"""
        version = random.choice(cls.SAFARI_VERSIONS)
        mac_version = random.choice(cls.MACOS_VERSIONS)
        webkit = f"605.1.15"
        return f"Mozilla/5.0 (Macintosh; Intel Mac OS X {mac_version}) AppleWebKit/{webkit} (KHTML, like Gecko) Version/{version} Safari/{webkit}"
    
    @classmethod
    def _edge_ua(cls, os: OSType) -> str:
        """Generate Edge user agent"""
        chrome_version = random.choice(cls.CHROME_VERSIONS)
        edge_version = chrome_version.replace(".", ".0.", 1)
        
        if os == OSType.WINDOWS:
            win_version = random.choice(cls.WINDOWS_VERSIONS)
            return f"Mozilla/5.0 (Windows NT {win_version}; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{chrome_version} Safari/537.36 Edg/{edge_version}"
        
        return f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{chrome_version} Safari/537.36 Edg/{edge_version}"


class FingerprintGenerator:
    """Generate complete browser fingerprints"""
    
    # Common screen resolutions
    DESKTOP_RESOLUTIONS = [
        ScreenResolution(1920, 1080, 1.0),
        ScreenResolution(1920, 1200, 1.0),
        ScreenResolution(2560, 1440, 1.0),
        ScreenResolution(1366, 768, 1.0),
        ScreenResolution(1536, 864, 1.25),
        ScreenResolution(1440, 900, 1.0),
        ScreenResolution(3840, 2160, 2.0),  # 4K
    ]
    
    MOBILE_RESOLUTIONS = [
        ScreenResolution(390, 844, 3.0),    # iPhone 12/13
        ScreenResolution(428, 926, 3.0),    # iPhone 12/13 Pro Max
        ScreenResolution(360, 800, 3.0),    # Samsung Galaxy
        ScreenResolution(412, 915, 2.625),  # Pixel 6
    ]
    
    # Timezones
    TIMEZONES = [
        "America/New_York", "America/Chicago", "America/Denver",
        "America/Los_Angeles", "Europe/London", "Europe/Paris",
        "Europe/Berlin", "Asia/Tokyo", "Asia/Shanghai"
    ]
    
    # Languages
    LANGUAGE_SETS = [
        ["en-US", "en"],
        ["en-GB", "en"],
        ["de-DE", "de", "en"],
        ["fr-FR", "fr", "en"],
        ["es-ES", "es", "en"],
        ["ja-JP", "ja", "en"],
    ]
    
    # WebGL renderers
    WEBGL_RENDERERS = [
        ("Google Inc. (Intel)", "ANGLE (Intel, Intel(R) UHD Graphics 630, OpenGL 4.6)"),
        ("Google Inc. (Intel)", "ANGLE (Intel, Intel(R) Iris Plus Graphics 655, OpenGL 4.6)"),
        ("Google Inc. (NVIDIA)", "ANGLE (NVIDIA, GeForce GTX 1060 6GB, OpenGL 4.6)"),
        ("Google Inc. (NVIDIA)", "ANGLE (NVIDIA, GeForce RTX 3070, OpenGL 4.6)"),
        ("Google Inc. (AMD)", "ANGLE (AMD, Radeon RX 580, OpenGL 4.6)"),
        ("Intel Inc.", "Intel(R) UHD Graphics 630"),
        ("Apple Inc.", "Apple M1"),
    ]
    
    @classmethod
    def generate(cls, 
                 device: DeviceType = DeviceType.DESKTOP,
                 browser: Optional[BrowserType] = None,
                 os: Optional[OSType] = None) -> BrowserFingerprint:
        """Generate complete browser fingerprint"""
        
        # Generate user agent
        ua, browser_type, os_type = UserAgentGenerator.generate(browser, os, device)
        
        # Extract versions
        browser_version = cls._extract_browser_version(ua, browser_type)
        os_version = cls._extract_os_version(ua, os_type)
        
        # Select screen resolution
        if device == DeviceType.DESKTOP:
            screen = random.choice(cls.DESKTOP_RESOLUTIONS)
        else:
            screen = random.choice(cls.MOBILE_RESOLUTIONS)
        
        # Select WebGL
        webgl_vendor, webgl_renderer = random.choice(cls.WEBGL_RENDERERS)
        
        # Select language
        languages = random.choice(cls.LANGUAGE_SETS)
        
        # Platform
        platform = cls._get_platform(os_type, device)
        
        return BrowserFingerprint(
            user_agent=ua,
            browser_type=browser_type,
            browser_version=browser_version,
            os_type=os_type,
            os_version=os_version,
            device_type=device,
            screen=screen,
            language=languages[0],
            languages=languages,
            timezone=random.choice(cls.TIMEZONES),
            platform=platform,
            hardware_concurrency=random.choice([4, 8, 12, 16]),
            device_memory=random.choice([4, 8, 16, 32]),
            webgl_vendor=webgl_vendor,
            webgl_renderer=webgl_renderer,
            canvas_noise=random.uniform(0.0001, 0.001)
        )
    
    @classmethod
    def _extract_browser_version(cls, ua: str, browser: BrowserType) -> str:
        """Extract browser version from user agent"""
        import re
        
        patterns = {
            BrowserType.CHROME: r'Chrome/(\d+\.\d+\.\d+\.\d+)',
            BrowserType.FIREFOX: r'Firefox/(\d+\.\d+)',
            BrowserType.SAFARI: r'Version/(\d+\.\d+)',
            BrowserType.EDGE: r'Edg/(\d+\.\d+\.\d+\.\d+)'
        }
        
        pattern = patterns.get(browser)
        if pattern:
            match = re.search(pattern, ua)
            if match:
                return match.group(1)
        
        return "100.0"
    
    @classmethod
    def _extract_os_version(cls, ua: str, os: OSType) -> str:
        """Extract OS version from user agent"""
        import re
        
        if os == OSType.WINDOWS:
            match = re.search(r'Windows NT (\d+\.\d+)', ua)
            if match:
                return match.group(1)
        elif os == OSType.MACOS:
            match = re.search(r'Mac OS X (\d+[_\d]+)', ua)
            if match:
                return match.group(1).replace('_', '.')
        
        return "10.0"
    
    @classmethod
    def _get_platform(cls, os: OSType, device: DeviceType) -> str:
        """Get platform string"""
        platforms = {
            OSType.WINDOWS: "Win32",
            OSType.MACOS: "MacIntel",
            OSType.LINUX: "Linux x86_64",
            OSType.IOS: "iPhone",
            OSType.ANDROID: "Linux armv81"
        }
        return platforms.get(os, "Win32")


class BehaviorSimulator:
    """Simulate human-like browsing behavior"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def random_delay(self, min_delay: float = 0.5, max_delay: float = 2.0) -> float:
        """Generate random delay with human-like distribution"""
        # Use log-normal distribution for more realistic delays
        mean = (min_delay + max_delay) / 2
        sigma = (max_delay - min_delay) / 4
        
        delay = random.lognormvariate(math.log(mean), sigma)
        return max(min_delay, min(max_delay, delay))
    
    def typing_delay(self) -> float:
        """Generate delay between keystrokes"""
        # Human typing is ~40-60 WPM = 100-150ms per character
        base = random.uniform(0.08, 0.15)
        # Occasional pauses
        if random.random() < 0.1:
            base += random.uniform(0.2, 0.5)
        return base
    
    def reading_time(self, content_length: int) -> float:
        """Estimate reading time for content"""
        # Average reading speed: 200-250 WPM
        # Average word length: 5 characters
        words = content_length / 5
        wpm = random.uniform(180, 280)
        base_time = (words / wpm) * 60
        
        # Add variation
        return base_time * random.uniform(0.8, 1.3)
    
    def scroll_pattern(self, page_height: int) -> List[Tuple[int, float]]:
        """Generate human-like scroll pattern"""
        scrolls = []
        current_position = 0
        
        while current_position < page_height:
            # Random scroll amount (100-500 pixels)
            scroll_amount = random.randint(100, 500)
            
            # Delay before scroll
            delay = random.uniform(0.5, 2.0)
            
            current_position += scroll_amount
            scrolls.append((min(current_position, page_height), delay))
            
            # Occasional pause (reading)
            if random.random() < 0.3:
                scrolls.append((current_position, random.uniform(2.0, 5.0)))
            
            # Occasional scroll up (re-reading)
            if random.random() < 0.1:
                back_amount = random.randint(50, 200)
                current_position = max(0, current_position - back_amount)
                scrolls.append((current_position, random.uniform(0.3, 0.8)))
        
        return scrolls
    
    def mouse_path(self, start: Tuple[int, int], end: Tuple[int, int],
                   steps: int = 20) -> List[Tuple[int, int, float]]:
        """Generate human-like mouse movement path"""
        path = []
        
        # Use bezier curve for natural movement
        control1 = (
            start[0] + (end[0] - start[0]) * random.uniform(0.2, 0.4) + random.randint(-50, 50),
            start[1] + (end[1] - start[1]) * random.uniform(0.2, 0.4) + random.randint(-50, 50)
        )
        control2 = (
            start[0] + (end[0] - start[0]) * random.uniform(0.6, 0.8) + random.randint(-50, 50),
            start[1] + (end[1] - start[1]) * random.uniform(0.6, 0.8) + random.randint(-50, 50)
        )
        
        for i in range(steps + 1):
            t = i / steps
            
            # Cubic bezier formula
            x = ((1-t)**3 * start[0] + 
                 3*(1-t)**2*t * control1[0] + 
                 3*(1-t)*t**2 * control2[0] + 
                 t**3 * end[0])
            y = ((1-t)**3 * start[1] + 
                 3*(1-t)**2*t * control1[1] + 
                 3*(1-t)*t**2 * control2[1] + 
                 t**3 * end[1])
            
            # Variable speed (slow at start and end)
            speed_factor = 4 * t * (1 - t)  # Peaks at 0.5
            delay = random.uniform(0.01, 0.03) / max(0.5, speed_factor)
            
            path.append((int(x), int(y), delay))
        
        return path
    
    def click_position_variance(self, target: Tuple[int, int],
                                radius: int = 5) -> Tuple[int, int]:
        """Add human-like variance to click position"""
        # Humans don't click exactly in center
        offset_x = random.gauss(0, radius / 2)
        offset_y = random.gauss(0, radius / 2)
        
        return (
            int(target[0] + offset_x),
            int(target[1] + offset_y)
        )


class RequestHeaderManager:
    """Manage request headers for anti-detection"""
    
    ACCEPT_HEADERS = {
        'html': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'json': 'application/json, text/plain, */*',
        'image': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
        'css': 'text/css,*/*;q=0.1',
        'js': '*/*'
    }
    
    ACCEPT_ENCODINGS = [
        'gzip, deflate, br',
        'gzip, deflate',
        'gzip, deflate, br, zstd'
    ]
    
    SEC_CH_UA_PLATFORMS = {
        OSType.WINDOWS: '"Windows"',
        OSType.MACOS: '"macOS"',
        OSType.LINUX: '"Linux"'
    }
    
    def __init__(self, fingerprint: BrowserFingerprint):
        self.fingerprint = fingerprint
    
    def get_headers(self, url: str, referer: Optional[str] = None) -> Dict[str, str]:
        """Get headers for request"""
        headers = {
            'User-Agent': self.fingerprint.user_agent,
            'Accept': self.ACCEPT_HEADERS['html'],
            'Accept-Language': ','.join(f"{lang};q={1.0-i*0.1:.1f}" 
                                       for i, lang in enumerate(self.fingerprint.languages)),
            'Accept-Encoding': random.choice(self.ACCEPT_ENCODINGS),
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        # Add referer if provided
        if referer:
            headers['Referer'] = referer
        
        # Add Sec-CH-UA headers for Chrome
        if self.fingerprint.browser_type == BrowserType.CHROME:
            headers.update(self._get_sec_ch_headers())
        
        # DNT (Do Not Track) - randomize
        if random.random() > 0.7:
            headers['DNT'] = '1'
        
        return headers
    
    def _get_sec_ch_headers(self) -> Dict[str, str]:
        """Get Sec-CH-UA headers for Chrome"""
        version = self.fingerprint.browser_version.split('.')[0]
        
        return {
            'Sec-CH-UA': f'"Chromium";v="{version}", "Google Chrome";v="{version}", "Not=A?Brand";v="99"',
            'Sec-CH-UA-Mobile': '?0' if self.fingerprint.device_type == DeviceType.DESKTOP else '?1',
            'Sec-CH-UA-Platform': self.SEC_CH_UA_PLATFORMS.get(self.fingerprint.os_type, '"Windows"'),
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1'
        }


class AntiDetectionManager:
    """
    Centralized anti-detection management.
    
    Features:
    - Fingerprint rotation
    - Behavior simulation
    - Header management
    - Session management
    """
    
    def __init__(self, 
                 rotate_interval: int = 100,
                 max_fingerprints: int = 10):
        self.logger = logging.getLogger(__name__)
        self.rotate_interval = rotate_interval
        self.max_fingerprints = max_fingerprints
        
        # Fingerprint pool
        self._fingerprints: deque = deque(maxlen=max_fingerprints)
        self._current_fingerprint: Optional[BrowserFingerprint] = None
        self._request_count = 0
        
        # Components
        self._behavior = BehaviorSimulator()
        self._header_manager: Optional[RequestHeaderManager] = None
        
        # Session tracking
        self._domain_sessions: Dict[str, Dict[str, Any]] = {}
        
        # Initialize fingerprints
        self._generate_fingerprints()
    
    def _generate_fingerprints(self) -> None:
        """Generate pool of fingerprints"""
        for _ in range(self.max_fingerprints):
            fp = FingerprintGenerator.generate()
            self._fingerprints.append(fp)
        
        self._rotate_fingerprint()
    
    def _rotate_fingerprint(self) -> None:
        """Rotate to next fingerprint"""
        if self._fingerprints:
            self._current_fingerprint = random.choice(self._fingerprints)
            self._header_manager = RequestHeaderManager(self._current_fingerprint)
            self.logger.debug(f"Rotated to fingerprint: {self._current_fingerprint.user_agent[:50]}...")
    
    def get_fingerprint(self) -> BrowserFingerprint:
        """Get current fingerprint"""
        self._request_count += 1
        
        # Rotate if needed
        if self._request_count >= self.rotate_interval:
            self._rotate_fingerprint()
            self._request_count = 0
        
        return self._current_fingerprint
    
    def get_headers(self, url: str, referer: Optional[str] = None) -> Dict[str, str]:
        """Get request headers"""
        if not self._header_manager:
            self._rotate_fingerprint()
        return self._header_manager.get_headers(url, referer)
    
    def get_delay(self, min_delay: float = 0.5, max_delay: float = 2.0) -> float:
        """Get human-like delay"""
        return self._behavior.random_delay(min_delay, max_delay)
    
    def get_scroll_pattern(self, page_height: int) -> List[Tuple[int, float]]:
        """Get human-like scroll pattern"""
        return self._behavior.scroll_pattern(page_height)
    
    def get_mouse_path(self, start: Tuple[int, int], 
                       end: Tuple[int, int]) -> List[Tuple[int, int, float]]:
        """Get human-like mouse movement path"""
        return self._behavior.mouse_path(start, end)
    
    def get_session(self, domain: str) -> Dict[str, Any]:
        """Get or create session for domain"""
        if domain not in self._domain_sessions:
            self._domain_sessions[domain] = {
                'fingerprint': self.get_fingerprint(),
                'cookies': {},
                'referer': None,
                'request_count': 0,
                'created_at': time.time()
            }
        
        session = self._domain_sessions[domain]
        session['request_count'] += 1
        return session
    
    def clear_session(self, domain: str) -> None:
        """Clear session for domain"""
        if domain in self._domain_sessions:
            del self._domain_sessions[domain]
    
    def get_stats(self) -> Dict[str, Any]:
        """Get anti-detection statistics"""
        return {
            'fingerprints_pool': len(self._fingerprints),
            'current_fingerprint': self._current_fingerprint.user_agent[:50] if self._current_fingerprint else None,
            'requests_since_rotation': self._request_count,
            'rotate_interval': self.rotate_interval,
            'active_sessions': len(self._domain_sessions),
            'session_domains': list(self._domain_sessions.keys())
        }


# Global instance
_anti_detection: Optional[AntiDetectionManager] = None


def get_anti_detection() -> AntiDetectionManager:
    """Get or create global anti-detection manager"""
    global _anti_detection
    if _anti_detection is None:
        _anti_detection = AntiDetectionManager()
    return _anti_detection


def create_anti_detection(config: Optional[Dict[str, Any]] = None) -> AntiDetectionManager:
    """Factory function for anti-detection manager"""
    config = config or {}
    return AntiDetectionManager(
        rotate_interval=config.get('rotate_interval', 100),
        max_fingerprints=config.get('max_fingerprints', 10)
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("Anti-Detection Demo")
    print("=" * 60)
    
    # Create manager
    manager = AntiDetectionManager(rotate_interval=5, max_fingerprints=5)
    
    # Generate fingerprints
    print("\n1. Generated Fingerprints:")
    for i in range(3):
        fp = manager.get_fingerprint()
        print(f"\n   Fingerprint {i+1}:")
        print(f"     Browser: {fp.browser_type.value} {fp.browser_version}")
        print(f"     OS: {fp.os_type.value} {fp.os_version}")
        print(f"     Screen: {fp.screen.width}x{fp.screen.height}")
        print(f"     Language: {fp.language}")
        print(f"     UA: {fp.user_agent[:60]}...")
    
    # Request headers
    print("\n2. Request Headers:")
    headers = manager.get_headers("https://example.com", referer="https://google.com")
    for key, value in list(headers.items())[:5]:
        print(f"   {key}: {value[:60]}...")
    
    # Behavior simulation
    print("\n3. Behavior Simulation:")
    
    # Delays
    delays = [manager.get_delay() for _ in range(5)]
    print(f"   Random delays: {[f'{d:.2f}s' for d in delays]}")
    
    # Scroll pattern
    scrolls = manager.get_scroll_pattern(2000)
    print(f"   Scroll pattern ({len(scrolls)} steps):")
    for pos, delay in scrolls[:3]:
        print(f"     Scroll to {pos}px, wait {delay:.2f}s")
    print("     ...")
    
    # Mouse path
    path = manager.get_mouse_path((100, 100), (500, 300))
    print(f"   Mouse path ({len(path)} points):")
    for x, y, delay in path[:3]:
        print(f"     ({x}, {y}) delay {delay:.3f}s")
    print("     ...")
    
    # Session management
    print("\n4. Session Management:")
    session = manager.get_session("example.com")
    print(f"   Domain: example.com")
    print(f"   Request count: {session['request_count']}")
    print(f"   Fingerprint: {session['fingerprint'].user_agent[:50]}...")
    
    # Stats
    print("\n5. Statistics:")
    stats = manager.get_stats()
    for key, value in stats.items():
        print(f"   {key}: {value}")
    
    print("\nDemo complete!")
