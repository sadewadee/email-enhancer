"""
Proxy Manager Module
Handles proxy loading, rotation, and management for web scraping.
"""

import os
import random
import logging
import threading
from typing import List, Optional, Dict, Any
from urllib.parse import urlparse
import time


class ProxyManager:
    """Manages proxy rotation and validation."""
    
    def __init__(self, proxy_file: str = "proxy.txt"):
        """
        Initialize proxy manager.
        
        Args:
            proxy_file: Path to proxy file (default: proxy.txt)
        """
        self.proxy_file = proxy_file
        self.proxies: List[Dict[str, Any]] = []
        self.current_index = 0
        self.lock = threading.Lock()
        self.failed_proxies = set()
        self.logger = logging.getLogger(__name__)
        
        # Load proxies if file exists
        self._load_proxies()
    
    def _load_proxies(self) -> None:
        """Load proxies from proxy file."""
        if not os.path.exists(self.proxy_file):
            self.logger.info(f"📡 No proxy file found at {self.proxy_file}. Running without proxy.")
            return
        
        try:
            with open(self.proxy_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            proxy_count = 0
            for line_num, line in enumerate(lines, 1):
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                proxy_info = self._parse_proxy_line(line)
                if proxy_info:
                    self.proxies.append(proxy_info)
                    proxy_count += 1
                else:
                    self.logger.warning(f"⚠️  Invalid proxy format at line {line_num}: {line}")
            
            if proxy_count > 0:
                self.logger.info(f"🔄 Loaded {proxy_count} proxies from {self.proxy_file}")
                # Shuffle proxies for better distribution
                random.shuffle(self.proxies)
            else:
                self.logger.warning(f"⚠️  No valid proxies found in {self.proxy_file}")
                
        except Exception as e:
            self.logger.error(f"❌ Error loading proxy file {self.proxy_file}: {str(e)}")
    
    def _parse_proxy_line(self, line: str) -> Optional[Dict[str, Any]]:
        """
        Parse a single proxy line into proxy configuration.
        
        Supported formats:
        - host:port
        - protocol://host:port
        - protocol://username:password@host:port
        - host:port:username:password
        
        Args:
            line: Proxy line to parse
            
        Returns:
            Dictionary with proxy configuration or None if invalid
        """
        try:
            line = line.strip()
            
            # Format: protocol://username:password@host:port
            if '://' in line:
                parsed = urlparse(line)
                if not parsed.hostname or not parsed.port:
                    return None
                
                proxy_config = {
                    'server': f"{parsed.hostname}:{parsed.port}",
                    'protocol': parsed.scheme.lower()
                }
                
                if parsed.username and parsed.password:
                    proxy_config['username'] = parsed.username
                    proxy_config['password'] = parsed.password
                
                return proxy_config
            
            # Format: host:port:username:password
            if line.count(':') == 3:
                parts = line.split(':')
                return {
                    'server': f"{parts[0]}:{parts[1]}",
                    'protocol': 'http',  # Default to HTTP
                    'username': parts[2],
                    'password': parts[3]
                }
            
            # Format: host:port
            elif line.count(':') == 1:
                parts = line.split(':')
                try:
                    port = int(parts[1])
                    return {
                        'server': f"{parts[0]}:{port}",
                        'protocol': 'http'  # Default to HTTP
                    }
                except ValueError:
                    return None
            
            return None
            
        except Exception:
            return None
    
    def get_next_proxy(self) -> Optional[Dict[str, Any]]:
        """
        Get next available proxy with round-robin rotation.
        
        Returns:
            Proxy configuration dict or None if no proxies available
        """
        if not self.proxies:
            return None
        
        with self.lock:
            # Filter out failed proxies
            available_proxies = [p for p in self.proxies if p['server'] not in self.failed_proxies]
            
            if not available_proxies:
                # Reset failed proxies if all are failed (give them another chance)
                self.failed_proxies.clear()
                available_proxies = self.proxies
                self.logger.info("🔄 Reset failed proxies list - giving all proxies another chance")
            
            # Round-robin selection
            proxy = available_proxies[self.current_index % len(available_proxies)]
            self.current_index += 1
            
            return proxy.copy()
    
    def mark_proxy_failed(self, proxy_server: str, reason: str = "Unknown error") -> None:
        """
        Mark a proxy as failed.

        Args:
            proxy_server: Proxy server (host:port) to mark as failed
            reason: Reason for failure (error message)
        """
        with self.lock:
            self.failed_proxies.add(proxy_server)
            # Use logger.debug to avoid progress bar collision (INFO only shows on console)
            self.logger.debug(f"❌ Proxy failed: {proxy_server} | Reason: {reason[:100]}")
            # Summary to console without details to avoid collision
            available = len(self.proxies) - len(self.failed_proxies)
            self.logger.info(f"⚠️  Proxy {proxy_server} failed ({available}/{len(self.proxies)} proxies still available)")
    
    def get_random_proxy(self) -> Optional[Dict[str, Any]]:
        """
        Get a random proxy from available list.
        
        Returns:
            Random proxy configuration dict or None if no proxies available
        """
        if not self.proxies:
            return None
        
        with self.lock:
            # Filter out failed proxies
            available_proxies = [p for p in self.proxies if p['server'] not in self.failed_proxies]
            
            if not available_proxies:
                # Reset failed proxies if all are failed
                self.failed_proxies.clear()
                available_proxies = self.proxies
            
            return random.choice(available_proxies).copy()
    
    def has_proxies(self) -> bool:
        """Check if any proxies are loaded."""
        return len(self.proxies) > 0
    
    def get_proxy_count(self) -> int:
        """Get total number of loaded proxies."""
        return len(self.proxies)
    
    def get_failed_proxy_count(self) -> int:
        """Get number of failed proxies."""
        with self.lock:
            return len(self.failed_proxies)
    
    def reload_proxies(self) -> None:
        """Reload proxies from file."""
        with self.lock:
            self.proxies.clear()
            self.failed_proxies.clear()
            self.current_index = 0
        
        self._load_proxies()
    
    def convert_to_playwright_format(self, proxy_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert proxy configuration to Playwright format.
        
        Args:
            proxy_config: Proxy configuration from get_next_proxy()
            
        Returns:
            Playwright-compatible proxy configuration
        """
        playwright_config = {
            'server': proxy_config['server']
        }
        
        if 'username' in proxy_config:
            playwright_config['username'] = proxy_config['username']
        
        if 'password' in proxy_config:
            playwright_config['password'] = proxy_config['password']
        
        return playwright_config
    
    def get_status_info(self) -> Dict[str, Any]:
        """Get proxy manager status information."""
        with self.lock:
            return {
                'total_proxies': len(self.proxies),
                'failed_proxies': len(self.failed_proxies),
                'available_proxies': len(self.proxies) - len(self.failed_proxies),
                'current_index': self.current_index,
                'has_proxy_file': os.path.exists(self.proxy_file),
                'proxy_file_path': self.proxy_file
            }