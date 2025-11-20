"""
URL Cleaner Module
Handles cleanup and normalization of URLs before scraping.
Removes tracking parameters, decodes Google redirects, and validates URL format.
"""

import logging
import re
from urllib.parse import urlparse, parse_qs, unquote, urlunparse
from typing import Optional, Dict, Any
import json

logger = logging.getLogger(__name__)


class URLCleaner:
    """
    Cleans and normalizes URLs to prevent scraping errors.

    Handles:
    - Google search redirect URLs (converts /url?q=... to actual URL)
    - Tracking parameters (utm_*, opi, sa, ved, usg, etc.)
    - URL encoding issues
    - Invalid URL formats
    - Protocol normalization (http -> https)
    """

    # Common tracking parameters that should be removed
    TRACKING_PARAMS = {
        # Google Analytics and Search tracking
        'utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term',
        'utm_id', 'utm_source_platform', 'utm_creative_format',
        # Google specific
        'opi', 'sa', 'ved', 'usg', 'ei', 'hl',
        # Facebook
        'fbclid', 'efg',
        # Other trackers
        'gclid', 'msclkid', 'kwid', '_ga', 'mc_cid', 'mc_eid',
        # General tracking
        'ref', 'referrer', 'source', 'campaign',
        # Session/Cache parameters
        'sid', 'sessionid', 'PHPSESSID',
        'nocache', 'cache', '_cachebust',
        # Ad networks
        'an', 'aid', 'an_id', 'adid', 'adset_id', 'campaign_id',
        # Analytics
        'piwik_id', 'pk_campaign', 'pk_kwd',
    }

    @staticmethod
    def is_google_redirect_url(url: str) -> bool:
        """
        Check if URL is a Google search redirect URL.
        Format: /url?q=<actual_url>&other_params...
        """
        try:
            parsed = urlparse(url)
            # Check if this is a Google redirect path
            if '/url' in parsed.path and 'q=' in (parsed.query or ''):
                return True
        except Exception:
            pass
        return False

    @staticmethod
    def extract_google_redirect_url(url: str) -> Optional[str]:
        """
        Extract the actual URL from a Google search redirect.
        Example:
            Input: /url?q=http://www.example.com/&opi=79508299&sa=U&ved=...
            Output: http://www.example.com/
        """
        try:
            parsed = urlparse(url)
            params = parse_qs(parsed.query)

            if 'q' in params and params['q']:
                actual_url = params['q'][0]
                # URL decode if necessary
                actual_url = unquote(actual_url)
                return actual_url
        except Exception as e:
            logger.debug(f"Failed to extract Google redirect URL: {url} | error: {e}")

        return None

    @staticmethod
    def remove_tracking_parameters(url: str, tracking_params: set = None) -> str:
        """
        Remove tracking and analytics parameters from URL.

        Args:
            url: URL to clean
            tracking_params: Set of parameter names to remove (uses default if None)

        Returns:
            URL without tracking parameters
        """
        if tracking_params is None:
            tracking_params = URLCleaner.TRACKING_PARAMS

        try:
            parsed = urlparse(url)

            # Parse query string
            params = parse_qs(parsed.query, keep_blank_values=True)

            # Remove tracking parameters
            for param in list(params.keys()):
                if param.lower() in tracking_params:
                    del params[param]

            # Reconstruct query string
            # Keep only first value of each param (parse_qs returns lists)
            cleaned_params = [(k, v[0] if v else '') for k, v in params.items()]

            # Reconstruct URL
            new_query = '&'.join([f"{k}={v}" if v else k for k, v in cleaned_params])

            # Build clean URL
            clean_url = urlunparse((
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                new_query,
                ''  # Remove fragment (hash) as well
            ))

            return clean_url
        except Exception as e:
            logger.debug(f"Failed to remove tracking parameters from {url}: {e}")
            return url

    @staticmethod
    def normalize_protocol(url: str, prefer_https: bool = True) -> str:
        """
        Normalize URL protocol.
        - Add protocol if missing
        - Convert http to https if prefer_https=True
        - Normalize domain to lowercase

        Args:
            url: URL to normalize
            prefer_https: Whether to prefer https over http

        Returns:
            URL with normalized protocol
        """
        url = url.strip()

        # If no protocol, add http (will be converted to https if preferred)
        if not url.startswith(('http://', 'https://', '//')):
            url = f'http://{url}'

        # Convert protocol-relative URLs to https
        if url.startswith('//'):
            url = f'https:{url}'

        # Convert http to https if preferred
        if prefer_https and url.startswith('http://'):
            url = url.replace('http://', 'https://', 1)

        # Normalize domain to lowercase (scheme and netloc)
        try:
            parsed = urlparse(url)
            if parsed.netloc:
                # Reconstruct URL with lowercase netloc
                url = urlunparse((
                    parsed.scheme.lower(),  # scheme lowercase
                    parsed.netloc.lower(),  # domain/netloc lowercase
                    parsed.path,
                    parsed.params,
                    parsed.query,
                    parsed.fragment
                ))
        except Exception:
            pass

        return url

    @staticmethod
    def clean_url(url: str, aggressive: bool = False) -> Optional[str]:
        """
        Comprehensive URL cleaning pipeline.

        Steps:
        1. Strip whitespace
        2. Decode URL encoding
        3. Handle Google redirect URLs
        4. Remove tracking parameters
        5. Normalize protocol
        6. Validate final URL

        Args:
            url: Raw URL to clean
            aggressive: If True, also remove fragments and apply more strict cleaning

        Returns:
            Cleaned URL or None if invalid
        """
        if not url:
            return None

        try:
            # Step 1: Strip whitespace
            url = str(url).strip()

            if not url:
                return None

            # Step 2: Handle URL encoding (decode if needed)
            # Be careful: only decode once
            if '%' in url:
                try:
                    decoded = unquote(url)
                    # Use decoded version if it looks better (has more readable chars)
                    if len(decoded) < len(url) and decoded != url:
                        url = decoded
                except Exception:
                    pass

            # Step 3: Check for Google redirect format
            if URLCleaner.is_google_redirect_url(url):
                logger.debug(f"Detected Google redirect URL: {url}")
                extracted = URLCleaner.extract_google_redirect_url(url)
                if extracted:
                    url = extracted
                    logger.debug(f"Extracted actual URL from Google redirect: {url}")

            # Step 4: Remove tracking parameters
            url = URLCleaner.remove_tracking_parameters(url)

            # Step 5: Normalize protocol
            url = URLCleaner.normalize_protocol(url)

            # Step 6: Additional aggressive cleaning if requested
            if aggressive:
                parsed = urlparse(url)
                # Remove fragment (everything after #)
                url = urlunparse((
                    parsed.scheme,
                    parsed.netloc,
                    parsed.path,
                    parsed.params,
                    parsed.query,
                    ''  # Remove fragment
                ))

            # Step 7: Validate cleaned URL
            parsed = urlparse(url)

            # Must have scheme and netloc
            if not parsed.scheme or not parsed.netloc:
                logger.debug(f"Invalid URL after cleaning (missing scheme or netloc): {url}")
                return None

            # Check for valid TLD (at least 2 chars after last dot in domain)
            domain_parts = parsed.netloc.split('.')
            if len(domain_parts) < 2 or len(domain_parts[-1]) < 2:
                logger.debug(f"Invalid domain format in URL: {url}")
                return None

            return url

        except Exception as e:
            logger.debug(f"Error cleaning URL {url}: {e}")
            return None

    @staticmethod
    def get_cleanup_stats(original: str, cleaned: str) -> Dict[str, Any]:
        """
        Get statistics about what was cleaned from a URL.

        Returns:
            Dict with details about cleaning operations performed
        """
        stats = {
            'original_url': original,
            'cleaned_url': cleaned,
            'was_google_redirect': URLCleaner.is_google_redirect_url(original),
            'original_length': len(original),
            'cleaned_length': len(cleaned),
            'chars_removed': len(original) - len(cleaned),
            'is_valid': cleaned is not None,
        }
        return stats


# Convenience functions for quick access
def clean_url(url: str, aggressive: bool = False) -> Optional[str]:
    """
    Quick cleanup function for a single URL.
    Usage: from url_cleaner import clean_url
    """
    return URLCleaner.clean_url(url, aggressive=aggressive)


def is_google_redirect(url: str) -> bool:
    """Check if URL is a Google redirect."""
    return URLCleaner.is_google_redirect_url(url)


if __name__ == "__main__":
    # Test examples
    test_urls = [
        # Google redirect URL
        "/url?q=http://www.allgoodpilates.com/&opi=79508299&sa=U&ved=0ahUKEwjc-Pmd1_mQAxVH_rsIHV-vAGAQ61gIEigO&usg=AOvVaw1m90NxHswwfuN1m1MmCfH9",
        # Normal URL with tracking params
        "https://example.com/?utm_source=google&utm_medium=cpc&utm_campaign=test",
        # URL with encoding
        "https://example.com/page%20with%20spaces",
        # Missing protocol
        "example.com",
        # Mixed case with fragment
        "HTTPS://EXAMPLE.COM/path#section?utm_id=123",
        # Valid clean URL
        "https://www.example.com/contact",
    ]

    logging.basicConfig(level=logging.DEBUG)

    print("URL Cleaner Test Results")
    print("=" * 80)

    for url in test_urls:
        print(f"\nOriginal: {url}")
        cleaned = URLCleaner.clean_url(url, aggressive=False)
        print(f"Cleaned:  {cleaned}")

        if cleaned:
            stats = URLCleaner.get_cleanup_stats(url, cleaned)
            print(f"Stats: {json.dumps(stats, indent=2)}")
