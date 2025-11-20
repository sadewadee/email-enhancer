"""
SSL Error Handler Module
Handles SSL certificate errors and provides detailed logging and recovery strategies.
"""

import ssl
import logging
import re
from typing import Optional, Dict, Any
from urllib.error import URLError
import urllib.error

logger = logging.getLogger(__name__)


class SSLErrorHandler:
    """
    Handles and categorizes SSL/certificate-related errors.
    Provides detailed logging, classification, and retry strategies.
    """

    # SSL error categories
    class SSLErrorType:
        HOSTNAME_MISMATCH = "hostname_mismatch"  # SSL_ERROR_BAD_CERT_DOMAIN
        CERTIFICATE_EXPIRED = "certificate_expired"
        SELF_SIGNED = "self_signed_certificate"
        UNTRUSTED_ROOT = "untrusted_root"
        CHAIN_INCOMPLETE = "chain_incomplete"
        CERTIFICATE_REVOKED = "certificate_revoked"
        UNKNOWN = "unknown_ssl_error"
        NOT_SSL_ERROR = "not_ssl_error"

    # Patterns to detect SSL error types from error messages
    ERROR_PATTERNS = {
        SSLErrorType.HOSTNAME_MISMATCH: [
            r'hostname.*mismatch',
            r'certificate verify failed.*hostname',
            r'doesn\'t match',
            r'SSL_ERROR_BAD_CERT_DOMAIN',
            r'CERTIFICATE_VERIFY_FAILED.*doesn\'t match',
        ],
        SSLErrorType.CERTIFICATE_EXPIRED: [
            r'certificate.*expired',
            r'certificate.*verify failed.*expired',
            r'notAfter',
        ],
        SSLErrorType.SELF_SIGNED: [
            r'self.signed',
            r'self-signed certificate',
            r'certificate verify failed.*self',
        ],
        SSLErrorType.UNTRUSTED_ROOT: [
            r'CERTIFICATE_VERIFY_FAILED',
            r'certificate verify failed',
            r'unable to get local issuer certificate',
            r'untrusted',
        ],
        SSLErrorType.CHAIN_INCOMPLETE: [
            r'chain.*incomplete',
            r'missing.*chain',
            r'intermediate.*certificate',
        ],
        SSLErrorType.CERTIFICATE_REVOKED: [
            r'revoked',
            r'certificate.*revoked',
        ],
    }

    @staticmethod
    def is_ssl_error(error: Exception) -> bool:
        """
        Check if an exception is SSL/certificate-related.

        Args:
            error: Exception to check

        Returns:
            True if SSL-related, False otherwise
        """
        if isinstance(error, ssl.SSLError):
            return True

        if isinstance(error, URLError):
            error_str = str(error)
            if 'ssl' in error_str.lower() or 'certificate' in error_str.lower():
                return True
            if isinstance(error.reason, ssl.SSLError):
                return True

        return False

    @staticmethod
    def classify_ssl_error(error: Exception) -> str:
        """
        Classify SSL error into a specific type.

        Args:
            error: Exception to classify

        Returns:
            Error type string (one of SSLErrorType constants)
        """
        error_str = str(error)
        error_lower = error_str.lower()

        # Check if it's an SSL error at all
        if not SSLErrorHandler.is_ssl_error(error):
            return SSLErrorHandler.SSLErrorType.NOT_SSL_ERROR

        # Try to match error type patterns
        for error_type, patterns in SSLErrorHandler.ERROR_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, error_str, re.IGNORECASE):
                    return error_type

        # If we know it's SSL but don't recognize it, return generic
        return SSLErrorHandler.SSLErrorType.UNKNOWN

    @staticmethod
    def get_error_details(error: Exception) -> Dict[str, Any]:
        """
        Extract detailed information from SSL error.

        Args:
            error: Exception to analyze

        Returns:
            Dictionary with error details
        """
        error_str = str(error)
        error_type = SSLErrorHandler.classify_ssl_error(error)
        is_ssl = SSLErrorHandler.is_ssl_error(error)

        details = {
            'is_ssl_error': is_ssl,
            'error_type': error_type,
            'error_message': error_str,
            'exception_class': error.__class__.__name__,
            'full_exception': repr(error),
        }

        # Extract specific details based on error type
        if error_type == SSLErrorHandler.SSLErrorType.HOSTNAME_MISMATCH:
            # Try to extract hostname info
            match = re.search(r"doesn't match '([^']+)'", error_str)
            if match:
                details['expected_hostname'] = match.group(1)

        elif error_type == SSLErrorHandler.SSLErrorType.CERTIFICATE_EXPIRED:
            # Certificate expiration detected
            details['reason'] = 'certificate_expired'

        elif error_type == SSLErrorHandler.SSLErrorType.SELF_SIGNED:
            # Self-signed certificate
            details['reason'] = 'self_signed'

        return details

    @staticmethod
    def should_retry_with_proxy(error: Exception) -> bool:
        """
        Determine if error should be retried with proxy.

        Hostname mismatch and self-signed issues might be bypassed with proxy,
        but certificate expiration or revocation should not.

        Args:
            error: Exception to evaluate

        Returns:
            True if should retry with proxy, False otherwise
        """
        error_type = SSLErrorHandler.classify_ssl_error(error)

        # These might be bypassable with proxy
        retryable = {
            SSLErrorHandler.SSLErrorType.HOSTNAME_MISMATCH,
            SSLErrorHandler.SSLErrorType.SELF_SIGNED,
            SSLErrorHandler.SSLErrorType.UNTRUSTED_ROOT,
        }

        return error_type in retryable

    @staticmethod
    def should_skip_url(error: Exception) -> bool:
        """
        Determine if URL should be skipped (not retried).

        Certain SSL errors indicate the URL is legitimately problematic.

        Args:
            error: Exception to evaluate

        Returns:
            True if should skip, False if should retry
        """
        error_type = SSLErrorHandler.classify_ssl_error(error)

        # These indicate fundamental issues - don't retry
        skip_errors = {
            SSLErrorHandler.SSLErrorType.CERTIFICATE_EXPIRED,
            SSLErrorHandler.SSLErrorType.CERTIFICATE_REVOKED,
        }

        return error_type in skip_errors

    @staticmethod
    def log_ssl_error(error: Exception, url: str, context: str = "") -> None:
        """
        Log SSL error with appropriate detail level.

        Args:
            error: Exception to log
            url: URL that caused the error
            context: Additional context about what was being done
        """
        is_ssl = SSLErrorHandler.is_ssl_error(error)

        if not is_ssl:
            return  # Not an SSL error, let other handlers deal with it

        details = SSLErrorHandler.get_error_details(error)
        error_type = details['error_type']
        error_msg = details['error_message']

        # Log with appropriate level
        log_context = f" ({context})" if context else ""
        base_msg = f"SSL Error for {url}{log_context}"

        if error_type == SSLErrorHandler.SSLErrorType.HOSTNAME_MISMATCH:
            logger.warning(
                f"{base_msg} | Type: HOSTNAME_MISMATCH | "
                f"Details: {error_msg}"
            )
        elif error_type == SSLErrorHandler.SSLErrorType.CERTIFICATE_EXPIRED:
            logger.warning(
                f"{base_msg} | Type: CERTIFICATE_EXPIRED | "
                f"The website's SSL certificate has expired"
            )
        elif error_type == SSLErrorHandler.SSLErrorType.SELF_SIGNED:
            logger.info(
                f"{base_msg} | Type: SELF_SIGNED_CERTIFICATE | "
                f"Website uses self-signed certificate (not trusted)"
            )
        else:
            logger.error(
                f"{base_msg} | Type: {error_type} | "
                f"Details: {error_msg}"
            )

    @staticmethod
    def get_recovery_strategy(error: Exception, url: str) -> Dict[str, Any]:
        """
        Determine recovery strategy for SSL error.

        Args:
            error: Exception to handle
            url: URL that caused the error

        Returns:
            Dictionary with recommended recovery strategy
        """
        details = SSLErrorHandler.get_error_details(error)
        error_type = details['error_type']

        strategy = {
            'error_type': error_type,
            'is_ssl_error': details['is_ssl_error'],
            'url': url,
            'should_skip': SSLErrorHandler.should_skip_url(error),
            'should_retry_with_proxy': SSLErrorHandler.should_retry_with_proxy(error),
            'max_retries': 0,
            'timeout_increase': 0,
            'error_message': details['error_message'],
        }

        if error_type == SSLErrorHandler.SSLErrorType.HOSTNAME_MISMATCH:
            strategy.update({
                'should_skip': False,
                'should_retry_with_proxy': True,
                'max_retries': 2,
                'timeout_increase': 10,
                'reason': 'Hostname mismatch - might work with proxy',
                'action': 'Retry with proxy if available, otherwise skip'
            })

        elif error_type == SSLErrorHandler.SSLErrorType.SELF_SIGNED:
            strategy.update({
                'should_skip': False,
                'should_retry_with_proxy': True,
                'max_retries': 1,
                'timeout_increase': 5,
                'reason': 'Self-signed certificate - might work with proxy',
                'action': 'Retry with proxy, mark as untrusted in output'
            })

        elif error_type == SSLErrorHandler.SSLErrorType.UNTRUSTED_ROOT:
            strategy.update({
                'should_skip': False,
                'should_retry_with_proxy': True,
                'max_retries': 2,
                'timeout_increase': 10,
                'reason': 'Untrusted root CA - might work with proxy',
                'action': 'Retry with proxy if available'
            })

        elif error_type == SSLErrorHandler.SSLErrorType.CERTIFICATE_EXPIRED:
            strategy.update({
                'should_skip': True,
                'should_retry_with_proxy': False,
                'max_retries': 0,
                'reason': 'Certificate is expired - security risk',
                'action': 'Skip this URL - certificate is expired'
            })

        elif error_type == SSLErrorHandler.SSLErrorType.CERTIFICATE_REVOKED:
            strategy.update({
                'should_skip': True,
                'should_retry_with_proxy': False,
                'max_retries': 0,
                'reason': 'Certificate is revoked - security risk',
                'action': 'Skip this URL - certificate is revoked'
            })

        else:  # UNKNOWN or other
            strategy.update({
                'should_skip': False,
                'should_retry_with_proxy': True,
                'max_retries': 1,
                'timeout_increase': 5,
                'reason': f'Unknown SSL error: {error_type}',
                'action': 'Try with proxy or skip'
            })

        return strategy


def wrap_error_handling(func):
    """
    Decorator to add SSL error logging to functions.

    Usage:
        @wrap_error_handling
        def scrape_url(self, url: str):
            ...
    """
    def wrapper(self, url: str, *args, **kwargs):
        try:
            return func(self, url, *args, **kwargs)
        except Exception as e:
            if SSLErrorHandler.is_ssl_error(e):
                SSLErrorHandler.log_ssl_error(e, url, context=func.__name__)
                # Re-raise so caller can decide what to do
            raise

    return wrapper


# Example usage in logging config
def setup_ssl_error_logging():
    """Setup detailed SSL error logging."""
    logger = logging.getLogger('ssl_error_handler')
    logger.setLevel(logging.DEBUG)

    # File handler for SSL errors
    handler = logging.FileHandler('ssl_errors.log')
    handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)


if __name__ == "__main__":
    # Test examples
    import ssl as ssl_module

    logging.basicConfig(level=logging.DEBUG)

    # Test case 1: Hostname mismatch error
    print("\n=== Test 1: Hostname Mismatch ===")
    error1 = URLError(ssl_module.SSLError(
        "hostname 'example.com' doesn't match 'sub.example.com'"
    ))
    details1 = SSLErrorHandler.get_error_details(error1)
    strategy1 = SSLErrorHandler.get_recovery_strategy(error1, "https://example.com")
    print(f"Error Type: {details1['error_type']}")
    print(f"Strategy: {strategy1}")
    SSLErrorHandler.log_ssl_error(error1, "https://example.com", "static_fetch")

    # Test case 2: Certificate expired
    print("\n=== Test 2: Certificate Expired ===")
    error2 = URLError(ssl_module.SSLError(
        "certificate verify failed: certificate has expired"
    ))
    details2 = SSLErrorHandler.get_error_details(error2)
    strategy2 = SSLErrorHandler.get_recovery_strategy(error2, "https://expired.example.com")
    print(f"Error Type: {details2['error_type']}")
    print(f"Strategy: {strategy2}")
    SSLErrorHandler.log_ssl_error(error2, "https://expired.example.com", "static_fetch")

    # Test case 3: Self-signed certificate
    print("\n=== Test 3: Self-Signed Certificate ===")
    error3 = URLError(ssl_module.SSLError(
        "certificate verify failed: self-signed certificate"
    ))
    details3 = SSLErrorHandler.get_error_details(error3)
    strategy3 = SSLErrorHandler.get_recovery_strategy(error3, "https://selfsigned.example.com")
    print(f"Error Type: {details3['error_type']}")
    print(f"Strategy: {strategy3}")
    SSLErrorHandler.log_ssl_error(error3, "https://selfsigned.example.com", "static_fetch")

    # Test case 4: Non-SSL error (should not be handled)
    print("\n=== Test 4: Non-SSL Error ===")
    error4 = ValueError("This is not an SSL error")
    is_ssl = SSLErrorHandler.is_ssl_error(error4)
    print(f"Is SSL Error: {is_ssl}")
