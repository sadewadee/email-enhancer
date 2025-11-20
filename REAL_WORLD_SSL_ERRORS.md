# Real-World SSL Error Examples & Solutions

## Case Study 1: SEC_ERROR_UNKNOWN_ISSUER (allgoodpilates.com)

### 🔴 The Error

```
ERROR - Dynamic fetch error: https://www.allgoodpilates.com/ |
Page.goto: SEC_ERROR_UNKNOWN_ISSUER
```

**Error Code**: `SEC_ERROR_UNKNOWN_ISSUER`
**Browser**: Playwright (Firefox engine)
**Root Cause**: SSL certificate issuer is not recognized/trusted

---

## What This Error Means

### Analysis

```
SSL Certificate Issue:
├─ URL: https://www.allgoodpilates.com/
├─ Certificate: Valid but issuer unknown
├─ Validation Result: ✗ FAILED (untrusted root CA)
└─ Browser Response: SEC_ERROR_UNKNOWN_ISSUER
```

### Possible Causes

1. **Self-signed Certificate**: Server uses own CA (not in browser's trust store)
2. **Intermediate Certificate Missing**: Server didn't send intermediate certificate
3. **Custom/Internal CA**: Organization uses private CA not recognized by Firefox
4. **Expired CA**: Certificate issuer's certificate expired
5. **CAA Record Issue**: DNS CAA record misconfiguration
6. **Let's Encrypt Migration**: Expired intermediate during renewal

### Is It a Security Issue?

**Risk Level**: MEDIUM
- ✗ Certificate validation correctly FAILED (secure)
- ✓ Website is not compromised (in most cases)
- ⚠️ May indicate misconfigured server or internal-only certificate

---

## How It's Handled Now

### Static Fetch (urllib) - Did NOT Reach This Point
```python
Static fetch would also fail with similar error, but
_try_static_fetch() catches URLError and falls back to dynamic fetch
```

### Dynamic Fetch (Playwright) - WHERE ERROR OCCURRED
```
1. Playwright attempts to navigate to URL
   └─ Firefox engine validates SSL certificate

2. Validation FAILS with: SEC_ERROR_UNKNOWN_ISSUER
   └─ Error is raised from Playwright

3. BEFORE Enhancement:
   └─ Generic Exception catch
   └─ Error logged as: "Page.goto: SEC_ERROR_UNKNOWN_ISSUER"
   └─ No classification, no recovery strategy
   └─ CSV: "failed" with generic error message

4. AFTER Enhancement:
   └─ Error is detected as SSL-related
   └─ Classified as: UNTRUSTED_ROOT
   └─ Recovery strategy: Retry with proxy
   └─ Detailed logging with context
   └─ CSV: "failed" with specific SSL error type
```

---

## Enhanced Error Handling

### Error Detection & Classification

```python
# In _subprocess_fetch() - Line 685-704
error_msg = "Page.goto: SEC_ERROR_UNKNOWN_ISSUER"

if 'SEC_ERROR' in error_msg:
    error_type = "ssl_certificate (SEC_ERROR_UNKNOWN_ISSUER)"
    # Passed to queue for further processing
    q.put({'ok': False, 'error': error_msg, 'error_type': error_type})
```

### Dynamic Fetch Error Handling

```python
# In _fetch_with_timeout() - Line 1603-1633
error_msg = "Page.goto: SEC_ERROR_UNKNOWN_ISSUER"
error_type = "ssl_certificate (SEC_ERROR_UNKNOWN_ISSUER)"

# Detect SSL error
if 'SEC_ERROR' in error_msg:
    # Create exception for SSLErrorHandler
    ssl_exception = PlaywrightSSLError(error_msg)

    # Classify using SSLErrorHandler
    error_class = SSLErrorHandler.classify_ssl_error(ssl_exception)
    # Result: 'untrusted_root'

    # Get recovery strategy
    strategy = SSLErrorHandler.get_recovery_strategy(ssl_exception, url)
    # Result: {
    #     'error_type': 'untrusted_root',
    #     'should_skip': False,
    #     'should_retry_with_proxy': True,
    #     'max_retries': 2,
    #     'reason': 'Untrusted root CA - might work with proxy'
    # }

    # Log with detail
    SSLErrorHandler.log_ssl_error(ssl_exception, url, context="dynamic_fetch")
    # Log output:
    # ERROR: SSL Error for https://www.allgoodpilates.com/ (dynamic_fetch)
    #        Type: UNTRUSTED_ROOT
    #        Details: Page.goto: SEC_ERROR_UNKNOWN_ISSUER
```

### CSV Output

**Before Enhancement**:
```csv
url,scraping_status,scraping_error
https://www.allgoodpilates.com,failed,"Page.goto: SEC_ERROR_UNKNOWN_ISSUER"
```

**After Enhancement**:
```csv
url,scraping_status,scraping_error,ssl_error_type
https://www.allgoodpilates.com,failed,"SSL Error (untrusted_root): Untrusted root CA - might work with proxy",untrusted_root
```

---

## Supported Playwright SSL Error Codes

| Error Code | Type | Meaning | Action |
|-----------|------|---------|--------|
| **SEC_ERROR_UNKNOWN_ISSUER** | UNTRUSTED_ROOT | Issuer not recognized | Retry with proxy |
| **SEC_ERROR_UNTRUSTED_ISSUER** | UNTRUSTED_ROOT | Issuer not trusted | Retry with proxy |
| **SEC_ERROR_SELF_SIGNED_CERT** | SELF_SIGNED | Self-signed cert | Retry with proxy |
| **SEC_ERROR_EXPIRED_CERTIFICATE** | CERTIFICATE_EXPIRED | Cert expired | Skip (security) |
| **SEC_ERROR_REVOKED_CERTIFICATE** | CERTIFICATE_REVOKED | Cert revoked | Skip (security) |
| **SEC_ERROR_INCOMPLETE_CERT_CHAIN** | CHAIN_INCOMPLETE | Missing intermediate | Retry with timeout |
| **MOZILLA_PKIX_ERROR_UNKNOWN_ISSUER** | UNTRUSTED_ROOT | Firefox NSS error | Retry with proxy |

---

## Solutions for Website Operators

If you encounter `SEC_ERROR_UNKNOWN_ISSUER` on your website:

### 1. **Check Certificate Chain**
```bash
# Download certificate and check chain
openssl s_client -connect www.allgoodpilates.com:443 -showcerts

# Look for: "Verify return code: 0 (ok)"
# If not 0, certificate chain is incomplete
```

### 2. **Verify Intermediate Certificate**
```bash
# The server should send BOTH:
# 1. Website certificate
# 2. Intermediate certificate
# (Root certificate is on client side)

# Check if intermediate is included:
openssl s_client -connect www.allgoodpilates.com:443 \
  -showcerts | grep -A 2 "issuer="
```

### 3. **Common Fixes**

**For Let's Encrypt users**:
```
Nginx:
  ssl_certificate /etc/letsencrypt/live/domain/fullchain.pem;
  # ✓ fullchain.pem includes intermediate

Apache:
  SSLCertificateFile /etc/letsencrypt/live/domain/cert.pem
  SSLCertificateChainFile /etc/letsencrypt/live/domain/chain.pem
  SSLCertificateKeyFile /etc/letsencrypt/live/domain/privkey.pem
```

**For Custom CA**:
```
1. Export the certificate chain (including intermediate)
2. Set SSLCertificateChainFile to the chain file
3. Verify with: openssl s_client -showcerts
```

### 4. **Test the Fix**
```bash
# After fixing, test with:
curl -v https://www.allgoodpilates.com

# Or test with Firefox:
# Navigate to site and check certificate details
# Should show valid chain with no warnings
```

---

## How Our System Handles It

### Current Behavior Flow

```
URL: https://www.allgoodpilates.com/

1. process_single_url()
   ├─ URLCleaner.clean_url() ✓ (cleans any tracking params)
   └─ Validation ✓ (URL format valid)

2. scraper.gather_contact_info(url)
   ├─ scrape_url(url) attempts fetching
   │  ├─ Attempt 1: _try_static_fetch()
   │  │  └─ urllib.urlopen() → URLError → SSL error
   │  │     └─ SSLErrorHandler detects → Fallback to dynamic
   │  │
   │  └─ Attempt 2: _fetch_with_timeout()
   │     └─ Playwright.Page.goto() → SEC_ERROR_UNKNOWN_ISSUER
   │        └─ SSLErrorHandler.classify() → UNTRUSTED_ROOT
   │        └─ SSLErrorHandler.log_ssl_error() → detailed logging
   │        └─ SSLErrorHandler.get_recovery_strategy() → Retry with proxy
   │        └─ Returns None (error result)
   │
   └─ Result: status='failed', error='SSL Error (untrusted_root)...'

3. CSV Output
   └─ URL written with specific SSL error
```

### Logging Output

```
2025-11-20 13:15:30 ERROR:web_scraper:Dynamic fetch error:
  https://www.allgoodpilates.com/ | Type: ssl_certificate (SEC_ERROR_UNKNOWN_ISSUER) |
  Page.goto: SEC_ERROR_UNKNOWN_ISSUER

2025-11-20 13:15:30 ERROR:ssl_error_handler:SSL Error for
  https://www.allgoodpilates.com/ (dynamic_fetch) |
  Type: UNTRUSTED_ROOT | Details: Page.goto: SEC_ERROR_UNKNOWN_ISSUER

2025-11-20 13:15:30 DEBUG:web_scraper:SSL recovery strategy: {
  'error_type': 'untrusted_root',
  'should_skip': False,
  'should_retry_with_proxy': True,
  'max_retries': 2,
  'reason': 'Untrusted root CA - might work with proxy'
}
```

---

## Future Enhancement: Automatic Proxy Retry

When this occurs, the system could automatically:

```python
# Pseudo-code for future enhancement
if strategy['should_retry_with_proxy'] and not proxy_used:
    # Get next proxy
    proxy = proxy_manager.get_next_proxy()

    # Retry with proxy
    result = _fetch_with_timeout(url, proxy_config=proxy)

    if result:
        # Success! Extract contacts
        return extract_contacts(result)
    else:
        # Proxy also failed
        return error_result
```

---

## Related Documentation

- [SSL_ERROR_HANDLING.md](SSL_ERROR_HANDLING.md) - Complete SSL error handling guide
- [ssl_error_handler.py](ssl_error_handler.py) - Source code
- [web_scraper.py](web_scraper.py) - Integration points

---

## Testing

To test this error handling:

```python
from ssl_error_handler import SSLErrorHandler

# Test SEC_ERROR_UNKNOWN_ISSUER classification
error_msg = "Page.goto: SEC_ERROR_UNKNOWN_ISSUER"

class TestError(Exception):
    pass

test_error = TestError(error_msg)

# Check detection
is_ssl = SSLErrorHandler.is_ssl_error(test_error)
print(f"Is SSL Error: {is_ssl}")  # True

# Classify
error_type = SSLErrorHandler.classify_ssl_error(test_error)
print(f"Error Type: {error_type}")  # untrusted_root

# Get strategy
strategy = SSLErrorHandler.get_recovery_strategy(test_error, "https://example.com")
print(f"Strategy: {strategy['reason']}")  # "Untrusted root CA - might work with proxy"
```

---

**Last Updated:** 2025-11-20
**Status:** Production Ready
