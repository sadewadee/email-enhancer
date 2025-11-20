"""
Test script for URL Cleaner module.
Verifies URL cleanup functionality with various invalid/non-standard URLs.
"""

import logging
import sys
from url_cleaner import URLCleaner, clean_url, is_google_redirect
import json

# Setup logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(levelname)-8s | %(message)s'
)
logger = logging.getLogger(__name__)


def print_section(title):
    """Print a formatted section header."""
    print(f"\n{'=' * 80}")
    print(f"  {title}")
    print(f"{'=' * 80}")


def test_google_redirect_urls():
    """Test Google search redirect URL detection and extraction."""
    print_section("Test 1: Google Redirect URLs")

    test_cases = [
        {
            'url': "/url?q=http://www.allgoodpilates.com/&opi=79508299&sa=U&ved=0ahUKEwjc-Pmd1_mQAxVH_rsIHV-vAGAQ61gIEigO&usg=AOvVaw1m90NxHswwfuN1m1MmCfH9",
            'expected': "http://www.allgoodpilates.com/",
            'description': "Google search redirect with tracking params"
        },
        {
            'url': "https://www.google.com/url?q=https://example.com/page&hl=en",
            'expected': "https://example.com/page",
            'description': "Google URL redirect (absolute path)"
        },
        {
            'url': "http://www.example.com",
            'expected': None,  # Should be recognized as NOT a redirect
            'description': "Normal URL (not a redirect)"
        },
    ]

    for i, test in enumerate(test_cases, 1):
        url = test['url']
        is_redirect = URLCleaner.is_google_redirect_url(url)
        extracted = URLCleaner.extract_google_redirect_url(url) if is_redirect else None
        expected = test['expected']

        print(f"\n  Test 1.{i}: {test['description']}")
        print(f"    Input:    {url[:80]}...")
        print(f"    Is redirect: {is_redirect}")
        if is_redirect:
            print(f"    Extracted: {extracted}")
            print(f"    Expected:  {expected}")
            status = "✓ PASS" if extracted == expected else "✗ FAIL"
        else:
            status = "✓ PASS" if expected is None else "✗ FAIL"
        print(f"    Status:    {status}")


def test_tracking_parameter_removal():
    """Test removal of tracking and analytics parameters."""
    print_section("Test 2: Tracking Parameter Removal")

    test_cases = [
        {
            'url': "https://example.com/?utm_source=google&utm_medium=cpc&utm_campaign=test&page=1",
            'expected': "https://example.com/?page=1",
            'description': "Remove Google Analytics UTM parameters"
        },
        {
            'url': "https://example.com/page?fbclid=IwAR123&id=5&gclid=ABC123",
            'expected': "https://example.com/page?id=5",
            'description': "Remove Facebook and Google Ads tracking"
        },
        {
            'url': "https://example.com/?opi=1&sa=U&ved=123&usg=ABC",
            'expected': "https://example.com/",
            'description': "Remove Google Search parameters"
        },
        {
            'url': "https://example.com/page",
            'expected': "https://example.com/page",
            'description': "Clean URL (no params to remove)"
        },
    ]

    for i, test in enumerate(test_cases, 1):
        url = test['url']
        cleaned = URLCleaner.remove_tracking_parameters(url)
        expected = test['expected']

        print(f"\n  Test 2.{i}: {test['description']}")
        print(f"    Input:    {url}")
        print(f"    Cleaned:  {cleaned}")
        print(f"    Expected: {expected}")
        status = "✓ PASS" if cleaned == expected else "✗ FAIL"
        print(f"    Status:   {status}")


def test_protocol_normalization():
    """Test URL protocol normalization."""
    print_section("Test 3: Protocol Normalization")

    test_cases = [
        {
            'url': "example.com",
            'expected': "https://example.com",
            'description': "Add protocol to URL without one (prefer HTTPS)"
        },
        {
            'url': "http://example.com",
            'expected': "https://example.com",
            'description': "Convert HTTP to HTTPS"
        },
        {
            'url': "https://example.com",
            'expected': "https://example.com",
            'description': "HTTPS URL unchanged"
        },
        {
            'url': "//example.com",
            'expected': "https://example.com",
            'description': "Protocol-relative URL converted to HTTPS"
        },
    ]

    for i, test in enumerate(test_cases, 1):
        url = test['url']
        normalized = URLCleaner.normalize_protocol(url, prefer_https=True)
        expected = test['expected']

        print(f"\n  Test 3.{i}: {test['description']}")
        print(f"    Input:    {url}")
        print(f"    Normalized: {normalized}")
        print(f"    Expected:   {expected}")
        status = "✓ PASS" if normalized == expected else "✗ FAIL"
        print(f"    Status:     {status}")


def test_comprehensive_cleanup():
    """Test comprehensive URL cleanup pipeline."""
    print_section("Test 4: Comprehensive URL Cleanup Pipeline")

    test_cases = [
        {
            'url': "/url?q=http://www.allgoodpilates.com/&opi=79508299&sa=U&ved=0ahUKEwjc-Pmd1_mQAxVH_rsIHV-vAGAQ61gIEigO&usg=AOvVaw1m90NxHswwfuN1m1MmCfH9",
            'expected': "https://www.allgoodpilates.com/",
            'description': "Google redirect + tracking params → clean URL"
        },
        {
            'url': "  EXAMPLE.COM  ",
            'expected': "https://example.com",
            'description': "Whitespace + uppercase + no protocol"
        },
        {
            'url': "https://example.com/page?utm_source=facebook&utm_medium=feed&id=123#section",
            'expected': "https://example.com/page?id=123",
            'description': "Tracking params + fragment removed"
        },
        {
            'url': "INVALID_URL_FORMAT",
            'expected': None,
            'description': "Invalid URL format (should return None)"
        },
        {
            'url': "",
            'expected': None,
            'description': "Empty URL"
        },
        {
            'url': "https://example.com",
            'expected': "https://example.com",
            'description': "Already clean URL (unchanged)"
        },
    ]

    results = {'pass': 0, 'fail': 0}

    for i, test in enumerate(test_cases, 1):
        url = test['url']
        cleaned = clean_url(url, aggressive=False)
        expected = test['expected']

        print(f"\n  Test 4.{i}: {test['description']}")
        print(f"    Input:    '{url}'")
        print(f"    Cleaned:  {cleaned}")
        print(f"    Expected: {expected}")

        if cleaned == expected:
            status = "✓ PASS"
            results['pass'] += 1
        else:
            status = "✗ FAIL"
            results['fail'] += 1

        print(f"    Status:   {status}")

    print(f"\n  Summary: {results['pass']} passed, {results['fail']} failed")
    return results


def test_csv_integration_scenario():
    """Test realistic CSV processing scenario."""
    print_section("Test 5: CSV Integration Scenario")

    print("\n  Simulating CSV row processing with various URL formats...")

    # Simulate URLs from a CSV file
    csv_urls = [
        "http://www.allgoodpilates.com/",  # Already clean
        "/url?q=http://example.com&opi=123&ved=456",  # Google redirect
        "https://example.com/?utm_source=google&utm_campaign=2024",  # Tracking params
        "EXAMPLE.COM",  # Missing protocol + uppercase
        "  https://example.com/page  ",  # Whitespace
        "INVALID!!!",  # Invalid URL
        "https://example.com/contact?ref=other&id=789",  # Valid with some params
    ]

    valid_count = 0
    invalid_count = 0
    cleaned_count = 0

    print("\n  Processing URLs:")
    for i, url in enumerate(csv_urls, 1):
        cleaned = clean_url(url)
        if cleaned:
            valid_count += 1
            if cleaned != url:
                cleaned_count += 1
            print(f"    {i}. ✓ {url[:50]:50s} → {cleaned}")
        else:
            invalid_count += 1
            print(f"    {i}. ✗ {url[:50]:50s} → INVALID (skipped)")

    print(f"\n  Summary:")
    print(f"    Total URLs:     {len(csv_urls)}")
    print(f"    Valid:          {valid_count}")
    print(f"    Invalid:        {invalid_count}")
    print(f"    Cleaned:        {cleaned_count} (changed format)")
    print(f"    Already clean:  {valid_count - cleaned_count}")


def main():
    """Run all tests."""
    print("\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 20 + "URL CLEANER TEST SUITE" + " " * 35 + "║")
    print("╚" + "=" * 78 + "╝")

    try:
        # Run individual tests
        test_google_redirect_urls()
        test_tracking_parameter_removal()
        test_protocol_normalization()
        results = test_comprehensive_cleanup()
        test_csv_integration_scenario()

        # Final summary
        print_section("Test Summary")
        if results['fail'] == 0:
            print("\n  ✓ All tests PASSED!")
        else:
            print(f"\n  ✗ Some tests FAILED: {results['fail']} failures")
            sys.exit(1)

    except Exception as e:
        print(f"\n✗ Test execution failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
