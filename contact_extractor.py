"""
Contact Extractor Module
Handles extraction of emails, phone numbers, and WhatsApp contacts from HTML content.
"""

import re
import phonenumbers
from bs4 import BeautifulSoup
from typing import Set, List, Dict, Tuple, Optional
from urllib.parse import urljoin, urlparse, unquote


def get_best_parser() -> str:
    """Get the best available HTML parser (lxml > html.parser)."""
    try:
        import lxml
        return 'lxml'
    except ImportError:
        return 'html.parser'


class ContactExtractor:
    """Extract and normalize contact information from HTML content."""

    def __init__(self):
        # Use best available parser (lxml is 3-5x faster than html.parser)
        self.parser = get_best_parser()

        # Email regex pattern with stricter validation:
        # - Local part must start with letter (not number)
        # - Domain must be valid (not file extensions)
        # - TLD must be 2+ letters (not png, jpg, etc)
        self.email_pattern = re.compile(
            r'\b[A-Za-z][A-Za-z0-9._%+-]*@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b'
        )

        # Phone number pattern - optimized single regex combining all 6 patterns
        # Matches: +1(234)567-8900, (234) 567-8900, 234-567-8900, tel: links, etc.
        self.phone_pattern = re.compile(
            r'(?:tel:)?'  # Optional tel: prefix
            r'(?:'
            r'(?:\+?\d{1,3}[-.\s]?)?'  # Optional country code
            r'\(?(?:\d{2,4}|[0-9().\s\-]{3,10})\)?'  # Area code (various formats)
            r'(?:[-.\s]?(?:\d{3,4}))?'  # Prefix
            r'(?:[-.\s]?(?:\d{3,4}))?'  # Line number
            r'|'
            r'[\+\d\-\(\)\s\.]{10,}'  # Tel link format
            r')',
            re.VERBOSE
        )

        # WhatsApp patterns
        self.whatsapp_patterns = [
            re.compile(r'wa\.me/(\+?\d+)', re.IGNORECASE),
            re.compile(r'api\.whatsapp\.com/send\?phone=(\+?\d+)', re.IGNORECASE),
            re.compile(r'whatsapp://send\?phone=(\+?\d+)', re.IGNORECASE),
        ]

        # Social media patterns - matches URLs and extracts handles/usernames
        self.social_patterns = {
            'facebook': [
                re.compile(r'(?:https?://)?(?:www\.)?(?:facebook\.com|fb\.com)/([^/?#\s&]+)', re.IGNORECASE),
            ],
            'instagram': [
                re.compile(r'(?:https?://)?(?:www\.)?instagram\.com/([^/?#\s&]+)', re.IGNORECASE),
            ],
            'tiktok': [
                re.compile(r'(?:https?://)?(?:www\.)?tiktok\.com/@?([^/?#\s&]+)', re.IGNORECASE),
            ],
            'youtube': [
                re.compile(r'(?:https?://)?(?:www\.)?youtube\.com/(?:channel/|c/|@|user/)?([^/?#\s&]+)', re.IGNORECASE),
            ],
        }

        # Email obfuscation patterns (restricted to safe variants)
        # Avoid broad replacements like "at"/"dot" without boundaries which caused false positives
        self.obfuscation_patterns = {
            r'\[at\]': '@',
            r'\(at\)': '@',
            r'\bat\b': '@',
            r'\[dot\]': '.',
            r'\(dot\)': '.',
            r'\bdot\b': '.',
        }
        # Load Public Suffix List tokens (last label) for safer TLD checks
        self._psl_tld_tokens: Optional[Set[str]] = None
        try:
            import tldextract
            ext = tldextract.TLDExtract()
            # Convert PSL rules to last label tokens (e.g., 'co.uk' -> 'uk')
            tokens = set()
            for rule in ext.tlds:
                rule = str(rule).lstrip('*.!')
                if not rule:
                    continue
                tokens.add(rule.split('.')[-1].lower())
            self._psl_tld_tokens = tokens
        except Exception:
            self._psl_tld_tokens = None

        # Common alias local-parts used for sitewide contacts
        self._common_aliases = {
            'info', 'contact', 'hello', 'support', 'sales', 'admin', 'office',
            'booking', 'reservations', 'service', 'team', 'studio', 'manager',
            'care', 'customerservice', 'enquiry', 'billing', 'accounts'
        }

        # Common country or region prefixes sometimes glued before alias
        self._country_prefixes = {
            'us', 'uk', 'au', 'ca', 'de', 'fr', 'it', 'es', 'nl', 'jp', 'sg', 'my', 'id',
            'ph', 'th', 'vn', 'br', 'mx', 'se', 'no', 'fi', 'dk', 'pl', 'cz', 'sk', 'hu',
            'ro', 'bg', 'gr', 'pt', 'tr', 'sa', 'ae', 'qa', 'kw', 'om', 'tw', 'kr', 'il',
            'ar', 'cl', 'pe', 'uy', 'nz', 'ie', 'ch', 'at', 'be', 'lu', 'li'
        }

        # Placeholder domains to ignore (samples, builders, defaults)
        self._placeholder_domains = {
            'mysite.com', 'example.com', 'yourdomain.com', 'domain.com', 'godaddy.com',
            'cms.hhs.gov'
        }

    def _is_html_like(self, s: str) -> bool:
        """Detect if input string looks like HTML markup rather than a URL/plain text."""
        if not isinstance(s, str) or not s.strip():
            return False
        s_strip = s.strip()
        # If it resembles a URL or locator (http, https, mailto, tel, whatsapp), treat as non-HTML
        parsed = urlparse(s_strip)
        if parsed.scheme:
            return False
        # Basic check for presence of HTML tags
        return bool(re.search(r"<[^>]+>", s_strip))

    def extract_emails(self, html: str, base_url: str = "") -> List[Dict]:
        """
        Extract unique email addresses from HTML content.

        Args:
            html (str): HTML content as string
            base_url (str): Base URL for context

        Returns:
            List[Dict]: List of email dictionaries with metadata
        """
        emails = []
        html_str = html or ""
        is_html = self._is_html_like(html_str)
        soup = BeautifulSoup(html_str, self.parser) if is_html else None

        # Extract from mailto links
        if is_html and soup is not None:
            mailto_links = soup.find_all('a', href=re.compile(r'^mailto:', re.IGNORECASE))
            for link in mailto_links:
                href = link.get('href', '')
                email_match = re.search(r'mailto:([^?&\s]+)', href, re.IGNORECASE)
                if email_match:
                    raw_email = email_match.group(1)
                    normalized = self._normalize_email(raw_email)
                    if normalized and not self._is_placeholder_email(normalized):
                        emails.append({
                            'field': 'email',
                            'value_raw': raw_email,
                            'value_normalized': normalized,
                            'contact_source_page': f"mailto:{raw_email}",
                            'source_url': base_url
                        })
        else:
            # Non-HTML: scan string for mailto: occurrences
            for raw_email in re.findall(r'mailto:([^?&\s]+)', html_str, flags=re.IGNORECASE):
                normalized = self._normalize_email(raw_email)
                if normalized and not self._is_placeholder_email(normalized):
                    emails.append({
                        'field': 'email',
                        'value_raw': raw_email,
                        'value_normalized': normalized,
                        'contact_source_page': f"mailto:{raw_email}",
                        'source_url': base_url
                    })

        # Extract Cloudflare-protected emails (data-cfemail)
        def _decode_cfemail(encoded_hex: str) -> Optional[str]:
            try:
                encoded_hex = encoded_hex.strip()
                data = bytes.fromhex(encoded_hex)
                key = data[0]
                decoded = ''.join(chr(b ^ key) for b in data[1:])
                return self._normalize_email(decoded)
            except Exception:
                return None

        # 1) span.__cf_email__ with data-cfemail
        if is_html and soup is not None:
            cf_spans = soup.find_all('span', attrs={'data-cfemail': True})
            for span in cf_spans:
                hexstr = span.get('data-cfemail')
                decoded = _decode_cfemail(hexstr) if hexstr else None
                if decoded and not self._is_placeholder_email(decoded):
                    emails.append({
                        'field': 'email',
                        'value_raw': decoded,
                        'value_normalized': decoded,
                        'contact_source_page': 'cloudflare:data-cfemail',
                        'source_url': base_url
                    })

        # 2) anchors pointing to /cdn-cgi/l/email-protection#<hex>
        if is_html and soup is not None:
            cf_links = soup.find_all('a', href=re.compile(r'/cdn-cgi/l/email-protection#', re.IGNORECASE))
            for link in cf_links:
                href = link.get('href', '')
                try:
                    hex_part = href.split('#', 1)[1]
                except IndexError:
                    hex_part = ''
                decoded = _decode_cfemail(hex_part) if hex_part else None
                if decoded and not self._is_placeholder_email(decoded):
                    emails.append({
                        'field': 'email',
                        'value_raw': decoded,
                        'value_normalized': decoded,
                        'contact_source_page': 'cloudflare:href',
                        'source_url': base_url
                    })

        # Extract from element attributes that often carry emails (onclick/data-email/...)
        # 1) onclick/onClick containing mailto:
        if is_html and soup is not None:
            attr_mailto_elems = soup.find_all(attrs={
                'onclick': re.compile(r'mailto:', re.IGNORECASE)
            })
            # Also handle camelCase onClick
            attr_mailto_elems += soup.find_all(attrs={
                'onClick': re.compile(r'mailto:', re.IGNORECASE)
            })
            for elem in attr_mailto_elems:
                onclick = elem.get('onclick', '')
                if not onclick:
                    onclick = elem.get('onClick', '')
                email_match = re.search(r"mailto:([^\"'\s]+)", onclick, re.IGNORECASE)
                if email_match:
                    raw_email = email_match.group(1)
                    normalized = self._normalize_email(raw_email)
                    if normalized:
                        emails.append({
                            'field': 'email',
                            'value_raw': raw_email,
                            'value_normalized': normalized,
                            'contact_source_page': 'onclick',
                            'source_url': base_url
                        })

        # 2) Any attribute whose name suggests email and contains an email-like value
        if is_html and soup is not None:
            for elem in soup.find_all(True):
                for attr_key, attr_val in list(elem.attrs.items()):
                    if not isinstance(attr_val, str):
                        continue
                    key_l = str(attr_key).lower()
                    if ('email' in key_l or 'mail' in key_l):
                        # Prefer a strict email pattern over free-form regex in text
                        for match in self.email_pattern.findall(attr_val):
                            normalized = self._normalize_email(match)
                            if normalized and not self._is_placeholder_email(normalized):
                                emails.append({
                                    'field': 'email',
                                    'value_raw': match,
                                    'value_normalized': normalized,
                                    'contact_source_page': f'attr:{attr_key}',
                                    'source_url': base_url
                                })

        # Extract from text content - simplified approach like sampler.txt
        text_content = soup.get_text() if is_html and soup is not None else html_str

        # Apply deobfuscation first (safe variants only)
        deobfuscated_text = text_content
        for pattern, replacement in self.obfuscation_patterns.items():
            deobfuscated_text = re.sub(pattern, replacement, deobfuscated_text, flags=re.IGNORECASE)

        # Find emails in deobfuscated text - using set for automatic deduplication
        email_matches = set(self.email_pattern.findall(deobfuscated_text))
        for raw_email in email_matches:
            normalized = self._normalize_email(raw_email)
            if normalized and not self._is_placeholder_email(normalized):
                emails.append({
                    'field': 'email',
                    'value_raw': raw_email,
                    'value_normalized': normalized,
                    'contact_source_page': base_url,
                    'source_url': base_url
                })

        # Remove duplicates based on normalized email
        seen = set()
        unique_emails = []
        for email in emails:
            if email['value_normalized'] not in seen:
                seen.add(email['value_normalized'])
                unique_emails.append(email)

        # Filter out implausible local-parts (e.g., includes 'email', 'mailto', etc.)
        plausible_emails = []
        for email in unique_emails:
            try:
                local = email['value_normalized'].split('@', 1)[0]
            except Exception:
                continue
            if self._is_plausible_local_part(local):
                plausible_emails.append(email)

        # Dedupe suspicious variants (e.g., hypmalta vs maltaemailhypmalta)
        filtered_emails = self._dedupe_suspicious_variants(plausible_emails)

        return filtered_emails

    def extract_phones(self, html: str, base_url: str = "", country_code: str = None) -> List[Dict]:
        """
        Extract and normalize phone numbers from HTML content.

        Args:
            html (str): HTML content as string
            base_url (str): Base URL for context
            country_code (str): Country code for phone number parsing (e.g., 'ID', 'US')

        Returns:
            List[Dict]: List of phone dictionaries with metadata
        """
        phones = []
        html_str = html or ""
        is_html = self._is_html_like(html_str)
        soup = BeautifulSoup(html_str, self.parser) if is_html else None

        # Extract from tel links
        if is_html and soup is not None:
            tel_links = soup.find_all('a', href=re.compile(r'^tel:', re.IGNORECASE))
            for link in tel_links:
                href = link.get('href', '')
                phone_match = re.search(r'tel:([^?&\s]+)', href, re.IGNORECASE)
                if phone_match:
                    raw_phone = phone_match.group(1)
                    normalized, number_type = self._normalize_phone(raw_phone, country_code)
                    if normalized:
                        phones.append({
                            'field': 'phone',
                            'value_raw': raw_phone,
                            'value_normalized': normalized,
                            'number_type': number_type,
                            'contact_source_page': f"tel:{raw_phone}",
                            'source_url': base_url
                        })
        else:
            # Non-HTML: scan string for tel: occurrences
            for raw_phone in re.findall(r'tel:([^?&\s]+)', html_str, flags=re.IGNORECASE):
                normalized, number_type = self._normalize_phone(raw_phone, country_code)
                if normalized:
                    phones.append({
                        'field': 'phone',
                        'value_raw': raw_phone,
                        'value_normalized': normalized,
                        'number_type': number_type,
                        'contact_source_page': f"tel:{raw_phone}",
                        'source_url': base_url
                    })

        # Extract from text content - simplified approach like sampler.txt
        text_content = soup.get_text() if is_html and soup is not None else html_str

        # Find potential phone numbers using optimized single pattern
        all_phone_matches = set()
        matches = self.phone_pattern.findall(text_content)
        for match in matches:
            # Handle tuple results from regex groups
            if isinstance(match, tuple):
                # Take the full match (first element) or the longest non-empty element
                phone_str = match[0] if match[0] else max(match, key=len) if any(match) else ""
            else:
                phone_str = match

            # Clean up the match
            phone_str = phone_str.replace('tel:', '').strip()
            if phone_str and len(phone_str) >= 7:  # Minimum phone length
                all_phone_matches.add(phone_str)

        # Process all unique phone matches
        for raw_phone in all_phone_matches:
            normalized, number_type = self._normalize_phone(raw_phone, country_code)
            if normalized:
                phones.append({
                    'field': 'phone',
                    'value_raw': raw_phone,
                    'value_normalized': normalized,
                    'number_type': number_type,
                    'contact_source_page': base_url,
                    'source_url': base_url
                })

        # Remove duplicates based on normalized phone
        seen = set()
        unique_phones = []
        for phone in phones:
            if phone['value_normalized'] not in seen:
                seen.add(phone['value_normalized'])
                unique_phones.append(phone)

        return unique_phones

    def extract_whatsapp(self, html: str, base_url: str = "") -> List[Dict]:
        """
        Extract WhatsApp contact information from HTML content.

        Args:
            html (str): HTML content as string
            base_url (str): Base URL for context

        Returns:
            List[Dict]: List of WhatsApp dictionaries with metadata
        """
        whatsapp_contacts = []
        html_str = html or ""
        is_html = self._is_html_like(html_str)
        soup = BeautifulSoup(html_str, self.parser) if is_html else None

        # Extract from links
        if is_html and soup is not None:
            all_links = soup.find_all('a', href=True)
            for link in all_links:
                href = link.get('href', '')

                for pattern in self.whatsapp_patterns:
                    match = pattern.search(href)
                    if match:
                        raw_number = match.group(1)
                        normalized, _ = self._normalize_phone(raw_number, None)
                        if normalized:
                            whatsapp_contacts.append({
                                'field': 'whatsapp',
                                'value_raw': raw_number,
                                'value_normalized': normalized,
                                'whatsapp_link': href,
                                'contact_source_page': href,
                                'source_url': base_url
                            })

        # Extract from text content
        text_content = html_str
        for pattern in self.whatsapp_patterns:
            matches = pattern.findall(text_content)
            for raw_number in matches:
                normalized, _ = self._normalize_phone(raw_number, None)
                if normalized:
                    # Reconstruct the WhatsApp link
                    if 'wa.me' in text_content:
                        wa_link = f"https://wa.me/{raw_number.lstrip('+')}"
                    else:
                        wa_link = f"https://api.whatsapp.com/send?phone={raw_number.lstrip('+')}"

                    whatsapp_contacts.append({
                        'field': 'whatsapp',
                        'value_raw': raw_number,
                        'value_normalized': normalized,
                        'whatsapp_link': wa_link,
                        'contact_source_page': base_url,
                        'source_url': base_url
                    })

        # Remove duplicates based on normalized number
        seen = set()
        unique_whatsapp = []
        for wa in whatsapp_contacts:
            if wa['value_normalized'] not in seen:
                seen.add(wa['value_normalized'])
                unique_whatsapp.append(wa)

        return unique_whatsapp

    def extract_social_media(self, html: str, base_url: str = "") -> List[Dict]:
        """
        Extract social media profile URLs from HTML content.

        Only extracts the FIRST occurrence per platform to avoid duplicates.

        Extraction methods (in order):
        1. <a> tags with href attributes (standard websites)
        2. <script> tags containing JSON data (Taplink, Linktree, etc.)
        3. Text content (fallback)

        Args:
            html (str): HTML content as string
            base_url (str): Base URL for context

        Returns:
            List[Dict]: List of social media dictionaries with metadata (max 1 per platform)
        """
        social_contacts = []
        html_str = html or ""
        is_html = self._is_html_like(html_str)
        soup = BeautifulSoup(html_str, self.parser) if is_html else None

        # Track first occurrence per platform
        found_platforms = {}

        # METHOD 1: Extract from <a> tags (standard websites)
        if is_html and soup is not None:
            all_links = soup.find_all('a', href=True)
            for link in all_links:
                href = link.get('href', '')

                # Check each platform
                for platform, patterns in self.social_patterns.items():
                    # Skip if already found for this platform
                    if platform in found_platforms:
                        continue

                    for pattern in patterns:
                        match = pattern.search(href)
                        if match:
                            # Extract username/handle
                            username = match.group(1) if match.lastindex else ""
                            if username and username.strip():
                                # Construct the full URL
                                if platform == 'facebook':
                                    full_url = f"https://facebook.com/{username}"
                                elif platform == 'instagram':
                                    full_url = f"https://instagram.com/{username}"
                                elif platform == 'tiktok':
                                    # Ensure @ prefix for TikTok
                                    username_clean = username.lstrip('@')
                                    full_url = f"https://tiktok.com/@{username_clean}"
                                elif platform == 'youtube':
                                    full_url = f"https://youtube.com/{username}"
                                else:
                                    full_url = href

                                social_contacts.append({
                                    'field': 'social_media',
                                    'platform': platform,
                                    'username': username,
                                    'url': full_url,
                                    'contact_source_page': href,
                                    'source_url': base_url
                                })
                                found_platforms[platform] = True
                                break

        # METHOD 2: Extract from <script> JSON data (Taplink, Linktree, Beacons, etc.)
        if is_html and soup is not None:
            script_tags = soup.find_all('script')
            for script in script_tags:
                script_content = script.string or ""
                if not script_content:
                    continue

                # Try to find JSON objects in script content
                # Common patterns: window.data = {...}, window.__NEXT_DATA__ = {...}, var config = {...}
                import json

                # Pattern 1: window.data = {...}
                if 'window.data' in script_content or 'window.__data' in script_content:
                    try:
                        # Extract JSON portion
                        start_idx = script_content.find('{')
                        if start_idx != -1:
                            # Find matching closing brace
                            brace_count = 0
                            end_idx = start_idx
                            for i in range(start_idx, len(script_content)):
                                if script_content[i] == '{':
                                    brace_count += 1
                                elif script_content[i] == '}':
                                    brace_count -= 1
                                    if brace_count == 0:
                                        end_idx = i + 1
                                        break

                            if end_idx > start_idx:
                                json_str = script_content[start_idx:end_idx]
                                try:
                                    data = json.loads(json_str)
                                    # Recursively search for social media URLs in JSON
                                    self._extract_social_from_json(data, found_platforms, social_contacts, base_url)
                                except json.JSONDecodeError:
                                    pass
                    except Exception:
                        pass

                # Pattern 2: Direct URL pattern matching in script content
                for platform, patterns in self.social_patterns.items():
                    if platform in found_platforms:
                        continue

                    for pattern in patterns:
                        matches = pattern.findall(script_content)
                        if matches:
                            username = matches[0] if isinstance(matches[0], str) else ""
                            if username and username.strip():
                                if platform == 'facebook':
                                    full_url = f"https://facebook.com/{username}"
                                elif platform == 'instagram':
                                    full_url = f"https://instagram.com/{username}"
                                elif platform == 'tiktok':
                                    username_clean = username.lstrip('@')
                                    full_url = f"https://tiktok.com/@{username_clean}"
                                elif platform == 'youtube':
                                    full_url = f"https://youtube.com/{username}"
                                else:
                                    continue

                                social_contacts.append({
                                    'field': 'social_media',
                                    'platform': platform,
                                    'username': username,
                                    'url': full_url,
                                    'contact_source_page': '<script> JSON extraction',
                                    'source_url': base_url
                                })
                                found_platforms[platform] = True
                                break

        # METHOD 3: Extract from text content (fallback)
        text_content = soup.get_text() if is_html and soup is not None else html_str

        for platform, patterns in self.social_patterns.items():
            # Skip if already found for this platform
            if platform in found_platforms:
                continue

            for pattern in patterns:
                matches = pattern.findall(text_content)
                if matches:
                    # Take first match only
                    username = matches[0] if isinstance(matches[0], str) else matches[0][0] if isinstance(matches[0], tuple) else ""
                    if username and username.strip():
                        # Construct the full URL
                        if platform == 'facebook':
                            full_url = f"https://facebook.com/{username}"
                        elif platform == 'instagram':
                            full_url = f"https://instagram.com/{username}"
                        elif platform == 'tiktok':
                            username_clean = username.lstrip('@')
                            full_url = f"https://tiktok.com/@{username_clean}"
                        elif platform == 'youtube':
                            full_url = f"https://youtube.com/{username}"
                        else:
                            full_url = ""

                        if full_url:
                            social_contacts.append({
                                'field': 'social_media',
                                'platform': platform,
                                'username': username,
                                'url': full_url,
                                'contact_source_page': text_content[:100],  # First 100 chars as reference
                                'source_url': base_url
                            })
                            found_platforms[platform] = True
                            break

        return social_contacts

    def _extract_social_from_json(self, obj, found_platforms: dict, social_contacts: list, base_url: str):
        """
        Recursively extract social media URLs from JSON object.

        Args:
            obj: JSON object (dict, list, or primitive)
            found_platforms: Dict tracking which platforms have been found
            social_contacts: List to append found contacts to
            base_url: Base URL for context
        """
        if isinstance(obj, dict):
            for key, value in obj.items():
                # Check if key suggests social media
                key_lower = str(key).lower()

                # Direct platform key matches
                for platform in ['facebook', 'instagram', 'tiktok', 'youtube']:
                    if platform in found_platforms:
                        continue

                    if platform in key_lower and isinstance(value, str):
                        # Extract username/URL from value
                        for pattern in self.social_patterns.get(platform, []):
                            match = pattern.search(value)
                            if match:
                                username = match.group(1) if match.lastindex else ""
                                if username and username.strip():
                                    if platform == 'facebook':
                                        full_url = f"https://facebook.com/{username}"
                                    elif platform == 'instagram':
                                        full_url = f"https://instagram.com/{username}"
                                    elif platform == 'tiktok':
                                        username_clean = username.lstrip('@')
                                        full_url = f"https://tiktok.com/@{username_clean}"
                                    elif platform == 'youtube':
                                        full_url = f"https://youtube.com/{username}"
                                    else:
                                        continue

                                    social_contacts.append({
                                        'field': 'social_media',
                                        'platform': platform,
                                        'username': username,
                                        'url': full_url,
                                        'contact_source_page': '<script> JSON extraction',
                                        'source_url': base_url
                                    })
                                    found_platforms[platform] = True
                                    break

                # Recurse into nested structures
                self._extract_social_from_json(value, found_platforms, social_contacts, base_url)

        elif isinstance(obj, list):
            for item in obj:
                self._extract_social_from_json(item, found_platforms, social_contacts, base_url)

    def _normalize_email(self, email: str) -> Optional[str]:
        """Normalize email address and strip any trailing/leading non-email tokens.

        This ensures cases like "user@example.comsendthank" or
        "maltaemailhypmalta@gmail.comphone" are reduced to the strict
        email "user@example.com".
        """
        if not email:
            return None

        # Trim wrappers, decode %xx, and lowercase early
        candidate = str(email).strip().strip('<>').strip('"\'').lower()
        try:
            candidate = unquote(candidate)
        except Exception:
            pass

        # Prefer a capture that stops at email end (after TLD or non-letter)
        m = re.search(r'([A-Za-z0-9._%+-]+)@([A-Za-z0-9.-]+)\.([A-Za-z]{2,})(?:[^A-Za-z]|$)', candidate)
        if not m:
            # Fallback: capture up to TLD with explicit end boundary
            strict_end = re.search(r'([A-Za-z0-9._%+-]+)@([A-Za-z0-9.-]+)\.([A-Za-z]{2,})(?=[^A-Za-z]|$)', candidate)
            if not strict_end:
                return None
            local, dom_main, tld = strict_end.group(1), strict_end.group(2), strict_end.group(3)
        else:
            local, dom_main, tld = m.group(1), m.group(2), m.group(3)

        # EARLY CHECK: Reject file extensions BEFORE any trimming/normalization
        file_extensions = {
            'png', 'jpg', 'jpeg', 'gif', 'svg', 'webp', 'bmp', 'ico', 'tiff',  # Images
            'pdf', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx',  # Documents
            'zip', 'rar', 'tar', 'gz', '7z',  # Archives
            'mp3', 'mp4', 'avi', 'mov', 'wmv', 'flv',  # Media
            'css', 'js', 'json', 'xml', 'html', 'htm',  # Web files
            'txt', 'log', 'csv'  # Text files
        }
        if tld.lower() in file_extensions:
            return None

        # Heuristic: if TLD starts with a common base then has extra letters (e.g., comname),
        # trim to the base so emails like example.comphone become example.com.
        # Guard: do NOT trim if the captured tld is a valid PSL token (e.g., 'company').
        common_bases = {
            'com', 'net', 'org', 'edu', 'gov', 'mil', 'io', 'ai', 'app', 'dev', 'me',
            'us', 'uk', 'de', 'fr', 'es', 'it', 'nl', 'ru', 'jp', 'cn', 'in', 'br', 'au', 'ca',
            'sg', 'za', 'mx', 'se', 'no', 'fi', 'dk', 'pl', 'cz', 'sk', 'hu', 'ro', 'bg', 'gr',
            'pt', 'tr', 'sa', 'ae', 'qa', 'kw', 'om', 'my', 'id', 'ph', 'th', 'vn', 'hk', 'tw',
            'kr', 'il', 'ar', 'cl', 'pe', 'uy', 'nz', 'ie', 'ch', 'at', 'be', 'lu', 'li',
            'name', 'biz', 'info', 'tv', 'cc', 'xyz', 'site', 'online', 'shop', 'pro', 'mobi',
            'asia', 'int'
        }

        # If current tld is a valid PSL token, keep as-is
        if not (self._psl_tld_tokens and tld in self._psl_tld_tokens):
            # If tld contains known base as prefix and has extras, cut to the base when remainder is alphabetic
            for base in sorted(common_bases, key=len, reverse=True):
                if tld.startswith(base) and len(tld) > len(base):
                    remainder = tld[len(base):]
                    if remainder.isalpha():
                        tld = base
                        break

        # If local-part starts with digit+separator blocks (likely phone glued), trim them
        # Examples: 904-1978info -> info, 651.330.8661matthew -> matthew
        if re.match(r'^\d+[._-]+', local):
            local = re.sub(r'^(?:\d+[._-]*)+', '', local)

        # If local-part ends with a common alias and has noisy prefix tokens, keep the alias only
        for alias in sorted(self._common_aliases, key=len, reverse=True):
            if local.endswith(alias):
                prefix = local[:-len(alias)]
                if prefix and (re.search(r'(?:^|[0-9._-])(line|phone|tel|whatsapp|call|contact|email|mail|site|web|page|location|studio|manager)$', prefix) or re.search(r'[0-9]|[._-]', prefix)):
                    local = alias
                    break

        # Country/region prefix glued before alias (e.g., ushello -> hello, uk.info -> info)
        for alias in sorted(self._common_aliases, key=len, reverse=True):
            for cc in self._country_prefixes:
                if local == f"{cc}{alias}" or local == f"{cc}.{alias}" or local == f"{cc}-{alias}" or local == f"{cc}_{alias}":
                    local = alias
                    break
            else:
                continue
            break

        # Recompose
        if not local or not dom_main or not tld:
            return None

        # Basic sanity: avoid consecutive dots or invalid starting/ending characters
        dom_main = dom_main.strip('.-')
        dom_main = re.sub(r'\.\.+', '.', dom_main)

        # Additional domain validation: reject if domain has invalid structure
        # Example: gmail.comthesingingbowlgallery should be rejected
        full_domain = f"{dom_main}.{tld}"

        # Check if TLD is suspiciously long (likely has extra text appended)
        if len(tld) > 10:  # Most valid TLDs are short (com, org, info, etc)
            return None

        # Check if domain has multiple TLDs concatenated (e.g., ".comname", ".comsite")
        # Split domain and check each part
        # IMPORTANT: Only reject if it looks like garbage (e.g., "comname", "comsite")
        # DO NOT reject valid words like "company", "community", "network"
        domain_parts = full_domain.split('.')

        # Known valid domain words that start with TLD-like prefixes
        valid_domain_words = {
            'company', 'community', 'commercial', 'communication', 'compute', 'computer',
            'network', 'networking', 'organic', 'organization', 'organisations',
            'education', 'educational', 'government', 'information', 'international'
        }

        for part in domain_parts:
            # Skip if this is a known valid word
            if part.lower() in valid_domain_words:
                continue

            # If any part contains a known TLD as prefix with extra text, reject
            for known_tld in ['com', 'net', 'org', 'edu', 'gov', 'io', 'ai', 'sg', 'uk', 'de']:
                if part.startswith(known_tld) and len(part) > len(known_tld) and part != known_tld:
                    # Check if remainder after TLD looks like garbage (not a valid word)
                    remainder = part[len(known_tld):]
                    # Only reject if remainder is short (< 6 chars) and all lowercase alpha
                    # This catches "comname", "comsite" but not "company", "network"
                    if remainder.isalpha() and len(remainder) < 6 and remainder.islower():
                        return None

        # CRITICAL FIX: Reject local-part starting with numbers
        # Valid emails should start with letter (RFC 5321 allows numbers, but it's uncommon and often spam)
        if local and local[0].isdigit():
            # Check if it's a phone number glued to email (e.g., "123-456-7890name@domain.com")
            # If most of local-part is digits, reject
            digit_count = sum(1 for c in local if c.isdigit())
            if digit_count > len(local) * 0.5:  # More than 50% digits
                return None
            # If starts with 4+ consecutive digits, likely invalid
            if re.match(r'^\d{4,}', local):
                return None

        return f"{local}@{dom_main}.{tld}"

    def _is_placeholder_email(self, normalized_email: str) -> bool:
        """Return True if email looks like a placeholder or sample address."""
        try:
            if not normalized_email or '@' not in normalized_email:
                return True
            local, domain = normalized_email.split('@', 1)
            domain = domain.strip().lower()
            local = local.strip().lower()

            if domain in self._placeholder_domains:
                return True

            # Obvious placeholders in local part
            if local in { 'placeholder', 'sample', 'test', 'testing', 'demo', 'filler', 'noreply', 'no-reply', 'donotreply' }:
                return True

            # Special known dev/example endings
            if domain.endswith('.example') or domain.endswith('.invalid'):
                return True

            return False
        except Exception:
            return False

    def _is_plausible_local_part(self, local: str) -> bool:
        """Heuristic check for plausible email local-part.

        Conservative rules to avoid false positives while keeping common patterns
        like 'info', 'contact', 'sales', names, etc.
        """
        if not local:
            return False

        lcl = str(local).lower()

        # Max local-part per RFC is 64; we keep reasonable upper bound
        if len(lcl) > 64:
            return False

        # Avoid starting/ending with invalid characters
        if lcl[0] in '.-' or lcl[-1] in '.-':
            return False

        # Drop uncommon tokens indicative of concatenated text
        suspicious_tokens = (
            'email', 'mailto', 'whatsapp', 'phone', 'address'
        )
        if any(tok in lcl for tok in suspicious_tokens):
            return False

        # CRITICAL: Reject emails with embedded state codes + phone numbers (malformed parsing)
        # Pattern: 2-letter state code at start + phone number + location name
        # Examples: "ca949-509-1050losangeles", "il847-332-1018evanston"
        if re.match(r'^[a-z]{2}\d{3}-?\d{3}-?\d{4}', lcl):
            # Matches pattern like "ca9495091050" or "ca949-509-1050" at start
            return False

        # Also reject if it contains the phone number pattern anywhere (robustness)
        if re.search(r'\d{3}-?\d{3}-?\d{4}.*[a-z]{2,}$', lcl):
            # Matches pattern like "1234567890cityname" anywhere in local-part
            return False

        # Reject if starts with state code followed immediately by numbers
        if re.match(r'^[a-z]{2}\d+', lcl):
            # e.g., "ca949509..." likely state + phone concatenation
            return False

        # Reject excessive repetition of the same token (e.g., 'malta.malta.malta')
        tokens = re.split(r'[._+-]+', lcl)
        # Count repetitions ignoring empty tokens
        counts = {}
        for t in tokens:
            if not t:
                continue
            counts[t] = counts.get(t, 0) + 1
        if any(c >= 3 for c in counts.values()):
            return False

        # Avoid long runs of identical characters
        if re.search(r'([a-z0-9])\1{3,}', lcl):
            return False

        # Digit-only local-part extremely long is unlikely (allow short numeric IDs)
        if lcl.isdigit() and len(lcl) > 8:
            return False

        return True

    def _dedupe_suspicious_variants(self, emails: List[Dict]) -> List[Dict]:
        """Remove suspicious variant emails when a cleaner variant exists.

        Example: keep 'hypmalta@gmail.com' and drop
        'maltaemailhypmalta@gmail.com' when 'hypmalta' is a substring of the
        longer local-part and the longer contains the token 'email'.
        """
        if not emails:
            return []

        # Group by domain
        by_domain: Dict[str, List[Dict]] = {}
        for e in emails:
            norm = e.get('value_normalized', '')
            parts = norm.split('@', 1)
            if len(parts) != 2:
                continue
            local, domain = parts[0], parts[1]
            by_domain.setdefault(domain, []).append(e)

        result: List[Dict] = []
        for domain, group in by_domain.items():
            if len(group) == 1:
                result.extend(group)
                continue

            # Build locals list and track indices to keep
            locals = [g['value_normalized'].split('@', 1)[0] for g in group]
            keep = set(range(len(group)))

            # If a longer local contains a shorter local and includes 'email', drop longer
            for i, li in enumerate(locals):
                for j, lj in enumerate(locals):
                    if i == j:
                        continue
                    if len(lj) > len(li) and li and li in lj and ('email' in lj):
                        if j in keep:
                            keep.remove(j)

            for idx in range(len(group)):
                if idx in keep:
                    result.append(group[idx])

        return result

    def _normalize_phone(self, phone: str, country_code: str = None) -> Tuple[Optional[str], Optional[str]]:
        """
        Normalize phone number to E.164 format.

        Args:
            phone (str): Raw phone number
            country_code (str): Country code for parsing (e.g., 'ID', 'US')

        Returns:
            Tuple[Optional[str], Optional[str]]: (E.164 format, number type)
        """
        if not phone:
            return None, None

        # Clean the phone number
        cleaned = re.sub(r'[^\d+]', '', str(phone))
        if not cleaned:
            return None, None

        try:
            # Parse the phone number
            region = country_code if not cleaned.startswith('+') else None
            parsed_number = phonenumbers.parse(cleaned, region)
        except phonenumbers.NumberParseException:
            # If parsing failed and number doesn't start with +, try adding + prefix
            # This handles WhatsApp numbers like "393518013001" (Italy) without explicit country code
            if not cleaned.startswith('+') and country_code is None:
                try:
                    parsed_number = phonenumbers.parse('+' + cleaned, None)
                except phonenumbers.NumberParseException:
                    return None, None
            else:
                return None, None

        try:

            # Validate the number
            if phonenumbers.is_valid_number(parsed_number):
                # Format to E.164
                e164 = phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.E164)

                # Get number type
                number_type = phonenumbers.number_type(parsed_number)
                type_mapping = {
                    phonenumbers.PhoneNumberType.MOBILE: 'mobile',
                    phonenumbers.PhoneNumberType.FIXED_LINE: 'fixed_line',
                    phonenumbers.PhoneNumberType.FIXED_LINE_OR_MOBILE: 'fixed_line_or_mobile',
                    phonenumbers.PhoneNumberType.VOIP: 'voip',
                    phonenumbers.PhoneNumberType.TOLL_FREE: 'toll_free',
                    phonenumbers.PhoneNumberType.PREMIUM_RATE: 'premium_rate',
                }

                type_str = type_mapping.get(number_type, 'unknown')

                return e164, type_str

        except phonenumbers.NumberParseException:
            pass

        return None, None

    def extract_all_contacts(self, html: str, base_url: str = "", country_code: str = None) -> List[Dict]:
        """
        Extract all types of contacts from HTML content.

        Args:
            html (str): HTML content as string
            base_url (str): Base URL for context
            country_code (str): Country code for phone number parsing

        Returns:
            List[Dict]: Combined list of all contact types
        """
        all_contacts = []

        # Extract emails
        emails = self.extract_emails(html, base_url)
        all_contacts.extend(emails)

        # Extract phones
        phones = self.extract_phones(html, base_url, country_code)
        all_contacts.extend(phones)

        # Extract WhatsApp
        whatsapp = self.extract_whatsapp(html, base_url)
        all_contacts.extend(whatsapp)

        # Extract social media
        social_media = self.extract_social_media(html, base_url)
        all_contacts.extend(social_media)

        return all_contacts