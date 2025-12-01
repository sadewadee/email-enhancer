"""
WhatsApp Number Validator Module
Validates WhatsApp numbers using phonenumbers library and WAHA API.
Checks format, country code, mobile number type, and actual WhatsApp availability.
"""

import logging
import os
import requests
from typing import Dict, List, Optional
import phonenumbers
from phonenumbers import NumberParseException

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


class WhatsAppValidator:
    """
    Validates WhatsApp numbers using multiple methods:
    1. phonenumbers library for format and mobile type validation
    2. WAHA API for actual WhatsApp availability checking
    """

    def __init__(self, waha_base_url: str = None, waha_api_key: str = None,
                 waha_session: str = None, waha_timeout: int = None):
        """
        Initialize WhatsApp validator with WAHA configuration from env or parameters.
        
        Args:
            waha_base_url: Base URL of WAHA server (default from WAHA_BASE_URL env)
            waha_api_key: API key for WAHA authentication (default from WAHA_API_KEY env)
            waha_session: WAHA session name (default from WAHA_SESSION env or "default")
            waha_timeout: Timeout for WAHA API requests in seconds (default from WAHA_TIMEOUT env or 10)
        """
        self.logger = logging.getLogger(__name__)
        
        # Load from env if not provided
        self.waha_base_url = (waha_base_url or os.getenv('WAHA_BASE_URL', '')).rstrip('/')
        self.waha_api_key = waha_api_key or os.getenv('WAHA_API_KEY', '')
        self.waha_session = waha_session or os.getenv('WAHA_SESSION', 'default')
        self.waha_timeout = int(waha_timeout or os.getenv('WAHA_TIMEOUT', '10'))
        
        # WAHA is enabled if base_url and api_key are configured
        self.waha_enabled = bool(self.waha_base_url and self.waha_api_key)
        
        # Headers for WAHA API requests
        self.waha_headers = {"X-Api-Key": self.waha_api_key} if self.waha_api_key else {}
        
        # Test WAHA connection if enabled
        if self.waha_enabled:
            self._test_waha_connection()

    def _test_waha_connection(self):
        """Test connection to WAHA server."""
        try:
            response = requests.get(
                f"{self.waha_base_url}/api/sessions/{self.waha_session}",
                headers=self.waha_headers,
                timeout=self.waha_timeout
            )
            if response.status_code == 200:
                data = response.json()
                status = data.get('status', 'UNKNOWN')
                if status == 'WORKING':
                    self.logger.info(f"WAHA connected: {self.waha_base_url} (session: {self.waha_session})")
                else:
                    self.logger.warning(f"WAHA session '{self.waha_session}' status: {status}")
            elif response.status_code == 401:
                self.logger.error(f"WAHA authentication failed - invalid API key")
                self.waha_enabled = False
            else:
                self.logger.warning(f"WAHA server responded with status {response.status_code}")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to connect to WAHA server at {self.waha_base_url}: {e}")
            self.waha_enabled = False

    def check_whatsapp_exists(self, phone_number: str) -> Dict[str, any]:
        """
        Check if phone number exists on WhatsApp using WAHA API.
        
        Args:
            phone_number: Phone number (with or without + prefix)
            
        Returns:
            Dictionary with:
            - exists: bool - True if number exists on WhatsApp
            - chat_id: str - WhatsApp chat ID if exists
            - error: str - Error message if failed
        """
        if not self.waha_enabled:
            return {'exists': False, 'error': 'waha_disabled'}

        # Clean phone number - remove + prefix for API
        clean_phone = str(phone_number).strip().lstrip('+')
        
        if not clean_phone:
            return {'exists': False, 'error': 'empty_number'}

        try:
            response = requests.get(
                f"{self.waha_base_url}/api/contacts/check-exists",
                params={"phone": clean_phone, "session": self.waha_session},
                headers=self.waha_headers,
                timeout=self.waha_timeout
            )
            
            if response.status_code == 200:
                data = response.json()
                exists = data.get('numberExists', False)
                chat_id = data.get('chatId', '')
                return {
                    'exists': exists,
                    'chat_id': chat_id,
                    'error': None
                }
            else:
                return {
                    'exists': False,
                    'error': f'api_error_{response.status_code}'
                }
                
        except requests.exceptions.Timeout:
            return {'exists': False, 'error': 'timeout'}
        except requests.exceptions.RequestException as e:
            self.logger.debug(f"WAHA check failed for {phone_number}: {e}")
            return {'exists': False, 'error': f'request_error'}
        except Exception as e:
            self.logger.debug(f"Unexpected WAHA error for {phone_number}: {e}")
            return {'exists': False, 'error': f'unexpected_error'}

    def normalize_phone_number(self, phone: str, country_code: str = None) -> Optional[str]:
        """
        Normalize phone number to E.164 format.
        
        Args:
            phone: Phone number string
            country_code: ISO country code (e.g., 'MT', 'ID', 'SG')
            
        Returns:
            Normalized phone number in E.164 format (e.g., +35679856030) or None if invalid
        """
        if not phone:
            return None
        
        phone = str(phone).strip()
        
        # If already has + prefix, try to parse directly
        if phone.startswith("+"):
            try:
                parsed = phonenumbers.parse(phone, None)
                if phonenumbers.is_valid_number(parsed):
                    return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
            except NumberParseException:
                pass
        
        # Try with country code hint
        if country_code:
            try:
                parsed = phonenumbers.parse(phone, country_code.upper())
                if phonenumbers.is_valid_number(parsed):
                    return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
            except NumberParseException:
                pass
        
        # Fallback: try adding + prefix if it looks like international
        if not phone.startswith("+") and len(phone) > 10:
            try:
                parsed = phonenumbers.parse("+" + phone, None)
                if phonenumbers.is_valid_number(parsed):
                    return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
            except NumberParseException:
                pass
        
        return None

    def validate_for_whatsapp(self, whatsapp: str = None, phone_number: str = None, 
                              country_code: str = None) -> Optional[str]:
        """
        Validate and return a working WhatsApp number.
        
        Logic:
        1. If whatsapp has value, check via WAHA. If valid, return it.
        2. If whatsapp invalid or empty, check phone_number via WAHA.
        3. If both invalid, return None (empty).
        
        Args:
            whatsapp: Existing WhatsApp number from CSV
            phone_number: Phone number from CSV
            country_code: ISO country code for normalization (e.g., 'MT', 'ID')
            
        Returns:
            Valid WhatsApp number in E.164 format, or None if both invalid
        """
        if not self.waha_enabled:
            # Return existing whatsapp if WAHA disabled
            return whatsapp if whatsapp else None
        
        # Step 1: Check existing whatsapp column first
        if whatsapp:
            normalized_wa = self.normalize_phone_number(whatsapp, country_code)
            if normalized_wa:
                result = self.check_whatsapp_exists(normalized_wa)
                if result.get('exists'):
                    self.logger.debug(f"WhatsApp valid: {normalized_wa}")
                    return normalized_wa
        
        # Step 2: If whatsapp invalid/empty, check phone_number
        if phone_number:
            normalized_phone = self.normalize_phone_number(phone_number, country_code)
            if normalized_phone:
                result = self.check_whatsapp_exists(normalized_phone)
                if result.get('exists'):
                    self.logger.debug(f"Phone valid for WhatsApp: {normalized_phone}")
                    return normalized_phone
        
        # Step 3: Both invalid - return None
        self.logger.debug(f"No valid WhatsApp found for wa={whatsapp}, phone={phone_number}")
        return None

    def validate_number(self, number: str, default_region: Optional[str] = None) -> Dict[str, any]:
        """
        Validate a single WhatsApp number using phonenumbers and WAHA (if enabled).

        Args:
            number: Phone number string to validate
            default_region: Optional default region code (e.g., 'SG' for Singapore)

        Returns:
            Dictionary with validation results:
            {
                'valid': bool,              # Format and mobile type valid
                'formatted': str,           # E.164 format (+6512345678)
                'country': str,             # Country code (SG, US, etc)
                'type': str,                # mobile, fixed_line, etc
                'reason': str,              # Validation failure reason
                'waha_available': bool,     # Actually available on WhatsApp
                'waha_reason': str,         # WAHA validation result reason
                'waha_exists': bool,        # Number exists on WhatsApp
                'waha_can_receive': bool    # Can receive messages on WhatsApp
            }
        """
        if not number or not str(number).strip():
            return {
                'valid': False,
                'formatted': '',
                'country': '',
                'type': 'unknown',
                'reason': 'empty_number'
            }

        try:
            # Clean and prepare number
            number_str = str(number).strip()

            # Parse number with optional default region
            parsed = phonenumbers.parse(number_str, default_region)

            # Basic validation checks
            is_valid_format = phonenumbers.is_valid_number(parsed)
            is_possible = phonenumbers.is_possible_number(parsed)

            if not is_valid_format:
                return {
                    'valid': False,
                    'formatted': number_str,
                    'country': '',
                    'type': 'unknown',
                    'reason': 'invalid_format'
                }

            if not is_possible:
                return {
                    'valid': False,
                    'formatted': number_str,
                    'country': '',
                    'type': 'unknown',
                    'reason': 'impossible_number'
                }

            # Get number type (WhatsApp only works with mobile)
            number_type = phonenumbers.number_type(parsed)

            # Map number type to readable string
            type_map = {
                phonenumbers.PhoneNumberType.MOBILE: 'mobile',
                phonenumbers.PhoneNumberType.FIXED_LINE: 'fixed_line',
                phonenumbers.PhoneNumberType.FIXED_LINE_OR_MOBILE: 'mobile',
                phonenumbers.PhoneNumberType.TOLL_FREE: 'toll_free',
                phonenumbers.PhoneNumberType.PREMIUM_RATE: 'premium_rate',
                phonenumbers.PhoneNumberType.SHARED_COST: 'shared_cost',
                phonenumbers.PhoneNumberType.VOIP: 'voip',
                phonenumbers.PhoneNumberType.PERSONAL_NUMBER: 'personal',
                phonenumbers.PhoneNumberType.PAGER: 'pager',
                phonenumbers.PhoneNumberType.UAN: 'uan',
                phonenumbers.PhoneNumberType.VOICEMAIL: 'voicemail',
                phonenumbers.PhoneNumberType.UNKNOWN: 'unknown'
            }

            type_str = type_map.get(number_type, 'unknown')

            # WhatsApp only works with mobile numbers
            is_mobile = number_type in [
                phonenumbers.PhoneNumberType.MOBILE,
                phonenumbers.PhoneNumberType.FIXED_LINE_OR_MOBILE
            ]

            # Format number to E.164 (international format)
            formatted = phonenumbers.format_number(
                parsed,
                phonenumbers.PhoneNumberFormat.E164
            )

            # Get country code
            country = phonenumbers.region_code_for_number(parsed)

            # Final validation result
            is_whatsapp_valid = is_valid_format and is_possible and is_mobile

            result = {
                'valid': is_whatsapp_valid,
                'formatted': formatted,
                'country': country or '',
                'type': type_str,
                'reason': 'valid' if is_whatsapp_valid else ('not_mobile' if not is_mobile else 'invalid')
            }

            # Add WAHA validation if enabled and format is valid
            if self.waha_enabled and is_whatsapp_valid:
                waha_result = self._check_waha_availability(formatted)
                result.update(waha_result)
            else:
                # Add default WAHA fields if not enabled
                result.update({
                    'waha_available': False,
                    'waha_reason': 'waha_disabled',
                    'waha_exists': False,
                    'waha_can_receive': False
                })

            self.logger.debug(f"WhatsApp validation: {number_str} -> {result}")
            return result

        except NumberParseException as e:
            self.logger.debug(f"Failed to parse number {number}: {str(e)}")
            # error_type is an integer, convert to string
            error_code = getattr(e, 'error_type', 'unknown')
            return {
                'valid': False,
                'formatted': str(number).strip(),
                'country': '',
                'type': 'unknown',
                'reason': f'parse_error_{error_code}'
            }
        except Exception as e:
            self.logger.error(f"Unexpected error validating WhatsApp number {number}: {str(e)}")
            return {
                'valid': False,
                'formatted': str(number).strip(),
                'country': '',
                'type': 'unknown',
                'reason': f'error: {str(e)}'
            }

    def validate_batch(self, numbers: List[str], default_region: Optional[str] = None) -> Dict[str, Dict]:
        """
        Validate a batch of WhatsApp numbers.

        Args:
            numbers: List of phone numbers to validate
            default_region: Optional default region code

        Returns:
            Dictionary mapping each number to its validation result
        """
        results = {}
        for number in numbers:
            if number and str(number).strip():
                number_str = str(number).strip()
                results[number_str] = self.validate_number(number_str, default_region)

        return results

    def get_valid_numbers(self, numbers: List[str], default_region: Optional[str] = None) -> List[str]:
        """
        Filter and return only valid WhatsApp numbers from a list.

        Args:
            numbers: List of phone numbers
            default_region: Optional default region code

        Returns:
            List of formatted valid WhatsApp numbers
        """
        valid_numbers = []
        results = self.validate_batch(numbers, default_region)

        for number, result in results.items():
            if result['valid']:
                valid_numbers.append(result['formatted'])

        return valid_numbers


# Example usage
if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(level=logging.DEBUG)

    validator = WhatsAppValidator()

    # Test cases
    test_numbers = [
        "+6591234567",      # Valid Singapore mobile
        "+14155552671",     # Valid US mobile
        "+442071234567",    # UK fixed line (not valid for WhatsApp)
        "91234567",         # Missing country code
        "+65123",           # Too short
        "+999999999",       # Invalid country code
    ]

    print("WhatsApp Number Validation Tests:")
    print("=" * 60)

    for number in test_numbers:
        result = validator.validate_number(number)
        print(f"\nNumber: {number}")
        print(f"  Valid: {result['valid']}")
        print(f"  Formatted: {result['formatted']}")
        print(f"  Country: {result['country']}")
        print(f"  Type: {result['type']}")
        print(f"  Reason: {result['reason']}")
