"""
WhatsApp Number Validator Module
Validates WhatsApp numbers using phonenumbers library.
Checks format, country code, and mobile number type.
"""

import logging
import phonenumbers
from typing import Dict, List, Optional
from phonenumbers import NumberParseException


class WhatsAppValidator:
    """
    Validates WhatsApp numbers using phonenumbers library.
    WhatsApp only works with valid mobile numbers.
    """

    def __init__(self):
        """Initialize WhatsApp validator."""
        self.logger = logging.getLogger(__name__)

    def validate_number(self, number: str, default_region: Optional[str] = None) -> Dict[str, any]:
        """
        Validate a single WhatsApp number.

        Args:
            number: Phone number string to validate
            default_region: Optional default region code (e.g., 'SG' for Singapore)

        Returns:
            Dictionary with validation results:
            {
                'valid': bool,
                'formatted': str,  # E.164 format (+6512345678)
                'country': str,    # Country code (SG, US, etc)
                'type': str,       # mobile, fixed_line, etc
                'reason': str      # Validation failure reason
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
