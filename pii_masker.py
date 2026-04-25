"""
PII / DLP Masking Utility for SAP S/4HANA Telegram Bot.

Masks sensitive data in log messages, database entries, and JSON payloads:
  - Email addresses
  - Phone numbers
  - SAP Customer/Vendor IDs (Business Partners)
  - Financial amounts
  - API tokens / Bearer tokens
  - Credit card numbers
  - SAP credentials (passwords in URLs or headers)
  - IBAN / Bank account numbers

Usage:
    from pii_masker import PIIMasker
    masker = PIIMasker()
    safe_text = masker.mask(raw_log_text)

Integration with logging:
    Use MaskedFormatter as a drop-in replacement in logger_setup.py
"""

import re
import json
from typing import Union


class PIIMasker:
    """
    Enterprise PII masking engine with configurable patterns.
    All patterns are compiled once at init for performance.
    """

    def __init__(self):
        # Ordered list of (compiled_regex, replacement, description)
        self._patterns = [
            # API Keys / Bearer tokens (long alphanumeric strings with dashes/underscores)
            (re.compile(
                r'(?i)((?:bearer|token|api[_-]?key|authorization|x-csrf-token)'
                r'["\s:=]+)([A-Za-z0-9_\-./+=]{20,})'
            ), r'\1****REDACTED****', "API Token"),

            # SAP Basic Auth in URLs: user:password@host
            (re.compile(
                r'(https?://)([^:]+):([^@]+)@'
            ), r'\1****:****@', "URL Credentials"),

            # Email addresses
            (re.compile(
                r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
            ), '****@****.***', "Email"),

            # Phone numbers (international format, 8+ digits with optional + and separators)
            (re.compile(
                r'(?<!\d)(\+?\d{1,4}[\s\-.]?\(?\d{1,4}\)?[\s\-.]?\d{2,4}[\s\-.]?\d{2,4}[\s\-.]?\d{0,4})(?!\d)'
            ), _mask_phone, "Phone"),

            # Credit card numbers (13-19 digits with optional separators)
            (re.compile(
                r'\b(\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{1,7})\b'
            ), _mask_cc, "Credit Card"),

            # IBAN (international format)
            (re.compile(
                r'\b([A-Z]{2}\d{2}[\s]?[\dA-Z]{4,30})\b'
            ), _mask_iban, "IBAN"),

            # Financial amounts: currency symbol + digits or digits + currency code
            # Mask the numeric part, keep currency indicator
            (re.compile(
                r'(?i)(?:(?:USD|EUR|GBP|SAR|AED|INR|CHF|JPY|CNY)\s*)'
                r'(\d{1,3}(?:[,.\s]\d{3})*(?:[.,]\d{1,2})?)'
            ), _mask_amount_prefix, "Amount (prefix)"),
            (re.compile(
                r'(\d{1,3}(?:[,.\s]\d{3})*(?:[.,]\d{1,2})?)\s*'
                r'(?:USD|EUR|GBP|SAR|AED|INR|CHF|JPY|CNY)'
            ), _mask_amount_suffix, "Amount (suffix)"),

            # Person names in structured contexts (JSON keys, log key=value patterns)
            (re.compile(
                r'(?i)((?:first_name|last_name|full_name|customer_name|vendor_name'
                r'|contact_name|employee_name|requester_name)'
                r'["\s:=]+)"?([A-Za-z][A-Za-z\s.\'-]{2,40})"?'
            ), r'\1"***NAME***"', "Person Name"),

            # SAP Business Partner IDs (7-10 digit numbers that look like customer/vendor IDs)
            # Only mask when preceded by contextual keywords
            (re.compile(
                r'(?i)((?:customer|vendor|partner|soldtoparty|businesspartner|bp)'
                r'["\s:=]+)(\d{7,10})\b'
            ), r'\1***BP***', "Business Partner ID"),

            # Generic long numeric strings (potential IDs) — only in JSON contexts
            (re.compile(
                r'("(?:user_id|chat_id|customer_id|vendor_id)":\s*)(\d{5,})'
            ), r'\1"***MASKED***"', "Numeric ID in JSON"),

            # Password fields in JSON
            (re.compile(
                r'(?i)("(?:password|passwd|pwd|secret|sap_password)":\s*)"([^"]*)"'
            ), r'\1"****"', "Password in JSON"),
        ]

    def mask(self, text: str) -> str:
        """Apply all PII masking patterns to a text string."""
        if not text:
            return text
        result = text
        for pattern, replacement, _desc in self._patterns:
            if callable(replacement):
                result = pattern.sub(replacement, result)
            else:
                result = pattern.sub(replacement, result)
        return result

    def mask_dict(self, data: dict, depth: int = 0) -> dict:
        """
        Recursively mask PII in a dictionary (e.g., JSON payloads).
        Sensitive keys are fully redacted; other string values get pattern masking.
        """
        if depth > 10:  # Prevent infinite recursion
            return data

        SENSITIVE_KEYS = {
            'password', 'passwd', 'pwd', 'secret', 'token', 'api_key',
            'apikey', 'authorization', 'auth', 'sap_password', 'smtp_password',
            'pin', 'pin_hash', 'pin_salt', 'credit_card', 'iban',
        }

        PARTIAL_MASK_KEYS = {
            'email', 'phone', 'mobile', 'telephone', 'fax',
            'customer_id', 'vendor_id', 'user_id',
            'first_name', 'last_name', 'full_name', 'customer_name',
            'vendor_name', 'contact_name', 'employee_name',
        }

        masked = {}
        for key, value in data.items():
            key_lower = key.lower()

            if key_lower in SENSITIVE_KEYS:
                masked[key] = '****REDACTED****'
            elif key_lower in PARTIAL_MASK_KEYS:
                if isinstance(value, str):
                    masked[key] = self.mask(value)
                else:
                    masked[key] = '***'
            elif isinstance(value, dict):
                masked[key] = self.mask_dict(value, depth + 1)
            elif isinstance(value, list):
                masked[key] = [
                    self.mask_dict(item, depth + 1) if isinstance(item, dict)
                    else self.mask(str(item)) if isinstance(item, str)
                    else item
                    for item in value
                ]
            elif isinstance(value, str):
                masked[key] = self.mask(value)
            else:
                masked[key] = value

        return masked

    def mask_json(self, json_str: str) -> str:
        """Mask PII in a JSON string. Falls back to plain text masking."""
        try:
            data = json.loads(json_str)
            if isinstance(data, dict):
                return json.dumps(self.mask_dict(data), default=str)
            elif isinstance(data, list):
                return json.dumps(
                    [self.mask_dict(item) if isinstance(item, dict) else item
                     for item in data],
                    default=str)
        except (json.JSONDecodeError, TypeError):
            pass
        return self.mask(json_str)


# ---------------------------------------------------------------------------
# Masking helper functions (used as regex replacements)
# ---------------------------------------------------------------------------

def _mask_phone(match) -> str:
    """Keep first 3 and last 2 digits of phone numbers."""
    full = match.group(0)
    digits = re.sub(r'[^\d+]', '', full)
    if len(digits) < 6:
        return full
    return digits[:3] + '****' + digits[-2:]


def _mask_cc(match) -> str:
    """Show only last 4 digits of credit card."""
    full = match.group(1)
    digits = re.sub(r'[^\d]', '', full)
    if len(digits) < 8:
        return full
    return '****-****-****-' + digits[-4:]


def _mask_iban(match) -> str:
    """Show country code + last 4 characters."""
    iban = match.group(1).replace(' ', '')
    if len(iban) < 8:
        return match.group(0)
    return iban[:2] + '****' + iban[-4:]


def _mask_amount_prefix(match) -> str:
    """Mask amount but keep currency prefix."""
    return match.group(0).split()[0] + ' ***.**'


def _mask_amount_suffix(match) -> str:
    """Mask amount but keep currency suffix."""
    parts = match.group(0).rsplit(maxsplit=1)
    return '***.**' + (' ' + parts[-1] if len(parts) > 1 else '')


# ---------------------------------------------------------------------------
# Singleton instance (import and use directly)
# ---------------------------------------------------------------------------
_default_masker = PIIMasker()
mask_pii = _default_masker.mask
mask_pii_dict = _default_masker.mask_dict
mask_pii_json = _default_masker.mask_json
