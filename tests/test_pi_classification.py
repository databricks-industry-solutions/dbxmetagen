"""
Unit tests for PI classification logic.

Tests verify that table classification is correctly determined based on column types,
particularly ensuring that tables with all "None" column types don't get incorrectly
classified as "protected". Also tests Luhn checksum validation for credit card detection.

Run with: pytest tests/test_pi_classification.py -v
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from conftest import install_processing_stubs, uninstall_processing_stubs

_saved = install_processing_stubs()
from dbxmetagen.processing import (
    get_protected_classification_for_table,
    determine_table_classification,
)
from dbxmetagen.deterministic_pi import luhn_checksum
uninstall_processing_stubs(_saved)


class TestGetProtectedClassificationForTable:
    """Test protected classification logic for tables."""

    def test_none_string_returns_none_string(self):
        """Table with 'None' subclassification should return string 'None', not 'protected'."""
        result = get_protected_classification_for_table("None")
        assert result == "None"

    def test_python_none_returns_none_string(self):
        """Table with Python None subclassification should return string 'None'."""
        result = get_protected_classification_for_table(None)
        assert result == "None"

    def test_pii_returns_protected(self):
        """Table with PII returns protected."""
        result = get_protected_classification_for_table("pii")
        assert result == "protected"

    def test_phi_returns_protected(self):
        """Table with PHI returns protected."""
        result = get_protected_classification_for_table("phi")
        assert result == "protected"

    def test_pci_returns_protected(self):
        """Table with PCI returns protected."""
        result = get_protected_classification_for_table("pci")
        assert result == "protected"

    def test_medical_information_returns_protected(self):
        """Table with medical_information returns protected."""
        result = get_protected_classification_for_table("medical_information")
        assert result == "protected"

    def test_all_returns_protected(self):
        """Table with 'all' classification returns protected."""
        result = get_protected_classification_for_table("all")
        assert result == "protected"


def _mock_pi_df(type_values):
    """Create a mock DataFrame whose select(collect_set('type')).first()[0] returns type_values."""
    from unittest.mock import MagicMock
    df = MagicMock()
    row = MagicMock()
    row.__getitem__ = lambda self, idx: type_values
    df.select.return_value.first.return_value = row
    return df


class TestDetermineTableClassification:
    """Test determine_table_classification with mock DataFrames."""

    def test_all_none_types_returns_none(self):
        result = determine_table_classification(_mock_pi_df(["None", "None", "None"]))
        assert result == "None"

    def test_empty_dataframe_returns_none(self):
        result = determine_table_classification(_mock_pi_df([]))
        assert result == "None"

    def test_only_pii_returns_pii(self):
        result = determine_table_classification(_mock_pi_df(["pii"]))
        assert result == "pii"

    def test_pii_with_none_returns_pii(self):
        result = determine_table_classification(_mock_pi_df(["pii", "None"]))
        assert result == "pii"

    def test_pii_and_medical_returns_phi(self):
        result = determine_table_classification(_mock_pi_df(["pii", "medical_information"]))
        assert result == "phi"

    def test_pci_alone_returns_pci(self):
        result = determine_table_classification(_mock_pi_df(["pci", "None"]))
        assert result == "pci"

    def test_pci_with_phi_returns_all(self):
        result = determine_table_classification(_mock_pi_df(["pci", "phi"]))
        assert result == "all"


class TestEndToEndProtectedClassification:
    """Test the full flow: determine_table_classification -> get_protected_classification_for_table."""

    def test_all_none_columns_not_protected(self):
        subclassification = determine_table_classification(_mock_pi_df(["None", "None", "None"]))
        assert subclassification == "None"
        classification = get_protected_classification_for_table(subclassification)
        assert classification == "None"

    def test_table_with_pii_is_protected(self):
        subclassification = determine_table_classification(_mock_pi_df(["pii", "None"]))
        assert subclassification == "pii"
        classification = get_protected_classification_for_table(subclassification)
        assert classification == "protected"


class TestLuhnChecksum:
    """Test Luhn algorithm for credit card validation (MG-19 fix)."""

    def test_valid_test_pan_passes_luhn(self):
        """Well-known test PAN (Visa) should pass Luhn validation."""
        # 4111111111111111 is a commonly used test Visa card number
        result = luhn_checksum("4111111111111111")
        assert result is True, "Valid test PAN should pass Luhn checksum"

    def test_valid_mastercard_pan_passes_luhn(self):
        """Well-known test MasterCard PAN should pass Luhn validation."""
        # 5555555555554444 is a commonly used test MasterCard number
        result = luhn_checksum("5555555555554444")
        assert result is True, "Valid test MasterCard should pass Luhn checksum"

    def test_invalid_16_digit_fails_luhn(self):
        """A non-Luhn 16-digit number should fail validation."""
        # 1234567890123456 is a valid length but fails Luhn
        result = luhn_checksum("1234567890123456")
        assert result is False, "Invalid card number should fail Luhn checksum"

    def test_order_reference_fails_luhn(self):
        """Order reference (16 digits but non-Luhn) should be rejected."""
        # 1000000000000001 is 16 digits but fails Luhn (non-Luhn format)
        result = luhn_checksum("1000000000000001")
        assert result is False, "Order reference should fail Luhn checksum guard"

    def test_luhn_with_spaces_and_dashes(self):
        """Luhn should work with spaces and dashes in card numbers."""
        # Same valid test PAN but with spaces and dashes
        result = luhn_checksum("4111 1111 1111 1111")
        assert result is True, "Valid PAN with spaces should pass Luhn"

        result = luhn_checksum("4111-1111-1111-1111")
        assert result is True, "Valid PAN with dashes should pass Luhn"

    def test_too_short_card_fails_luhn(self):
        """Card number shorter than 13 digits should fail."""
        result = luhn_checksum("123456789012")
        assert result is False, "Card too short should fail Luhn"

    def test_too_long_card_fails_luhn(self):
        """Card number longer than 19 digits should fail."""
        result = luhn_checksum("12345678901234567890")
        assert result is False, "Card too long should fail Luhn"

    def test_non_numeric_fails_luhn(self):
        """Non-numeric input should fail Luhn."""
        result = luhn_checksum("411111111111111a")
        assert result is False, "Non-numeric input should fail Luhn"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

