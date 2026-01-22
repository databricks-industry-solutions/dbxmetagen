"""
Unit tests for PI classification logic.

Tests verify that table classification is correctly determined based on column types,
particularly ensuring that tables with all "None" column types don't get incorrectly
classified as "protected".

Run with: pytest tests/test_pi_classification.py -v
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.dbxmetagen.processing import (
    get_protected_classification_for_table,
    determine_table_classification,
)


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


class TestDetermineTableClassification:
    """Test determine_table_classification with mock DataFrames."""

    @pytest.fixture
    def spark(self):
        """Create a local SparkSession for testing."""
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        yield spark
        spark.stop()

    def test_all_none_types_returns_none(self, spark):
        """When all columns have type='None', table classification should be 'None'."""
        df = spark.createDataFrame(
            [("col1", "None"), ("col2", "None"), ("col3", "None")],
            ["column_name", "type"]
        )
        result = determine_table_classification(df)
        assert result == "None"

    def test_empty_dataframe_returns_none(self, spark):
        """Empty DataFrame should return 'None'."""
        df = spark.createDataFrame([], "column_name: string, type: string")
        result = determine_table_classification(df)
        assert result == "None"

    def test_only_pii_returns_pii(self, spark):
        """DataFrame with only PII type returns 'pii'."""
        df = spark.createDataFrame(
            [("col1", "pii"), ("col2", "pii")],
            ["column_name", "type"]
        )
        result = determine_table_classification(df)
        assert result == "pii"

    def test_pii_with_none_returns_pii(self, spark):
        """DataFrame with PII and None types returns 'pii'."""
        df = spark.createDataFrame(
            [("col1", "pii"), ("col2", "None"), ("col3", "None")],
            ["column_name", "type"]
        )
        result = determine_table_classification(df)
        assert result == "pii"

    def test_pii_and_medical_returns_phi(self, spark):
        """DataFrame with PII and medical_information returns 'phi'."""
        df = spark.createDataFrame(
            [("col1", "pii"), ("col2", "medical_information")],
            ["column_name", "type"]
        )
        result = determine_table_classification(df)
        assert result == "phi"

    def test_pci_alone_returns_pci(self, spark):
        """DataFrame with only PCI returns 'pci'."""
        df = spark.createDataFrame(
            [("col1", "pci"), ("col2", "None")],
            ["column_name", "type"]
        )
        result = determine_table_classification(df)
        assert result == "pci"

    def test_pci_with_phi_returns_all(self, spark):
        """DataFrame with PCI and PHI returns 'all'."""
        df = spark.createDataFrame(
            [("col1", "pci"), ("col2", "phi")],
            ["column_name", "type"]
        )
        result = determine_table_classification(df)
        assert result == "all"


class TestEndToEndProtectedClassification:
    """Test the full flow: determine_table_classification -> get_protected_classification_for_table."""

    @pytest.fixture
    def spark(self):
        """Create a local SparkSession for testing."""
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        yield spark
        spark.stop()

    def test_all_none_columns_not_protected(self, spark):
        """Critical test: Table with all None columns should NOT be protected."""
        df = spark.createDataFrame(
            [("col1", "None"), ("col2", "None"), ("col3", "None")],
            ["column_name", "type"]
        )
        subclassification = determine_table_classification(df)
        assert subclassification == "None"

        classification = get_protected_classification_for_table(subclassification)
        assert classification == "None", (
            f"Table with all 'None' columns should have classification='None', "
            f"not '{classification}'"
        )

    def test_table_with_pii_is_protected(self, spark):
        """Table with at least one PII column should be protected."""
        df = spark.createDataFrame(
            [("col1", "pii"), ("col2", "None")],
            ["column_name", "type"]
        )
        subclassification = determine_table_classification(df)
        assert subclassification == "pii"

        classification = get_protected_classification_for_table(subclassification)
        assert classification == "protected"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

