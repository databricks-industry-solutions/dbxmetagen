"""Unit tests for DataOperations and MetadataProcessor in app/core/data_ops.py"""

import pytest
import sys
import os
from unittest.mock import Mock, MagicMock, patch
from io import StringIO, BytesIO

# Mock streamlit and databricks before importing app modules
sys.modules["streamlit"] = MagicMock()
sys.modules["databricks"] = MagicMock()
sys.modules["databricks.sdk"] = MagicMock()

# Import pandas (assuming it's available in test environment)
try:
    import pandas as pd
except ImportError:
    # If pandas not available, create minimal mock
    pd = MagicMock()
    sys.modules["pandas"] = pd

# Add app directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))


class TestDataOperationsTableValidation:
    """Test DataOperations table name validation"""

    def setup_method(self):
        """Import after mocking"""
        from core.data_ops import DataOperations

        self.DataOperations = DataOperations

    def test_validate_table_names_valid_format(self):
        """Test that properly formatted table names are validated"""
        data_ops = self.DataOperations()
        tables = [
            "catalog.schema.table",
            "my_catalog.my_schema.my_table",
            "catalog1.schema_2.table_name_3",
        ]

        valid, invalid = data_ops.validate_table_names(tables)

        assert len(valid) == 3
        assert len(invalid) == 0
        assert valid == tables

    def test_validate_table_names_invalid_format(self):
        """Test that improperly formatted table names are rejected"""
        data_ops = self.DataOperations()
        tables = [
            "schema.table",  # Missing catalog
            "table",  # Missing catalog and schema
            "catalog.schema.",  # Empty table name
            ".schema.table",  # Empty catalog
            "catalog..table",  # Empty schema
        ]

        valid, invalid = data_ops.validate_table_names(tables)

        assert len(valid) == 0
        assert len(invalid) == 5

    def test_validate_table_names_mixed(self):
        """Test validation with mix of valid and invalid names"""
        data_ops = self.DataOperations()
        tables = [
            "catalog.schema.valid_table",
            "invalid.table",
            "catalog.schema.another_valid",
            "also_invalid",
        ]

        valid, invalid = data_ops.validate_table_names(tables)

        assert len(valid) == 2
        assert len(invalid) == 2
        assert "catalog.schema.valid_table" in valid
        assert "catalog.schema.another_valid" in valid

    def test_validate_table_names_empty_strings(self):
        """Test that empty strings are filtered out"""
        data_ops = self.DataOperations()
        tables = ["", "   ", "catalog.schema.table", ""]

        valid, invalid = data_ops.validate_table_names(tables)

        assert len(valid) == 1
        assert len(invalid) == 0

    def test_validate_table_names_special_characters(self):
        """Test that table names with special characters are rejected"""
        data_ops = self.DataOperations()
        tables = [
            "catalog.schema.table-name",  # Hyphen not allowed
            "catalog.schema.table name",  # Space not allowed
            "catalog.schema.table@name",  # @ not allowed
        ]

        valid, invalid = data_ops.validate_table_names(tables)

        assert len(valid) == 0
        assert len(invalid) == 3


class TestDataOperationsCSVProcessing:
    """Test DataOperations CSV file processing"""

    def setup_method(self):
        """Import after mocking"""
        from core.data_ops import DataOperations

        self.DataOperations = DataOperations

    def test_process_uploaded_csv_with_table_name_column(self):
        """Test processing CSV with standard 'table_name' column"""
        data_ops = self.DataOperations()
        csv_content = "table_name\ncatalog.schema.table1\ncatalog.schema.table2\n"
        uploaded_file = Mock()
        uploaded_file.read.return_value = csv_content.encode("utf-8")
        uploaded_file.name = "tables.csv"

        result = data_ops.process_uploaded_csv(uploaded_file)

        assert len(result) == 2
        assert "catalog.schema.table1" in result
        assert "catalog.schema.table2" in result

    def test_process_uploaded_csv_with_alternative_column_names(self):
        """Test processing CSV with alternative column names"""
        data_ops = self.DataOperations()

        # Test with 'table' column
        csv_content = "table\ncatalog.schema.table1\n"
        uploaded_file = Mock()
        uploaded_file.read.return_value = csv_content
        uploaded_file.name = "tables.csv"

        result = data_ops.process_uploaded_csv(uploaded_file)

        assert len(result) == 1
        assert "catalog.schema.table1" in result

    def test_process_uploaded_csv_filters_nan_and_empty(self):
        """Test that CSV processing filters out NaN and empty values"""
        data_ops = self.DataOperations()
        csv_content = "table_name\ncatalog.schema.table1\n\ncatalog.schema.table2\n"
        uploaded_file = Mock()
        uploaded_file.read.return_value = csv_content.encode("utf-8")
        uploaded_file.name = "tables.csv"

        result = data_ops.process_uploaded_csv(uploaded_file)

        assert len(result) == 2
        assert "" not in result

    def test_process_uploaded_csv_handles_bytes_and_strings(self):
        """Test that CSV processing handles both bytes and string content"""
        data_ops = self.DataOperations()
        csv_content = "table_name\ncatalog.schema.table1\n"

        # Test with bytes
        uploaded_file = Mock()
        uploaded_file.read.return_value = csv_content.encode("utf-8")
        uploaded_file.name = "tables.csv"
        result = data_ops.process_uploaded_csv(uploaded_file)
        assert len(result) == 1

        # Test with string
        uploaded_file = Mock()
        uploaded_file.read.return_value = csv_content
        uploaded_file.name = "tables.csv"
        result = data_ops.process_uploaded_csv(uploaded_file)
        assert len(result) == 1

    def test_save_table_list_creates_valid_csv(self):
        """Test that save_table_list creates valid CSV content"""
        data_ops = self.DataOperations()
        tables = ["catalog.schema.table1", "catalog.schema.table2"]

        result = data_ops.save_table_list(tables)

        assert result is not None
        assert b"table_name" in result
        assert b"catalog.schema.table1" in result
        assert b"catalog.schema.table2" in result


class TestMetadataProcessorDDLGeneration:
    """Test MetadataProcessor DDL generation methods"""

    def setup_method(self):
        """Import after mocking and mock streamlit session state"""
        from core.data_ops import MetadataProcessor

        self.MetadataProcessor = MetadataProcessor

        # Mock streamlit session state
        import streamlit as st

        st.session_state = {"config": {}}

    def test_generate_ddl_for_comment_metadata_column_level(self):
        """Test DDL generation for column-level comments"""
        processor = self.MetadataProcessor()
        df = pd.DataFrame(
            {
                "catalog": ["catalog1"],
                "schema": ["schema1"],
                "table_name": ["table1"],
                "column_name": ["col1"],
                "column_content": ["This is a test description"],
            }
        )

        # Mock DBR version for consistent DDL syntax
        with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "16.0"}):
            result = processor._generate_ddl_for_comment_metadata(df)

        assert len(result) == 1
        assert "ddl" in result.columns
        assert "COMMENT ON COLUMN" in result.iloc[0]["ddl"]
        assert "This is a test description" in result.iloc[0]["ddl"]
        assert "catalog1.schema1.table1" in result.iloc[0]["ddl"]

    def test_generate_ddl_for_comment_metadata_escapes_quotes(self):
        """Test that DDL generation properly escapes quotes"""
        processor = self.MetadataProcessor()
        df = pd.DataFrame(
            {
                "catalog": ["catalog1"],
                "schema": ["schema1"],
                "table_name": ["table1"],
                "column_name": ["col1"],
                "column_content": ['Description with "quotes"'],
            }
        )

        with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "16.0"}):
            result = processor._generate_ddl_for_comment_metadata(df)

        ddl = result.iloc[0]["ddl"]
        # Should replace double quotes with single quotes
        assert "'" in ddl

    def test_generate_ddl_for_pi_metadata_with_classification(self):
        """Test DDL generation for PI classification metadata"""
        processor = self.MetadataProcessor()
        df = pd.DataFrame(
            {
                "catalog": ["catalog1"],
                "schema": ["schema1"],
                "table_name": ["table1"],
                "column_name": ["email_col"],
                "classification": ["PII"],
                "type": ["EMAIL"],
            }
        )

        result = processor._generate_ddl_for_pi_metadata(df)

        assert len(result) == 1
        assert "ddl" in result.columns
        ddl = result.iloc[0]["ddl"]
        assert "ALTER TABLE" in ddl
        assert "SET TAGS" in ddl
        assert "data_classification" in ddl
        assert "PII" in ddl

    def test_generate_ddl_for_pi_metadata_skips_none_classification(self):
        """Test that PI metadata skips rows with None classification"""
        processor = self.MetadataProcessor()
        df = pd.DataFrame(
            {
                "catalog": ["catalog1", "catalog1"],
                "schema": ["schema1", "schema1"],
                "table_name": ["table1", "table1"],
                "column_name": ["col1", "col2"],
                "classification": ["PII", None],
                "type": ["EMAIL", None],
            }
        )

        result = processor._generate_ddl_for_pi_metadata(df)

        # Should only have DDL for the first row
        assert pd.notna(result.iloc[0].get("ddl"))
        assert pd.isna(result.iloc[1].get("ddl", pd.NA))

    def test_generate_ddl_for_domain_metadata(self):
        """Test DDL generation for domain classification metadata"""
        processor = self.MetadataProcessor()
        df = pd.DataFrame(
            {
                "table": ["catalog1.schema1.table1"],
                "domain": ["Finance"],
                "subdomain": ["Accounting"],
            }
        )

        result = processor._generate_ddl_for_domain_metadata(df)

        assert len(result) == 1
        assert "ddl" in result.columns
        ddl = result.iloc[0]["ddl"]
        assert "ALTER TABLE" in ddl
        assert "SET TAGS" in ddl
        assert "domain" in ddl
        assert "Finance" in ddl
        assert "subdomain" in ddl
        assert "Accounting" in ddl

    def test_generate_ddl_for_domain_metadata_without_subdomain(self):
        """Test DDL generation for domain without subdomain"""
        processor = self.MetadataProcessor()
        df = pd.DataFrame(
            {
                "table": ["catalog1.schema1.table1"],
                "domain": ["Finance"],
                "subdomain": [None],
            }
        )

        result = processor._generate_ddl_for_domain_metadata(df)

        ddl = result.iloc[0]["ddl"]
        assert "'domain' = 'Finance'" in ddl
        # subdomain should not be in DDL when None
        assert ddl.count("=") == 1  # Only domain tag

    def test_generate_ddl_for_comment_metadata_table_level(self):
        """Test DDL generation for table-level comments"""
        processor = self.MetadataProcessor()
        df = pd.DataFrame(
            {
                "catalog": ["catalog1"],
                "schema": ["schema1"],
                "table_name": ["table1"],
                "column_name": [None],  # Table-level when column is None
                "column_content": ["Table description"],
            }
        )

        with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "16.0"}):
            result = processor._generate_ddl_for_comment_metadata(df)

        ddl = result.iloc[0]["ddl"]
        assert "COMMENT ON TABLE" in ddl
        assert "Table description" in ddl

    def test_generate_ddl_dbr_14_uses_alter_column_syntax(self):
        """Test that DBR 14-15 uses ALTER COLUMN syntax"""
        processor = self.MetadataProcessor()
        df = pd.DataFrame(
            {
                "catalog": ["catalog1"],
                "schema": ["schema1"],
                "table_name": ["table1"],
                "column_name": ["col1"],
                "column_content": ["Description"],
            }
        )

        with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.3"}):
            result = processor._generate_ddl_for_comment_metadata(df)

        ddl = result.iloc[0]["ddl"]
        assert "ALTER TABLE" in ddl
        assert "ALTER COLUMN" in ddl
        assert "COMMENT" in ddl

    def test_generate_ddl_serverless_uses_modern_syntax(self):
        """Test that serverless (client.X.X) uses modern syntax"""
        processor = self.MetadataProcessor()
        df = pd.DataFrame(
            {
                "catalog": ["catalog1"],
                "schema": ["schema1"],
                "table_name": ["table1"],
                "column_name": ["col1"],
                "column_content": ["Description"],
            }
        )

        with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "client.3.0"}):
            result = processor._generate_ddl_for_comment_metadata(df)

        ddl = result.iloc[0]["ddl"]
        assert "COMMENT ON COLUMN" in ddl


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
