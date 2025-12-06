"""
Unit tests for app/core/data_ops.py helper functions.
Tests the parsing and utility functions that don't require Databricks connectivity.
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from concurrent.futures import TimeoutError as FuturesTimeoutError
from io import StringIO
import logging

import sys
import os

# Mock streamlit before importing app modules
sys.modules["streamlit"] = MagicMock()

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from core.data_ops import parse_tsv_content, download_file_content


class TestParseTsvContent:
    """Tests for the parse_tsv_content function."""

    def test_valid_tsv_content(self):
        """Test parsing valid TSV content."""
        content = "col1\tcol2\tcol3\nval1\tval2\tval3\nval4\tval5\tval6"
        df = parse_tsv_content(content)

        assert df is not None
        assert len(df) == 2
        assert list(df.columns) == ["col1", "col2", "col3"]
        assert df.iloc[0]["col1"] == "val1"

    def test_single_row_tsv(self):
        """Test parsing TSV with a single data row."""
        content = "table\tcolumn\tdescription\ntest_table\ttest_col\ttest desc"
        df = parse_tsv_content(content)

        assert df is not None
        assert len(df) == 1
        assert df.iloc[0]["table"] == "test_table"

    def test_empty_content_returns_none(self):
        """Test that empty content returns None."""
        assert parse_tsv_content("") is None
        assert parse_tsv_content("   ") is None
        assert parse_tsv_content(None) is None

    def test_header_only_returns_none(self):
        """Test that TSV with only headers (no data) returns None."""
        content = "col1\tcol2\tcol3"
        df = parse_tsv_content(content)
        assert df is None

    def test_content_with_special_characters(self):
        """Test parsing content with quotes and special characters."""
        content = 'table\tdescription\ntest\t"Description with ""quotes"" and tabs"'
        df = parse_tsv_content(content)

        assert df is not None
        assert len(df) == 1

    def test_real_metadata_format(self):
        """Test parsing actual metadata TSV format from the app."""
        content = (
            "table\ttokenized_table\tddl_type\tcolumn_name\tcolumn_content\n"
            "dbxmetagen.default.test_table\tdbxmetagen.default.test_table\ttable\tNone\tTest table description\n"
            "dbxmetagen.default.test_table\tdbxmetagen.default.test_table\tcolumn\tsome_col\tColumn description"
        )
        df = parse_tsv_content(content)

        assert df is not None
        assert len(df) == 2
        assert "table" in df.columns
        assert "column_content" in df.columns
        assert df.iloc[0]["ddl_type"] == "table"
        assert df.iloc[1]["ddl_type"] == "column"


class TestDownloadFileContent:
    """Tests for download_file_content - OBO token tests skipped (require real Streamlit)."""

    def test_missing_host_raises_error(self):
        """Test that missing DATABRICKS_HOST raises ValueError."""
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("DATABRICKS_HOST", None)
            
            with pytest.raises(ValueError) as exc_info:
                download_file_content(None, "/Volumes/cat/sch/vol/file.tsv")
            
            assert "DATABRICKS_HOST" in str(exc_info.value)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
